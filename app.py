import json
import os
import uuid
import logging
import base64
import boto3
import decimal
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE_NAME = os.getenv("TABLE_NAME", "lab1-items")
DDB = boto3.resource("dynamodb")
TABLE = DDB.Table(TABLE_NAME)

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 == 0:
                return int(o)
            return float(o)
        return super().default(o)

def resp(status: int, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }

def normalize_event(event: dict):
    ctx = event.get("requestContext", {})
    http = ctx.get("http", {})
    method = http.get("method") or event.get("httpMethod") or ""
    raw_path = event.get("rawPath") or event.get("path") or "/"

    stage = ctx.get("stage")
    if stage and raw_path.startswith(f"/{stage}"):
        raw_path = raw_path[len(stage) + 1 :]
        if not raw_path.startswith("/"):
            raw_path = "/" + raw_path

    body = event.get("body")
    if body and event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    try:
        data = json.loads(body) if body else {}
    except Exception:
        data = {}

    path_params = event.get("pathParameters") or {}
    item_id = path_params.get("id")
    if not item_id:
        parts = [p for p in raw_path.split("/") if p]
        if len(parts) == 2 and parts[0] == "items":
            item_id = parts[1]

    return method.upper(), raw_path, data, item_id

def list_items():
    res = TABLE.scan(Limit=50)
    return resp(200, {"items": res.get("Items", []), "count": res.get("Count", 0)})

def create_item(data: dict):
    if not isinstance(data, dict):
        return resp(400, {"message": "Invalid JSON"})
    item = {"id": data.get("id") or str(uuid.uuid4())}
    item.update({k: v for k, v in data.items() if k != "id"})
    TABLE.put_item(Item=item)
    return resp(201, item)

def read_item(item_id: str):
    if not item_id:
        return resp(400, {"message": "Missing id"})
    res = TABLE.get_item(Key={"id": item_id})
    item = res.get("Item")
    if not item:
        return resp(404, {"message": "Not found"})
    return resp(200, item)

def update_item(item_id: str, data: dict):
    if not item_id:
        return resp(400, {"message": "Missing id"})
    if not isinstance(data, dict):
        return resp(400, {"message": "Invalid JSON"})
    item = {"id": item_id, **{k: v for k, v in data.items() if k != "id"}}
    TABLE.put_item(Item=item)
    return resp(200, item)

def delete_item(item_id: str):
    if not item_id:
        return resp(400, {"message": "Missing id"})
    TABLE.delete_item(Key={"id": item_id})
    return resp(204, {})

def handler(event, context):
    log.info({
        "event_summary": {
            "path": event.get("rawPath") or event.get("path"),
            "stage": event.get("requestContext", {}).get("stage"),
            "method": (event.get("requestContext", {}).get("http", {}) or {}).get("method")
                      or event.get("httpMethod")
        }
    })

    if (event.get("requestContext", {}).get("http", {}) or {}).get("method") == "OPTIONS" \
       or event.get("httpMethod") == "OPTIONS":
        return resp(200, {})

    try:
        method, raw_path, data, item_id = normalize_event(event)

        if raw_path == "/items":
            if method == "GET":
                return list_items()
            if method == "POST":
                return create_item(data)

        if raw_path.startswith("/items/"):
            if method == "GET":
                return read_item(item_id)
            if method == "PUT":
                return update_item(item_id, data)
            if method == "DELETE":
                return delete_item(item_id)

        return resp(400, {"message": "Unsupported route", "path": raw_path, "method": method})

    except ClientError as ce:
        log.exception("DynamoDB client error")
        code = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
        return resp(code, {"message": "DynamoDB error", "error": str(ce)})

    except Exception as e:
        log.exception("Unhandled error")
        return resp(500, {"message": "Internal error", "error": str(e)})
