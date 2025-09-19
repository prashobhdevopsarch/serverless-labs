# Lab 1: Serverless API with DynamoDB

## Goal
Build a REST API using API Gateway + Lambda + DynamoDB.
- API Gateway triggers Lambda
- Lambda performs CRUD on DynamoDB table
- Public, serverless stack (no VPC needed)

## Architecture
Client → API Gateway (HTTP API) → Lambda (Python 3.12) → DynamoDB

## Resources
- API Gateway: `lab1-http-api` (stage: prod)
- Lambda: `lab1-crud-fn` (handler: app.handler)
- DynamoDB Table: `lab1-items` (partition key: id)
- IAM Role: `lab1-lambda-dynamodb-role`

## Setup
1. Create DynamoDB table `lab1-items` (PK = id, String).
2. Create IAM role `lab1-lambda-dynamodb-role` with CRUD permissions on table.
3. Deploy Lambda with `app.py` code, handler = `app.handler`, runtime = Python 3.12.
4. Create API Gateway HTTP API:
   - POST /items
   - GET /items
   - GET /items/{id}
   - PUT /items/{id}
   - DELETE /items/{id}
5. Enable CORS (allow *, GET/POST/PUT/DELETE/OPTIONS).
6. Test with curl or Postman.

## Example Requests
```bash
API="https://<api-id>.execute-api.<region>.amazonaws.com/prod"

# Create
curl -X POST "$API/items" -H "Content-Type: application/json" \
  -d '{"name":"Notebook","price":99}'

# List
curl "$API/items"

# Read
curl "$API/items/<id>"

# Update
curl -X PUT "$API/items/<id>" -H "Content-Type: application/json" \
  -d '{"name":"Notebook (Hardcover)","price":129}'

# Delete
curl -X DELETE "$API/items/<id>"
