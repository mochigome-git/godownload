# Supabase Download Service (AWS Lambda)

A Go-based AWS Lambda service for authenticated batch downloads from Supabase tables.

## Quick Start

```bash
# 1. Setup environment
cp .env.example .env
# Edit .env with your Supabase credentials

# 2. Run locally
make dev

# 3. Test with curl
curl -X POST http://localhost:8080/download \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{"tables":[{"table":"sensor_data"}],"roll_numbers":[1,2,3]}'
```

## Project Structure

```
supabase-download-service/
├── cmd/
│   ├── lambda/          # Lambda entry point
│   │   └── main.go
│   └── server/          # Local HTTP server for development
│       └── main.go
├── internal/
│   ├── auth/            # JWT verification (Supabase JWKS)
│   ├── config/          # Configuration loading
│   ├── handler/         # Download handler with batching
│   ├── logger/          # Zap logger
│   └── supabase/        # Supabase client with query builder
├── events/              # Test events for SAM local
├── .env.example         # Environment template
├── env.json             # SAM local environment
├── Makefile             # Build & deploy commands
├── samconfig.toml       # SAM deployment config
└── template.yaml        # SAM/CloudFormation template
```

## Development Workflow

### 1. Local Development (Fastest)

```bash
# Start local HTTP server
make dev

# Server runs at http://localhost:8080
# Endpoints:
#   GET  /health   - Health check
#   POST /download - Download data
```

### 2. Local Lambda Testing (with SAM CLI)

Requires [SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) and Docker.

```bash
# Edit env.json with your credentials first!

# Option A: Start local API Gateway
make local-api
# Test at http://localhost:3000/download

# Option B: Invoke with test event
make local-invoke
```

### 3. Run Tests

```bash
make test              # Run tests
make test-coverage     # With coverage report
make lint              # Lint code
```

## Deployment Methods

### Method 1: SAM CLI (Recommended)

SAM handles Lambda, API Gateway, IAM roles, and CloudFormation automatically.

```bash
# First time - interactive setup
make deploy-guided

# You'll be prompted for:
# - Stack name: supabase-download-stack
# - Region: ap-southeast-1 (or your region)
# - SupabaseUrl: https://xxx.supabase.co
# - SupabaseServiceKey: your-service-key
# - Confirm changes: y

# Subsequent deployments (uses saved config)
make deploy
```

**What SAM does:**

1. Builds ARM64 binary (`make build-arm`)
2. Creates S3 bucket for deployment artifacts
3. Uploads code to S3
4. Creates/updates CloudFormation stack with:
   - Lambda function
   - API Gateway
   - IAM execution role
   - CloudWatch log group

### Method 2: AWS CLI (Manual)

For more control or CI/CD pipelines.

```bash
# 1. Create IAM role first (one time)
aws iam create-role \
  --role-name lambda-supabase-download-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach basic execution policy
aws iam attach-role-policy \
  --role-name lambda-supabase-download-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# 2. Create Lambda function
export LAMBDA_ROLE_ARN=arn:aws:iam::YOUR_ACCOUNT:role/lambda-supabase-download-role
export SUPABASE_URL=https://xxx.supabase.co
export SUPABASE_SERVICE_KEY=your-key

make create-function

# 3. Update code (subsequent deploys)
make update-function

# 4. Create API Gateway manually or use AWS Console
```

### Method 3: Deploy to Specific Environment

```bash
# Deploy to production (no confirmation prompt)
make deploy-prod

# Or use SAM profiles
sam deploy --config-env prod
sam deploy --config-env staging
```

## API Reference

### POST /download

Download data from multiple tables with batching.

**Headers:**

```
Authorization: Bearer <supabase_jwt_token>
Content-Type: application/json
```

**Request:**

```json
{
  "tables": [
    {
      "table": "sensor_data",
      "select": "*",
      "in_column": "lot_id",
      "in_values": ["bb3044dd-928a-46b9-94e4-76e15715e037"],
      "filters": {
        "device_id": "abc123"
      },
      "order_by": [{ "column": "created_at", "ascending": true }]
    }
  ],
  "batch_size": 20,
  "concurrency": 5,
  "is_five_min": false
}
```

**Response:**

```json
{
  "tables": [
    {
      "table": "sensor_data",
      "data": [...],
      "count": 150
    }
  ]
}
```

### POST /export

Download data and export to excel

**Headers:**

```
Authorization: Bearer <supabase_jwt_token>
Content-Type: application/json
```

**Request:**

```json
{
  "tables": [
    {
      "table": "sensor_data",
      "select": "*",
      "in_column": "lot_id",
      "in_values": ["bb3044dd-928a-46b9-94e4-76e15715e037"],
      "filters": {
        "device_id": "abc123"
      },
      "order_by": [{ "column": "created_at", "ascending": true }]
    }
  ],
  "batch_size": 20,
  "concurrency": 5,
  "is_five_min": false
}
```

**or**

```json
curl -X POST http://localhost:8081/download \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciOi..." \
  -d '{
    "tables": [
      {
       "schema": "analytics",
        "table": "realtime_metrics",
        "in_column": "lot_id",
        "select": "*",
        "in_values": ["55c46b9f-2ec9-4254-af1d-7f2380227107",
        "5fae8624-1916-4425-bd5a-f62029a7cddb"],
        "order_by": [{ "column": "created_at", "ascending": true }]
      }
    ],
    "batch_size": 20,
    "concurrency": 5,
    "is_five_min": false
  }'

```

**Response:** Binary Excel file download

## Features

| Feature                  | Description                                                   |
| ------------------------ | ------------------------------------------------------------- |
| **Multiple sheets**      | Each table becomes a separate sheet                           |
| **Header mapping**       | Optional `header_map` to rename columns (e.g., `uuid` → `ID`) |
| **Auto column width**    | Columns sized based on content                                |
| **Header styling**       | Bold + gray background                                        |
| **Date formatting**      | Dates detected and formatted                                  |
| **Sorted by created_at** | Data auto-sorted by timestamp                                 |
| **Custom filename**      | Optional `file_name` parameter                                |

## Endpoints

| Endpoint    | Returns            |
| ----------- | ------------------ |
| `/download` | JSON data          |
| `/export`   | Excel file (.xlsx) |
| `/health`   | Health check       |

## Monitoring

```bash
# Tail logs in real-time
make logs

# Get recent logs
make logs-recent

# View function details
make describe
```

## Configuration

### Environment Variables

| Variable               | Required | Description                       |
| ---------------------- | -------- | --------------------------------- |
| `SUPABASE_URL`         | Yes      | Supabase project URL              |
| `SUPABASE_SERVICE_KEY` | Yes      | Supabase service role key         |
| `ENV`                  | No       | `development` or `production`     |
| `PORT`                 | No       | Local server port (default: 8080) |

### Lambda Settings

| Setting      | Value           | Notes                     |
| ------------ | --------------- | ------------------------- |
| Runtime      | provided.al2023 | Custom runtime for Go     |
| Architecture | arm64           | Graviton2 (~20% cheaper)  |
| Memory       | 256 MB          | Adjust for large payloads |
| Timeout      | 30 sec          | Increase for many batches |

## Cost Estimate

| Requests/Month | Lambda Cost | Notes                         |
| -------------- | ----------- | ----------------------------- |
| 10,000         | ~$0.20      | Including API Gateway         |
| 100,000        | ~$2.00      |                               |
| 1,000,000      | ~$20.00     | Consider reserved concurrency |

ARM64 (Graviton2) is ~20% cheaper than x86_64.

## Troubleshooting

### Build Errors

```bash
# Clean and rebuild
make clean
make build-arm
```

### SAM Deployment Issues

```bash
# Validate template
sam validate --template template.yaml

# View detailed errors
sam deploy --debug
```

### Lambda Timeout

- Reduce `batch_size` or `concurrency`
- Increase Lambda timeout in template.yaml
- Check Supabase connection/rate limits

### 401 Unauthorized

- Verify JWT token is valid and not expired
- Check Supabase JWKS endpoint is accessible
- Ensure token audience is "authenticated"

## All Makefile Commands

```bash
make help
```

```
LOCAL DEVELOPMENT:
  make dev            - Run local HTTP server (port 8080)
  make dev-lambda     - Run Lambda handler directly
  make test           - Run tests
  make test-coverage  - Run tests with coverage report

LOCAL LAMBDA TESTING (requires SAM CLI + Docker):
  make local-api      - Start local API Gateway
  make local-invoke   - Invoke function with test event

BUILD:
  make build          - Build for x86_64
  make build-arm      - Build for ARM64 (Graviton2)
  make clean          - Clean build artifacts

DEPLOY WITH SAM:
  make deploy-guided  - First time deployment (interactive)
  make deploy         - Subsequent deployments
  make deploy-prod    - Deploy to production

DEPLOY WITH AWS CLI:
  make create-function - Create new Lambda
  make update-function - Update Lambda code
  make update-config   - Update Lambda configuration

MONITORING:
  make logs           - Tail Lambda logs
  make logs-recent    - Get recent logs (1 hour)
  make describe       - Get function details
```

## License

[MIT](LICENSE)
