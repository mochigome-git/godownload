include .env
export

.PHONY: build build-arm clean test run-local deploy deploy-guided create-function update-function logs dev


# Variables
FUNCTION_NAME ?= go-download
AWS_REGION ?= ap-southeast-1
STACK_NAME ?= go-download-stack

# =============================================================================
# LOCAL DEVELOPMENT
# =============================================================================

# Run local HTTP server (fastest for development)
dev:
	@echo "🚀 Starting local HTTP server..."
	go run cmd/server/main.go

# Run Lambda handler directly (for testing Lambda-specific code)
dev-lambda:
	@echo "🚀 Running Lambda handler..."
	go run cmd/lambda/main.go

# Run tests
test:
	@echo "🧪 Running tests..."
	go test -v ./...

# Run tests with coverage
test-coverage:
	@echo "🧪 Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "📊 Coverage report: coverage.html"

# Lint code
lint:
	@echo "🔍 Linting..."
	golangci-lint run ./...

# Format code
fmt:
	@echo "📝 Formatting..."
	go fmt ./...

# =============================================================================
# LOCAL LAMBDA TESTING (with SAM)
# =============================================================================

# Start local API Gateway (requires SAM CLI + Docker)
local-api:
	@echo "🌐 Starting local API Gateway..."
	sam local start-api --host 0.0.0.0 --template template.yaml --env-vars env.json

# Invoke function locally with test event
local-invoke:
	@echo "⚡ Invoking function locally..."
	sam local invoke DownloadFunction --template template.yaml --event events/test-event.json --env-vars env.json

# =============================================================================
# BUILD
# =============================================================================

# Build for x86_64 (standard)
build:
	@echo "🔨 Building for x86_64..."
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap cmd/lambda/main.go
	zip -j deployment.zip bootstrap
	@echo "✅ Built: deployment.zip (x86_64)"

# Build for ARM64 (Graviton2 - ~20% cheaper)
build-arm:
	@echo "🔨 Building for ARM64 (Graviton2)..."
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap cmd/lambda/main.go
	zip -j deployment.zip bootstrap
	@echo "✅ Built: deployment.zip (arm64)"

# Clean build artifacts
clean:
	@echo "🧹 Cleaning..."
	rm -f bootstrap deployment.zip coverage.out coverage.html
	rm -rf .aws-sam

# =============================================================================
# DEPLOY WITH SAM (Recommended)
# =============================================================================

# First time deployment (interactive)
deploy-guided: build-arm
	@echo "🚀 Deploying with SAM (guided)..."
	sam deploy --guided --template template.yaml

# Subsequent deployments (uses samconfig.toml)
deploy: build-arm
	@echo "🚀 Deploying with SAM..."
	sam deploy --template template.yaml

# Deploy with specific parameters
deploy-prod: build-arm
	@echo "🚀 Deploying to production..."
	sam deploy --template template.yaml \
		--stack-name $(STACK_NAME)-prod \
		--parameter-overrides \
			SupabaseUrl=$(SUPABASE_URL) \
			SupabaseServiceKey=$(SUPABASE_SERVICE_KEY) \
		--no-confirm-changeset

# =============================================================================
# DEPLOY WITH AWS CLI (Manual)
# =============================================================================

# Create new Lambda function (first time only)
create-function: build-arm
	@echo "🆕 Creating Lambda function..."
	aws lambda create-function \
		--function-name $(FUNCTION_NAME) \
		--runtime provided.al2023 \
		--handler bootstrap \
		--architecture arm64 \
		--role $(LAMBDA_ROLE_ARN) \
		--zip-file fileb://deployment.zip \
		--environment "Variables={SUPABASE_URL=$(SUPABASE_URL),SUPABASE_SERVICE_KEY=$(SUPABASE_SERVICE_KEY),ENV=production}" \
		--timeout 30 \
		--memory-size 256 \
		--region $(AWS_REGION)
	@echo "✅ Function created!"

# Update existing Lambda function code
update-function: build-arm
	@echo "🔄 Updating Lambda function code..."
	aws lambda update-function-code \
		--function-name $(FUNCTION_NAME) \
		--zip-file fileb://deployment.zip \
		--region $(AWS_REGION)
	@echo "✅ Function updated!"

# Update Lambda function configuration
update-config:
	@echo "⚙️ Updating Lambda configuration..."
	aws lambda update-function-configuration \
		--function-name $(FUNCTION_NAME) \
		--environment "Variables={SUPABASE_URL=$(SUPABASE_URL),SUPABASE_SERVICE_KEY=$(SUPABASE_SERVICE_KEY),ENV=production}" \
		--timeout 30 \
		--memory-size 256 \
		--region $(AWS_REGION)

# =============================================================================
# MONITORING & DEBUGGING
# =============================================================================

# Tail Lambda logs
logs:
	@echo "📋 Tailing logs..."
	aws logs tail /aws/lambda/$(FUNCTION_NAME) --follow --region $(AWS_REGION)

# Get recent logs
logs-recent:
	@echo "📋 Getting recent logs..."
	aws logs tail /aws/lambda/$(FUNCTION_NAME) --since 1h --region $(AWS_REGION)

# Describe function
describe:
	@echo "📊 Function details..."
	aws lambda get-function --function-name $(FUNCTION_NAME) --region $(AWS_REGION)

# =============================================================================
# API GATEWAY (if using manual deployment)
# =============================================================================

# Create HTTP API Gateway
create-api:
	@echo "🌐 Creating API Gateway..."
	aws apigatewayv2 create-api \
		--name $(FUNCTION_NAME)-api \
		--protocol-type HTTP \
		--target arn:aws:lambda:$(AWS_REGION):$(AWS_ACCOUNT_ID):function:$(FUNCTION_NAME) \
		--region $(AWS_REGION)

# =============================================================================
# HELP
# =============================================================================

help:
	@echo ""
	@echo "📦 godownload Service - Makefile Commands"
	@echo "================================================"
	@echo ""
	@echo "LOCAL DEVELOPMENT:"
	@echo "  make dev            - Run local HTTP server (port 8080)"
	@echo "  make dev-lambda     - Run Lambda handler directly"
	@echo "  make test           - Run tests"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make lint           - Lint code"
	@echo "  make fmt            - Format code"
	@echo ""
	@echo "LOCAL LAMBDA TESTING (requires SAM CLI + Docker):"
	@echo "  make local-api      - Start local API Gateway"
	@echo "  make local-invoke   - Invoke function with test event"
	@echo ""
	@echo "BUILD:"
	@echo "  make build          - Build for x86_64"
	@echo "  make build-arm      - Build for ARM64 (Graviton2, recommended)"
	@echo "  make clean          - Clean build artifacts"
	@echo ""
	@echo "DEPLOY WITH SAM (Recommended):"
	@echo "  make deploy-guided  - First time deployment (interactive)"
	@echo "  make deploy         - Subsequent deployments"
	@echo "  make deploy-prod    - Deploy to production"
	@echo ""
	@echo "DEPLOY WITH AWS CLI (Manual):"
	@echo "  make create-function - Create new Lambda (first time)"
	@echo "  make update-function - Update Lambda code"
	@echo "  make update-config   - Update Lambda configuration"
	@echo ""
	@echo "MONITORING:"
	@echo "  make logs           - Tail Lambda logs"
	@echo "  make logs-recent    - Get recent logs (1 hour)"
	@echo "  make describe       - Get function details"
	@echo ""
	@echo "ENVIRONMENT VARIABLES:"
	@echo "  FUNCTION_NAME       - Lambda function name (default: supabase-download-service)"
	@echo "  AWS_REGION          - AWS region (default: ap-southeast-1)"
	@echo "  SUPABASE_URL        - Supabase project URL"
	@echo "  SUPABASE_SERVICE_KEY - Supabase service key"
	@echo "  LAMBDA_ROLE_ARN     - Lambda execution role ARN"
	@echo "  PORT                - Local server port (default: 8080)"
	@echo ""