.PHONY: build clean deploy test

# Build the Lambda function
build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap cmd/lambda/main.go
	zip deployment.zip bootstrap

# Build for ARM64 (Graviton2 - cheaper)
build-arm:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap cmd/lambda/main.go
	zip deployment.zip bootstrap

# Clean build artifacts
clean:
	rm -f bootstrap deployment.zip

# Run tests
test:
	go test -v ./...

# Deploy using AWS CLI (requires AWS CLI configured)
deploy: build
	aws lambda update-function-code \
		--function-name supabase-download-service \
		--zip-file fileb://deployment.zip

# Create new Lambda function
create-lambda: build
	aws lambda create-function \
		--function-name supabase-download-service \
		--runtime provided.al2023 \
		--handler bootstrap \
		--architecture x86_64 \
		--role arn:aws:iam::$(AWS_ACCOUNT_ID):role/lambda-execution-role \
		--zip-file fileb://deployment.zip \
		--environment Variables="{SUPABASE_URL=$(SUPABASE_URL),SUPABASE_SERVICE_KEY=$(SUPABASE_SERVICE_KEY)}" \
		--timeout 30 \
		--memory-size 256

# Create ARM Lambda (Graviton2 - ~20% cheaper)
create-lambda-arm: build-arm
	aws lambda create-function \
		--function-name supabase-download-service \
		--runtime provided.al2023 \
		--handler bootstrap \
		--architecture arm64 \
		--role arn:aws:iam::$(AWS_ACCOUNT_ID):role/lambda-execution-role \
		--zip-file fileb://deployment.zip \
		--environment Variables="{SUPABASE_URL=$(SUPABASE_URL),SUPABASE_SERVICE_KEY=$(SUPABASE_SERVICE_KEY)}" \
		--timeout 30 \
		--memory-size 256