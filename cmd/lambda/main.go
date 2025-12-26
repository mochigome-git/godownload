package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"godownload/internal/auth"
	"godownload/internal/config"
	"godownload/internal/handler"
	"godownload/internal/logger"
	"godownload/internal/supabase"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"go.uber.org/zap"
)

var (
	cfg            *config.Config
	supabaseClient *supabase.Client
	jwtVerifier    *auth.JWTVerifier
	log            *zap.SugaredLogger
	isLocal        bool
)

func init() {
	log = logger.Init()

	isLocal = os.Getenv("AWS_SAM_LOCAL") == "true" || os.Getenv("ENV") == "development"
	log.Infow("Lambda initializing", "is_local", isLocal)

	cfg = config.MustLoad()
	log.Infow("Configuration loaded", "supabase_url", cfg.SupabaseURL)

	supabaseClient = supabase.NewClient(cfg.SupabaseURL, cfg.SupabaseServiceKey)
	log.Info("Supabase client initialized")

	var err error
	jwtVerifier, err = auth.NewJWTVerifier(cfg.SupabaseURL)
	if err != nil {
		log.Fatalw("Failed to initialize JWT verifier", "error", err)
	}
	log.Info("JWT verifier initialized")
}

func getCORSHeaders() map[string]string {
	return map[string]string{
		"Access-Control-Allow-Origin":      "*",
		"Access-Control-Allow-Methods":     "GET, POST, PUT, DELETE, OPTIONS",
		"Access-Control-Allow-Headers":     "Content-Type, Authorization, X-Requested-With, Accept, Origin",
		"Access-Control-Expose-Headers":    "Content-Disposition, Content-Length, Content-Type",
		"Access-Control-Max-Age":           "86400",
		"Access-Control-Allow-Credentials": "false",
	}
}

// handleRequest handles both Lambda Function URL and API Gateway V2 requests
// They use the same event structure: events.LambdaFunctionURLRequest is an alias
func handleRequest(ctx context.Context, request events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	defer logger.Sync()

	headers := getCORSHeaders()

	method := request.RequestContext.HTTP.Method
	path := request.RequestContext.HTTP.Path

	// Fallback to RawPath if Path is empty
	if path == "" {
		path = request.RawPath
	}

	log.Infow("Incoming request",
		"method", method,
		"path", path,
		"raw_path", request.RawPath,
		"is_local", isLocal,
	)

	// Handle OPTIONS preflight
	if method == "OPTIONS" {
		log.Info("Handling OPTIONS preflight request")
		return events.LambdaFunctionURLResponse{
			StatusCode: 200,
			Headers:    headers,
			Body:       "",
		}, nil
	}

	// Health check - no auth required
	if strings.Contains(path, "/health") || path == "/health" {
		headers["Content-Type"] = "application/json"
		return events.LambdaFunctionURLResponse{
			StatusCode: 200,
			Headers:    headers,
			Body:       `{"status":"ok"}`,
		}, nil
	}

	// Authenticate user
	userID, err := authenticateRequest(request)
	if err != nil {
		headers["Content-Type"] = "application/json"
		return errorResponse(http.StatusUnauthorized, err.Error(), headers), nil
	}

	// Route based on path
	if strings.Contains(path, "/export") || path == "/export" {
		return handleExport(ctx, request, userID, headers)
	}
	if strings.Contains(path, "/download") || path == "/download" {
		return handleDownload(ctx, request, userID, headers)
	}

	// Unknown endpoint
	headers["Content-Type"] = "application/json"
	return errorResponse(http.StatusNotFound, fmt.Sprintf("Endpoint not found: %s", path), headers), nil
}

func authenticateRequest(request events.LambdaFunctionURLRequest) (string, error) {
	// Headers are lowercase in Function URL requests
	authHeader := request.Headers["authorization"]
	if authHeader == "" {
		authHeader = request.Headers["Authorization"]
	}

	log.Debugw("Request headers", "headers", request.Headers)

	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		log.Warn("Missing or invalid authorization header")
		return "", fmt.Errorf("missing or invalid authorization header")
	}

	claims, err := jwtVerifier.VerifyToken(token)
	if err != nil {
		log.Warnw("Token verification failed", "error", err)
		return "", fmt.Errorf("invalid token: %w", err)
	}

	userID, ok := claims["sub"].(string)
	if !ok || userID == "" {
		log.Warn("Invalid user ID in token")
		return "", fmt.Errorf("invalid user ID in token")
	}

	log.Debugw("User authenticated", "user_id", userID)
	return userID, nil
}

func handleDownload(ctx context.Context, request events.LambdaFunctionURLRequest, userID string, headers map[string]string) (events.LambdaFunctionURLResponse, error) {
	headers["Content-Type"] = "application/json"

	var req handler.DownloadRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		log.Warnw("Invalid request body", "error", err)
		return errorResponse(http.StatusBadRequest, "Invalid request body: "+err.Error(), headers), nil
	}

	log.Infow("Processing download request",
		"user_id", userID,
		"tables", len(req.Tables),
	)

	h := handler.NewDownloadHandler(supabaseClient, log)
	result, err := h.ProcessDownload(ctx, &req, userID)
	if err != nil {
		log.Errorw("Download failed", "error", err, "user_id", userID)
		return errorResponse(http.StatusInternalServerError, "Download failed: "+err.Error(), headers), nil
	}

	log.Infow("Download completed",
		"user_id", userID,
		"tables", len(result.Tables),
	)

	responseBody, _ := json.Marshal(result)
	return events.LambdaFunctionURLResponse{
		StatusCode: 200,
		Headers:    headers,
		Body:       string(responseBody),
	}, nil
}

func handleExport(ctx context.Context, request events.LambdaFunctionURLRequest, userID string, headers map[string]string) (events.LambdaFunctionURLResponse, error) {
	var req handler.ExportRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		log.Warnw("Invalid request body", "error", err)
		headers["Content-Type"] = "application/json"
		return errorResponse(http.StatusBadRequest, "Invalid request body: "+err.Error(), headers), nil
	}

	log.Infow("Processing export request",
		"user_id", userID,
		"tables", len(req.Tables),
		"file_name", req.FileName,
	)

	downloadHandler := handler.NewDownloadHandler(supabaseClient, log)
	exportHandler := handler.NewExportHandler(downloadHandler, log)

	result, err := exportHandler.ProcessExport(ctx, &req, userID)
	if err != nil {
		log.Errorw("Export failed", "error", err, "user_id", userID)
		headers["Content-Type"] = "application/json"
		return errorResponse(http.StatusInternalServerError, "Export failed: "+err.Error(), headers), nil
	}

	log.Infow("Export completed",
		"user_id", userID,
		"file_name", result.FileName,
		"file_size", result.FileSize,
	)

	headers["Content-Type"] = result.ContentType
	headers["Content-Disposition"] = fmt.Sprintf("attachment; filename=\"%s\"", result.FileName)

	return events.LambdaFunctionURLResponse{
		StatusCode:      200,
		Headers:         headers,
		Body:            base64.StdEncoding.EncodeToString(result.Data),
		IsBase64Encoded: true,
	}, nil
}

func errorResponse(statusCode int, message string, headers map[string]string) events.LambdaFunctionURLResponse {
	headers["Content-Type"] = "application/json"
	body, _ := json.Marshal(map[string]string{"error": message})
	return events.LambdaFunctionURLResponse{
		StatusCode: statusCode,
		Headers:    headers,
		Body:       string(body),
	}
}

func main() {
	lambda.Start(handleRequest)
}
