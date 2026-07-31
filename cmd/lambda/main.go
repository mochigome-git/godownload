package main

import (
	"context"
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
	behindProxy    bool
)

func init() {
	log = logger.Init()

	isLocal = os.Getenv("AWS_SAM_LOCAL") == "true" || os.Getenv("ENV") == "development"

	// BEHIND_PROXY controls whether this Lambda needs to emit its own CORS
	// headers, independent of isLocal (which is only for local-dev behavior).
	//
	//   BEHIND_PROXY=true  -> called via CloudFront (or API Gateway). The
	//     Function URL's own built-in CORS config is bypassed in that path,
	//     so Lambda must set Access-Control-* headers itself.
	//
	//   BEHIND_PROXY=false (default) -> called directly via the raw Function
	//     URL. AWS's Function URL service already injects CORS headers based
	//     on the console/CLI CORS config, so Lambda does NOT need to (and
	//     should not duplicate them here).
	behindProxy = os.Getenv("BEHIND_PROXY") == "true"

	log.Infow("Lambda initializing", "is_local", isLocal, "behind_proxy", behindProxy)

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

// getCORSHeaders returns a FRESH map every call, or nil.
//
// Only emit CORS headers ourselves when BEHIND_PROXY=true (i.e. traffic is
// coming through CloudFront/API Gateway, where the Function URL's built-in
// CORS handling is bypassed). When calling the raw Function URL directly
// (BEHIND_PROXY=false/unset), AWS's Function URL service already injects
// CORS headers per its own console/CLI CORS config — adding our own here
// would just duplicate or conflict with those.
//
// Allow-Headers lists X-App-Authorization (not Authorization) because the
// app's JWT travels in that custom header when going via CloudFront — OAC
// needs to own the "Authorization" header itself for SigV4 signing. If
// you're hitting the raw Function URL directly, its own CORS config
// (managed separately, e.g. via `aws lambda update-function-url-config`)
// still applies and is unaffected by this function.
func getCORSHeaders() map[string]string {
	if !behindProxy {
		return nil
	}
	return map[string]string{
		"Access-Control-Allow-Origin":      "*",
		"Access-Control-Allow-Methods":     "GET, HEAD, OPTIONS, PUT, POST, PATCH, DELETE",
		"Access-Control-Allow-Headers":     "Content-Type, X-App-Authorization, X-Requested-With, Accept, Origin",
		"Access-Control-Expose-Headers":    "Content-Disposition, Content-Length, Content-Type",
		"Access-Control-Max-Age":           "86400",
		"Access-Control-Allow-Credentials": "false",
	}
}

// handleRequest handles both Lambda Function URL and API Gateway V2 requests
// They use the same event structure: events.LambdaFunctionURLRequest is an alias
func handleRequest(ctx context.Context, request events.LambdaFunctionURLRequest) (events.LambdaFunctionURLResponse, error) {
	defer logger.Sync()

	// getCORSHeaders returns nil when not behind a proxy (raw Function URL
	// mode) — normalize to an empty map so later `headers["X"] = "Y"` writes
	// don't panic on a nil map.
	headers := getCORSHeaders()
	if headers == nil {
		headers = map[string]string{}
	}

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
	// IMPORTANT: We read the app's JWT from X-App-Authorization, NOT
	// Authorization. When CloudFront uses Origin Access Control (OAC) to sign
	// requests to a Lambda Function URL, it needs to own the "Authorization"
	// header itself for its SigV4 signature. If the app also sends its own
	// bearer token in "Authorization", the two collide and CloudFront/Lambda
	// reject the request with InvalidSignatureException. Using a separate
	// header name avoids the collision entirely.
	//
	// Function URL headers arrive lowercased.
	authHeader := request.Headers["x-app-authorization"]
	if authHeader == "" {
		authHeader = request.Headers["X-App-Authorization"]
	}

	log.Debugw("Request headers", "headers", request.Headers)

	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		log.Warn("Missing or invalid X-App-Authorization header")
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

	log.Infow("Export completed and uploaded to storage",
		"user_id", userID,
		"file_name", result.FileName,
		"file_path", result.FilePath,
		"file_size", result.FileSize,
		"expires_in_hours", result.ExpiresIn,
	)

	// Return JSON response with signed URL instead of binary data
	responseBody, err := json.Marshal(result)
	if err != nil {
		log.Errorw("Failed to marshal response", "error", err)
		headers["Content-Type"] = "application/json"
		return errorResponse(http.StatusInternalServerError, "Failed to marshal response: "+err.Error(), headers), nil
	}

	// Set headers for JSON response
	headers["Content-Type"] = "application/json"
	// Remove Content-Disposition since we're not sending the file directly
	delete(headers, "Content-Disposition")

	return events.LambdaFunctionURLResponse{
		StatusCode:      200,
		Headers:         headers,
		Body:            string(responseBody),
		IsBase64Encoded: false, // JSON response, not binary
	}, nil
}

func errorResponse(statusCode int, message string, headers map[string]string) events.LambdaFunctionURLResponse {
	if headers == nil {
		headers = make(map[string]string)
	}
	headers["Content-Type"] = "application/json"

	errorBody := map[string]string{
		"error": message,
	}
	body, _ := json.Marshal(errorBody)

	return events.LambdaFunctionURLResponse{
		StatusCode:      statusCode,
		Headers:         headers,
		Body:            string(body),
		IsBase64Encoded: false,
	}
}

func main() {
	lambda.Start(handleRequest)
}
