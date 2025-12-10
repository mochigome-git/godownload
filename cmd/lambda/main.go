package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"godownload/internal/auth"
	"godownload/internal/handler"
	"godownload/internal/supabase"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var (
	supabaseClient *supabase.Client
	jwtVerifier    *auth.JWTVerifier
)

func init() {
	supabaseURL := os.Getenv("SUPABASE_URL")
	supabaseKey := os.Getenv("SUPABASE_SERVICE_KEY")

	supabaseClient = supabase.NewClient(supabaseURL, supabaseKey)

	// Initialize JWT verifier with Supabase JWKS
	var err error
	jwtVerifier, err = auth.NewJWTVerifier(supabaseURL)
	if err != nil {
		panic("failed to initialize JWT verifier: " + err.Error())
	}
}

func handleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// CORS headers
	headers := map[string]string{
		"Content-Type":                 "application/json",
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Headers": "Content-Type,Authorization",
		"Access-Control-Allow-Methods": "POST,OPTIONS",
	}

	// Handle preflight
	if request.HTTPMethod == "OPTIONS" {
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Headers:    headers,
		}, nil
	}

	// Extract token from Authorization header
	authHeader := request.Headers["Authorization"]
	if authHeader == "" {
		authHeader = request.Headers["authorization"]
	}

	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		return errorResponse(http.StatusUnauthorized, "Missing or invalid authorization header", headers), nil
	}

	// Verify JWT token
	claims, err := jwtVerifier.VerifyToken(token)
	if err != nil {
		return errorResponse(http.StatusUnauthorized, "Invalid token: "+err.Error(), headers), nil
	}

	// Get user ID from claims
	userID, ok := claims["sub"].(string)
	if !ok || userID == "" {
		return errorResponse(http.StatusUnauthorized, "Invalid user ID in token", headers), nil
	}

	// Parse request body
	var req handler.DownloadRequest
	if err := json.Unmarshal([]byte(request.Body), &req); err != nil {
		return errorResponse(http.StatusBadRequest, "Invalid request body: "+err.Error(), headers), nil
	}

	// Process download request
	h := handler.NewDownloadHandler(supabaseClient)
	result, err := h.ProcessDownload(ctx, &req, userID)
	if err != nil {
		return errorResponse(http.StatusInternalServerError, "Download failed: "+err.Error(), headers), nil
	}

	// Return response
	responseBody, _ := json.Marshal(result)
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers:    headers,
		Body:       string(responseBody),
	}, nil
}

func errorResponse(statusCode int, message string, headers map[string]string) events.APIGatewayProxyResponse {
	body, _ := json.Marshal(map[string]string{"error": message})
	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Headers:    headers,
		Body:       string(body),
	}
}

func main() {
	lambda.Start(handleRequest)
}
