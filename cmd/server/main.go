package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"godownload/internal/auth"
	"godownload/internal/config"
	"godownload/internal/handler"
	"godownload/internal/logger"
	"godownload/internal/supabase"

	"go.uber.org/zap"
)

var (
	cfg            *config.Config
	supabaseClient *supabase.Client
	jwtVerifier    *auth.JWTVerifier
	log            *zap.SugaredLogger
)

func init() {
	// Initialize logger
	log = logger.Init()

	// Load configuration from .env or environment variables
	cfg = config.MustLoad()
	log.Infow("Configuration loaded",
		"supabase_url", cfg.SupabaseURL,
	)

	// Initialize Supabase client
	supabaseClient = supabase.NewClient(cfg.SupabaseURL, cfg.SupabaseServiceKey)
	log.Info("Supabase client initialized")

	// Initialize JWT verifier with Supabase JWKS
	var err error
	jwtVerifier, err = auth.NewJWTVerifier(cfg.SupabaseURL)
	if err != nil {
		log.Fatalw("Failed to initialize JWT verifier", "error", err)
	}
	log.Info("JWT verifier initialized")
}

func main() {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// Download endpoint (returns JSON)
	mux.HandleFunc("/download", handleDownload)

	// Export endpoint (returns Excel file)
	mux.HandleFunc("/export", handleExport)

	// CORS middleware
	h := corsMiddleware(mux)

	// Server configuration
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      h,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second, // Longer for Excel generation
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		log.Info("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Errorw("Server shutdown error", "error", err)
		}
	}()

	log.Infow("🚀 Server starting",
		"port", port,
		"endpoints", []string{"/health", "/download", "/export"},
	)

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalw("Server error", "error", err)
	}

	log.Info("Server stopped")
	logger.Sync()
}

// setCORSHeaders sets CORS headers on the response
func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept, Origin")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Disposition, Content-Length, Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	// Set CORS headers first
	setCORSHeaders(w)

	// Handle preflight
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract token from Authorization header
	authHeader := r.Header.Get("Authorization")
	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		log.Warn("Missing or invalid authorization header")
		errorResponse(w, http.StatusUnauthorized, "Missing or invalid authorization header")
		return
	}

	// Verify JWT token
	claims, err := jwtVerifier.VerifyToken(token)
	if err != nil {
		log.Warnw("Token verification failed", "error", err)
		errorResponse(w, http.StatusUnauthorized, "Invalid token: "+err.Error())
		return
	}

	// Get user ID from claims
	userID, ok := claims["sub"].(string)
	if !ok || userID == "" {
		log.Warn("Invalid user ID in token")
		errorResponse(w, http.StatusUnauthorized, "Invalid user ID in token")
		return
	}

	log.Debugw("User authenticated", "user_id", userID)

	// Parse request body
	var req handler.DownloadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Warnw("Invalid request body", "error", err)
		errorResponse(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	log.Infow("Processing download request",
		"user_id", userID,
		"tables", len(req.Tables),
	)

	// Process download request
	h := handler.NewDownloadHandler(supabaseClient, log)
	result, err := h.ProcessDownload(r.Context(), &req, userID)
	if err != nil {
		log.Errorw("Download failed", "error", err, "user_id", userID)
		errorResponse(w, http.StatusInternalServerError, "Download failed: "+err.Error())
		return
	}

	log.Infow("Download completed",
		"user_id", userID,
		"tables", len(result.Tables),
	)

	// Return response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleExport(w http.ResponseWriter, r *http.Request) {
	setCORSHeaders(w)

	// Preflight
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		errorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// --- Auth ---
	authHeader := r.Header.Get("Authorization")
	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == "" || token == authHeader {
		log.Warn("Missing or invalid authorization header")
		errorResponse(w, http.StatusUnauthorized, "Missing or invalid authorization header")
		return
	}

	claims, err := jwtVerifier.VerifyToken(token)
	if err != nil {
		log.Warnw("Token verification failed", "error", err)
		errorResponse(w, http.StatusUnauthorized, "Invalid token")
		return
	}

	userID, ok := claims["sub"].(string)
	if !ok || userID == "" {
		log.Warn("Invalid user ID in token")
		errorResponse(w, http.StatusUnauthorized, "Invalid user ID in token")
		return
	}

	// --- Parse request ---
	var req handler.ExportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Warnw("Invalid request body", "error", err)
		errorResponse(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	log.Infow("Processing export request",
		"user_id", userID,
		"tables", len(req.Tables),
		"file_name", req.FileName,
	)

	// --- Process export ---
	downloadHandler := handler.NewDownloadHandler(supabaseClient, log)
	exportHandler := handler.NewExportHandler(downloadHandler, log)

	result, err := exportHandler.ProcessExport(r.Context(), &req, userID)
	if err != nil {
		log.Errorw("Export failed", "error", err, "user_id", userID)
		errorResponse(w, http.StatusInternalServerError, "Export failed")
		return
	}

	log.Infow("Export completed",
		"user_id", userID,
		"file_name", result.FileName,
		"file_size", result.FileSize,
	)

	// --- Return JSON instead of file ---
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Errorw("Failed to encode export response", "error", err)
	}
}

func errorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers for all requests
		setCORSHeaders(w)

		// Handle preflight OPTIONS request
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
