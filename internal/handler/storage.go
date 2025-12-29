package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"godownload/internal/config"
	"godownload/internal/supabase"

	"go.uber.org/zap"
)

// StorageHandler handles file uploads to Supabase Storage
type StorageHandler struct {
	config         *config.Config
	supabaseClient *supabase.Client
	logger         *zap.SugaredLogger
	httpClient     *http.Client
}

// SignedURLResponse represents the signed URL response
type SignedURLResponse struct {
	SignedURL string `json:"signedURL"`
}

// NewStorageHandler creates a new storage handler
func NewStorageHandler(supabaseClient *supabase.Client, cfg *config.Config, logger *zap.SugaredLogger) *StorageHandler {
	return &StorageHandler{
		config:         cfg,
		supabaseClient: supabaseClient,
		logger:         logger,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// UploadFile uploads a file to Supabase Storage and returns the file path
func (s *StorageHandler) UploadFile(ctx context.Context, fileName string, data []byte, contentType string) (string, error) {
	// Generate file path with timestamp to avoid collisions
	timestamp := time.Now().Format("20060102")
	filePath := fmt.Sprintf("%s%s/%s", s.config.S3ExportPath, timestamp, fileName)

	s.logger.Infow("Uploading file to Supabase Storage",
		"bucket", s.config.S3Bucket,
		"path", filePath,
		"size_bytes", len(data),
	)

	// Supabase Storage upload endpoint
	url := fmt.Sprintf("%s/storage/v1/object/%s/%s",
		s.config.SupabaseURL,
		s.config.S3Bucket,
		filePath,
	)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("failed to create upload request: %w", err)
	}

	// Use Supabase service key for authentication (NOT JWT)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.config.SupabaseServiceKey))
	req.Header.Set("apikey", s.config.SupabaseServiceKey)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-upsert", "true") // Overwrite if exists

	// Execute request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to upload file: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		s.logger.Errorw("Upload failed",
			"status", resp.StatusCode,
			"body", string(body),
		)
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	s.logger.Infow("File uploaded successfully",
		"path", filePath,
		"status", resp.StatusCode,
	)

	return filePath, nil
}

// GenerateSignedURL generates a signed URL that expires after configured hours
func (s *StorageHandler) GenerateSignedURL(ctx context.Context, filePath string) (string, error) {
	expiresIn := s.config.S3ExpiryHours * 3600 // Convert hours to seconds

	s.logger.Infow("Generating signed URL",
		"bucket", s.config.S3Bucket,
		"path", filePath,
		"expires_in_hours", s.config.S3ExpiryHours,
	)

	// Supabase Storage signed URL endpoint
	url := fmt.Sprintf("%s/storage/v1/object/sign/%s/%s",
		s.config.SupabaseURL,
		s.config.S3Bucket,
		filePath,
	)

	// Request body
	requestBody := map[string]int{
		"expiresIn": expiresIn,
	}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return "", fmt.Errorf("failed to create signed URL request: %w", err)
	}

	// Use Supabase service key for authentication (NOT JWT)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.config.SupabaseServiceKey))
	req.Header.Set("apikey", s.config.SupabaseServiceKey)
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to generate signed URL: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		s.logger.Errorw("Failed to generate signed URL",
			"status", resp.StatusCode,
			"body", string(body),
		)
		return "", fmt.Errorf("signed URL generation failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var signedURLResp SignedURLResponse
	if err := json.Unmarshal(body, &signedURLResp); err != nil {
		return "", fmt.Errorf("failed to parse signed URL response: %w", err)
	}

	// Construct full URL
	fullSignedURL := fmt.Sprintf("%s%s", s.config.SupabaseURL, signedURLResp.SignedURL)

	s.logger.Infow("Signed URL generated successfully",
		"path", filePath,
		"expires_in_hours", s.config.S3ExpiryHours,
	)

	return fullSignedURL, nil
}

// DeleteFile deletes a file from Supabase Storage
func (s *StorageHandler) DeleteFile(ctx context.Context, filePath string) error {
	s.logger.Infow("Deleting file from storage",
		"bucket", s.config.S3Bucket,
		"path", filePath,
	)

	url := fmt.Sprintf("%s/storage/v1/object/%s/%s",
		s.config.SupabaseURL,
		s.config.S3Bucket,
		filePath,
	)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}

	// Use Supabase service key for authentication (NOT JWT)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", s.config.SupabaseServiceKey))
	req.Header.Set("apikey", s.config.SupabaseServiceKey)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
	}

	s.logger.Infow("File deleted successfully", "path", filePath)
	return nil
}
