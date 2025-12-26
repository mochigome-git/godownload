package handler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"godownload/internal/config"
	"godownload/internal/supabase"

	"go.uber.org/zap"
)

const (
	DefaultBatchSize   = 5   // 5 lot_ids per batch
	DefaultConcurrency = 2   // 2 concurrent requests
	MaxRetries         = 3   // Retry failed batches
	RetryDelayMs       = 500 // Base retry delay in milliseconds
)

// TableConfig represents configuration for a single table query
type TableConfig struct {
	Schema   string         `json:"schema,omitempty"`
	Table    string         `json:"table"`
	Filters  map[string]any `json:"filters,omitempty"`
	Select   string         `json:"select,omitempty"`
	OrderBy  []OrderConfig  `json:"order_by,omitempty"`
	Limit    int            `json:"limit,omitempty"`
	InColumn string         `json:"in_column,omitempty"`
	InValues []any          `json:"in_values,omitempty"`
}

// OrderConfig represents ordering configuration
type OrderConfig struct {
	Column    string `json:"column"`
	Ascending bool   `json:"ascending"`
}

// DownloadRequest represents the incoming download request
type DownloadRequest struct {
	Tables      []TableConfig `json:"tables"`
	IsFiveMin   bool          `json:"is_five_min,omitempty"`
	BatchSize   int           `json:"batch_size,omitempty"`
	Concurrency int           `json:"concurrency,omitempty"`
}

// DownloadResponse contains the downloaded data
type DownloadResponse struct {
	Tables []TableData `json:"tables"`
}

// TableData represents data from a single table
type TableData struct {
	Schema string           `json:"schema,omitempty"`
	Table  string           `json:"table"`
	Data   []map[string]any `json:"data"`
	Count  int              `json:"count"`
}

// DownloadHandler handles data download operations
type DownloadHandler struct {
	client      *supabase.Client
	config      *config.Config
	transformer *DataTransformer
	logger      *zap.SugaredLogger
}

// NewDownloadHandler creates a new download handler
func NewDownloadHandler(client *supabase.Client, logger *zap.SugaredLogger) *DownloadHandler {
	cfg := config.MustLoad()
	return &DownloadHandler{
		client:      client,
		config:      cfg,
		transformer: NewDataTransformer(client, cfg, logger),
		logger:      logger,
	}
}

// ProcessDownload downloads and transforms data from multiple tables
func (h *DownloadHandler) ProcessDownload(ctx context.Context, req *DownloadRequest, userID string) (*DownloadResponse, error) {
	h.logger.Infow("Starting download process",
		"user_id", userID,
		"tables_count", len(req.Tables),
		"is_five_min", req.IsFiveMin,
	)

	results := make([]TableData, 0, len(req.Tables))
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, len(req.Tables))

	for _, tableConfig := range req.Tables {
		wg.Add(1)
		go func(tc TableConfig) {
			defer wg.Done()

			schema := tc.Schema
			if schema == "" {
				schema = "public"
			}

			data, err := h.processTable(ctx, tc, req.BatchSize, req.Concurrency)
			if err != nil {
				h.logger.Errorw("Failed to process table",
					"schema", schema,
					"table", tc.Table,
					"error", err,
				)
				errChan <- fmt.Errorf("table %s.%s: %w", schema, tc.Table, err)
				return
			}

			// Transform data
			transformedData, err := h.transformer.TransformTableData(ctx, schema, tc.Table, data)
			if err != nil {
				h.logger.Warnw("Transform failed, using raw data",
					"error", err,
				)
				transformedData = data
			}

			// Apply five minute grouping if requested
			if req.IsFiveMin {
				transformedData = groupByFiveMinutes(transformedData)
			}

			mu.Lock()
			results = append(results, TableData{
				Schema: schema,
				Table:  tc.Table,
				Data:   transformedData,
				Count:  len(transformedData),
			})
			mu.Unlock()

			h.logger.Infow("Table processed successfully",
				"schema", schema,
				"table", tc.Table,
				"rows", len(transformedData),
			)
		}(tableConfig)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		return nil, err
	}

	h.logger.Infow("Download process completed",
		"user_id", userID,
		"tables_processed", len(results),
	)

	return &DownloadResponse{Tables: results}, nil
}

// ProcessDownloadRaw downloads data without transformation (for export handler to group first)
func (h *DownloadHandler) ProcessDownloadRaw(ctx context.Context, req *DownloadRequest, userID string) (*DownloadResponse, error) {
	h.logger.Infow("Starting raw download process",
		"user_id", userID,
		"tables_count", len(req.Tables),
	)

	results := make([]TableData, 0, len(req.Tables))
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, len(req.Tables))

	for _, tableConfig := range req.Tables {
		wg.Add(1)
		go func(tc TableConfig) {
			defer wg.Done()

			schema := tc.Schema
			if schema == "" {
				schema = "public"
			}

			data, err := h.processTable(ctx, tc, req.BatchSize, req.Concurrency)
			if err != nil {
				h.logger.Errorw("Failed to process table",
					"schema", schema,
					"table", tc.Table,
					"error", err,
				)
				errChan <- fmt.Errorf("table %s.%s: %w", schema, tc.Table, err)
				return
			}

			mu.Lock()
			results = append(results, TableData{
				Schema: schema,
				Table:  tc.Table,
				Data:   data,
				Count:  len(data),
			})
			mu.Unlock()

			h.logger.Infow("Table processed successfully (raw)",
				"schema", schema,
				"table", tc.Table,
				"rows", len(data),
			)
		}(tableConfig)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return nil, err
	}

	h.logger.Infow("Raw download process completed",
		"user_id", userID,
		"tables_processed", len(results),
	)

	return &DownloadResponse{Tables: results}, nil
}

// processTable handles data download for a single table with batching
func (h *DownloadHandler) processTable(ctx context.Context, tc TableConfig, batchSize, concurrency int) ([]map[string]any, error) {
	// If no IN clause, do a simple query
	if tc.InColumn == "" || len(tc.InValues) == 0 {
		return h.executeSimpleQuery(ctx, tc)
	}

	// Use batched queries for IN clause
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	// Create batches
	batches := h.createBatches(tc.InValues, batchSize)

	h.logger.Infow("Processing batched IN query",
		"schema", tc.Schema,
		"table", tc.Table,
		"in_column", tc.InColumn,
		"total_in_values", len(tc.InValues),
		"total_batches", len(batches),
		"batch_size", batchSize,
		"concurrency", concurrency,
	)

	// Process batches SEQUENTIALLY to avoid overwhelming Supabase
	var allData []map[string]any

	for i, batch := range batches {
		// Retry logic
		var data []map[string]any
		var err error

		for retry := 0; retry < MaxRetries; retry++ {
			data, err = h.executeBatchQuery(ctx, tc, batch, i)
			if err == nil {
				break
			}

			h.logger.Warnw("Batch query failed, retrying",
				"batch_index", i,
				"retry", retry+1,
				"max_retries", MaxRetries,
				"error", err,
			)

			// Exponential backoff
			sleepDuration := time.Duration(RetryDelayMs*(retry+1)) * time.Millisecond
			time.Sleep(sleepDuration)
		}

		if err != nil {
			h.logger.Errorw("Batch query failed after retries",
				"schema", tc.Schema,
				"table", tc.Table,
				"batch_index", i,
				"error", err,
			)
			return nil, err
		}

		allData = append(allData, data...)

		h.logger.Debugw("Batch query completed",
			"schema", tc.Schema,
			"table", tc.Table,
			"batch_index", i,
			"total_batches", len(batches),
			"rows_returned", len(data),
			"total_rows_so_far", len(allData),
		)

		// Small delay between batches to avoid rate limiting
		if i < len(batches)-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	h.logger.Infow("All batches collected",
		"schema", tc.Schema,
		"table", tc.Table,
		"total_rows", len(allData),
	)

	return allData, nil
}

func (h *DownloadHandler) createBatches(values []any, batchSize int) [][]any {
	var batches [][]any
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		batches = append(batches, values[i:end])
	}
	return batches
}

func (h *DownloadHandler) executeSimpleQuery(ctx context.Context, tc TableConfig) ([]map[string]any, error) {
	query := h.client.From(tc.Table).WithContext(ctx)

	if tc.Schema != "" && tc.Schema != "public" {
		query = query.Schema(tc.Schema)
	}

	selectStr := tc.Select
	if selectStr == "" {
		selectStr = "*"
	}
	query = query.Select(selectStr)

	// Apply filters
	for key, value := range tc.Filters {
		query = query.Eq(key, value)
	}

	// Apply ordering
	for _, order := range tc.OrderBy {
		query = query.Order(order.Column, order.Ascending)
	}

	if tc.Limit > 0 {
		query = query.Limit(tc.Limit)
	}

	return query.Execute()
}

func (h *DownloadHandler) executeBatchQuery(ctx context.Context, tc TableConfig, batchValues []any, batchIndex int) ([]map[string]any, error) {
	h.logger.Debugw("Executing batch query",
		"schema", tc.Schema,
		"table", tc.Table,
		"in_column", tc.InColumn,
		"batch_index", batchIndex,
		"batch_in_values_count", len(batchValues),
	)

	query := h.client.From(tc.Table).WithContext(ctx)

	if tc.Schema != "" && tc.Schema != "public" {
		query = query.Schema(tc.Schema)
	}

	selectStr := tc.Select
	if selectStr == "" {
		selectStr = "*"
	}
	query = query.Select(selectStr)

	// Apply IN filter
	query = query.In(tc.InColumn, batchValues)

	// Apply additional filters
	for key, value := range tc.Filters {
		query = query.Eq(key, value)
	}

	// Apply ordering
	for _, order := range tc.OrderBy {
		query = query.Order(order.Column, order.Ascending)
	}

	if tc.Limit > 0 {
		query = query.Limit(tc.Limit)
	}

	return query.Execute()
}

// groupByFiveMinutes groups data by 5-minute intervals
func groupByFiveMinutes(data []map[string]any) []map[string]any {
	if len(data) == 0 {
		return data
	}

	// Sort by created_at first
	sort.SliceStable(data, func(i, j int) bool {
		aVal, aOk := data[i]["created_at"]
		bVal, bOk := data[j]["created_at"]
		if !aOk || !bOk {
			return false
		}
		aStr, aStrOk := aVal.(string)
		bStr, bStrOk := bVal.(string)
		if !aStrOk || !bStrOk {
			return false
		}
		return aStr < bStr
	})

	var result []map[string]any
	var lastTime time.Time

	for _, row := range data {
		createdAt, ok := row["created_at"].(string)
		if !ok {
			result = append(result, row)
			continue
		}

		t, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05.999999", createdAt)
			if err != nil {
				result = append(result, row)
				continue
			}
		}

		// Round to 5-minute interval
		rounded := t.Truncate(5 * time.Minute)

		// Only keep if different from last interval
		if rounded.After(lastTime) || lastTime.IsZero() {
			result = append(result, row)
			lastTime = rounded
		}
	}

	return result
}
