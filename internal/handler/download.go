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
	DefaultBatchSize   = 20
	DefaultConcurrency = 5
)

// DownloadRequest represents the incoming request structure
type DownloadRequest struct {
	Tables      []TableConfig `json:"tables"`
	IsFiveMin   bool          `json:"is_five_min,omitempty"`
	BatchSize   int           `json:"batch_size,omitempty"`  // How many in_values per batch query (default: 20)
	Concurrency int           `json:"concurrency,omitempty"` // Max concurrent queries (default: 5)
}

// TableConfig represents configuration for a single table query
type TableConfig struct {
	Schema  string         `json:"schema,omitempty"` // PostgreSQL schema (default: public)
	Table   string         `json:"table"`
	Filters map[string]any `json:"filters,omitempty"`
	Select  string         `json:"select,omitempty"`
	OrderBy []OrderConfig  `json:"order_by,omitempty"`
	Limit   int            `json:"limit,omitempty"` // Limit per query

	// For batch IN queries
	InColumn string `json:"in_column,omitempty"`
	InValues []any  `json:"in_values,omitempty"`
}

type OrderConfig struct {
	Column    string `json:"column"`
	Ascending bool   `json:"ascending"`
}

// DownloadResponse represents the response structure
type DownloadResponse struct {
	Tables []TableData `json:"tables"`
}

type TableData struct {
	Schema string           `json:"schema,omitempty"`
	Table  string           `json:"table"`
	Data   []map[string]any `json:"data"`
	Count  int              `json:"count"`
}

type DownloadHandler struct {
	client      *supabase.Client
	config      *config.Config
	transformer *DataTransformer
	logger      *zap.SugaredLogger
}

func NewDownloadHandler(client *supabase.Client, logger *zap.SugaredLogger) *DownloadHandler {
	cfg := config.Get()
	return &DownloadHandler{
		client:      client,
		config:      cfg,
		transformer: NewDataTransformer(client, cfg, logger),
		logger:      logger,
	}
}

// ProcessDownload handles the download request with transformation applied
func (h *DownloadHandler) ProcessDownload(ctx context.Context, req *DownloadRequest, userID string) (*DownloadResponse, error) {
	// Get raw data first
	resp, err := h.ProcessDownloadRaw(ctx, req, userID)
	if err != nil {
		return nil, err
	}

	// Apply transformations to each table
	for i, table := range resp.Tables {
		schema := table.Schema
		if schema == "" {
			schema = "public"
		}

		transformedData, err := h.transformer.TransformTableData(ctx, schema, table.Table, table.Data)
		if err != nil {
			h.logger.Warnw("Failed to transform data, using raw data",
				"schema", schema,
				"table", table.Table,
				"error", err,
			)
			continue
		}

		// Apply five-minute grouping if requested
		if req.IsFiveMin {
			transformedData = groupByFiveMinutes(transformedData)
		}

		resp.Tables[i].Data = transformedData
		resp.Tables[i].Count = len(transformedData)
	}

	return resp, nil
}

// ProcessDownloadRaw handles the download request WITHOUT transformation
// This is used by export handler to get raw data before grouping
func (h *DownloadHandler) ProcessDownloadRaw(ctx context.Context, req *DownloadRequest, userID string) (*DownloadResponse, error) {
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	concurrency := req.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	h.logger.Debugw("Starting raw download process",
		"user_id", userID,
		"batch_size", batchSize,
		"concurrency", concurrency,
		"tables_count", len(req.Tables),
	)

	// Create semaphore for concurrency control
	sem := make(chan struct{}, concurrency)

	// Process all tables
	results := make([]TableData, len(req.Tables))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, tableConfig := range req.Tables {
		wg.Add(1)
		go func(idx int, cfg TableConfig) {
			defer wg.Done()

			h.logger.Debugw("Processing table",
				"schema", cfg.Schema,
				"table", cfg.Table,
				"in_column", cfg.InColumn,
				"in_values_count", len(cfg.InValues),
				"filters", cfg.Filters,
				"index", idx,
			)

			data, err := h.processTable(ctx, cfg, batchSize, sem)
			if err != nil {
				h.logger.Errorw("Failed to process table",
					"schema", cfg.Schema,
					"table", cfg.Table,
					"error", err,
				)
				mu.Lock()
				if firstErr == nil {
					tableName := cfg.Table
					if cfg.Schema != "" {
						tableName = cfg.Schema + "." + cfg.Table
					}
					firstErr = fmt.Errorf("table %s: %w", tableName, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			results[idx] = TableData{
				Schema: cfg.Schema,
				Table:  cfg.Table,
				Data:   data,
				Count:  len(data),
			}
			mu.Unlock()

			h.logger.Debugw("Table processed successfully (raw)",
				"schema", cfg.Schema,
				"table", cfg.Table,
				"rows", len(data),
			)
		}(i, tableConfig)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	h.logger.Infow("Raw download process completed",
		"user_id", userID,
		"tables_processed", len(results),
	)

	return &DownloadResponse{Tables: results}, nil
}

func (h *DownloadHandler) processTable(
	ctx context.Context,
	config TableConfig,
	batchSize int,
	sem chan struct{},
) ([]map[string]any, error) {
	// If no IN column/values specified, just execute a single query with filters
	if config.InColumn == "" || len(config.InValues) == 0 {
		h.logger.Debugw("Executing single query (no batch IN filter)",
			"schema", config.Schema,
			"table", config.Table,
		)
		return h.executeSingleQuery(ctx, config)
	}

	// Batch the in_values and query each batch concurrently
	batches := splitIntoBatches(config.InValues, batchSize)

	h.logger.Debugw("Processing batched IN query",
		"schema", config.Schema,
		"table", config.Table,
		"in_column", config.InColumn,
		"total_in_values", len(config.InValues),
		"total_batches", len(batches),
		"batch_size", batchSize,
	)

	type batchResult struct {
		index int
		data  []map[string]any
		err   error
	}

	resultsChan := make(chan batchResult, len(batches))

	var wg sync.WaitGroup

	for batchIdx, batch := range batches {
		wg.Add(1)
		go func(idx int, batchValues []any) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			h.logger.Debugw("Executing batch query",
				"schema", config.Schema,
				"table", config.Table,
				"in_column", config.InColumn,
				"batch_index", idx,
				"batch_in_values_count", len(batchValues),
			)

			data, err := h.executeBatchQuery(ctx, config, batchValues)
			if err != nil {
				h.logger.Errorw("Batch query failed",
					"schema", config.Schema,
					"table", config.Table,
					"batch_index", idx,
					"error", err,
				)
				resultsChan <- batchResult{index: idx, err: err}
				return
			}

			h.logger.Debugw("Batch query completed",
				"schema", config.Schema,
				"table", config.Table,
				"batch_index", idx,
				"rows_returned", len(data),
			)

			resultsChan <- batchResult{index: idx, data: data}
		}(batchIdx, batch)
	}

	wg.Wait()
	close(resultsChan)

	var allData []map[string]any
	var firstErr error

	for result := range resultsChan {
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		allData = append(allData, result.data...)
	}

	if firstErr != nil {
		return nil, firstErr
	}

	h.logger.Debugw("All batches collected",
		"schema", config.Schema,
		"table", config.Table,
		"total_rows", len(allData),
	)

	return allData, nil
}

func (h *DownloadHandler) executeSingleQuery(
	ctx context.Context,
	config TableConfig,
) ([]map[string]any, error) {
	query := h.client.From(config.Table).WithContext(ctx)

	if config.Schema != "" {
		query = query.Schema(config.Schema)
	}

	if config.Select != "" {
		query = query.Select(config.Select)
	} else {
		query = query.Select("*")
	}

	for key, value := range config.Filters {
		query = query.Eq(key, value)
	}

	if len(config.OrderBy) > 0 {
		for _, o := range config.OrderBy {
			query = query.Order(o.Column, o.Ascending)
		}
	}

	if config.Limit > 0 {
		query = query.Limit(config.Limit)
	}

	return query.Execute()
}

func (h *DownloadHandler) executeBatchQuery(
	ctx context.Context,
	config TableConfig,
	batchValues []any,
) ([]map[string]any, error) {
	query := h.client.From(config.Table).WithContext(ctx)

	if config.Schema != "" {
		query = query.Schema(config.Schema)
	}

	if config.Select != "" {
		query = query.Select(config.Select)
	} else {
		query = query.Select("*")
	}

	query = query.In(config.InColumn, batchValues)

	for key, value := range config.Filters {
		query = query.Eq(key, value)
	}

	if len(config.OrderBy) > 0 {
		for _, o := range config.OrderBy {
			query = query.Order(o.Column, o.Ascending)
		}
	}

	if config.Limit > 0 {
		query = query.Limit(config.Limit)
	}

	return query.Execute()
}

func splitIntoBatches(items []any, batchSize int) [][]any {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	var batches [][]any
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}

// groupByFiveMinutes groups data by 5-minute intervals
func groupByFiveMinutes(data []map[string]any) []map[string]any {
	if len(data) == 0 {
		return data
	}

	grouped := make(map[string][]map[string]any)

	for _, row := range data {
		createdAt, ok := row["created_at"].(string)
		if !ok {
			continue
		}

		t, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05", createdAt)
			if err != nil {
				t, err = time.Parse("2006-01-02T15:04:05.999999", createdAt)
				if err != nil {
					continue
				}
			}
		}

		minute := t.Minute()
		roundedMinute := (minute / 5) * 5
		roundedTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), roundedMinute, 0, 0, t.Location())
		key := roundedTime.Format(time.RFC3339)

		grouped[key] = append(grouped[key], row)
	}

	var result []map[string]any
	var keys []string
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		for _, item := range grouped[k] {
			item["grouped_time"] = k
			result = append(result, item)
		}
	}

	return result
}
