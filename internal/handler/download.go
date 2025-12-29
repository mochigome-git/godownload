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
	DefaultBatchSize      = 20
	DefaultConcurrency    = 5
	MaxRowsPerQuery       = 5000 // Supabase/PostgREST limit
	DefaultPaginationSize = 1000 // Fetch 1000 rows per page for better performance
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
	resp, err := h.ProcessDownloadRaw(ctx, req, userID)
	if err != nil {
		return nil, err
	}

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

		if req.IsFiveMin {
			transformedData = groupByFiveMinutes(transformedData)
		}

		resp.Tables[i].Data = transformedData
		resp.Tables[i].Count = len(transformedData)
	}

	return resp, nil
}

// ProcessDownloadRaw handles the download request WITHOUT transformation
func (h *DownloadHandler) ProcessDownloadRaw(ctx context.Context, req *DownloadRequest, userID string) (*DownloadResponse, error) {
	h.logger.Infow("Starting raw download process",
		"user_id", userID,
		"tables_count", len(req.Tables),
	)

	sem := make(chan struct{}, concurrency)
	results := make([]TableData, len(req.Tables))
	var wg sync.WaitGroup
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

func (h *DownloadHandler) processTable(
	ctx context.Context,
	config TableConfig,
	batchSize int,
	sem chan struct{},
) ([]map[string]any, error) {
	// If we have IN values, use the old batch IN query method
	if config.InColumn != "" && len(config.InValues) > 0 {
		return h.processTableWithInValues(ctx, config, batchSize, sem)
	}

	// Otherwise, use pagination to fetch all rows
	return h.processTableWithPagination(ctx, config)
}

// processTableWithPagination fetches all rows using server-side pagination
func (h *DownloadHandler) processTableWithPagination(
	ctx context.Context,
	config TableConfig,
) ([]map[string]any, error) {
	var allData []map[string]any
	pageSize := DefaultPaginationSize
	offset := 0

	// Determine the ordering column for pagination
	orderColumn := "created_at" // default
	orderAscending := true
	if len(config.OrderBy) > 0 {
		orderColumn = config.OrderBy[0].Column
		orderAscending = config.OrderBy[0].Ascending
	}

	h.logger.Infow("Starting paginated fetch",
		"schema", config.Schema,
		"table", config.Table,
		"page_size", pageSize,
		"order_column", orderColumn,
		"order_ascending", orderAscending,
	)

	for {
		h.logger.Debugw("Fetching page",
			"schema", config.Schema,
			"table", config.Table,
			"offset", offset,
			"limit", pageSize,
		)

		// Build query with pagination
		query := h.client.From(config.Table).WithContext(ctx)

		if config.Schema != "" {
			query = query.Schema(config.Schema)
		}

		if config.Select != "" {
			query = query.Select(config.Select)
		} else {
			query = query.Select("*")
		}

		// Apply filters with operator support
		for key, value := range config.Filters {
			if valueMap, ok := value.(map[string]any); ok {
				for operator, opValue := range valueMap {
					switch operator {
					case "gte":
						query = query.Gte(key, opValue)
					case "lte":
						query = query.Lte(key, opValue)
					case "gt":
						query = query.Gt(key, opValue)
					case "lt":
						query = query.Lt(key, opValue)
					case "eq":
						query = query.Eq(key, opValue)
					case "neq":
						query = query.Neq(key, opValue)
					case "like":
						query = query.Like(key, fmt.Sprintf("%v", opValue))
					case "ilike":
						query = query.ILike(key, fmt.Sprintf("%v", opValue))
					case "is":
						query = query.Is(key, opValue)
					default:
						h.logger.Warnw("Unknown filter operator",
							"operator", operator,
							"column", key,
						)
					}
				}
			} else {
				query = query.Eq(key, value)
			}
		}

		// Apply ordering
		if len(config.OrderBy) > 0 {
			for _, o := range config.OrderBy {
				query = query.Order(o.Column, o.Ascending)
			}
		}

		// Apply pagination
		query = query.Limit(pageSize).Offset(offset)

		// Execute query
		pageData, err := query.Execute()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch page at offset %d: %w", offset, err)
		}

		h.logger.Debugw("Page fetched",
			"schema", config.Schema,
			"table", config.Table,
			"rows_in_page", len(pageData),
			"total_so_far", len(allData)+len(pageData),
		)

		// No more data
		if len(pageData) == 0 {
			break
		}

		// Append to results
		allData = append(allData, pageData...)

		// Check if we got less than pageSize (last page)
		if len(pageData) < pageSize {
			break
		}

		// Check if user specified a limit
		if config.Limit > 0 && len(allData) >= config.Limit {
			allData = allData[:config.Limit]
			break
		}

		// Move to next page
		offset += pageSize
	}

	h.logger.Infow("Pagination complete",
		"schema", config.Schema,
		"table", config.Table,
		"total_rows", len(allData),
	)

	return allData, nil
}

// processTableWithInValues handles batch IN queries (original logic)
func (h *DownloadHandler) processTableWithInValues(
	ctx context.Context,
	config TableConfig,
	batchSize int,
	sem chan struct{},
) ([]map[string]any, error) {
	batches := splitIntoBatches(config.InValues, batchSize)

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

	// Apply filters with operator support
	for key, value := range config.Filters {
		if valueMap, ok := value.(map[string]any); ok {
			for operator, opValue := range valueMap {
				switch operator {
				case "gte":
					query = query.Gte(key, opValue)
				case "lte":
					query = query.Lte(key, opValue)
				case "gt":
					query = query.Gt(key, opValue)
				case "lt":
					query = query.Lt(key, opValue)
				case "eq":
					query = query.Eq(key, opValue)
				case "neq":
					query = query.Neq(key, opValue)
				case "like":
					query = query.Like(key, fmt.Sprintf("%v", opValue))
				case "ilike":
					query = query.ILike(key, fmt.Sprintf("%v", opValue))
				case "is":
					query = query.Is(key, opValue)
				default:
					h.logger.Warnw("Unknown filter operator",
						"operator", operator,
						"column", key,
					)
				}
			}
		} else {
			query = query.Eq(key, value)
		}
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
