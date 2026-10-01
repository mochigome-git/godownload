package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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

	// Export layout (export only; ignored by /download)
	SheetName   string   `json:"sheet_name,omitempty"`   // tables sharing a name stack onto one sheet
	Columns     []string `json:"columns,omitempty"`      // put first, in this order
	HideColumns []string `json:"hide_columns,omitempty"` // dropped after transformation

	// Inline rows: used as-is, no query. Still transformed (reject labels).
	Rows []map[string]any `json:"rows,omitempty"`
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
				"error", err)
			transformedData = table.Data
		}
		if req.IsFiveMin {
			transformedData = groupByFiveMinutes(transformedData, FiveMinLast)
		}

		resp.Tables[i].Data = transformedData
		resp.Tables[i].Count = len(transformedData)
	}

	return resp, nil
}

// ProcessDownloadRaw handles the download request WITHOUT transformation
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

	sem := make(chan struct{}, concurrency)
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
	// Inline rows: the caller already has the data
	if len(config.Rows) > 0 {
		return config.Rows, nil
	}

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
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}

// FiveMinMode controls how rows inside one bucket collapse into one row.
type FiveMinMode string

const (
	FiveMinLast FiveMinMode = "last" // snapshot: last row in the bucket (safe for counters)
	FiveMinAvg  FiveMinMode = "avg"  // average numeric fields, last value for the rest
)

// Columns that identify an entity after transformation (FK → display name).
var fiveMinEntityKeys = []string{"machine_name", "device_name", "lot_number"}

func parseAnyTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case string:
		formats := []string{
			time.RFC3339Nano,
			"2006-01-02T15:04:05.999999-07:00",
			"2006-01-02T15:04:05.999999",
			"2006-01-02 15:04:05.999999-07:00",
			"2006-01-02 15:04:05.999999-07",
			"2006-01-02 15:04:05-07",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
		}
		for _, f := range formats {
			if p, err := time.Parse(f, t); err == nil {
				return p, true
			}
		}
	}
	return time.Time{}, false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// groupByFiveMinutes collapses rows into one row per (entity, 5-min bucket).
func groupByFiveMinutes(data []map[string]any, mode FiveMinMode) []map[string]any {
	if len(data) == 0 {
		return data
	}

	type bucket struct {
		bucketTime time.Time
		lastTime   time.Time
		last       map[string]any
		sums       map[string]float64
		counts     map[string]int
	}

	buckets := make(map[string]*bucket)
	var order []string

	for _, row := range data {
		ts, ok := parseAnyTime(row["created_at"])
		if !ok {
			continue // still skip rows without a usable timestamp
		}
		bt := ts.Truncate(5 * time.Minute) // works for any UTC offset

		var sb strings.Builder
		for _, k := range fiveMinEntityKeys {
			sb.WriteString(fmt.Sprint(row[k]))
			sb.WriteByte('|')
		}
		sb.WriteString(bt.UTC().Format(time.RFC3339))
		key := sb.String()

		b, exists := buckets[key]
		if !exists {
			b = &bucket{
				bucketTime: bt,
				sums:       map[string]float64{},
				counts:     map[string]int{},
			}
			buckets[key] = b
			order = append(order, key)
		}

		if b.last == nil || !ts.Before(b.lastTime) {
			b.last = row
			b.lastTime = ts
		}

		if mode == FiveMinAvg {
			for col, val := range row {
				if f, ok := toFloat(val); ok {
					b.sums[col] += f
					b.counts[col]++
				}
			}
		}
	}

	result := make([]map[string]any, 0, len(buckets))
	for _, key := range order {
		b := buckets[key]
		out := make(map[string]any, len(b.last)+1)
		for k, v := range b.last {
			out[k] = v
		}
		if mode == FiveMinAvg {
			for col, sum := range b.sums {
				out[col] = sum / float64(b.counts[col])
			}
		}
		out["created_at"] = b.bucketTime // the row now represents the bucket start
		result = append(result, out)
	}

	sort.SliceStable(result, func(i, j int) bool {
		return result[i]["created_at"].(time.Time).Before(result[j]["created_at"].(time.Time))
	})
	return result
}
