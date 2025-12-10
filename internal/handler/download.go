package handler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"godownload/internal/supabase"
)

const (
	DefaultBatchSize   = 20
	DefaultConcurrency = 5
)

// DownloadRequest represents the incoming request structure
type DownloadRequest struct {
	Tables      []TableConfig `json:"tables"`
	RollNumbers []int64       `json:"roll_numbers"`
	IsFiveMin   bool          `json:"is_five_min,omitempty"`
	BatchSize   int           `json:"batch_size,omitempty"`
	Concurrency int           `json:"concurrency,omitempty"`
}

// TableConfig can be either a string (table name) or an object with filters
type TableConfig struct {
	Table   string                 `json:"table"`
	Filters map[string]interface{} `json:"filters,omitempty"`
	Select  string                 `json:"select,omitempty"`
	OrderBy []OrderConfig          `json:"order_by,omitempty"`
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
	Table string                   `json:"table"`
	Data  []map[string]interface{} `json:"data"`
	Count int                      `json:"count"`
}

type DownloadHandler struct {
	client *supabase.Client
}

func NewDownloadHandler(client *supabase.Client) *DownloadHandler {
	return &DownloadHandler{client: client}
}

// ProcessDownload handles the download request with batching and concurrency
func (h *DownloadHandler) ProcessDownload(ctx context.Context, req *DownloadRequest, userID string) (*DownloadResponse, error) {
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	concurrency := req.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	// Create semaphore for concurrency control
	sem := make(chan struct{}, concurrency)

	// Process all tables
	results := make([]TableData, len(req.Tables))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, config := range req.Tables {
		wg.Add(1)
		go func(idx int, cfg TableConfig) {
			defer wg.Done()

			data, err := h.processTable(ctx, cfg, req.RollNumbers, batchSize, sem)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("table %s: %w", cfg.Table, err)
				}
				mu.Unlock()
				return
			}

			// Apply five-minute grouping if requested
			if req.IsFiveMin {
				data = groupByFiveMinutes(data)
			}

			mu.Lock()
			results[idx] = TableData{
				Table: cfg.Table,
				Data:  data,
				Count: len(data),
			}
			mu.Unlock()
		}(i, config)
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &DownloadResponse{Tables: results}, nil
}

func (h *DownloadHandler) processTable(
	ctx context.Context,
	config TableConfig,
	rollNumbers []int64,
	batchSize int,
	sem chan struct{},
) ([]map[string]interface{}, error) {
	// Split roll numbers into batches
	batches := splitIntoBatches(rollNumbers, batchSize)

	// Results channel
	resultsChan := make(chan []map[string]interface{}, len(batches))
	errorsChan := make(chan error, len(batches))

	var wg sync.WaitGroup

	for _, batch := range batches {
		wg.Add(1)
		go func(b []int64) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			data, err := h.executeBatchQuery(ctx, config, b)
			if err != nil {
				errorsChan <- err
				return
			}
			resultsChan <- data
		}(batch)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(resultsChan)
	close(errorsChan)

	// Check for errors
	select {
	case err := <-errorsChan:
		return nil, err
	default:
	}

	// Collect all results
	var allData []map[string]interface{}
	for data := range resultsChan {
		allData = append(allData, data...)
	}

	return allData, nil
}

func (h *DownloadHandler) executeBatchQuery(
	ctx context.Context,
	config TableConfig,
	batch []int64,
) ([]map[string]interface{}, error) {
	// Build query
	query := h.client.From(config.Table).WithContext(ctx)

	// Set select columns
	if config.Select != "" {
		query = query.Select(config.Select)
	} else {
		query = query.Select("*")
	}

	// Add IN filter for batch
	batchValues := make([]interface{}, len(batch))
	for i, v := range batch {
		batchValues[i] = v
	}
	query = query.In("i_h_seq", batchValues)

	// Apply additional filters
	for key, value := range config.Filters {
		query = query.Eq(key, value)
	}

	// Apply ordering
	if len(config.OrderBy) > 0 {
		for _, o := range config.OrderBy {
			query = query.Order(o.Column, o.Ascending)
		}
	} else {
		// Default ordering
		query = query.Order("created_at", true) // ascending
	}

	return query.Execute()
}

func splitIntoBatches(items []int64, batchSize int) [][]int64 {
	var batches [][]int64
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
func groupByFiveMinutes(data []map[string]interface{}) []map[string]interface{} {
	if len(data) == 0 {
		return data
	}

	grouped := make(map[string][]map[string]interface{})

	for _, row := range data {
		createdAt, ok := row["created_at"].(string)
		if !ok {
			continue
		}

		t, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			// Try alternative format
			t, err = time.Parse("2006-01-02T15:04:05", createdAt)
			if err != nil {
				continue
			}
		}

		// Round down to nearest 5 minutes
		minute := t.Minute()
		roundedMinute := (minute / 5) * 5
		roundedTime := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), roundedMinute, 0, 0, t.Location())
		key := roundedTime.Format(time.RFC3339)

		grouped[key] = append(grouped[key], row)
	}

	// Convert map to slice and sort by time
	var result []map[string]interface{}
	var keys []string
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		// You can either return the first item of each group, or aggregate
		// Here we return all items with the grouped timestamp
		for _, item := range grouped[k] {
			item["grouped_time"] = k
			result = append(result, item)
		}
	}

	return result
}
