package handler

import (
	"context"
	"fmt"
	"sync"

	"godownload/internal/config"
	"godownload/internal/supabase"

	"go.uber.org/zap"
)

// DataTransformer handles data transformations based on config
type DataTransformer struct {
	client *supabase.Client
	config *config.Config
	logger *zap.SugaredLogger

	// Cache for foreign key lookups: "schema.table" -> { "fk_value" -> "display_value" }
	fkCache   map[string]map[string]string
	fkCacheMu sync.RWMutex
}

// NewDataTransformer creates a new data transformer
func NewDataTransformer(client *supabase.Client, cfg *config.Config, logger *zap.SugaredLogger) *DataTransformer {
	return &DataTransformer{
		client:  client,
		config:  cfg,
		logger:  logger,
		fkCache: make(map[string]map[string]string),
	}
}

// TransformTableData applies all transformations to table data
func (t *DataTransformer) TransformTableData(
	ctx context.Context,
	schema, table string,
	data []map[string]any,
) ([]map[string]any, error) {
	if len(data) == 0 {
		return data, nil
	}

	t.logger.Infow("Transforming table data",
		"schema", schema,
		"table", table,
		"rows", len(data),
		"join_mappings_total", len(t.config.JoinMappings),
		"hidden_columns_total", len(t.config.HiddenColumns),
		"flatten_jsonb_total", len(t.config.FlattenJSONB),
	)

	// Log all configured join mappings for debugging
	for i, m := range t.config.JoinMappings {
		t.logger.Infow("Configured join mapping",
			"index", i,
			"source", fmt.Sprintf("%s.%s.%s", m.Schema, m.Table, m.Column),
			"foreign", fmt.Sprintf("%s.%s.%s.%s", m.ForeignSchema, m.ForeignTable, m.ForeignKey, m.DisplayColumn),
			"output", m.OutputName,
		)
	}

	// Find applicable mappings for this table
	var applicableMappings []config.JoinMapping
	for _, m := range t.config.JoinMappings {
		t.logger.Debugw("Checking mapping",
			"mapping_schema", m.Schema,
			"mapping_table", m.Table,
			"current_schema", schema,
			"current_table", table,
			"match", m.Schema == schema && m.Table == table,
		)
		if m.Schema == schema && m.Table == table {
			applicableMappings = append(applicableMappings, m)
			t.logger.Infow("Found applicable join mapping",
				"column", m.Column,
				"foreign_table", m.ForeignSchema+"."+m.ForeignTable,
				"foreign_key", m.ForeignKey,
				"display_column", m.DisplayColumn,
				"output_name", m.OutputName,
			)
		}
	}

	t.logger.Infow("Applicable mappings found",
		"count", len(applicableMappings),
		"schema", schema,
		"table", table,
	)

	// Batch fetch all FK display values
	if len(applicableMappings) > 0 {
		if err := t.prefetchFKValues(ctx, applicableMappings, data); err != nil {
			t.logger.Warnw("Failed to prefetch FK values, continuing without joins",
				"error", err,
			)
		}
	}

	// Transform each row
	result := make([]map[string]any, 0, len(data))
	for _, row := range data {
		transformed := t.transformRow(schema, table, row, applicableMappings)
		result = append(result, transformed)
	}

	// Log sample of transformed data
	if len(result) > 0 {
		sampleKeys := make([]string, 0)
		for k := range result[0] {
			sampleKeys = append(sampleKeys, k)
		}
		t.logger.Infow("Transformation complete",
			"output_columns", sampleKeys,
			"row_count", len(result),
		)
	}

	return result, nil
}

// prefetchFKValues batch fetches FK display values for all mappings
func (t *DataTransformer) prefetchFKValues(ctx context.Context, mappings []config.JoinMapping, data []map[string]any) error {
	for _, mapping := range mappings {
		// Collect unique FK values for this mapping
		seen := make(map[string]bool)
		var values []any

		for _, row := range data {
			if val, ok := row[mapping.Column]; ok && val != nil {
				strVal := fmt.Sprintf("%v", val)
				if strVal != "" && strVal != "<nil>" && !seen[strVal] {
					seen[strVal] = true
					values = append(values, val)
				}
			}
		}

		if len(values) == 0 {
			t.logger.Warnw("No FK values found in data for column",
				"column", mapping.Column,
				"data_sample_keys", func() []string {
					if len(data) > 0 {
						keys := make([]string, 0)
						for k := range data[0] {
							keys = append(keys, k)
						}
						return keys
					}
					return nil
				}(),
			)
			continue
		}

		t.logger.Infow("Fetching FK display values",
			"source_column", mapping.Column,
			"foreign_schema", mapping.ForeignSchema,
			"foreign_table", mapping.ForeignTable,
			"foreign_key", mapping.ForeignKey,
			"display_column", mapping.DisplayColumn,
			"values_count", len(values),
			"sample_values", values[:min(3, len(values))],
		)

		// Query foreign table for display values
		fkColumn := mapping.ForeignKey
		if fkColumn == "" {
			fkColumn = "id"
		}

		selectColumns := fmt.Sprintf("%s,%s", fkColumn, mapping.DisplayColumn)
		t.logger.Debugw("Building FK query",
			"table", mapping.ForeignTable,
			"select", selectColumns,
			"in_column", fkColumn,
			"schema", mapping.ForeignSchema,
		)

		query := t.client.From(mapping.ForeignTable).
			WithContext(ctx).
			Select(selectColumns).
			In(fkColumn, values)

		// Apply schema if not public
		if mapping.ForeignSchema != "" && mapping.ForeignSchema != "public" {
			query = query.Schema(mapping.ForeignSchema)
			t.logger.Debugw("Applied non-public schema", "schema", mapping.ForeignSchema)
		}

		results, err := query.Execute()
		if err != nil {
			t.logger.Errorw("Failed to fetch FK values",
				"foreign_table", mapping.ForeignSchema+"."+mapping.ForeignTable,
				"error", err,
				"query_select", selectColumns,
				"query_in_column", fkColumn,
			)
			continue
		}

		t.logger.Infow("FK query returned results",
			"foreign_table", mapping.ForeignSchema+"."+mapping.ForeignTable,
			"results_count", len(results),
		)

		// Log sample result for debugging
		if len(results) > 0 {
			t.logger.Debugw("FK query sample result",
				"sample", results[0],
			)
		}

		// Cache the results
		cacheKey := fmt.Sprintf("%s.%s", mapping.ForeignSchema, mapping.ForeignTable)
		t.fkCacheMu.Lock()
		if t.fkCache[cacheKey] == nil {
			t.fkCache[cacheKey] = make(map[string]string)
		}
		for _, row := range results {
			if fkVal, ok := row[fkColumn]; ok && fkVal != nil {
				fkStr := fmt.Sprintf("%v", fkVal)
				if displayVal, ok := row[mapping.DisplayColumn]; ok && displayVal != nil {
					t.fkCache[cacheKey][fkStr] = fmt.Sprintf("%v", displayVal)
					t.logger.Debugw("Cached FK value",
						"cache_key", cacheKey,
						"fk", fkStr,
						"display_value", displayVal,
					)
				} else {
					t.logger.Warnw("Display column not found in FK result",
						"display_column", mapping.DisplayColumn,
						"row_keys", func() []string {
							keys := make([]string, 0)
							for k := range row {
								keys = append(keys, k)
							}
							return keys
						}(),
					)
				}
			}
		}
		t.fkCacheMu.Unlock()

		t.logger.Infow("Cached FK display values",
			"foreign_table", cacheKey,
			"cached_count", len(results),
		)
	}

	return nil
}

// transformRow transforms a single row
func (t *DataTransformer) transformRow(schema, table string, row map[string]any, mappings []config.JoinMapping) map[string]any {
	result := make(map[string]any)

	// First, copy all columns and flatten JSONB if configured
	for key, value := range row {
		if t.config.ShouldFlattenJSONB(schema, table, key) {
			// Flatten JSONB data
			t.flattenJSONB(result, value)
			// Don't include the original JSONB column
			continue
		}
		result[key] = value
	}

	// Apply FK joins (add display columns)
	for _, mapping := range mappings {
		// Get the FK value
		fkValue, ok := row[mapping.Column]
		if !ok || fkValue == nil {
			t.logger.Debugw("FK value not found or nil",
				"column", mapping.Column,
			)
			continue
		}

		// Look up display value from cache
		cacheKey := fmt.Sprintf("%s.%s", mapping.ForeignSchema, mapping.ForeignTable)
		idStr := fmt.Sprintf("%v", fkValue)

		t.fkCacheMu.RLock()
		if cache, ok := t.fkCache[cacheKey]; ok {
			if displayVal, ok := cache[idStr]; ok {
				result[mapping.OutputName] = displayVal
				t.logger.Debugw("Applied FK join",
					"column", mapping.Column,
					"fk_value", idStr,
					"output_name", mapping.OutputName,
					"display_value", displayVal,
				)
			} else {
				t.logger.Debugw("FK value not found in cache",
					"column", mapping.Column,
					"fk_value", idStr,
					"cache_key", cacheKey,
				)
			}
		} else {
			t.logger.Debugw("Cache not found for foreign table",
				"cache_key", cacheKey,
			)
		}
		t.fkCacheMu.RUnlock()

		// Remove the original FK column (we're replacing it with the display value)
		delete(result, mapping.Column)
	}

	// Remove hidden columns
	for _, hidden := range t.config.HiddenColumns {
		if hidden.Schema == schema && hidden.Table == table {
			delete(result, hidden.Column)
		}
	}

	return result
}

// flattenJSONB extracts JSONB fields into the result map
func (t *DataTransformer) flattenJSONB(result map[string]any, value any) {
	if value == nil {
		return
	}

	switch v := value.(type) {
	case map[string]any:
		for key, val := range v {
			result[key] = val
		}
	case map[string]string:
		for key, val := range v {
			result[key] = val
		}
	default:
		t.logger.Debugw("JSONB value is not a map, skipping flatten",
			"type", fmt.Sprintf("%T", value),
		)
	}
}

// ClearCache clears the FK cache
func (t *DataTransformer) ClearCache() {
	t.fkCacheMu.Lock()
	t.fkCache = make(map[string]map[string]string)
	t.fkCacheMu.Unlock()
}

// min returns the smaller of two ints
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
