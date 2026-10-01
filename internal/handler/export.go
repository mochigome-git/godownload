package handler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"godownload/internal/config"
	"godownload/internal/supabase"

	"github.com/xuri/excelize/v2"
	"go.uber.org/zap"
)

// ExportRequest represents the request for Excel export
type ExportRequest struct {
	Tables      []TableConfig     `json:"tables"`
	FileName    string            `json:"file_name,omitempty"`
	HeaderMap   map[string]string `json:"header_map,omitempty"`
	IsFiveMin   bool              `json:"is_five_min,omitempty"`
	BatchSize   int               `json:"batch_size,omitempty"`
	Concurrency int               `json:"concurrency,omitempty"`
	GroupBy     string            `json:"group_by,omitempty"`
}

// ExportResponse contains the file information and signed URL
type ExportResponse struct {
	FileName    string `json:"file_name"`
	FilePath    string `json:"file_path"`
	SignedURL   string `json:"signed_url"`
	ExpiresIn   int    `json:"expires_in"` // Hours
	ContentType string `json:"content_type"`
	FileSize    int64  `json:"file_size"`
}

// ExportHandler handles Excel export operations
type ExportHandler struct {
	downloadHandler *DownloadHandler
	supabaseClient  *supabase.Client
	config          *config.Config
	logger          *zap.SugaredLogger
	timezone        *time.Location // Timezone for display
}

// NewExportHandler creates a new export handler
func NewExportHandler(downloadHandler *DownloadHandler, logger *zap.SugaredLogger) *ExportHandler {
	// Load timezone - default to UTC+8 (Asia/Kuala_Lumpur) or configure as needed
	tz, err := time.LoadLocation("Asia/Kuala_Lumpur")
	if err != nil {
		logger.Warnw("Failed to load timezone, using UTC", "error", err)
		tz = time.UTC
	}

	downloadHandler.transformer.rejectLabelsOn = true

	return &ExportHandler{
		downloadHandler: downloadHandler,
		supabaseClient:  downloadHandler.client,
		config:          downloadHandler.config,
		logger:          logger,
		timezone:        tz,
	}
}

// ProcessExport downloads data, converts to Excel, uploads to S3, and returns signed URL
func (h *ExportHandler) ProcessExport(ctx context.Context, req *ExportRequest, userID string) (*ExportResponse, error) {
	h.logger.Infow("Starting Excel export",
		"user_id", userID,
		"tables_count", len(req.Tables),
		"group_by", req.GroupBy,
		"has_header_map", len(req.HeaderMap) > 0,
		"is_five_min", req.IsFiveMin,
	)

	downloadReq := &DownloadRequest{
		Tables:      req.Tables,
		IsFiveMin:   false,
		BatchSize:   req.BatchSize,
		Concurrency: req.Concurrency,
	}

	downloadResp, err := h.downloadHandler.ProcessDownloadRaw(ctx, downloadReq, userID)
	if err != nil {
		h.logger.Errorw("Failed to download data for export",
			"user_id", userID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to download data: %w", err)
	}

	var excelData []byte
	switch {
	case usesSheetLayout(req.Tables):
		excelData, err = h.convertToExcelBySheet(ctx, req.Tables, downloadResp.Tables, req.HeaderMap, req.IsFiveMin)
	case req.GroupBy != "":
		excelData, err = h.convertToExcelGrouped(ctx, downloadResp.Tables, req.HeaderMap, req.GroupBy, req.IsFiveMin)
	default:
		excelData, err = h.convertToExcel(ctx, downloadResp.Tables, req.HeaderMap, req.IsFiveMin)
	}

	if err != nil {
		h.logger.Errorw("Failed to convert data to Excel",
			"user_id", userID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to convert to Excel: %w", err)
	}

	fileName := req.FileName
	if fileName == "" {
		fileName = h.generateFileName(req.Tables)
	}
	if len(fileName) < 5 || fileName[len(fileName)-5:] != ".xlsx" {
		fileName += ".xlsx"
	}

	// Upload to Supabase Storage using service key (NOT JWT)
	storageHandler := NewStorageHandler(h.supabaseClient, h.config, h.logger)
	filePath, err := storageHandler.UploadFile(ctx, fileName, excelData, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	if err != nil {
		h.logger.Errorw("Failed to upload file to storage",
			"user_id", userID,
			"file_name", fileName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to upload file: %w", err)
	}

	// Generate signed URL with expiry (also uses service key)
	signedURL, err := storageHandler.GenerateSignedURL(ctx, filePath)
	if err != nil {
		h.logger.Errorw("Failed to generate signed URL",
			"user_id", userID,
			"file_path", filePath,
			"error", err,
		)
		return nil, fmt.Errorf("failed to generate signed URL: %w", err)
	}

	h.logger.Infow("Excel export completed and uploaded to storage",
		"user_id", userID,
		"file_name", fileName,
		"file_path", filePath,
		"file_size", len(excelData),
		"expires_in_hours", h.config.S3ExpiryHours,
	)

	return &ExportResponse{
		FileName:    fileName,
		FilePath:    filePath,
		SignedURL:   signedURL,
		ExpiresIn:   h.config.S3ExpiryHours,
		ContentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		FileSize:    int64(len(excelData)),
	}, nil
}

// convertToExcelGrouped converts table data to Excel with sheets grouped by a column
func (h *ExportHandler) convertToExcelGrouped(ctx context.Context, tables []TableData, headerMap map[string]string, groupBy string, isFiveMin bool) ([]byte, error) {
	f := excelize.NewFile()
	defer f.Close()

	defaultSheet := f.GetSheetName(0)
	sheetCreated := false
	usedSheetNames := make(map[string]int)

	for _, table := range tables {
		if len(table.Data) == 0 {
			h.logger.Warnw("Table has no data", "table", table.Table)
			continue
		}

		sampleKeys := make([]string, 0, len(table.Data[0]))
		for k := range table.Data[0] {
			sampleKeys = append(sampleKeys, k)
		}
		if _, ok := table.Data[0][groupBy]; !ok {
			h.logger.Errorw("GroupBy column not found in data",
				"table", table.Table,
				"group_by", groupBy,
				"available_columns", sampleKeys,
			)
		} else {
			h.logger.Infow("Raw data columns before grouping",
				"table", table.Table,
				"columns", sampleKeys,
				"group_by", groupBy,
				"row_count", len(table.Data),
			)
		}

		schema := table.Schema
		if schema == "" {
			schema = "public"
		}

		groups := h.groupDataByColumn(table.Data, groupBy)
		groupNames := h.resolveGroupNames(ctx, schema, table.Table, groupBy, groups)

		// Deterministic sheet order: sort by resolved sheet name, then key
		groupKeys := make([]string, 0, len(groups))
		for k := range groups {
			groupKeys = append(groupKeys, k)
		}
		displayName := func(k string) string {
			if n := groupNames[k]; n != "" {
				return n
			}
			return k
		}
		sort.SliceStable(groupKeys, func(i, j int) bool {
			ni, nj := displayName(groupKeys[i]), displayName(groupKeys[j])
			if ni != nj {
				return ni < nj
			}
			return groupKeys[i] < groupKeys[j]
		})

		h.logger.Infow("Grouped data",
			"table", table.Table,
			"group_by", groupBy,
			"groups_count", len(groups),
			"group_names", groupNames,
		)

		excludeColumn := h.getOutputNameForColumn(schema, table.Table, groupBy)
		if excludeColumn == "" {
			excludeColumn = groupBy
		}

		for _, groupKey := range groupKeys {
			groupData := groups[groupKey]
			if len(groupData) == 0 {
				continue
			}

			// Transform + aggregate BEFORE creating the sheet
			transformedData, err := h.downloadHandler.transformer.TransformTableData(ctx, schema, table.Table, groupData)
			if err != nil {
				h.logger.Warnw("Failed to transform group data, using raw data",
					"group_key", groupKey,
					"error", err,
				)
				transformedData = groupData
			}

			if isFiveMin {
				before := len(transformedData)
				transformedData = groupByFiveMinutes(transformedData, FiveMinLast)
				h.logger.Infow("Applied 5-min aggregation",
					"group_key", groupKey,
					"rows_before", before,
					"rows_after", len(transformedData),
				)
			}

			if len(transformedData) == 0 {
				continue
			}

			sheetName := h.getUniqueSheetName(displayName(groupKey), usedSheetNames)

			if !sheetCreated {
				f.SetSheetName(defaultSheet, sheetName)
				sheetCreated = true
			} else if _, err := f.NewSheet(sheetName); err != nil {
				h.logger.Warnw("Failed to create sheet", "sheet_name", sheetName, "error", err)
				continue
			}

			h.logger.Debugw("Writing sheet data",
				"sheet_name", sheetName,
				"exclude_column", excludeColumn,
				"rows", len(transformedData),
			)

			h.writeSheetData(f, sheetName, transformedData, headerMap, excludeColumn)
		}
	}

	if !sheetCreated {
		f.SetCellValue(defaultSheet, "A1", "No data")
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		return nil, fmt.Errorf("failed to write Excel buffer: %w", err)
	}

	return buf.Bytes(), nil
}

func (h *ExportHandler) getOutputNameForColumn(schema, table, column string) string {
	for _, m := range h.config.JoinMappings {
		if m.Schema == schema && m.Table == table && m.Column == column {
			return m.OutputName
		}
	}
	return ""
}

func (h *ExportHandler) groupDataByColumn(data []map[string]any, column string) map[string][]map[string]any {
	groups := make(map[string][]map[string]any)

	for _, row := range data {
		var key string
		if val, ok := row[column]; ok && val != nil {
			key = fmt.Sprintf("%v", val)
		} else {
			key = "unknown"
		}

		groups[key] = append(groups[key], row)
	}

	return groups
}

func (h *ExportHandler) resolveGroupNames(ctx context.Context, schema, table, groupBy string, groups map[string][]map[string]any) map[string]string {
	names := make(map[string]string)

	for key := range groups {
		names[key] = key
	}

	var mapping *config.JoinMapping
	for i := range h.config.JoinMappings {
		m := &h.config.JoinMappings[i]
		h.logger.Debugw("Checking join mapping for group names",
			"mapping_schema", m.Schema,
			"mapping_table", m.Table,
			"mapping_column", m.Column,
			"looking_for_schema", schema,
			"looking_for_table", table,
			"looking_for_column", groupBy,
			"match", m.Schema == schema && m.Table == table && m.Column == groupBy,
		)
		if m.Schema == schema && m.Table == table && m.Column == groupBy {
			mapping = m
			break
		}
	}

	if mapping == nil {
		h.logger.Warnw("No join mapping found for group column",
			"schema", schema,
			"table", table,
			"column", groupBy,
			"available_mappings", len(h.config.JoinMappings),
		)
		return names
	}

	h.logger.Infow("Found join mapping for group column",
		"mapping", fmt.Sprintf("%s.%s.%s -> %s.%s.%s",
			mapping.Schema, mapping.Table, mapping.Column,
			mapping.ForeignSchema, mapping.ForeignTable, mapping.DisplayColumn),
	)

	var fkValues []any
	for key := range groups {
		if key != "" && key != "unknown" && key != "<nil>" {
			fkValues = append(fkValues, key)
		}
	}

	if len(fkValues) == 0 {
		h.logger.Warnw("No valid FK values to lookup for group names")
		return names
	}

	h.logger.Infow("Looking up FK display names for sheets",
		"foreign_schema", mapping.ForeignSchema,
		"foreign_table", mapping.ForeignTable,
		"foreign_key", mapping.ForeignKey,
		"display_column", mapping.DisplayColumn,
		"fk_count", len(fkValues),
		"fk_values", fkValues,
	)

	fkColumn := mapping.ForeignKey
	if fkColumn == "" {
		fkColumn = "id"
	}

	selectStr := fmt.Sprintf("%s,%s", fkColumn, mapping.DisplayColumn)

	query := h.supabaseClient.From(mapping.ForeignTable).
		WithContext(ctx).
		Select(selectStr).
		In(fkColumn, fkValues)

	if mapping.ForeignSchema != "" && mapping.ForeignSchema != "public" {
		query = query.Schema(mapping.ForeignSchema)
	}

	h.logger.Debugw("Executing FK lookup query",
		"table", mapping.ForeignTable,
		"schema", mapping.ForeignSchema,
		"select", selectStr,
		"in_column", fkColumn,
		"in_values", fkValues,
	)

	results, err := query.Execute()
	if err != nil {
		h.logger.Errorw("Failed to resolve FK names for sheets",
			"error", err,
			"foreign_table", mapping.ForeignSchema+"."+mapping.ForeignTable,
		)
		return names
	}

	h.logger.Infow("FK lookup results for sheets",
		"results_count", len(results),
		"results", results,
	)

	for _, row := range results {
		if fkVal, ok := row[fkColumn]; ok && fkVal != nil {
			key := fmt.Sprintf("%v", fkVal)
			if displayVal, ok := row[mapping.DisplayColumn]; ok && displayVal != nil {
				names[key] = fmt.Sprintf("%v", displayVal)
				h.logger.Infow("Resolved FK name for sheet",
					"key", key,
					"name", names[key],
				)
			}
		}
	}

	return names
}

func (h *ExportHandler) getUniqueSheetName(name string, used map[string]int) string {
	name = strings.Map(func(r rune) rune {
		if r == ':' || r == '\\' || r == '/' || r == '?' || r == '*' || r == '[' || r == ']' {
			return '_'
		}
		return r
	}, name)

	// Excel caps sheet names at 31 characters; 28 leaves room for "_N".
	// Cut by rune, not byte, so Japanese names aren't split mid-character.
	if r := []rune(name); len(r) > 28 {
		name = string(r[:28])
	}

	if name == "" {
		name = "Sheet"
	}

	baseName := name
	count := used[baseName]
	if count > 0 {
		name = fmt.Sprintf("%s_%d", baseName, count)
	}
	used[baseName] = count + 1

	return name
}

func (h *ExportHandler) writeSheetData(f *excelize.File, sheetName string, data []map[string]any, headerMap map[string]string, excludeColumn string) {
	if len(data) == 0 {
		return
	}

	h.sortByCreatedAt(data)
	h.writeRows(f, sheetName, data, headerMap, h.getColumnOrder(data, excludeColumn))
}

// writeRows writes a header row plus data rows in the given column order.
func (h *ExportHandler) writeRows(f *excelize.File, sheetName string, data []map[string]any, headerMap map[string]string, columns []string) {
	for colIdx, colName := range columns {
		cell, _ := excelize.CoordinatesToCellName(colIdx+1, 1)
		headerName := colName
		if mapped, ok := headerMap[colName]; ok {
			headerName = mapped
		}
		f.SetCellValue(sheetName, cell, headerName)
	}

	headerStyle, _ := f.NewStyle(&excelize.Style{
		Font:      &excelize.Font{Bold: true},
		Fill:      excelize.Fill{Type: "pattern", Color: []string{"#E0E0E0"}, Pattern: 1},
		Alignment: &excelize.Alignment{Horizontal: "center"},
	})
	for colIdx := range columns {
		cell, _ := excelize.CoordinatesToCellName(colIdx+1, 1)
		f.SetCellStyle(sheetName, cell, cell, headerStyle)
	}

	for rowIdx, row := range data {
		excelRow := rowIdx + 2
		for colIdx, colName := range columns {
			cell, _ := excelize.CoordinatesToCellName(colIdx+1, excelRow)
			h.setCellValue(f, sheetName, cell, row[colName])
		}
	}

	for colIdx, colName := range columns {
		colLetter, _ := excelize.ColumnNumberToName(colIdx + 1)
		width := h.calculateColumnWidth(colName, data)
		f.SetColWidth(sheetName, colLetter, colLetter, width)
	}
}

func (h *ExportHandler) convertToExcel(ctx context.Context, tables []TableData, headerMap map[string]string, isFiveMin bool) ([]byte, error) {
	f := excelize.NewFile()
	defer f.Close()

	defaultSheet := f.GetSheetName(0)
	usedSheetNames := make(map[string]int)
	sheetCreated := false

	for _, table := range tables {
		if len(table.Data) == 0 {
			continue
		}

		schema := table.Schema
		if schema == "" {
			schema = "public"
		}

		transformedData, err := h.downloadHandler.transformer.TransformTableData(ctx, schema, table.Table, table.Data)
		if err != nil {
			h.logger.Warnw("Failed to transform data, using raw data",
				"schema", schema,
				"table", table.Table,
				"error", err,
			)
			transformedData = table.Data
		}

		if isFiveMin {
			before := len(transformedData)
			transformedData = groupByFiveMinutes(transformedData, FiveMinLast)
			h.logger.Infow("Applied 5-min aggregation",
				"schema", schema,
				"table", table.Table,
				"rows_before", before,
				"rows_after", len(transformedData),
			)
		}

		if len(transformedData) == 0 {
			continue // e.g. every row lacked a usable created_at
		}

		sheetName := table.Table
		if table.Schema != "" && table.Schema != "public" {
			sheetName = table.Schema + "_" + table.Table
		}
		sheetName = h.getUniqueSheetName(sheetName, usedSheetNames)

		if !sheetCreated {
			f.SetSheetName(defaultSheet, sheetName)
			sheetCreated = true
		} else if _, err := f.NewSheet(sheetName); err != nil {
			h.logger.Warnw("Failed to create sheet", "sheet_name", sheetName, "error", err)
			continue
		}

		h.writeSheetData(f, sheetName, transformedData, headerMap, "")
	}

	if !sheetCreated {
		f.SetCellValue(defaultSheet, "A1", "No data")
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		return nil, fmt.Errorf("failed to write Excel buffer: %w", err)
	}

	return buf.Bytes(), nil
}

func (h *ExportHandler) sortByCreatedAt(data []map[string]any) {
	sort.SliceStable(data, func(i, j int) bool {
		aVal, aOk := data[i]["created_at"]
		bVal, bOk := data[j]["created_at"]
		if !aOk || !bOk {
			return false
		}
		return h.parseTime(aVal).Before(h.parseTime(bVal))
	})
}

// parseTime parses time value and keeps original timezone
func (h *ExportHandler) parseTime(val any) time.Time {
	switch v := val.(type) {
	case string:
		// Try RFC3339 (with timezone info)
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t
		}
		// Try RFC3339Nano
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t
		}
		// Try with microseconds and timezone
		if t, err := time.Parse("2006-01-02T15:04:05.999999-07:00", v); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02T15:04:05.999999Z07:00", v); err == nil {
			return t
		}
		// Try without timezone (assume UTC)
		if t, err := time.Parse("2006-01-02T15:04:05.999999", v); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02T15:04:05", v); err == nil {
			return t
		}
		if t, err := time.Parse("2006-01-02", v); err == nil {
			return t
		}
	case time.Time:
		return v
	}
	return time.Time{}
}

func (h *ExportHandler) getColumnOrder(data []map[string]any, exclude string) []string {
	seen := make(map[string]struct{})
	for _, row := range data {
		for k := range row {
			if k == exclude {
				continue
			}
			seen[k] = struct{}{}
		}
	}

	columns := make([]string, 0, len(seen))
	for k := range seen {
		columns = append(columns, k)
	}

	priority := map[string]int{"id": 0, "uuid": 1, "created_at": 2, "updated_at": 3}

	sort.SliceStable(columns, func(i, j int) bool {
		pi, oki := priority[columns[i]]
		pj, okj := priority[columns[j]]
		if oki && okj {
			return pi < pj
		}
		if oki {
			return true
		}
		if okj {
			return false
		}
		return columns[i] < columns[j]
	})

	return columns
}

// setCellValue sets a cell value with proper timezone handling for dates
func (h *ExportHandler) setCellValue(f *excelize.File, sheet, cell string, value any) {
	if value == nil {
		f.SetCellValue(sheet, cell, "")
		return
	}

	switch v := value.(type) {
	case string:
		// Try to parse as time - handle timezone properly
		if t := h.parseTimeForExcel(v); !t.IsZero() {
			// Convert to local timezone for display
			localTime := t.In(h.timezone)
			f.SetCellValue(sheet, cell, localTime)
			// Use custom date format: YYYY/MM/DD HH:MM:SS
			dateStyle, _ := f.NewStyle(&excelize.Style{
				NumFmt:    22, // or use custom: "yyyy/mm/dd hh:mm:ss"
				Alignment: &excelize.Alignment{Horizontal: "center"},
			})
			f.SetCellStyle(sheet, cell, cell, dateStyle)
			return
		}

		// Try to parse as number
		if h.isNumericString(v) {
			if num, err := strconv.ParseFloat(v, 64); err == nil {
				if num == float64(int64(num)) && num >= -9007199254740991 && num <= 9007199254740991 {
					f.SetCellValue(sheet, cell, int64(num))
				} else {
					f.SetCellValue(sheet, cell, num)
				}
				return
			}
		}

		f.SetCellValue(sheet, cell, v)

	case float64:
		f.SetCellValue(sheet, cell, v)
	case float32:
		f.SetCellValue(sheet, cell, float64(v))
	case int:
		f.SetCellValue(sheet, cell, v)
	case int64:
		f.SetCellValue(sheet, cell, v)
	case int32:
		f.SetCellValue(sheet, cell, int64(v))
	case bool:
		f.SetCellValue(sheet, cell, v)
	case time.Time:
		// Convert to local timezone for display
		localTime := v.In(h.timezone)
		f.SetCellValue(sheet, cell, localTime)
		dateStyle, _ := f.NewStyle(&excelize.Style{
			NumFmt:    22,
			Alignment: &excelize.Alignment{Horizontal: "center"},
		})
		f.SetCellStyle(sheet, cell, cell, dateStyle)
	default:
		f.SetCellValue(sheet, cell, fmt.Sprintf("%v", v))
	}
}

// parseTimeForExcel parses time string and returns time.Time
// It handles various formats including Supabase timestamptz
func (h *ExportHandler) parseTimeForExcel(v string) time.Time {
	// Common formats from Supabase/PostgreSQL
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999-07:00",
		"2006-01-02T15:04:05.999999Z",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05.999999+07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, v); err == nil {
			return t
		}
	}

	return time.Time{}
}

func (h *ExportHandler) isNumericString(s string) bool {
	if s == "" {
		return false
	}

	if strings.Contains(s, "-") && len(s) > 10 {
		return false // Likely UUID
	}

	if len(s) > 15 {
		return false
	}

	hasDecimal := false
	hasDigit := false
	for i, r := range s {
		if r == '-' || r == '+' {
			if i != 0 {
				return false
			}
			continue
		}
		if r == '.' {
			if hasDecimal {
				return false
			}
			hasDecimal = true
			continue
		}
		if r >= '0' && r <= '9' {
			hasDigit = true
			continue
		}
		return false
	}

	return hasDigit
}

func (h *ExportHandler) calculateColumnWidth(colName string, data []map[string]any) float64 {
	maxLen := len(colName)
	sampleSize := 100
	if len(data) < sampleSize {
		sampleSize = len(data)
	}

	for i := 0; i < sampleSize; i++ {
		if val, ok := data[i][colName]; ok && val != nil {
			if strLen := len(fmt.Sprintf("%v", val)); strLen > maxLen {
				maxLen = strLen
			}
		}
	}

	width := float64(maxLen) + 2
	if width < 8 {
		width = 8
	}
	if width > 50 {
		width = 50
	}
	return width
}

func (h *ExportHandler) generateFileName(tables []TableConfig) string {
	timestamp := time.Now().Format("20060102_150405")
	if len(tables) == 1 {
		name := tables[0].Table
		if tables[0].Schema != "" && tables[0].Schema != "public" {
			name = tables[0].Schema + "_" + name
		}
		return fmt.Sprintf("%s_%s.xlsx", name, timestamp)
	}
	return fmt.Sprintf("export_%d_tables_%s.xlsx", len(tables), timestamp)
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// ----------------------------------------------------------------------
// Sheet layout: tables name their own sheet (sheet_name), tables sharing
// a name stack onto one sheet, inline rows are allowed.
// ----------------------------------------------------------------------

func usesSheetLayout(tables []TableConfig) bool {
	for _, t := range tables {
		if t.SheetName != "" || len(t.Rows) > 0 {
			return true
		}
	}
	return false
}

type layoutSheet struct {
	name    string
	rows    []map[string]any
	columns []string
	hidden  map[string]bool
	inline  bool // keep caller's row order instead of sorting by created_at
}

// convertToExcelBySheet writes each table to the sheet it names, in request
// order. ProcessDownloadRaw keeps results at the same index as the request,
// so cfgs[i] describes tables[i].
func (h *ExportHandler) convertToExcelBySheet(
	ctx context.Context,
	cfgs []TableConfig,
	tables []TableData,
	headerMap map[string]string,
	isFiveMin bool,
) ([]byte, error) {
	var order []*layoutSheet
	byName := make(map[string]*layoutSheet)

	for i, table := range tables {
		if i >= len(cfgs) || len(table.Data) == 0 {
			continue
		}
		cfg := cfgs[i]

		schema := table.Schema
		if schema == "" {
			schema = "public"
		}

		name := cfg.SheetName
		if name == "" {
			name = table.Table
			if table.Schema != "" && table.Schema != "public" {
				name = table.Schema + "_" + table.Table
			}
		}

		data, err := h.downloadHandler.transformer.TransformTableData(ctx, schema, table.Table, table.Data)
		if err != nil {
			h.logger.Warnw("Failed to transform data, using raw data", "table", table.Table, "error", err)
			data = table.Data
		}

		inline := len(cfg.Rows) > 0
		if isFiveMin && !inline {
			before := len(data)
			data = groupByFiveMinutes(data, FiveMinLast)
			h.logger.Infow("Applied 5-min aggregation",
				"sheet", name,
				"table", table.Table,
				"rows_before", before,
				"rows_after", len(data),
			)
		}

		if len(data) == 0 {
			continue
		}

		s := byName[name]
		if s == nil {
			s = &layoutSheet{name: name, hidden: make(map[string]bool), inline: inline}
			byName[name] = s
			order = append(order, s)
		}
		s.rows = append(s.rows, data...)
		for _, c := range cfg.Columns {
			if !containsString(s.columns, c) {
				s.columns = append(s.columns, c)
			}
		}
		for _, c := range cfg.HideColumns {
			s.hidden[c] = true
		}
	}

	f := excelize.NewFile()
	defer f.Close()

	defaultSheet := f.GetSheetName(0)
	used := make(map[string]int)
	created := false

	for _, s := range order {
		if len(s.rows) == 0 {
			continue
		}

		name := h.getUniqueSheetName(s.name, used)
		if !created {
			f.SetSheetName(defaultSheet, name)
			created = true
		} else if _, err := f.NewSheet(name); err != nil {
			h.logger.Warnw("Failed to create sheet", "sheet_name", name, "error", err)
			continue
		}

		if !s.inline {
			h.sortByCreatedAt(s.rows)
		}
		h.writeRows(f, name, s.rows, headerMap, h.orderedColumns(s.rows, s.columns, s.hidden))
	}

	if !created {
		f.SetCellValue(defaultSheet, "A1", "No data")
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		return nil, fmt.Errorf("failed to write Excel buffer: %w", err)
	}
	return buf.Bytes(), nil
}

// orderedColumns: preferred columns first (in their order, if present),
// then everything else in the usual order, minus hidden ones.
func (h *ExportHandler) orderedColumns(data []map[string]any, preferred []string, hidden map[string]bool) []string {
	rest := h.getColumnOrder(data, "")
	present := make(map[string]bool, len(rest))
	for _, c := range rest {
		present[c] = true
	}

	out := make([]string, 0, len(rest))
	taken := make(map[string]bool, len(rest))
	for _, c := range preferred {
		if present[c] && !hidden[c] && !taken[c] {
			out = append(out, c)
			taken[c] = true
		}
	}
	for _, c := range rest {
		if !hidden[c] && !taken[c] {
			out = append(out, c)
			taken[c] = true
		}
	}
	return out
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
