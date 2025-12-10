package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// QueryBuilder provides a fluent interface for building Supabase queries
type QueryBuilder struct {
	client     *Client
	table      string
	selectCols string
	filters    []filter
	orderBy    []orderClause
	inFilters  []inFilter
	limit      int
	offset     int
	ctx        context.Context
}

type filter struct {
	column   string
	operator string
	value    interface{}
}

type inFilter struct {
	column string
	values []interface{}
}

type orderClause struct {
	column    string
	ascending bool
}

// From starts a new query on the specified table
func (c *Client) From(table string) *QueryBuilder {
	return &QueryBuilder{
		client:     c,
		table:      table,
		selectCols: "*",
		filters:    make([]filter, 0),
		inFilters:  make([]inFilter, 0),
		orderBy:    make([]orderClause, 0),
		limit:      0,
		offset:     0,
		ctx:        context.Background(),
	}
}

// WithContext sets the context for the query
func (q *QueryBuilder) WithContext(ctx context.Context) *QueryBuilder {
	q.ctx = ctx
	return q
}

// Select specifies the columns to select
func (q *QueryBuilder) Select(columns string) *QueryBuilder {
	q.selectCols = columns
	return q
}

// Eq adds an equality filter
func (q *QueryBuilder) Eq(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "eq", value: value})
	return q
}

// Neq adds a not-equal filter
func (q *QueryBuilder) Neq(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "neq", value: value})
	return q
}

// Gt adds a greater-than filter
func (q *QueryBuilder) Gt(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "gt", value: value})
	return q
}

// Gte adds a greater-than-or-equal filter
func (q *QueryBuilder) Gte(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "gte", value: value})
	return q
}

// Lt adds a less-than filter
func (q *QueryBuilder) Lt(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "lt", value: value})
	return q
}

// Lte adds a less-than-or-equal filter
func (q *QueryBuilder) Lte(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "lte", value: value})
	return q
}

// Like adds a LIKE filter
func (q *QueryBuilder) Like(column string, pattern string) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "like", value: pattern})
	return q
}

// ILike adds a case-insensitive LIKE filter
func (q *QueryBuilder) ILike(column string, pattern string) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "ilike", value: pattern})
	return q
}

// Is adds an IS filter (for null checks)
func (q *QueryBuilder) Is(column string, value interface{}) *QueryBuilder {
	q.filters = append(q.filters, filter{column: column, operator: "is", value: value})
	return q
}

// In adds an IN filter
func (q *QueryBuilder) In(column string, values []interface{}) *QueryBuilder {
	q.inFilters = append(q.inFilters, inFilter{column: column, values: values})
	return q
}

// Order adds an ORDER BY clause
func (q *QueryBuilder) Order(column string, ascending bool) *QueryBuilder {
	q.orderBy = append(q.orderBy, orderClause{column: column, ascending: ascending})
	return q
}

// Limit sets the maximum number of rows to return
func (q *QueryBuilder) Limit(limit int) *QueryBuilder {
	q.limit = limit
	return q
}

// Offset sets the number of rows to skip
func (q *QueryBuilder) Offset(offset int) *QueryBuilder {
	q.offset = offset
	return q
}

// Execute runs the query and returns the results
func (q *QueryBuilder) Execute() ([]map[string]interface{}, error) {
	reqURL, err := q.buildURL()
	if err != nil {
		return nil, fmt.Errorf("failed to build URL: %w", err)
	}

	req, err := http.NewRequestWithContext(q.ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("apikey", q.client.apiKey)
	req.Header.Set("Authorization", "Bearer "+q.client.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "return=representation")

	resp, err := q.client.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("supabase error (status %d): %s", resp.StatusCode, string(body))
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return result, nil
}

func (q *QueryBuilder) buildURL() (string, error) {
	baseURL := fmt.Sprintf("%s/rest/v1/%s", q.client.baseURL, q.table)

	params := url.Values{}
	params.Set("select", q.selectCols)

	// Add equality and comparison filters
	for _, f := range q.filters {
		params.Add(f.column, fmt.Sprintf("%s.%v", f.operator, f.value))
	}

	// Add IN filters
	for _, inf := range q.inFilters {
		values := make([]string, len(inf.values))
		for i, v := range inf.values {
			values[i] = fmt.Sprintf("%v", v)
		}
		params.Add(inf.column, fmt.Sprintf("in.(%s)", strings.Join(values, ",")))
	}

	// Add ORDER BY
	for _, o := range q.orderBy {
		direction := "desc"
		if o.ascending {
			direction = "asc"
		}
		params.Add("order", fmt.Sprintf("%s.%s", o.column, direction))
	}

	// Add LIMIT
	if q.limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", q.limit))
	}

	// Add OFFSET
	if q.offset > 0 {
		params.Set("offset", fmt.Sprintf("%d", q.offset))
	}

	return fmt.Sprintf("%s?%s", baseURL, params.Encode()), nil
}

// RPC calls a stored procedure
func (c *Client) RPC(ctx context.Context, functionName string, params map[string]interface{}) ([]byte, error) {
	reqURL := fmt.Sprintf("%s/rest/v1/rpc/%s", c.baseURL, functionName)

	body, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal params: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("apikey", c.apiKey)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("supabase error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}
