package handler

// Reject key → reject name
//
// Machines post per-code reject counts in analytics.job_summary.meta as
// reject_<code> (reject_under_limit, reject_1 …). Run Review › Reject Codes
// (production.reject_code) gives each code a label. This renames those
// columns to the label, using the same rules as the database:
//
//   key parsing  = production.job_reject_keys
//   code winner  = production.resolve_reject_code
//                  (machine > line > category > tenant; inactive codes still
//                   resolve so old jobs keep their names, active preferred)
//
// Keys with no registered code keep their original name (they are what the
// "Unmapped" tab shows), so no data is ever dropped.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Every exported table carries the machine in this column; no config needed.
const rejectMachineColumn = "machine_id"

// hasRejectKeys reports whether any row has a reject_<code> key, at the top
// level or inside a JSONB column (e.g. job_summary.meta) that will be
// flattened. Lets tables without reject data skip the lookup queries.
func hasRejectKeys(data []map[string]any) bool {
	for _, row := range data {
		for k, v := range row {
			if isRejectKey(k) {
				return true
			}
			if m, ok := v.(map[string]any); ok {
				for k2 := range m {
					if isRejectKey(k2) {
						return true
					}
				}
			}
		}
	}
	return false
}

var (
	rejectKeyRe      = regexp.MustCompile(`(?i)^rej(?:e)?ct_(.+)$`)
	rejectCodeCleanR = regexp.MustCompile(`[^A-Z0-9_]`)

	// ch1_remark, ch2_remark … hold a reject code as their VALUE; the
	// value is translated, the column name stays.
	rejectRemarkRe = regexp.MustCompile(`(?i)^ch\d+_remark$`)
)

func isRejectKey(k string) bool {
	if _, ok := rejectCodeFromKey(k); ok {
		return true
	}
	return rejectRemarkRe.MatchString(k)
}

// rejectCodeFromValue normalises a remark value the same way codes are
// normalised from keys: 1 / 1.0 / "1" → "1", "under-limit" → "UNDER_LIMIT".
func rejectCodeFromValue(v any) (string, bool) {
	var s string
	switch x := v.(type) {
	case string:
		s = strings.TrimSpace(x)
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		s = strconv.FormatFloat(float64(x), 'f', -1, 32)
	case int:
		s = strconv.Itoa(x)
	case int64:
		s = strconv.FormatInt(x, 10)
	case int32:
		s = strconv.FormatInt(int64(x), 10)
	case json.Number:
		s = x.String()
	default:
		return "", false
	}
	if s == "" {
		return "", false
	}
	return rejectCodeCleanR.ReplaceAllString(strings.ToUpper(s), "_"), true
}

// rejectCodeFromKey mirrors production.job_reject_keys.
func rejectCodeFromKey(key string) (string, bool) {
	switch strings.ToLower(key) {
	case "reject_output", "reject_count":
		return "", false
	}
	m := rejectKeyRe.FindStringSubmatch(key)
	if m == nil {
		return "", false
	}
	return rejectCodeCleanR.ReplaceAllString(strings.ToUpper(m[1]), "_"), true
}

// job_reject_keys only counts JSON numbers.
func isRejectQty(v any) bool {
	switch v.(type) {
	case float64, float32, int, int64, int32, json.Number:
		return true
	}
	return false
}

type rejectCodeRow struct {
	code      string // upper-cased, as saved by the dashboard
	label     string
	active    bool
	machineID string
	line      string
	category  string
}

type machineScope struct {
	tenantID string
	line     string
	category string
}

func (r rejectCodeRow) rank() int {
	switch {
	case r.machineID != "":
		return 1
	case r.line != "":
		return 2
	case r.category != "":
		return 3
	}
	return 4
}

func (r rejectCodeRow) appliesTo(machineID string, m machineScope) bool {
	switch {
	case r.machineID != "":
		return r.machineID == machineID
	case r.line != "":
		return r.line == m.line
	case r.category != "":
		return m.category != "" && strings.EqualFold(r.category, m.category)
	}
	return true
}

// better reports whether c beats cur: lower rank first, then active first.
func (c rejectCodeRow) better(cur rejectCodeRow) bool {
	if c.rank() != cur.rank() {
		return c.rank() < cur.rank()
	}
	return c.active && !cur.active
}

func rejectStr(v any) string {
	if v == nil {
		return ""
	}
	s := strings.TrimSpace(fmt.Sprintf("%v", v))
	if s == "<nil>" {
		return ""
	}
	return s
}

// prefetchRejectLabels loads code → name for every machine in data that
// isn't cached yet. On any error it logs and leaves those machines uncached,
// so their columns simply keep the raw reject_<code> names.
func (t *DataTransformer) prefetchRejectLabels(ctx context.Context, machineCol string, data []map[string]any) {
	seen := make(map[string]bool)
	var missing []any

	t.rejectMu.RLock()
	for _, row := range data {
		id := rejectStr(row[machineCol])
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if _, ok := t.rejectLabels[id]; !ok {
			missing = append(missing, id)
		}
	}
	t.rejectMu.RUnlock()

	if len(missing) == 0 {
		return
	}

	// 1. Machine scope (tenant, line, category)
	machines, err := t.client.From("machine_list").
		WithContext(ctx).
		Schema("production").
		Select("id,tenant_id,line,category").
		In("id", missing).
		Execute()
	if err != nil {
		t.logger.Warnw("Reject labels: failed to load machines, keeping raw keys", "error", err)
		return
	}

	scopes := make(map[string]machineScope, len(machines))
	tenantSeen := make(map[string]bool)
	var tenants []any
	for _, m := range machines {
		id := rejectStr(m["id"])
		if id == "" {
			continue
		}
		s := machineScope{
			tenantID: rejectStr(m["tenant_id"]),
			line:     rejectStr(m["line"]),
			category: rejectStr(m["category"]),
		}
		scopes[id] = s
		if s.tenantID != "" && !tenantSeen[s.tenantID] {
			tenantSeen[s.tenantID] = true
			tenants = append(tenants, s.tenantID)
		}
	}

	// 2. All codes for those tenants, inactive included (service key
	//    bypasses RLS, so the tenant filter is what keeps tenants apart).
	byTenant := make(map[string][]rejectCodeRow)
	if len(tenants) > 0 {
		codes, err := t.client.From("reject_code").
			WithContext(ctx).
			Schema("production").
			Select("tenant_id,code,label,active,machine_id,line,category").
			In("tenant_id", tenants).
			Execute()
		if err != nil {
			t.logger.Warnw("Reject labels: failed to load reject codes, keeping raw keys", "error", err)
			return
		}
		for _, c := range codes {
			active, _ := c["active"].(bool)
			row := rejectCodeRow{
				code:      strings.ToUpper(rejectStr(c["code"])),
				label:     rejectStr(c["label"]),
				active:    active,
				machineID: rejectStr(c["machine_id"]),
				line:      rejectStr(c["line"]),
				category:  rejectStr(c["category"]),
			}
			if row.code == "" || row.label == "" {
				continue
			}
			tid := rejectStr(c["tenant_id"])
			byTenant[tid] = append(byTenant[tid], row)
		}
	}

	// 3. Resolve per machine and cache (unknown machines cache as empty).
	t.rejectMu.Lock()
	for _, v := range missing {
		id := v.(string)
		s, ok := scopes[id]
		if !ok {
			t.rejectLabels[id] = map[string]string{}
			continue
		}
		t.rejectLabels[id] = resolveRejectLabels(id, s, byTenant[s.tenantID])
	}
	t.rejectMu.Unlock()

	t.logger.Infow("Reject labels loaded", "machines", len(missing), "tenants", len(tenants))
}

// resolveRejectLabels picks the winning code per key for one machine and
// returns code → column name. If two codes on the same machine share a
// label, both get "label (CODE)" so neither column overwrites the other.
func resolveRejectLabels(machineID string, m machineScope, codes []rejectCodeRow) map[string]string {
	best := make(map[string]rejectCodeRow)
	for _, c := range codes {
		if !c.appliesTo(machineID, m) {
			continue
		}
		if cur, ok := best[c.code]; !ok || c.better(cur) {
			best[c.code] = c
		}
	}

	labelCount := make(map[string]int)
	for _, c := range best {
		labelCount[c.label]++
	}

	out := make(map[string]string, len(best))
	for code, c := range best {
		name := c.label
		if labelCount[name] > 1 {
			name = fmt.Sprintf("%s (%s)", c.label, code)
		}
		out[code] = name
	}
	return out
}

// applyRejectLabels translates one transformed row:
//
//	reject_<code>: N   → column renamed to the code's name
//	chN_remark: <code> → value replaced by the code's name
func (t *DataTransformer) applyRejectLabels(result map[string]any, machineID any) {
	id := rejectStr(machineID)
	if id == "" {
		return
	}

	t.rejectMu.RLock()
	labels := t.rejectLabels[id]
	t.rejectMu.RUnlock()
	if len(labels) == 0 {
		return
	}

	// chN_remark: translate the value, keep the column. Unregistered values
	// (e.g. 0 / "OK" when nothing was rejected) are left untouched.
	for key, val := range result {
		if !rejectRemarkRe.MatchString(key) {
			continue
		}
		if code, ok := rejectCodeFromValue(val); ok {
			if name, ok := labels[code]; ok {
				result[key] = name
			}
		}
	}

	type rename struct{ from, to, code string }
	var renames []rename
	for key, val := range result {
		if !isRejectQty(val) {
			continue
		}
		code, ok := rejectCodeFromKey(key)
		if !ok {
			continue
		}
		if name, ok := labels[code]; ok {
			renames = append(renames, rename{from: key, to: name, code: code})
		}
	}

	for _, r := range renames {
		to := r.to
		// Label clashes with a normal column (e.g. a label called "status")
		if _, taken := result[to]; taken && to != r.from {
			to = fmt.Sprintf("%s (%s)", r.to, r.code)
		}
		if to == r.from {
			continue
		}
		result[to] = result[r.from]
		delete(result, r.from)
	}
}
