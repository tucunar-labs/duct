package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/semantic"
)

const (
	extraRole  = "role"
	maxSQLRows = 1000
)

const instructions = `Duct semantic layer over the governed lake. Work in this order:
1. list_metrics: the metrics you may compute and the dimensions each can be grouped by.
2. describe: datasets, fields, relationships and ai_context (synonyms, instructions) when a name is unclear.
3. query_metrics(dims, measures, filters, limit, order): you never write joins; Duct builds the SQL
   and applies the caller's permissions (row filters and masks included).
Filters are structured: {"field": "region", "op": "=", "value": "CL"}; ops: = != < <= > >= in not_in like not_like between is_null is_not_null.`

// tools holds what the tool handlers share.
type tools struct {
	s *Server
	h contract.QueryHandler
}

func newTools(s *Server, h contract.QueryHandler) *sdk.Server {
	t := &tools{s: s, h: h}
	srv := sdk.NewServer(&sdk.Implementation{Name: "duct", Version: "0.1"}, &sdk.ServerOptions{Instructions: instructions})
	if s.model() != nil {
		sdk.AddTool(srv, &sdk.Tool{
			Name:        "list_metrics",
			Description: "List the metrics the caller can compute, with description, ai_context and the dimensions (dataset.field) each can be grouped or filtered by.",
			Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true},
		}, t.listMetrics)
		sdk.AddTool(srv, &sdk.Tool{
			Name:        "describe",
			Description: "Describe the semantic model as the caller sees it: datasets and fields, relationships, metrics and their ai_context. Optional name = one dataset or metric.",
			Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true},
		}, t.describe)
		sdk.AddTool(srv, &sdk.Tool{
			Name:        "query_metrics",
			Description: "Compute metrics grouped by dimensions, with structured filters. Duct generates the joins; rows are limited by the caller's role (row filters, masks).",
			Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true},
		}, t.queryMetrics)
	}
	if s.sql {
		sdk.AddTool(srv, &sdk.Tool{
			Name:        "sql",
			Description: "Run one read-only SELECT over the caller's views (role schema). Prefer query_metrics; use this for exploration only.",
			Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true},
		}, t.sql)
	}
	return srv
}

// identity extracts the authenticated principal of the HTTP request that
// carried the call.
func identity(req *sdk.CallToolRequest) (contract.Identity, error) {
	ex := req.GetExtra()
	if ex == nil || ex.TokenInfo == nil {
		return contract.Identity{}, errors.New("unauthenticated")
	}
	role, _ := ex.TokenInfo.Extra[extraRole].(string)
	if ex.TokenInfo.UserID == "" || role == "" {
		return contract.Identity{}, errors.New("unauthenticated")
	}
	return contract.Identity{User: ex.TokenInfo.UserID, Role: role}, nil
}

// audit records a call that does not reach the QueryHandler (or failed before).
func (t *tools) audit(ctx context.Context, id contract.Identity, desc string, start time.Time, rows int64, err error) {
	a, ok := t.h.(contract.Auditor)
	if !ok {
		return
	}
	rec := contract.AuditRecord{At: start.UTC(), User: id.User, Role: id.Role, Protocol: name, SQL: desc, Duration: time.Since(start), Rows: rows}
	if err != nil {
		rec.Err = err.Error()
	}
	a.Audit(ctx, rec)
}

// scope tells which views the role owns, when the handler can say (the
// governed handler can; a bare QueryHandler cannot and the guard decides).
func (t *tools) scope(ctx context.Context, id contract.Identity) (semantic.Scope, error) {
	sc := semantic.Scope{Role: id.Role}
	cat, ok := t.h.(interface {
		Catalog(ctx context.Context, id contract.Identity) (contract.Stream, error)
	})
	if !ok {
		return sc, nil
	}
	st, err := cat.Catalog(ctx, id)
	if err != nil {
		return sc, err
	}
	defer st.Release()
	sc.Tables = map[string]bool{}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			sc.Tables[strings.ToLower(strings.Clone(rec.Column(1).ValueStr(i)))] = true
		}
	}
	return sc, st.Err()
}

type empty struct{}

type listOut struct {
	Metrics []semantic.MetricInfo `json:"metrics"`
}

func (t *tools) listMetrics(ctx context.Context, req *sdk.CallToolRequest, _ empty) (*sdk.CallToolResult, listOut, error) {
	start := time.Now()
	id, err := identity(req)
	if err != nil {
		return nil, listOut{}, err
	}
	sc, err := t.scope(ctx, id)
	if err != nil {
		t.audit(ctx, id, "mcp list_metrics", start, 0, err)
		return nil, listOut{}, err
	}
	m, err := t.model()
	if err != nil {
		t.audit(ctx, id, "mcp list_metrics", start, 0, err)
		return nil, listOut{}, err
	}
	out := listOut{Metrics: m.ListMetrics(sc)}
	t.audit(ctx, id, "mcp list_metrics", start, int64(len(out.Metrics)), nil)
	return nil, out, nil
}

type describeIn struct {
	Name string `json:"name,omitempty" jsonschema:"optional: a dataset or metric name to describe on its own"`
}

func (t *tools) describe(ctx context.Context, req *sdk.CallToolRequest, in describeIn) (*sdk.CallToolResult, semantic.ModelInfo, error) {
	start := time.Now()
	desc := "mcp describe"
	if in.Name != "" {
		desc += " " + in.Name
	}
	id, err := identity(req)
	if err != nil {
		return nil, semantic.ModelInfo{}, err
	}
	sc, err := t.scope(ctx, id)
	if err != nil {
		t.audit(ctx, id, desc, start, 0, err)
		return nil, semantic.ModelInfo{}, err
	}
	m, err := t.model()
	if err != nil {
		t.audit(ctx, id, desc, start, 0, err)
		return nil, semantic.ModelInfo{}, err
	}
	info := m.Describe(sc)
	if in.Name != "" {
		if info, err = narrow(info, in.Name); err != nil {
			t.audit(ctx, id, desc, start, 0, err)
			return nil, semantic.ModelInfo{}, err
		}
	}
	t.audit(ctx, id, desc, start, int64(len(info.Datasets)+len(info.Metrics)), nil)
	return nil, info, nil
}

// narrow keeps the dataset or metric called name (and what it needs).
func narrow(info semantic.ModelInfo, name string) (semantic.ModelInfo, error) {
	out := semantic.ModelInfo{Models: info.Models, Datasets: []semantic.DatasetInfo{}, Relationships: []semantic.RelationshipInfo{}, Metrics: []semantic.MetricInfo{}}
	for _, d := range info.Datasets {
		if strings.EqualFold(d.Name, name) {
			out.Datasets = append(out.Datasets, d)
			for _, r := range info.Relationships {
				if strings.EqualFold(r.From, d.Name) || strings.EqualFold(r.To, d.Name) {
					out.Relationships = append(out.Relationships, r)
				}
			}
			return out, nil
		}
	}
	for _, m := range info.Metrics {
		if strings.EqualFold(m.Name, name) {
			out.Metrics = append(out.Metrics, m)
			for _, d := range info.Datasets {
				if strings.EqualFold(d.Name, m.Dataset) {
					out.Datasets = append(out.Datasets, d)
				}
			}
			return out, nil
		}
	}
	return out, fmt.Errorf("%q is not a dataset or metric available to you (see list_metrics)", name)
}

type filterIn struct {
	Field  string `json:"field,omitempty" jsonschema:"dimension as dataset.field, or the bare field name when unambiguous (filters rows, before aggregating)"`
	Metric string `json:"metric,omitempty" jsonschema:"instead of field: a metric name; keeps the groups whose metric satisfies the condition (HAVING; like/not_like do not apply)"`
	Op     string `json:"op" jsonschema:"one of = != < <= > >= in not_in like not_like between is_null is_not_null"`
	Value  any    `json:"value,omitempty" jsonschema:"string, number or boolean; a list for in/not_in; [low, high] for between; omit for is_null/is_not_null"`
}

type orderIn struct {
	By   string `json:"by" jsonschema:"a requested dimension or measure"`
	Desc bool   `json:"desc,omitempty"`
}

type queryIn struct {
	Dims     []string   `json:"dims,omitempty" jsonschema:"dimensions to group by (dataset.field or bare field name)"`
	Measures []string   `json:"measures" jsonschema:"metric names (see list_metrics); at least one"`
	Filters  []filterIn `json:"filters,omitempty" jsonschema:"conditions, all must hold"`
	Limit    int        `json:"limit,omitempty" jsonschema:"max rows (default and maximum come from the model)"`
	Order    []orderIn  `json:"order,omitempty" jsonschema:"sort; default: dimensions ascending"`
}

type columnOut struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Datatype string `json:"datatype,omitempty"`
}

type queryOut struct {
	Columns  []columnOut `json:"columns"`
	Rows     [][]any     `json:"rows"`
	RowCount int         `json:"row_count"`
	SQL      string      `json:"sql" jsonschema:"the SQL that ran, over the caller's role views"`
}

func (t *tools) queryMetrics(ctx context.Context, req *sdk.CallToolRequest, in queryIn) (*sdk.CallToolResult, queryOut, error) {
	start := time.Now()
	id, err := identity(req)
	if err != nil {
		return nil, queryOut{}, err
	}
	raw, _ := json.Marshal(in)
	desc := "mcp query_metrics " + string(raw)
	sr := semantic.Request{Dims: in.Dims, Measures: in.Measures, Limit: in.Limit}
	for _, f := range in.Filters {
		sr.Filters = append(sr.Filters, semantic.Filter{Field: f.Field, Metric: f.Metric, Op: f.Op, Value: f.Value})
	}
	for _, o := range in.Order {
		sr.Order = append(sr.Order, semantic.Order{By: o.By, Desc: o.Desc})
	}
	sc, err := t.scope(ctx, id)
	if err != nil {
		t.audit(ctx, id, desc, start, 0, err)
		return nil, queryOut{}, err
	}
	// The handler compiles and runs it when it can (preaggregates, audit);
	// otherwise compile here and run the SQL through the guard.
	if sq, ok := t.h.(contract.SemanticQuerier); ok {
		return t.viaQuerier(ctx, sq, id, in, desc, start)
	}
	m, err := t.model()
	if err != nil {
		t.audit(ctx, id, desc, start, 0, err)
		return nil, queryOut{}, err
	}
	c, err := m.Query(sc, sr)
	if err != nil {
		t.audit(ctx, id, desc, start, 0, err)
		return nil, queryOut{}, err
	}
	// From here the QueryHandler audits the statement itself (allowed or not).
	rows, _, err := runQuery(ctx, t.h, id, c.SQL, 0)
	if err != nil {
		return nil, queryOut{}, err
	}
	out := queryOut{Rows: rows, RowCount: len(rows), SQL: c.SQL}
	for _, col := range c.Columns {
		out.Columns = append(out.Columns, columnOut{Name: col.Name, Kind: col.Kind, Datatype: col.Datatype})
	}
	return nil, out, nil
}

// model is the current semantic model, or an error when it was removed.
func (t *tools) model() (*semantic.Model, error) {
	if m := t.s.model(); m != nil {
		return m, nil
	}
	return nil, errors.New("no semantic model: semantic/*.ossie.yaml was removed")
}

func (t *tools) viaQuerier(ctx context.Context, sq contract.SemanticQuerier, id contract.Identity, in queryIn, desc string, start time.Time) (*sdk.CallToolResult, queryOut, error) {
	req := contract.SemanticRequest{Dims: in.Dims, Measures: in.Measures, Limit: in.Limit}
	for _, f := range in.Filters {
		req.Filters = append(req.Filters, contract.SemanticFilter{Field: f.Field, Metric: f.Metric, Op: f.Op, Value: f.Value})
	}
	for _, o := range in.Order {
		req.Order = append(req.Order, contract.SemanticOrder{By: o.By, Desc: o.Desc})
	}
	res, err := sq.SemanticQuery(ctx, id, req)
	if err != nil {
		t.audit(ctx, id, desc, start, 0, err)
		return nil, queryOut{}, err
	}
	rows, _, err := collect(res.Stream, 0)
	if err != nil {
		return nil, queryOut{}, err
	}
	out := queryOut{Rows: rows, RowCount: len(rows), SQL: res.SQL}
	for _, col := range res.Columns {
		out.Columns = append(out.Columns, columnOut{Name: col.Name, Kind: col.Kind, Datatype: col.Datatype})
	}
	return nil, out, nil
}

type sqlIn struct {
	Query string `json:"query" jsonschema:"a single SELECT over your views"`
}

type sqlOut struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated,omitempty"`
}

func (t *tools) sql(ctx context.Context, req *sdk.CallToolRequest, in sqlIn) (*sdk.CallToolResult, sqlOut, error) {
	id, err := identity(req)
	if err != nil {
		return nil, sqlOut{}, err
	}
	rows, cols, err := runQuery(ctx, t.h, id, in.Query, maxSQLRows+1)
	if err != nil {
		return nil, sqlOut{}, err
	}
	out := sqlOut{Columns: cols}
	if len(rows) > maxSQLRows {
		rows, out.Truncated = rows[:maxSQLRows], true
	}
	out.Rows, out.RowCount = rows, len(rows)
	return nil, out, nil
}

// runQuery executes sql as id and collects up to max rows (0 = all).
func runQuery(ctx context.Context, h contract.QueryHandler, id contract.Identity, sql string, max int) ([][]any, []string, error) {
	st, err := h.Query(ctx, id, sql)
	if err != nil {
		return nil, nil, err
	}
	return collect(st, max)
}

// collect reads up to max rows (0 = all) of st and releases it.
func collect(st contract.Stream, max int) ([][]any, []string, error) {
	defer st.Release()
	var cols []string
	for _, f := range st.Schema().Fields() {
		cols = append(cols, f.Name)
	}
	rows := [][]any{}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]any, rec.NumCols())
			for j := range row {
				row[j] = cell(rec.Column(j), i)
			}
			rows = append(rows, row)
			if max > 0 && len(rows) >= max {
				return rows, cols, nil
			}
		}
	}
	if err := st.Err(); err != nil {
		return nil, nil, err
	}
	return rows, cols, nil
}

// cell returns a JSON-friendly copy of one value (strings are cloned: Arrow
// memory is gone once the stream is released).
func cell(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	if d, ok := col.(*array.Decimal128); ok {
		dt := d.DataType().(*arrow.Decimal128Type)
		v := d.Value(i)
		if dt.Scale == 0 {
			if bi := v.BigInt(); bi.IsInt64() {
				return bi.Int64()
			}
			return v.BigInt().String()
		}
		return v.ToFloat64(dt.Scale)
	}
	switch v := col.GetOneForMarshal(i).(type) {
	case string:
		return strings.Clone(v)
	case []byte:
		return append([]byte(nil), v...)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil
		}
		return v
	case float32:
		if f := float64(v); math.IsNaN(f) || math.IsInf(f, 0) {
			return nil
		}
		return v
	default:
		return v
	}
}
