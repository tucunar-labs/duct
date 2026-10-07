package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/txretry"
)

// RunsTable is the Duct-owned table where every refresh is recorded.
const RunsTable = "duct.duct_pipeline_runs"

// Statuses of a refresh.
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
	StatusSkipped = "skipped" // an upstream table failed or was skipped
)

// Violation is how many rows broke an expectation in one refresh.
type Violation struct {
	Name   string `json:"name"`
	Action Action `json:"action"`
	Count  int64  `json:"count"`
}

// Result is the outcome of refreshing one table.
type Result struct {
	ID         string
	Status     string
	Rows       int64 // rows in the table after the refresh (0 if not refreshed)
	Dropped    int64 // rows removed by DROP ROW expectations
	Violations []Violation
	Err        error
	Duration   time.Duration
}

// Report is the outcome of a run, in execution order.
type Report struct {
	RunID   int64
	Results []Result
}

// Failed reports whether any table failed or was skipped.
func (r *Report) Failed() bool {
	for _, x := range r.Results {
		if x.Status != StatusSuccess {
			return true
		}
	}
	return false
}

// Options select what to run.
type Options struct {
	Targets []string // schema.name or bare name (schema main); empty = all
	Only    bool     // do not run the upstream of the targets
	Gate    Gate     // optional: data contracts (see gate.go)
}

// NormalizeTarget turns "Silver.Orders" or "orders" into a Table.ID form.
func NormalizeTarget(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if !strings.Contains(s, ".") {
		return "main." + s
	}
	return s
}

// BuildGraph resolves the references of every table with DuckDB's own parser
// (json_serialize_sql) and builds the dependency graph.
func BuildGraph(ctx context.Context, eng contract.Engine, tables []Table) (*Graph, error) {
	nodes := map[string][]string{}
	for _, t := range tables {
		ast, err := queryOne(ctx, eng, "SELECT CAST(json_serialize_sql("+sqlString(t.Query)+") AS VARCHAR)")
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %s: %w", t.File, t.Line, t.ID(), err)
		}
		refs, err := RefsFromAST(ast)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %s: %w", t.File, t.Line, t.ID(), err)
		}
		ids := make([]string, 0, len(refs))
		for _, r := range refs {
			ids = append(ids, r.ID())
		}
		nodes[t.ID()] = ids
	}
	return NewGraph(nodes)
}

// Run refreshes the selected tables in dependency order (full refresh, ADR
// 0004 v1). A table that fails (expectation FAIL UPDATE or a SQL error) keeps
// its previous contents and every table downstream of it is skipped;
// independent branches still run. The returned error is non-nil only for
// problems that stop the whole run (bad graph, cancelled context); per-table
// failures are in the Report. Streaming tables are refreshed like views.
func Run(ctx context.Context, eng contract.Engine, tables []Table, opt Options) (*Report, error) {
	g, err := BuildGraph(ctx, eng, tables)
	if err != nil {
		return nil, err
	}
	targets := make([]string, 0, len(opt.Targets))
	for _, t := range opt.Targets {
		targets = append(targets, NormalizeTarget(t))
	}
	plan, err := g.Plan(targets, !opt.Only)
	if err != nil {
		return nil, err
	}
	byID := map[string]Table{}
	for _, t := range tables {
		byID[t.ID()] = t
	}
	rep := &Report{RunID: newRunID()}
	bad := map[string]bool{}
	for _, id := range plan {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		t := byID[id]
		start := time.Now()
		res := Result{ID: id}
		var failedUp string
		for _, d := range g.Upstream(id) {
			if bad[d] {
				failedUp = d
				break
			}
		}
		if failedUp != "" {
			res.Status = StatusSkipped
			res.Err = fmt.Errorf("upstream %s did not refresh", failedUp)
		} else {
			res = refresh(ctx, eng, t, opt.Gate)
		}
		res.Duration = time.Since(start)
		if res.Status != StatusSuccess {
			bad[id] = true
		}
		rep.Results = append(rep.Results, res)
		if err := record(ctx, eng, rep.RunID, start, res); err != nil {
			return rep, fmt.Errorf("record run of %s: %w", id, err)
		}
	}
	return rep, nil
}

// newRunID is unique across processes sharing one state: microseconds times
// 1000 plus a random 0..999 (still fits int64 until the year 2255). Two
// processes starting in the same microsecond do happen when launched together.
func newRunID() int64 { return time.Now().UnixMicro()*1000 + rand.Int64N(1000) }

// sqlString is a SQL string literal (json_serialize_sql needs a constant, not a parameter).
func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// StageCatalog is the in-memory DuckDB database where a refresh materializes
// its SELECT once. It is shared by the engine's connections (a TEMP table would
// be private to one connection, and the engine has no sessions), never touches
// the lake, and disappears with the process. Memory: the whole result is held
// here, with DuckDB spilling to its temp_directory when memory_limit is hit.
const StageCatalog = "duct_stage"

const expCol = "__duct_exp_"

// refresh computes one table evaluating its SELECT exactly once: the result is
// staged in memory with one boolean column per expectation (computed in the
// same pass; NULL counts as violation), counts come from the staging table,
// and only if no FAIL UPDATE is violated does a single lake transaction replace
// the target from the staging table. A failure never touches the old version.
func refresh(ctx context.Context, eng contract.Engine, t Table, gate Gate) (res Result) {
	res = Result{ID: t.ID(), Status: StatusFailed}
	governed := gate != nil && gate.Governs(t.ID())
	if governed {
		t.Expectations = append(slices.Clone(t.Expectations), gate.Expectations(t.ID())...)
	}
	if err := eng.Exec(ctx, "ATTACH IF NOT EXISTS ':memory:' AS "+StageCatalog); err != nil {
		res.Err = fmt.Errorf("stage: %w", err)
		return res
	}
	stage := StageCatalog + ".main." + quoteIdent(t.ID())
	defer func() {
		// Own context: the stage must go even if ctx was cancelled.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := eng.Exec(dctx, "DROP TABLE IF EXISTS "+stage); err != nil && res.Err == nil {
			res.Status, res.Err = StatusFailed, fmt.Errorf("drop stage: %w", err)
		}
	}()

	var flags []string
	var drops []string
	cols := []string{"count(*)"}
	for i, e := range t.Expectations {
		c := quoteIdent(expCol + strconv.Itoa(i))
		flags = append(flags, fmt.Sprintf("coalesce((%s), false) AS %s", e.Expr, c))
		cols = append(cols, "count(*) FILTER (WHERE NOT "+c+")")
		if e.Action == DropRow {
			drops = append(drops, c)
		}
	}
	keep := "true"
	if len(drops) > 0 {
		keep = strings.Join(drops, " AND ")
		cols = append(cols, "count(*) FILTER (WHERE NOT ("+keep+"))")
	}
	sel := "_duct_src.*"
	if len(flags) > 0 {
		sel += ", " + strings.Join(flags, ", ")
	}
	if err := eng.Exec(ctx, "CREATE OR REPLACE TABLE "+stage+" AS SELECT "+sel+" FROM (\n"+t.Query+"\n) AS _duct_src"); err != nil {
		res.Err = fmt.Errorf("evaluate: %w", err)
		return res
	}
	row, err := queryRow(ctx, eng, "SELECT "+strings.Join(cols, ", ")+" FROM "+stage)
	if err != nil {
		res.Err = fmt.Errorf("count: %w", err)
		return res
	}
	vals := make([]int64, len(row))
	for i, s := range row {
		if vals[i], err = strconv.ParseInt(s, 10, 64); err != nil {
			res.Err = fmt.Errorf("unexpected count %q", s)
			return res
		}
	}
	total := vals[0]
	var failing []string
	for i, e := range t.Expectations {
		v := Violation{Name: e.Name, Action: e.Action, Count: vals[1+i]}
		res.Violations = append(res.Violations, v)
		if e.Action == FailUpdate && v.Count > 0 {
			failing = append(failing, fmt.Sprintf("%s (%d rows)", e.Name, v.Count))
		}
	}
	if len(drops) > 0 {
		res.Dropped = vals[len(vals)-1]
	}
	var exclude []string
	for i := range t.Expectations {
		exclude = append(exclude, quoteIdent(expCol+strconv.Itoa(i)))
	}
	var failures []string
	if len(failing) > 0 {
		failures = append(failures, "expectation FAIL UPDATE violated: "+strings.Join(failing, ", "))
	}
	if governed {
		viol, gerr := runGate(ctx, eng, gate, t.ID(), stage, exclude, keep)
		res.Violations = append(res.Violations, viol...)
		if gerr != nil {
			failures = append(failures, gerr.Error())
		}
	}
	if len(failures) > 0 {
		res.Err = fmt.Errorf("%s; previous version kept", strings.Join(failures, "; "))
		return res
	}
	proj := "*"
	if len(exclude) > 0 {
		proj = "* EXCLUDE (" + strings.Join(exclude, ", ") + ")"
	}
	target := quoteIdent(t.Schema) + "." + quoteIdent(t.Name)
	// Schema and table in one transaction, retried on a commit conflict with
	// another process writing the same catalog (see doc.go: the staged rows are
	// not recomputed, so the last writer wins with data from its own read).
	err = txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return eng.ExecTx(ctx,
			"CREATE SCHEMA IF NOT EXISTS "+quoteIdent(t.Schema),
			"CREATE OR REPLACE TABLE "+target+" AS SELECT "+proj+" FROM "+stage+" WHERE "+keep)
	})
	if err != nil {
		res.Err = fmt.Errorf("write %s: %w", t.ID(), err)
		return res
	}
	res.Status = StatusSuccess
	res.Rows = total - res.Dropped
	return res
}

// runGate exposes the staged rows that would be written as a view and lets the
// gate (data contract) judge them. The view lives in the in-memory stage catalog.
func runGate(ctx context.Context, eng contract.Engine, gate Gate, id, stage string, exclude []string, keep string) ([]Violation, error) {
	view := StageCatalog + ".main." + quoteIdent(id+"__gate")
	proj := "*"
	if len(exclude) > 0 {
		proj = "* EXCLUDE (" + strings.Join(exclude, ", ") + ")"
	}
	if err := eng.Exec(ctx, "CREATE OR REPLACE VIEW "+view+" AS SELECT "+proj+" FROM "+stage+" WHERE "+keep); err != nil {
		return nil, fmt.Errorf("contract view: %w", err)
	}
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = eng.Exec(dctx, "DROP VIEW IF EXISTS "+view)
	}()
	return gate.Check(ctx, eng, id, view)
}

func record(ctx context.Context, eng contract.Engine, runID int64, start time.Time, r Result) error {
	viol, err := json.Marshal(r.Violations)
	if err != nil {
		return err
	}
	var msg any
	if r.Err != nil {
		msg = r.Err.Error()
	}
	return eng.Exec(ctx, "INSERT INTO "+RunsTable+
		" (run_id, target, status, rows_written, rows_dropped, expectations, error, started_at, finished_at, duration_ms)"+
		" VALUES (?, ?, ?, ?, ?, ?, ?, make_timestamp(?), make_timestamp(?), ?)",
		runID, r.ID, r.Status, r.Rows, r.Dropped, string(viol), msg,
		start.UnixMicro(), start.Add(r.Duration).UnixMicro(), r.Duration.Milliseconds())
}

func queryRow(ctx context.Context, eng contract.Engine, sql string, args ...any) ([]string, error) {
	st, err := eng.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var row []string
	for st.Next() {
		rec := st.RecordBatch()
		if row == nil && rec.NumRows() > 0 {
			for c := range int(rec.NumCols()) {
				// ValueStr may alias Arrow memory freed on Release.
				row = append(row, strings.Clone(rec.Column(c).ValueStr(0)))
			}
		}
	}
	if err := st.Err(); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.New("query returned no rows")
	}
	return row, nil
}

func queryOne(ctx context.Context, eng contract.Engine, sql string, args ...any) (string, error) {
	row, err := queryRow(ctx, eng, sql, args...)
	if err != nil {
		return "", err
	}
	return row[0], nil
}
