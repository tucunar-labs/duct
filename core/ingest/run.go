package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

// Result is the outcome of one ingestion.
type Result struct {
	Name      string
	Table     string // schema.table
	Rows      int64  // rows written by this run
	Watermark string // watermark stored after the run ("" in full mode)
}

const stageTable = "duct_stage"

// Run executes one ingestion: it instantiates the source module, loads its
// rows into the target table in a single transaction (create on first run,
// replace in full mode, append in incremental mode, new columns added with
// ALTER TABLE) and, only if that succeeded, stores the new watermark.
// root is the project directory; relative paths in options resolve against it.
func Run(ctx context.Context, eng contract.Engine, root string, c Config) (Result, error) {
	res := Result{Name: c.Name, Table: c.Target}
	if err := c.Validate(); err != nil {
		return res, err
	}
	tr, ok := eng.(contract.Transactor)
	if !ok {
		return res, errors.New("engine does not implement contract.Transactor")
	}
	info, ok := registry.Lookup(contract.KindSource, c.Source)
	if !ok {
		return res, fmt.Errorf("source module %q is not linked into this binary", c.Source)
	}
	mod := info.New()
	cfg := contract.Config{"name": c.Name, "mode": c.Mode, "watermark": c.Watermark, "root": root, "options": optionsOrEmpty(c.Options)}
	if err := mod.Init(ctx, cfg); err != nil {
		_ = mod.Close()
		return res, fmt.Errorf("source %s: %w", c.Source, err)
	}
	defer func() { _ = mod.Close() }()

	st := contract.IngestState{}
	if c.Mode == ModeIncremental {
		w, err := loadWatermark(ctx, eng, c.Name)
		if err != nil {
			return res, err
		}
		st.Watermark = w
	}
	if err := ensureSchema(ctx, eng, BronzeSchema); err != nil {
		return res, err
	}

	var (
		input   contract.Stream
		stageFn string // SELECT that fills the stage table
	)
	switch src := mod.(type) {
	case contract.SQLSource:
		plan, err := src.Plan(ctx, st)
		if err != nil {
			return res, fmt.Errorf("source %s: %w", c.Source, err)
		}
		for _, s := range plan.Setup {
			if err := eng.Exec(ctx, s); err != nil {
				return res, fmt.Errorf("source %s setup: %w", c.Source, err)
			}
		}
		stageFn = plan.Select
	case contract.Source:
		var err error
		if input, err = collect(ctx, src); err != nil {
			return res, fmt.Errorf("source %s: %w", c.Source, err)
		}
		if input == nil { // no rows, no schema: nothing to load
			res.Watermark = st.Watermark
			return res, saveState(ctx, eng, c, st.Watermark, 0)
		}
		defer input.Release()
		stageFn = "SELECT * FROM " + contract.InputView
	default:
		return res, fmt.Errorf("source module %q implements neither SQLSource nor Source", c.Source)
	}

	wm := st.Watermark
	err := tr.InTx(ctx, input, func(ctx context.Context, tx contract.Tx) error {
		n, newWM, err := load(ctx, tx, c, stageFn)
		if err != nil {
			return err
		}
		res.Rows = n
		if newWM != "" {
			wm = newWM
		}
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("load %s: %w", c.Target, err)
	}
	if c.Mode == ModeFull {
		wm = ""
	}
	res.Watermark = wm
	if err := saveState(ctx, eng, c, wm, res.Rows); err != nil {
		return res, fmt.Errorf("data loaded but state not saved (next run may repeat rows): %w", err)
	}
	return res, nil
}

func optionsOrEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// collect runs a batch Source to completion and returns its batches as one
// Stream (nil if it produced no rows). The caller releases the Stream.
func collect(ctx context.Context, src contract.Source) (contract.Stream, error) {
	var recs []arrow.RecordBatch
	release := func() {
		for _, r := range recs {
			r.Release()
		}
	}
	err := src.Read(ctx, func(b contract.Batch) error {
		if len(recs) > 0 && !recs[0].Schema().Equal(b.Schema()) {
			return errors.New("source emitted batches with different schemas")
		}
		b.Retain()
		recs = append(recs, b)
		return nil
	})
	if err != nil {
		release()
		return nil, err
	}
	if len(recs) == 0 {
		return nil, nil
	}
	rr, err := array.NewRecordReader(recs[0].Schema(), recs)
	release() // the reader holds its own references
	return rr, err
}

// load runs inside the transaction. It stages the source rows in a temporary
// table (so a remote query runs once and the watermark is known), evolves the
// target schema and writes.
func load(ctx context.Context, tx contract.Tx, c Config, selectSQL string) (rows int64, newWM string, err error) {
	target := c.Target
	if _, err = count(ctx, tx, "CREATE OR REPLACE TEMP TABLE "+stageTable+" AS "+selectSQL); err != nil {
		return 0, "", err
	}
	defer func() { _ = tx.Exec(ctx, "DROP TABLE IF EXISTS "+stageTable) }()

	if c.Watermark != "" {
		cols, err := describe(ctx, tx, stageTable)
		if err != nil {
			return 0, "", err
		}
		if _, ok := cols[strings.ToLower(c.Watermark)]; !ok {
			return 0, "", fmt.Errorf("watermark column %q is not in the source result (columns: %v)", c.Watermark, cols)
		}
		v, err := scalar(ctx, tx, fmt.Sprintf(`SELECT CAST(max("%s") AS VARCHAR) FROM %s`, c.Watermark, stageTable))
		if err != nil {
			return 0, "", err
		}
		newWM = v
	}

	schema, table := c.Table()
	exists, err := scalar(ctx, tx, fmt.Sprintf("SELECT count(*) FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = '%s' AND table_name = '%s'", schema, table))
	if err != nil {
		return 0, "", err
	}
	if exists == "0" {
		n, err := count(ctx, tx, "CREATE TABLE "+target+" AS SELECT * FROM "+stageTable)
		return n, newWM, err
	}
	if err := evolve(ctx, tx, target, stageTable); err != nil {
		return 0, "", err
	}
	if c.Mode == ModeFull {
		if _, err := count(ctx, tx, "DELETE FROM "+target); err != nil {
			return 0, "", err
		}
	}
	n, err := count(ctx, tx, "INSERT INTO "+target+" BY NAME SELECT * FROM "+stageTable)
	return n, newWM, err
}

// evolve adds to target the columns of stage that it lacks.
func evolve(ctx context.Context, tx contract.Tx, target, stage string) error {
	have, err := describe(ctx, tx, target)
	if err != nil {
		return err
	}
	want, err := describeOrdered(ctx, tx, stage)
	if err != nil {
		return err
	}
	for _, col := range want {
		if _, ok := have[strings.ToLower(col[0])]; ok {
			continue
		}
		stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN "%s" %s`, target, strings.ReplaceAll(col[0], `"`, `""`), col[1])
		if err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("add column %s: %w", col[0], err)
		}
	}
	return nil
}

func describe(ctx context.Context, tx contract.Tx, table string) (map[string]string, error) {
	list, err := describeOrdered(ctx, tx, table)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(list))
	for _, c := range list {
		m[strings.ToLower(c[0])] = c[1]
	}
	return m, nil
}

// describeOrdered returns [name, type] pairs in column order.
func describeOrdered(ctx context.Context, tx contract.Tx, table string) ([][2]string, error) {
	st, err := tx.Query(ctx, "SELECT column_name, column_type FROM (DESCRIBE "+table+")")
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out [][2]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			out = append(out, [2]string{strings.Clone(rec.Column(0).ValueStr(i)), strings.Clone(rec.Column(1).ValueStr(i))})
		}
	}
	return out, st.Err()
}

// scalar returns the first cell of the first row as text ("" for NULL or no rows).
func scalar(ctx context.Context, tx contract.Tx, query string) (string, error) {
	st, err := tx.Query(ctx, query)
	if err != nil {
		return "", err
	}
	defer st.Release()
	var out string
	got := false
	for st.Next() {
		rec := st.RecordBatch()
		if !got && rec.NumRows() > 0 && rec.Column(0).IsValid(0) {
			out, got = strings.Clone(rec.Column(0).ValueStr(0)), true
		}
	}
	return out, st.Err()
}

// count runs a statement that reports an affected-row Count (INSERT, CTAS, DELETE).
func count(ctx context.Context, tx contract.Tx, stmt string) (int64, error) {
	v, err := scalar(ctx, tx, stmt)
	if err != nil {
		return 0, err
	}
	if v == "" {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}

func loadWatermark(ctx context.Context, eng contract.Engine, name string) (string, error) {
	st, err := eng.Query(ctx, "SELECT watermark FROM duct.duct_ingest_state WHERE name = ?", name)
	if err != nil {
		return "", fmt.Errorf("read ingest state (run `duct migrate`): %w", err)
	}
	defer st.Release()
	var out string
	for st.Next() {
		rec := st.RecordBatch()
		if rec.NumRows() > 0 && rec.Column(0).IsValid(0) && out == "" {
			out = strings.Clone(rec.Column(0).ValueStr(0))
		}
	}
	return out, st.Err()
}

func saveState(ctx context.Context, eng contract.Engine, c Config, wm string, rows int64) error {
	q := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	wmSQL := "NULL"
	if wm != "" {
		wmSQL = q(wm)
	}
	return eng.ExecTx(ctx,
		"DELETE FROM duct.duct_ingest_state WHERE name = "+q(c.Name),
		fmt.Sprintf("INSERT INTO duct.duct_ingest_state (name, watermark, rows_loaded, updated_at) VALUES (%s, %s, %d, now())", q(c.Name), wmSQL, rows))
}

// ensureSchema is CREATE SCHEMA IF NOT EXISTS, retried on a commit conflict:
// with a catalog shared by several processes (Postgres) two first runs can
// create the schema at once; the loser fails with "Transaction conflict" and
// its retry then finds the schema.
func ensureSchema(ctx context.Context, eng contract.Engine, name string) error {
	var err error
	for i := range 5 {
		if err = eng.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+name); err == nil || !strings.Contains(err.Error(), "Transaction conflict") {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Duration(i+1) * 50 * time.Millisecond):
		}
	}
	return err
}
