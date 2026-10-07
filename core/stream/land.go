package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/ingest"
)

const stageTable = "duct_stage"

// Table names a destination. An empty Catalog is the lake (the engine's
// default catalog); `duct alerts` lands its window in the in-memory catalog.
type Table struct {
	Catalog string
	Schema  string
	Name    string
}

// LakeTable parses "schema.table" in the lake.
func LakeTable(target string) Table {
	s, t, _ := strings.Cut(target, ".")
	return Table{Schema: s, Name: t}
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// SQL is the qualified, quoted name.
func (t Table) SQL() string {
	parts := make([]string, 0, 3)
	if t.Catalog != "" {
		parts = append(parts, quoteIdent(t.Catalog))
	}
	parts = append(parts, quoteIdent(t.Schema), quoteIdent(t.Name))
	return strings.Join(parts, ".")
}

func (t Table) catalogName() string {
	if t.Catalog == "" {
		return "lake"
	}
	return t.Catalog
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Exists reports whether the table exists.
func (t Table) Exists(ctx context.Context, tx contract.Tx) (bool, error) {
	v, err := scalar(ctx, tx, fmt.Sprintf(
		"SELECT count(*) FROM information_schema.tables WHERE table_catalog = %s AND table_schema = %s AND table_name = %s",
		sqlString(t.catalogName()), sqlString(t.Schema), sqlString(t.Name)))
	return v != "" && v != "0", err
}

// Land writes the decoded batch into t inside tx. The batch must have been
// registered as the transaction's input (Transactor.InTx): this stages it in
// a temporary table, creates the target on first use or adds the columns it
// lacks, and appends the rows (BY NAME). Returns the rows written.
func Land(ctx context.Context, tx contract.Tx, t Table, d *Decoded) (int64, error) {
	if d.Rec == nil {
		return 0, nil
	}
	sel := stageSelect(d.Rec.Schema(), d.JSONCols)
	if err := tx.Exec(ctx, "CREATE OR REPLACE TEMP TABLE "+stageTable+" AS "+sel); err != nil {
		return 0, err
	}
	defer func() { _ = tx.Exec(ctx, "DROP TABLE IF EXISTS "+stageTable) }()
	exists, err := t.Exists(ctx, tx)
	if err != nil {
		return 0, err
	}
	if !exists {
		return count(ctx, tx, "CREATE TABLE "+t.SQL()+" AS SELECT * FROM "+stageTable)
	}
	if err := ingest.EvolveSchema(ctx, tx, t.SQL(), stageTable); err != nil {
		return 0, err
	}
	return count(ctx, tx, "INSERT INTO "+t.SQL()+" BY NAME SELECT * FROM "+stageTable)
}

// stageSelect reads contract.InputView, casting nested columns to JSON and
// stamping the landing time.
func stageSelect(s *arrow.Schema, jsonCols map[string]bool) string {
	cols := make([]string, 0, s.NumFields()+1)
	for _, f := range s.Fields() {
		q := quoteIdent(f.Name)
		if jsonCols[f.Name] {
			q = "CAST(" + q + " AS JSON) AS " + q
		}
		cols = append(cols, q)
	}
	cols = append(cols, "now() AS "+ColIngested)
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + contract.InputView
}

// Reader wraps one batch as the Stream that InTx exposes as the input view.
// A fresh Reader is needed for every attempt; the caller releases it.
func Reader(rec arrow.RecordBatch) (contract.Stream, error) {
	return array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
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

// count runs a statement that reports an affected-row Count.
func count(ctx context.Context, tx contract.Tx, stmt string) (int64, error) {
	v, err := scalar(ctx, tx, stmt)
	if err != nil || v == "" {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}
