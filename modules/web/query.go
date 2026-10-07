package web

import (
	"context"
	"fmt"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

type cell struct {
	Text string
	Null bool
}

type resultColumn struct {
	Name string
	Type string
}

// queryResult is a bounded, render-ready copy of a Stream.
type queryResult struct {
	Columns   []resultColumn
	Rows      [][]cell
	Truncated bool
	Limit     int
	Elapsed   time.Duration
	Err       string
}

const maxCellRunes = 2000

// runQuery executes sql as who and keeps at most limit rows. It stops reading
// (and releases the stream) as soon as the limit is exceeded, so a huge
// result never travels to the browser.
func runQuery(ctx context.Context, h contract.QueryHandler, who contract.Identity, sql string, limit int) (*queryResult, error) {
	return readStream(func() (contract.Stream, error) { return h.Query(ctx, who, sql) }, limit)
}

func readStream(open func() (contract.Stream, error), limit int) (*queryResult, error) {
	start := time.Now()
	st, err := open()
	if err != nil {
		return nil, err
	}
	defer st.Release()
	res := &queryResult{Limit: limit}
	for _, f := range st.Schema().Fields() {
		res.Columns = append(res.Columns, resultColumn{Name: f.Name, Type: f.Type.String()})
	}
	for st.Next() {
		b := st.RecordBatch()
		for i := 0; i < int(b.NumRows()); i++ {
			if len(res.Rows) >= limit {
				res.Truncated = true
				res.Elapsed = time.Since(start)
				return res, nil
			}
			row := make([]cell, b.NumCols())
			for c := range row {
				col := b.Column(c)
				if col.IsNull(i) {
					row[c] = cell{Null: true}
					continue
				}
				// ValueStr may alias the Arrow buffer (zero-copy strings); the
				// batch is freed after the stream moves on, so copy.
				row[c] = cell{Text: strings.Clone(clip(col.ValueStr(i)))}
			}
			res.Rows = append(res.Rows, row)
		}
	}
	if err := st.Err(); err != nil {
		return nil, err
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

func clip(s string) string {
	if len(s) <= maxCellRunes {
		return s
	}
	r := []rune(s)
	if len(r) <= maxCellRunes {
		return s
	}
	return string(r[:maxCellRunes]) + "…"
}

// quoteIdent quotes an SQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

const catalogSQL = `SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
ORDER BY table_schema, table_name, ordinal_position`

// catalogLister is an OPTIONAL interface of a QueryHandler (defined here, not
// in contract v1): a handler whose guard refuses information_schema (the
// governed one only lets a role SELECT from its own schema, ADR 0007) can
// list what an identity may see itself. The stream has the shape of
// catalogSQL: table_schema, table_name, column_name, data_type, ordered by
// schema, table and ordinal position.
type catalogLister interface {
	Catalog(ctx context.Context, id contract.Identity) (contract.Stream, error)
}

// loadCatalog lists the schemas, tables and columns visible to who.
func loadCatalog(ctx context.Context, h contract.QueryHandler, who contract.Identity) ([]schema, error) {
	var res *queryResult
	var err error
	if cl, ok := h.(catalogLister); ok {
		res, err = readStream(func() (contract.Stream, error) { return cl.Catalog(ctx, who) }, 100000)
	} else {
		res, err = runQuery(ctx, h, who, catalogSQL, 100000)
	}
	if err != nil {
		return nil, err
	}
	if len(res.Columns) < 4 {
		return nil, fmt.Errorf("unexpected information_schema shape: %d columns", len(res.Columns))
	}
	var out []schema
	for _, r := range res.Rows {
		sn, tn := r[0].Text, r[1].Text
		if len(out) == 0 || out[len(out)-1].Name != sn {
			out = append(out, schema{Name: sn})
		}
		s := &out[len(out)-1]
		if len(s.Tables) == 0 || s.Tables[len(s.Tables)-1].Name != tn {
			s.Tables = append(s.Tables, table{Name: tn})
		}
		t := &s.Tables[len(s.Tables)-1]
		t.Columns = append(t.Columns, column{Name: r[2].Text, Type: r[3].Text})
	}
	return out, nil
}
