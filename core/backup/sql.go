package backup

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

func ql(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func qi(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// session runs SQL on the engine and redacts errors (DuckDB can echo a
// connection string).
type session struct {
	eng    contract.Engine
	redact func(string) string
}

func (s session) err(q string, err error) error {
	f := strings.Fields(q)
	if len(f) > 3 {
		f = f[:3]
	}
	msg := err.Error()
	if s.redact != nil {
		msg = s.redact(msg)
	}
	return fmt.Errorf("%s: %s", strings.Join(f, " "), msg)
}

func (s session) exec(ctx context.Context, q string) error {
	if err := s.eng.Exec(ctx, q); err != nil {
		return s.err(q, err)
	}
	return nil
}

func (s session) tx(ctx context.Context, stmts []string) error {
	if len(stmts) == 0 {
		return nil
	}
	if err := s.eng.ExecTx(ctx, stmts...); err != nil {
		return s.err(stmts[0], err)
	}
	return nil
}

// rows returns every cell as text; NULL is "".
func (s session) rows(ctx context.Context, q string) ([][]string, error) {
	st, err := s.eng.Query(ctx, q)
	if err != nil {
		return nil, s.err(q, err)
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				if col := rec.Column(c); !col.IsNull(i) {
					row[c] = strings.Clone(col.ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	if err := st.Err(); err != nil {
		return nil, s.err(q, err)
	}
	return out, nil
}

func (s session) names(ctx context.Context, q string) ([]string, error) {
	r, err := s.rows(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(r))
	for i := range r {
		out[i] = r[i][0]
	}
	return out, nil
}

func (s session) int(ctx context.Context, q string) (int64, error) {
	r, err := s.rows(ctx, q)
	if err != nil {
		return 0, err
	}
	if len(r) == 0 || r[0][0] == "" {
		return 0, nil
	}
	return strconv.ParseInt(r[0][0], 10, 64)
}

func (s session) count(ctx context.Context, from string) (int64, error) {
	return s.int(ctx, "SELECT count(*) FROM "+from)
}
