package contracts

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// RuleResult is the outcome of one check.
type RuleResult struct {
	Check Check
	Value float64
	Pass  bool
	Err   error // the rule could not be measured (bad SQL, NULL result)
}

// Failed is true for a failed or unmeasurable rule.
func (r RuleResult) Failed() bool { return r.Err != nil || !r.Pass }

// Report is the outcome of testing one contract against a relation.
type Report struct {
	Contract string
	Table    string
	Schema   []string // mismatches between contract and table
	Rules    []RuleResult
	Skipped  []string
	Err      error // the table could not be read at all
}

// Failed is true when the schema does not match or a blocking rule fails
// (critical or error severity, or a rule that cannot be measured).
func (r *Report) Failed() bool {
	if r.Err != nil || len(r.Schema) > 0 {
		return true
	}
	for _, x := range r.Rules {
		if x.Failed() && x.Check.Blocking {
			return true
		}
	}
	return false
}

// Test checks the contract against rel (a quoted relation: the lake table, or
// a staged copy) and measures every rule.
func Test(ctx context.Context, eng contract.Engine, c *Contract, rel string) (*Report, error) {
	plan, err := c.Plan()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", c.File, err)
	}
	rep := &Report{Contract: c.File, Table: c.Table, Skipped: plan.Skipped}
	actual, err := ActualColumns(ctx, eng, rel)
	if err != nil {
		rep.Err = err
		return rep, nil
	}
	rep.Schema = c.SchemaIssues(actual)
	for _, ch := range plan.Checks {
		rep.Rules = append(rep.Rules, measure(ctx, eng, ch, rel))
	}
	return rep, nil
}

func measure(ctx context.Context, eng contract.Engine, ch Check, rel string) RuleResult {
	res := RuleResult{Check: ch}
	rows, err := queryRows(ctx, eng, ch.Query(rel))
	switch {
	case err != nil:
		res.Err = err
	case len(rows) != 1 || len(rows[0]) != 1:
		res.Err = errors.New("the query must return one row and one column")
	case rows[0][0] == "":
		res.Err = errors.New("the query returned NULL")
	default:
		if res.Value, err = strconv.ParseFloat(rows[0][0], 64); err != nil {
			res.Err = fmt.Errorf("the query returned %q, not a number", rows[0][0])
		} else {
			res.Pass = ch.Pass(res.Value)
		}
	}
	return res
}

// ActualColumns returns the columns of a table or view as DuckDB sees them.
func ActualColumns(ctx context.Context, eng contract.Engine, rel string) ([]ActualColumn, error) {
	rows, err := queryRows(ctx, eng, "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM "+rel+")")
	if err != nil {
		return nil, err
	}
	out := make([]ActualColumn, len(rows))
	for i, r := range rows {
		out[i] = ActualColumn{Name: r[0], Type: r[1]}
	}
	return out, nil
}

// LakeRel is the quoted name of the lake table for a normalised schema.name.
func LakeRel(table string) string {
	s, t, _ := strings.Cut(table, ".")
	return quoteIdent(s) + "." + quoteIdent(t)
}

// queryRows runs a statement and returns every cell as text (NULL = "").
func queryRows(ctx context.Context, eng contract.Engine, q string) ([][]string, error) {
	st, err := eng.Query(ctx, q)
	if err != nil {
		return nil, err
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
	return out, st.Err()
}
