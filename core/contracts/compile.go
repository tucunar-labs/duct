package contracts

import (
	"fmt"
	"io"
	"sort"
	"time"
)

// Describe prints what the contract generates, in the order it is applied.
func (c *Contract) Describe(w io.Writer, p *Plan) {
	pf := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format+"\n", a...) }
	pf("%s -> %s", c.File, c.Table)
	pf("  schema (checked on every refresh; any difference is FAIL UPDATE)")
	for _, col := range p.Columns {
		typ := col.Physical
		if typ == "" {
			typ = col.Logical
		}
		req := ""
		if col.Required {
			req = " NOT NULL"
		}
		pf("    %s %s%s", col.Name, typ, req)
	}
	pf("  checks (run on the staged result, before the lake is touched)")
	rel := LakeRel(c.Table)
	for _, ch := range p.Checks {
		if ch.DropRow {
			continue
		}
		action := "warn (recorded in duct_pipeline_runs)"
		if ch.Blocking {
			action = "FAIL UPDATE"
		}
		pf("    %s [%s] expects %s -> %s", ch.Name, ch.Severity, ch.Expect, action)
		pf("      %s", ch.Query(rel))
	}
	var drops []Check
	for _, ch := range p.Checks {
		if ch.DropRow {
			drops = append(drops, ch)
		}
	}
	if len(drops) > 0 {
		pf("  row expectations (merged into the pipeline)")
		for _, ch := range drops {
			expr, _ := ch.RowExpr()
			pf("    CONSTRAINT contract:%s EXPECT (%s) ON VIOLATION DROP ROW", ch.Name, expr)
		}
	}
	for _, s := range p.Skipped {
		pf("  not evaluated: %s", s)
	}
	for _, s := range c.skippedRoles() {
		pf("  not applied: %s", s)
	}
	if len(p.Roles) > 0 {
		pf("  roles (applied by `duct contract compile --apply` and after every `duct run` of the table)")
		for _, r := range p.Roles {
			pf("    GRANT SELECT ON %s TO %s", c.Table, r.Role)
			if r.Filter != "" {
				pf("    ALTER TABLE %s SET ROW FILTER %s (%s)", c.Table, r.Role, r.Filter)
			}
			cols := make([]string, 0, len(r.Masks))
			for col := range r.Masks {
				cols = append(cols, col)
			}
			sort.Strings(cols)
			for _, col := range cols {
				pf("    ALTER TABLE %s SET MASK %s.%s AS %s", c.Table, r.Role, col, r.Masks[col])
			}
		}
	}
	if max, src := p.SLA.Max(); max > 0 {
		pf("  freshness: stale after %s (%s); see `duct contract freshness`", max.Round(time.Second), src)
	}
}
