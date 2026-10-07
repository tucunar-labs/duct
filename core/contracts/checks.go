package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	odcs "github.com/alonsoburon/odcs-go"
)

// Check is one quality rule compiled to SQL plus the threshold that decides
// whether it passes. Query(rel) returns a statement with a single numeric cell.
type Check struct {
	Name     string // shown in reports; stable id of the rule
	Kind     string // required | unique | library | sql
	Metric   string // library metric, "" for sql rules
	Column   string // "" for table-level rules
	Severity string // as written in the contract ("critical" when implicit)
	Blocking bool   // critical or error: a failure stops the refresh (FAIL UPDATE)
	DropRow  bool   // duct.on_violation: drop_row (rows are removed instead)
	Expect   string // human text of the threshold, e.g. "<= 5"

	value  func(rel string) string
	rowOK  string // predicate true for good rows; "" when not expressible per row
	judge  func(v float64) bool
	source string // where it comes from, for compile output
}

// Query returns the SQL that measures the rule over the relation rel (a table
// or view name, already quoted).
func (c Check) Query(rel string) string { return c.value(rel) }

// Pass reports whether a measured value satisfies the threshold.
func (c Check) Pass(v float64) bool { return c.judge(v) }

// RowExpr is the boolean expression that is true for rows that are fine, for
// rules that can be applied row by row (null, missing and invalid values).
func (c Check) RowExpr() (string, bool) { return c.rowOK, c.rowOK != "" }

// Plan is everything a contract generates.
type Plan struct {
	Table   string
	Columns []Column
	Checks  []Check
	Skipped []string // rules Duct does not evaluate (text, custom engines)
	Roles   []RolePlan
	SLA     SLA
}

// Plan compiles the contract. It fails when a rule is malformed (unknown
// metric or argument, no threshold, unknown column, unknown duct. extension).
func (c *Contract) Plan() (*Plan, error) {
	p := &Plan{Table: c.Table, Columns: c.Columns()}
	var errs []error
	cols := map[string]bool{}
	for _, col := range p.Columns {
		cols[strings.ToLower(col.Name)] = true
	}

	// Implicit checks from the schema: required / unique / primaryKey.
	var pk []string
	for _, pr := range c.Object.Properties {
		n := colName(pr)
		if pr.PrimaryKey {
			pk = append(pk, n)
		}
		if pr.Required || pr.PrimaryKey {
			p.Checks = append(p.Checks, implicitNull(n))
		}
		if pr.Unique && !pr.PrimaryKey {
			p.Checks = append(p.Checks, implicitDup(n, []string{n}))
		}
	}
	switch len(pk) {
	case 0:
	case 1:
		p.Checks = append(p.Checks, implicitDup(pk[0], pk))
	default:
		p.Checks = append(p.Checks, implicitDup(strings.Join(pk, "+"), pk))
	}

	add := func(q odcs.Quality, col string) {
		ch, skip, err := compileQuality(q, col, cols)
		switch {
		case err != nil:
			errs = append(errs, err)
		case skip != "":
			p.Skipped = append(p.Skipped, skip)
		default:
			p.Checks = append(p.Checks, ch)
		}
	}
	for _, q := range c.Object.Quality {
		add(q, "")
	}
	for _, pr := range c.Object.Properties {
		for _, q := range pr.Quality {
			add(q, colName(pr))
		}
	}

	var err error
	if p.Roles, err = c.rolePlans(); err != nil {
		errs = append(errs, err)
	}
	p.SLA = c.sla()
	return p, errors.Join(errs...)
}

func implicitNull(col string) Check {
	return Check{
		Name: col + ".required", Kind: "required", Metric: "nullValues", Column: col,
		Severity: "critical", Blocking: true, Expect: "= 0",
		value:  func(rel string) string { return countSQL(rel, quoteIdent(col)+" IS NULL", false) },
		rowOK:  quoteIdent(col) + " IS NOT NULL",
		judge:  func(v float64) bool { return v == 0 },
		source: "schema: required",
	}
}

func implicitDup(name string, cols []string) Check {
	src := "schema: unique"
	return Check{
		Name: name + ".unique", Kind: "unique", Metric: "duplicateValues", Column: strings.Join(cols, ","),
		Severity: "critical", Blocking: true, Expect: "= 0",
		value:  func(rel string) string { return dupSQL(rel, cols, false) },
		judge:  func(v float64) bool { return v == 0 },
		source: src,
	}
}

// qualityArgs are the arguments each library metric understands.
var qualityArgs = map[string][]string{
	"rowCount":        {},
	"nullValues":      {},
	"missingValues":   {"missingValues"},
	"duplicateValues": {"properties"},
	"invalidValues":   {"validValues", "pattern"},
}

func compileQuality(q odcs.Quality, col string, cols map[string]bool) (Check, string, error) {
	where := "rule"
	if col != "" {
		where = "rule on " + col
	}
	typ := strings.ToLower(q.Type)
	metric := q.Metric
	if metric == "" {
		metric = q.Rule
	}
	if typ == "" {
		switch {
		case metric != "":
			typ = "library"
		case q.Query != "":
			typ = "sql"
		default:
			typ = "text"
		}
	}
	name := q.Name
	if name == "" {
		name = q.ID
	}
	if name == "" {
		name = metric
		if typ == "sql" {
			name = "sql"
		}
		if col != "" {
			name += "(" + col + ")"
		}
	}
	where = fmt.Sprintf("%s %q", where, name)
	if col != "" && !cols[strings.ToLower(col)] {
		return Check{}, "", fmt.Errorf("%s: unknown column %q", where, col)
	}
	switch typ {
	case "text":
		if q.Name == "" && q.ID == "" {
			name = "text"
			if d := strings.TrimSpace(q.Description); d != "" {
				name = d
			}
		}
		return Check{}, fmt.Sprintf("%s: text rule (informational)", name), nil
	case "custom":
		return Check{}, fmt.Sprintf("%s: custom rule for engine %q (not evaluated by Duct)", name, q.Engine), nil
	case "library", "sql":
	default:
		return Check{}, "", fmt.Errorf("%s: unknown rule type %q", where, q.Type)
	}
	judge, expect, err := thresholds(q)
	if err != nil {
		return Check{}, "", fmt.Errorf("%s: %w", where, err)
	}
	sev := strings.ToLower(q.Severity)
	ch := Check{Name: name, Kind: typ, Metric: metric, Column: col, Severity: q.Severity, Expect: expect, judge: judge,
		Blocking: sev == "critical" || sev == "error", source: "quality"}
	if ch.Severity == "" {
		ch.Severity = "warning"
	}
	for _, cp := range q.CustomProperties {
		if !strings.HasPrefix(cp.Property, extPrefix) {
			continue
		}
		if !knownExtension("quality", cp.Property) {
			return Check{}, "", fmt.Errorf("%s: unknown extension %q", where, cp.Property)
		}
		if v := strings.ToLower(fmt.Sprint(cp.Value)); v != "drop_row" {
			return Check{}, "", fmt.Errorf("%s: %s must be drop_row, got %q", where, extOnViolate, v)
		}
		ch.DropRow = true
	}

	percent := strings.EqualFold(q.Unit, "percent") || q.Unit == "%"
	if typ == "sql" {
		if strings.TrimSpace(q.Query) == "" {
			return Check{}, "", fmt.Errorf("%s: sql rule without query", where)
		}
		if ch.DropRow {
			return Check{}, "", fmt.Errorf("%s: %s needs a null/missing/invalid values rule", where, extOnViolate)
		}
		query := strings.TrimRight(strings.TrimSpace(q.Query), "; \n\t")
		ch.value = func(rel string) string {
			r := strings.NewReplacer(
				"${table}", rel, "${object}", rel, "{table}", rel, "{object}", rel,
				"${column}", quoteIdent(col), "${property}", quoteIdent(col), "{column}", quoteIdent(col), "{property}", quoteIdent(col))
			return "SELECT CAST((" + r.Replace(query) + ") AS DOUBLE)"
		}
		return ch, "", nil
	}

	allowed, ok := qualityArgs[metric]
	if !ok {
		return Check{}, "", fmt.Errorf("%s: unsupported metric %q (supported: rowCount, nullValues, missingValues, duplicateValues, invalidValues)", where, metric)
	}
	for k := range q.Arguments {
		if !contains(allowed, k) {
			return Check{}, "", fmt.Errorf("%s: metric %s does not take argument %q", where, metric, k)
		}
	}
	needCol := metric != "rowCount" && metric != "duplicateValues"
	if needCol && col == "" {
		return Check{}, "", fmt.Errorf("%s: metric %s must be on a property", where, metric)
	}
	var bad string // SQL boolean, true for a bad row
	switch metric {
	case "rowCount":
		if ch.DropRow {
			return Check{}, "", fmt.Errorf("%s: %s does not apply to rowCount", where, extOnViolate)
		}
		ch.value = func(rel string) string { return "SELECT CAST(count(*) AS DOUBLE) FROM " + rel }
		return ch, "", nil
	case "nullValues":
		bad = quoteIdent(col) + " IS NULL"
	case "missingValues":
		vals, err := argList(q.Arguments, "missingValues")
		if err != nil {
			return Check{}, "", fmt.Errorf("%s: %w", where, err)
		}
		bad = quoteIdent(col) + " IS NULL"
		if len(vals) > 0 {
			bad = "(" + bad + " OR CAST(" + quoteIdent(col) + " AS VARCHAR) IN (" + strings.Join(vals, ", ") + "))"
		}
	case "invalidValues":
		var conds []string
		if _, has := q.Arguments["validValues"]; has {
			vals, err := argList(q.Arguments, "validValues")
			if err != nil {
				return Check{}, "", fmt.Errorf("%s: %w", where, err)
			}
			if len(vals) == 0 {
				return Check{}, "", fmt.Errorf("%s: validValues is empty", where)
			}
			conds = append(conds, "CAST("+quoteIdent(col)+" AS VARCHAR) IN ("+strings.Join(vals, ", ")+")")
		}
		if pat, has := q.Arguments["pattern"]; has {
			s, ok := pat.(string)
			if !ok || s == "" {
				return Check{}, "", fmt.Errorf("%s: pattern must be a non-empty string", where)
			}
			conds = append(conds, "regexp_matches(CAST("+quoteIdent(col)+" AS VARCHAR), "+sqlStr(s)+")")
		}
		if len(conds) == 0 {
			return Check{}, "", fmt.Errorf("%s: invalidValues needs validValues or pattern", where)
		}
		// NULL is not invalid (use nullValues for that); every given condition must hold.
		bad = "(" + quoteIdent(col) + " IS NOT NULL AND NOT (" + strings.Join(conds, " AND ") + "))"
	case "duplicateValues":
		props := []string{col}
		if v, has := q.Arguments["properties"]; has {
			l, ok := v.([]any)
			if !ok || len(l) == 0 {
				return Check{}, "", fmt.Errorf("%s: properties must be a list of columns", where)
			}
			props = props[:0]
			for _, x := range l {
				s := fmt.Sprint(x)
				if !cols[strings.ToLower(s)] {
					return Check{}, "", fmt.Errorf("%s: unknown column %q in properties", where, s)
				}
				props = append(props, s)
			}
		} else if col == "" {
			return Check{}, "", fmt.Errorf("%s: duplicateValues at table level needs arguments.properties", where)
		}
		if ch.DropRow {
			return Check{}, "", fmt.Errorf("%s: %s does not apply to duplicateValues", where, extOnViolate)
		}
		ch.Column = strings.Join(props, ",")
		ch.value = func(rel string) string { return dupSQL(rel, props, percent) }
		return ch, "", nil
	}
	ch.value = func(rel string) string { return countSQL(rel, bad, percent) }
	ch.rowOK = "NOT (" + bad + ")"
	if ch.DropRow {
		// Rows are removed, so the aggregate can no longer fail.
		ch.Blocking = false
	}
	return ch, "", nil
}

func countSQL(rel, bad string, percent bool) string {
	cnt := "count(*) FILTER (WHERE " + bad + ")"
	if percent {
		return "SELECT CAST(coalesce(100.0 * " + cnt + " / nullif(count(*), 0), 0) AS DOUBLE) FROM " + rel
	}
	return "SELECT CAST(" + cnt + " AS DOUBLE) FROM " + rel
}

func dupSQL(rel string, cols []string, percent bool) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quoteIdent(c)
	}
	var dup string
	if len(q) == 1 {
		dup = "(count(" + q[0] + ") - count(DISTINCT " + q[0] + "))"
	} else {
		dup = "(count(*) - count(DISTINCT (" + strings.Join(q, ", ") + ")))"
	}
	if percent {
		return "SELECT CAST(coalesce(100.0 * " + dup + " / nullif(count(*), 0), 0) AS DOUBLE) FROM " + rel
	}
	return "SELECT CAST(" + dup + " AS DOUBLE) FROM " + rel
}

// argList renders a list argument as SQL string literals (compared as text,
// so the column type does not matter); null entries are dropped.
func argList(args map[string]any, key string) ([]string, error) {
	v, has := args[key]
	if !has {
		return nil, nil
	}
	l, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a list", key)
	}
	var out []string
	for _, x := range l {
		if x == nil {
			continue
		}
		out = append(out, sqlStr(fmt.Sprint(x)))
	}
	return out, nil
}

// thresholds turns the Must* operators of a rule into a judge. All of them
// must hold. A rule with none is an error.
func thresholds(q odcs.Quality) (func(float64) bool, string, error) {
	type cond struct {
		text string
		ok   func(float64) bool
	}
	var cs []cond
	num := func(name string, v any) (float64, error) {
		f, ok := toFloat(v)
		if !ok {
			return 0, fmt.Errorf("%s: %v is not a number", name, v)
		}
		return f, nil
	}
	var errs []error
	addNum := func(name, sym string, v any, ok func(x, t float64) bool) {
		t, err := num(name, v)
		if err != nil {
			errs = append(errs, err)
			return
		}
		cs = append(cs, cond{sym + " " + fmtNum(t), func(x float64) bool { return ok(x, t) }})
	}
	if q.MustBe != nil {
		addNum("mustBe", "=", q.MustBe, func(x, t float64) bool { return x == t })
	}
	if q.MustNotBe != nil {
		addNum("mustNotBe", "!=", q.MustNotBe, func(x, t float64) bool { return x != t })
	}
	if q.MustBeGreaterThan != nil {
		addNum("mustBeGreaterThan", ">", *q.MustBeGreaterThan, func(x, t float64) bool { return x > t })
	}
	if q.MustBeGreaterOrEqualTo != nil {
		addNum("mustBeGreaterOrEqualTo", ">=", *q.MustBeGreaterOrEqualTo, func(x, t float64) bool { return x >= t })
	}
	if q.MustBeLessThan != nil {
		addNum("mustBeLessThan", "<", *q.MustBeLessThan, func(x, t float64) bool { return x < t })
	}
	if q.MustBeLessOrEqualTo != nil {
		addNum("mustBeLessOrEqualTo", "<=", *q.MustBeLessOrEqualTo, func(x, t float64) bool { return x <= t })
	}
	rng := func(name string, r []odcs.Number, in bool) {
		if len(r) == 0 {
			return
		}
		if len(r) != 2 {
			errs = append(errs, fmt.Errorf("%s needs two numbers", name))
			return
		}
		lo, e1 := r[0].Float64()
		hi, e2 := r[1].Float64()
		if e1 != nil || e2 != nil {
			errs = append(errs, fmt.Errorf("%s: not numbers", name))
			return
		}
		word := "between"
		if !in {
			word = "not between"
		}
		cs = append(cs, cond{fmt.Sprintf("%s %s and %s", word, fmtNum(lo), fmtNum(hi)),
			func(x float64) bool { return (x >= lo && x <= hi) == in }})
	}
	rng("mustBeBetween", q.MustBeBetween, true)
	rng("mustNotBeBetween", q.MustNotBeBetween, false)
	if len(errs) > 0 {
		return nil, "", errors.Join(errs...)
	}
	if len(cs) == 0 {
		return nil, "", errors.New("no threshold (mustBe, mustBeLessThan, ...)")
	}
	texts := make([]string, len(cs))
	for i, c := range cs {
		texts[i] = c.text
	}
	return func(x float64) bool {
		for _, c := range cs {
			if !c.ok(x) {
				return false
			}
		}
		return true
	}, strings.Join(texts, " and "), nil
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case odcs.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

func fmtNum(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func sqlStr(s string) string     { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}
