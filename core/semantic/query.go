package semantic

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// ErrUnavailable wraps the errors for datasets the role has no view of.
var ErrUnavailable = errors.New("dataset not available to the role")

// ErrRequest wraps the errors caused by a bad request (unknown names, a join
// that would duplicate rows, a malformed filter...).
var ErrRequest = errors.New("invalid semantic query")

func reqErr(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrRequest}, a...)...)
}

var roleNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)

// Scope is who the query is compiled for: the SQL reads the views of the
// role's schema (role_<role>), never base tables.
type Scope struct {
	Role string
	// Tables are the views the role owns (lower case table names). nil means
	// "unknown": nothing is pre-checked and the guard of the server decides.
	Tables map[string]bool
}

func (s Scope) has(d *Dataset) bool { return s.Tables == nil || s.Tables[d.Table] }

func (s Scope) check(d *Dataset) error {
	if !s.has(d) {
		return fmt.Errorf("%w: %q (%s) is not granted to role %q", ErrUnavailable, d.Name, d.Source, s.Role)
	}
	return nil
}

// Filter is one structured condition. Op is one of =, !=, <, <=, >, >=, in,
// not_in, like, not_like, between, is_null, is_not_null. Value is a string,
// number, bool; a list for in/not_in; a [low, high] pair for between; absent
// for is_null/is_not_null. It is never SQL.
type Filter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value,omitempty"`
}

// Order sorts by a requested dimension or measure.
type Order struct {
	By   string `json:"by"`
	Desc bool   `json:"desc,omitempty"`
}

// Request is a semantic query: group the measures by the dimensions, keeping
// the rows that satisfy every filter. Dimensions are "dataset.field" (or a
// bare field name when it is unambiguous); measures are metric names.
type Request struct {
	Dims     []string `json:"dims,omitempty"`
	Measures []string `json:"measures"`
	Filters  []Filter `json:"filters,omitempty"`
	Limit    int      `json:"limit,omitempty"`
	Order    []Order  `json:"order,omitempty"`
}

// Column describes one column of the result.
type Column struct {
	Name     string
	Kind     string // "dimension" or "measure"
	Datatype string // Ossie datatype, if declared
}

// Compiled is the outcome of Query.
type Compiled struct {
	SQL     string
	Columns []Column
}

const (
	maxDims    = 20
	maxMeasure = 20
	maxFilters = 50
	maxInList  = 1000
)

type dimSel struct {
	ref   fieldRef
	alias string
}

// Query compiles req to one SELECT over the role views of sc.
//
// Strategy (no fan-out, ever): every metric is computed on its home dataset (the
// fact). For each fact the query aggregates once, joining only towards the
// "one" side of relationships (many-to-one: from -> to), which cannot
// multiply fact rows. A dimension or filter that lives on the "many" side of
// the fact is refused. Several facts are aggregated separately and combined
// with a FULL OUTER JOIN on the dimensions (conformed dimensions), so a
// customer with 3 orders and 10 order lines is never counted 30 times.
func (m *Model) Query(sc Scope, req Request) (*Compiled, error) {
	if !roleNameRe.MatchString(sc.Role) {
		return nil, reqErr("invalid role %q", sc.Role)
	}
	if len(req.Measures) == 0 {
		return nil, reqErr("at least one measure is required")
	}
	if len(req.Measures) > maxMeasure || len(req.Dims) > maxDims || len(req.Filters) > maxFilters {
		return nil, reqErr("too many dimensions, measures or filters")
	}
	limit := req.Limit
	switch {
	case limit < 0:
		return nil, reqErr("limit must be positive")
	case limit == 0:
		limit = m.defaultLimit
	case limit > m.maxLimit:
		return nil, reqErr("limit %d exceeds the maximum of %d", limit, m.maxLimit)
	}

	// Measures.
	var measures []*Metric
	seenM := map[*Metric]bool{}
	for _, name := range req.Measures {
		mt, ok := m.Metric(name)
		if !ok || mt.Hidden || mt.home.Hidden {
			return nil, reqErr("unknown measure %q (see list_metrics)", name)
		}
		if seenM[mt] {
			return nil, reqErr("measure %q is repeated", name)
		}
		seenM[mt] = true
		if err := sc.check(mt.home); err != nil {
			return nil, err
		}
		measures = append(measures, mt)
	}

	// Dimensions.
	dims := make([]dimSel, len(req.Dims))
	seenD := map[fieldRef]bool{}
	for i, name := range req.Dims {
		ref, err := m.resolveField(name)
		if err != nil {
			return nil, err
		}
		if seenD[ref] {
			return nil, reqErr("dimension %q is repeated", name)
		}
		seenD[ref] = true
		if err := sc.check(ref.ds); err != nil {
			return nil, err
		}
		dims[i] = dimSel{ref: ref}
	}
	nameCount := map[string]int{}
	for _, d := range dims {
		nameCount[strings.ToLower(d.ref.f.Name)]++
	}
	for i := range dims {
		dims[i].alias = dims[i].ref.f.Name
		if nameCount[strings.ToLower(dims[i].ref.f.Name)] > 1 {
			dims[i].alias = dims[i].ref.ds.Name + "." + dims[i].ref.f.Name
		}
	}
	for _, mt := range measures {
		for _, d := range dims {
			if strings.EqualFold(d.alias, mt.Name) {
				return nil, reqErr("dimension %q and measure %q have the same name", d.alias, mt.Name)
			}
		}
	}

	// Filters.
	type cond struct {
		ref fieldRef
		f   Filter
	}
	conds := make([]cond, len(req.Filters))
	for i, f := range req.Filters {
		ref, err := m.resolveField(f.Field)
		if err != nil {
			return nil, err
		}
		if err := sc.check(ref.ds); err != nil {
			return nil, err
		}
		if _, err := filterSQL("x", f); err != nil {
			return nil, err
		}
		conds[i] = cond{ref, f}
	}

	// Facts: the home datasets of the measures, in order of appearance.
	var facts []*Dataset
	for _, mt := range measures {
		if !containsDS(facts, mt.home) {
			facts = append(facts, mt.home)
		}
	}
	var needed []*Dataset
	for _, d := range dims {
		needed = appendDS(needed, d.ref.ds)
	}
	for _, c := range conds {
		needed = appendDS(needed, c.ref.ds)
	}

	used := map[*Dataset]bool{}
	type factPlan struct {
		fact  *Dataset
		steps []joinStep
		ms    []*Metric
	}
	plans := make([]factPlan, len(facts))
	for i, f := range facts {
		steps, err := m.planJoins(sc, f, needed)
		if err != nil {
			return nil, err
		}
		plans[i] = factPlan{fact: f, steps: steps}
		used[f] = true
		for _, s := range steps {
			used[s.child] = true
		}
	}
	for _, mt := range measures {
		for i := range plans {
			if plans[i].fact == mt.home {
				plans[i].ms = append(plans[i].ms, mt)
			}
		}
	}

	var b strings.Builder
	b.WriteString("WITH\n")
	first := true
	emit := func(name, body string) {
		if !first {
			b.WriteString(",\n")
		}
		first = false
		b.WriteString(name + " AS (" + body + ")")
	}
	for _, d := range m.Datasets {
		if used[d] {
			emit(quoteIdent(d.Name), datasetSelect(sc, d))
		}
	}

	measureCol := map[*Metric]string{}
	for i, p := range plans {
		var sel []string
		for j, d := range dims {
			sel = append(sel, refSQL(d.ref)+" AS "+quoteIdent(fmt.Sprintf("d%d", j)))
		}
		for _, mt := range p.ms {
			col := fmt.Sprintf("m%d", len(measureCol))
			measureCol[mt] = col
			sel = append(sel, metricSQL(mt)+" AS "+quoteIdent(col))
		}
		q := "SELECT " + strings.Join(sel, ", ") + "\n  FROM " + quoteIdent(p.fact.Name)
		for _, s := range p.steps {
			q += "\n  LEFT JOIN " + quoteIdent(s.child.Name) + " ON " + joinOn(s)
		}
		if len(conds) > 0 {
			var parts []string
			for _, c := range conds {
				w, _ := filterSQL(refSQL(c.ref), c.f)
				parts = append(parts, w)
			}
			q += "\n  WHERE " + strings.Join(parts, " AND ")
		}
		if len(dims) > 0 {
			var g []string
			for j := range dims {
				g = append(g, strconv.Itoa(j+1))
			}
			q += "\n  GROUP BY " + strings.Join(g, ", ")
		}
		emit(quoteIdent(fmt.Sprintf("q%d", i)), q)
	}
	b.WriteString("\n")

	// Final select: one fact reads its CTE; several are combined on the dims.
	qn := func(i int) string { return quoteIdent(fmt.Sprintf("q%d", i)) }
	dimExpr := func(j int, upto int) string {
		if upto == 1 {
			return qn(0) + "." + quoteIdent(fmt.Sprintf("d%d", j))
		}
		var parts []string
		for i := range upto {
			parts = append(parts, qn(i)+"."+quoteIdent(fmt.Sprintf("d%d", j)))
		}
		return "COALESCE(" + strings.Join(parts, ", ") + ")"
	}
	var cols []Column
	var final []string
	dimAlias := map[fieldRef]string{}
	for j, d := range dims {
		final = append(final, dimExpr(j, len(plans))+" AS "+quoteIdent(d.alias))
		cols = append(cols, Column{Name: d.alias, Kind: "dimension", Datatype: d.ref.f.Datatype})
		dimAlias[d.ref] = d.alias
	}
	measureAlias := map[*Metric]string{}
	for _, mt := range measures {
		measureAlias[mt] = mt.Name
	}
	for _, mt := range measures { // keep the order the caller asked
		var qi int
		for i, p := range plans {
			if p.fact == mt.home {
				qi = i
			}
		}
		final = append(final, qn(qi)+"."+quoteIdent(measureCol[mt])+" AS "+quoteIdent(mt.Name))
		cols = append(cols, Column{Name: mt.Name, Kind: "measure", Datatype: mt.Datatype})
	}
	b.WriteString("SELECT " + strings.Join(final, ", ") + "\nFROM " + qn(0))
	for i := 1; i < len(plans); i++ {
		var on []string
		for j := range dims {
			on = append(on, dimExpr(j, i)+" IS NOT DISTINCT FROM "+qn(i)+"."+quoteIdent(fmt.Sprintf("d%d", j)))
		}
		cond := "TRUE"
		if len(on) > 0 {
			cond = strings.Join(on, " AND ")
		}
		b.WriteString("\nFULL OUTER JOIN " + qn(i) + " ON " + cond)
	}

	// ORDER BY.
	var order []string
	if len(req.Order) == 0 {
		for _, d := range dims {
			order = append(order, quoteIdent(d.alias))
		}
	}
	for _, o := range req.Order {
		alias, err := m.orderTarget(o.By, dimAlias, measureAlias)
		if err != nil {
			return nil, err
		}
		dir := " ASC"
		if o.Desc {
			dir = " DESC"
		}
		order = append(order, quoteIdent(alias)+dir)
	}
	if len(order) > 0 {
		b.WriteString("\nORDER BY " + strings.Join(order, ", "))
	}
	b.WriteString("\nLIMIT " + strconv.Itoa(limit))
	return &Compiled{SQL: b.String(), Columns: cols}, nil
}

func (m *Model) orderTarget(by string, dimAlias map[fieldRef]string, measureAlias map[*Metric]string) (string, error) {
	if mt, ok := m.Metric(by); ok {
		if a, ok := measureAlias[mt]; ok {
			return a, nil
		}
		return "", reqErr("order by %q: it is not among the requested measures", by)
	}
	ref, err := m.resolveField(by)
	if err != nil {
		return "", reqErr("order by %q: not a requested dimension or measure", by)
	}
	if a, ok := dimAlias[ref]; ok {
		return a, nil
	}
	return "", reqErr("order by %q: it is not among the requested dimensions", by)
}

func containsDS(xs []*Dataset, d *Dataset) bool {
	for _, x := range xs {
		if x == d {
			return true
		}
	}
	return false
}

func appendDS(xs []*Dataset, d *Dataset) []*Dataset {
	if containsDS(xs, d) {
		return xs
	}
	return append(xs, d)
}

// resolveField resolves "dataset.field" or a bare field name that exists in
// exactly one (visible) dataset.
func (m *Model) resolveField(name string) (fieldRef, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return fieldRef{}, reqErr("empty field name")
	}
	if dsName, fName, ok := strings.Cut(name, "."); ok {
		d, ok := m.Dataset(dsName)
		if !ok || d.Hidden {
			return fieldRef{}, reqErr("unknown dataset %q in %q", dsName, name)
		}
		f, ok := d.Field(fName)
		if !ok || f.Hidden {
			return fieldRef{}, reqErr("dataset %s has no field %q", d.Name, fName)
		}
		return fieldRef{d, f}, nil
	}
	var found []fieldRef
	for _, d := range m.Datasets {
		if d.Hidden {
			continue
		}
		if f, ok := d.Field(name); ok && !f.Hidden {
			found = append(found, fieldRef{d, f})
		}
	}
	switch len(found) {
	case 0:
		return fieldRef{}, reqErr("unknown field %q", name)
	case 1:
		return found[0], nil
	}
	var opts []string
	for _, r := range found {
		opts = append(opts, r.ds.Name+"."+r.f.Name)
	}
	return fieldRef{}, reqErr("field %q is ambiguous, use one of: %s", name, strings.Join(opts, ", "))
}

func refSQL(r fieldRef) string { return quoteIdent(r.ds.Name) + "." + quoteIdent(r.f.Name) }

func metricSQL(mt *Metric) string {
	var b strings.Builder
	for _, s := range mt.segs {
		if s.kind == segRef {
			b.WriteString(quoteIdent(s.ds) + "." + quoteIdent(s.field))
		} else {
			b.WriteString(s.text)
		}
	}
	return b.String()
}

// datasetSelect is the body of the CTE of a dataset: its fields (and the key
// columns of relationships that are not fields) over the role view.
func datasetSelect(sc Scope, d *Dataset) string {
	var cols []string
	for _, f := range d.Fields {
		sql := "(" + f.SQL + ")"
		if ts, err := tokenize(f.SQL); err == nil {
			var sig []token
			for _, t := range ts {
				if t.kind != tkSpace {
					sig = append(sig, t)
				}
			}
			if len(sig) == 1 && isName(sig[0]) {
				sql = quoteIdent(sig[0].name)
			}
		}
		cols = append(cols, sql+" AS "+quoteIdent(f.Name))
	}
	for _, c := range d.extra {
		cols = append(cols, quoteIdent(c))
	}
	return "\n  SELECT " + strings.Join(cols, ", ") + "\n  FROM " + quoteIdent(contract.RoleSchema(sc.Role)) + "." + quoteIdent(d.Table) + " AS " + quoteIdent(d.Name) + "\n"
}

type joinStep struct {
	rel           *Relationship
	parent, child *Dataset
}

func joinOn(s joinStep) string {
	var on []string
	for i := range s.rel.FromColumns {
		on = append(on, quoteIdent(s.rel.from.Name)+"."+quoteIdent(s.rel.FromColumns[i])+" = "+
			quoteIdent(s.rel.to.Name)+"."+quoteIdent(s.rel.ToColumns[i]))
	}
	return strings.Join(on, " AND ")
}

// planJoins finds, for every dataset in need, the path from fact following
// relationships from the many side to the one side. The result lists the
// joins in an order where parents come before children.
func (m *Model) planJoins(sc Scope, fact *Dataset, need []*Dataset) ([]joinStep, error) {
	type edge struct {
		rel    *Relationship
		parent *Dataset
	}
	depth := map[*Dataset]int{fact: 0}
	ways := map[*Dataset][]edge{}
	order := []*Dataset{fact}
	for qi := 0; qi < len(order); qi++ {
		cur := order[qi]
		for _, r := range m.Relationships {
			if r.from != cur {
				continue
			}
			d, seen := depth[r.to]
			switch {
			case !seen:
				depth[r.to] = depth[cur] + 1
				ways[r.to] = []edge{{r, cur}}
				order = append(order, r.to)
			case d == depth[cur]+1:
				ways[r.to] = append(ways[r.to], edge{r, cur})
			}
		}
	}
	chosen := map[*Dataset]edge{}
	for _, n := range need {
		if n == fact {
			continue
		}
		if _, ok := depth[n]; !ok {
			return nil, m.unreachable(fact, n)
		}
		for cur := n; cur != fact; {
			ws := ways[cur]
			if len(ws) > 1 {
				var names []string
				for _, w := range ws {
					names = append(names, w.rel.Name)
				}
				return nil, reqErr("%s is reachable from %s through several relationships (%s): the query is ambiguous",
					cur.Name, fact.Name, strings.Join(names, ", "))
			}
			chosen[cur] = ws[0]
			cur = ws[0].parent
		}
	}
	var steps []joinStep
	for _, d := range order {
		e, ok := chosen[d]
		if !ok {
			continue
		}
		if err := sc.check(d); err != nil {
			return nil, err
		}
		steps = append(steps, joinStep{rel: e.rel, parent: e.parent, child: d})
	}
	return steps, nil
}

// unreachable explains why target cannot be joined to fact.
func (m *Model) unreachable(fact, target *Dataset) error {
	// Reachable going the other way means fan-out.
	seen := map[*Dataset]bool{target: true}
	queue := []*Dataset{target}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, r := range m.Relationships {
			if r.from == cur && !seen[r.to] {
				if r.to == fact {
					return reqErr("%s is on the many side of %s relative to %s: grouping or filtering %s by it would repeat each %s row and inflate its metrics; "+
						"use a dimension of %s or a metric computed on %s",
						target.Name, r.Name, fact.Name, fact.Name, fact.Name, fact.Name, target.Name)
				}
				seen[r.to] = true
				queue = append(queue, r.to)
			}
		}
	}
	return reqErr("%s is not related to %s (no chain of many-to-one relationships)", target.Name, fact.Name)
}

var opSQL = map[string]string{"=": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">="}

// filterSQL renders one filter over the column expression col.
func filterSQL(col string, f Filter) (string, error) {
	op := strings.ToLower(strings.TrimSpace(f.Op))
	switch op {
	case "is_null":
		return col + " IS NULL", nil
	case "is_not_null":
		return col + " IS NOT NULL", nil
	case "in", "not_in":
		list, ok := f.Value.([]any)
		if !ok || len(list) == 0 || len(list) > maxInList {
			return "", reqErr("filter %q: %s needs a non-empty list (max %d)", f.Field, op, maxInList)
		}
		lits := make([]string, len(list))
		for i, v := range list {
			l, err := literal(v)
			if err != nil {
				return "", reqErr("filter %q: %v", f.Field, err)
			}
			lits[i] = l
		}
		kw := " IN "
		if op == "not_in" {
			kw = " NOT IN "
		}
		return col + kw + "(" + strings.Join(lits, ", ") + ")", nil
	case "between":
		pair, ok := f.Value.([]any)
		if !ok || len(pair) != 2 {
			return "", reqErr("filter %q: between needs [low, high]", f.Field)
		}
		lo, err1 := literal(pair[0])
		hi, err2 := literal(pair[1])
		if err := errors.Join(err1, err2); err != nil {
			return "", reqErr("filter %q: %v", f.Field, err)
		}
		return col + " BETWEEN " + lo + " AND " + hi, nil
	case "like", "not_like":
		s, ok := f.Value.(string)
		if !ok {
			return "", reqErr("filter %q: %s needs a string pattern", f.Field, op)
		}
		kw := " LIKE "
		if op == "not_like" {
			kw = " NOT LIKE "
		}
		return col + kw + quoteString(s), nil
	}
	sqlOp, ok := opSQL[op]
	if !ok {
		return "", reqErr("filter %q: unknown operator %q (=, !=, <, <=, >, >=, in, not_in, like, not_like, between, is_null, is_not_null)", f.Field, f.Op)
	}
	if f.Value == nil {
		return "", reqErr("filter %q: %s needs a value (use is_null to test for NULL)", f.Field, op)
	}
	l, err := literal(f.Value)
	if err != nil {
		return "", reqErr("filter %q: %v", f.Field, err)
	}
	return col + " " + sqlOp + " " + l, nil
}

// literal renders a scalar value as a SQL literal.
func literal(v any) (string, error) {
	switch x := v.(type) {
	case string:
		if strings.ContainsRune(x, 0) {
			return "", errors.New("string with NUL byte")
		}
		return quoteString(x), nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int:
		return strconv.Itoa(x), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return "", errors.New("number is not finite")
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case interface{ String() string }: // json.Number
		s := x.String()
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return "", fmt.Errorf("%q is not a number", s)
		}
		return s, nil
	}
	return "", fmt.Errorf("unsupported value of type %T (use a string, number or boolean)", v)
}
