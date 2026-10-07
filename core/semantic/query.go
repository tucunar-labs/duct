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
	// Fresh maps the name of each preaggregate that may serve this role right
	// now (built from the role's views under its current policies, from the
	// current data) to its table in the semantic_cache schema. The caller
	// decides; the compiler only routes to what is listed. nil: never route.
	Fresh map[string]string
}

func (s Scope) has(d *Dataset) bool { return s.Tables == nil || s.Tables[d.Table] }

func (s Scope) check(d *Dataset) error {
	if !s.has(d) {
		return fmt.Errorf("%w: %q (%s) is not granted to role %q", ErrUnavailable, d.Name, d.Source, s.Role)
	}
	return nil
}

// Filter is one structured condition. Set Field to filter rows (WHERE, before
// aggregating) or Metric to filter groups by the value of a metric (HAVING,
// after aggregating); never both. Op is one of =, !=, <, <=, >, >=, in,
// not_in, like, not_like, between, is_null, is_not_null (like and not_like
// only on fields). Value is a string, number, bool; a list for in/not_in; a
// [low, high] pair for between; absent for is_null/is_not_null. It is never
// SQL.
type Filter struct {
	Field  string `json:"field,omitempty"`
	Metric string `json:"metric,omitempty"`
	Op     string `json:"op"`
	Value  any    `json:"value,omitempty"`
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

	unlimited bool // internal: no LIMIT (building a preaggregate)
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
	// Preagg is the preaggregate the query was routed to ("" when it reads the
	// role views).
	Preagg string
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

type cond struct {
	ref fieldRef
	f   Filter
}

type havingCond struct {
	mt *Metric
	f  Filter
}

// aliasesFor names the output column of each dimension: the field name, or
// dataset.field when two dimensions share the field name.
func aliasesFor(refs []fieldRef) []string {
	count := map[string]int{}
	for _, r := range refs {
		count[strings.ToLower(r.f.Name)]++
	}
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.f.Name
		if count[strings.ToLower(r.f.Name)] > 1 {
			out[i] = r.ds.Name + "." + r.f.Name
		}
	}
	return out
}

// Query compiles req to one SELECT over the role views of sc.
//
// Strategy (no fan-out, ever): every base metric is computed on its home
// dataset (the fact). For each fact the query aggregates once, joining only
// towards the "one" side of relationships (many-to-one: from -> to), which
// cannot multiply fact rows. A dimension or filter that lives on the "many"
// side of the fact is refused. Several facts are aggregated separately and
// combined with a FULL OUTER JOIN on the dimensions (conformed dimensions), so
// a customer with 3 orders and 10 order lines is never counted 30 times. A
// derived metric (revenue / units, each on its own fact) is evaluated after
// that join, over the already aggregated parts. Filters on metrics (HAVING)
// are applied on the combined result.
//
// When sc.Fresh lists a compatible preaggregate the SQL reads it instead.
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
	case req.unlimited:
	case limit < 0:
		return nil, reqErr("limit must be positive")
	case limit == 0:
		limit = m.defaultLimit
	case limit > m.maxLimit:
		return nil, reqErr("limit %d exceeds the maximum of %d", limit, m.maxLimit)
	}
	if req.unlimited {
		limit = 0
	}

	// Measures.
	var measures []*Metric
	seenM := map[*Metric]bool{}
	for _, name := range req.Measures {
		mt, ok := m.Metric(name)
		if !ok || !mt.visible() {
			return nil, reqErr("unknown measure %q (see list_metrics)", name)
		}
		if seenM[mt] {
			return nil, reqErr("measure %q is repeated", name)
		}
		seenM[mt] = true
		for _, h := range mt.Homes() {
			if err := sc.check(h); err != nil {
				return nil, err
			}
		}
		measures = append(measures, mt)
	}

	// Dimensions.
	dims := make([]dimSel, len(req.Dims))
	seenD := map[fieldRef]bool{}
	var dimRefs []fieldRef
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
		dimRefs = append(dimRefs, ref)
	}
	for i, a := range aliasesFor(dimRefs) {
		dims[i].alias = a
	}
	for _, mt := range measures {
		for _, d := range dims {
			if strings.EqualFold(d.alias, mt.Name) {
				return nil, reqErr("dimension %q and measure %q have the same name", d.alias, mt.Name)
			}
		}
	}

	// Filters: on fields (WHERE) and on metrics (HAVING).
	var conds []cond
	var having []havingCond
	for _, f := range req.Filters {
		if f.Metric != "" {
			if f.Field != "" {
				return nil, reqErr("a filter has a field or a metric, not both")
			}
			mt, ok := m.Metric(f.Metric)
			if !ok || !mt.visible() {
				return nil, reqErr("unknown metric %q in a filter (see list_metrics)", f.Metric)
			}
			for _, h := range mt.Homes() {
				if err := sc.check(h); err != nil {
					return nil, err
				}
			}
			switch strings.ToLower(strings.TrimSpace(f.Op)) {
			case "like", "not_like":
				return nil, reqErr("filter on metric %q: %s only applies to fields", f.Metric, f.Op)
			}
			probe := f
			probe.Field = f.Metric
			if _, err := filterSQL("x", probe); err != nil {
				return nil, err
			}
			having = append(having, havingCond{mt, f})
			continue
		}
		if strings.TrimSpace(f.Field) == "" {
			return nil, reqErr("a filter needs a field or a metric")
		}
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
		conds = append(conds, cond{ref, f})
	}

	// A fresh preaggregate that covers the query serves it.
	if c, err := m.routed(sc, req, dims, measures, conds, having, limit); err != nil || c != nil {
		return c, err
	}

	// Base metrics to aggregate: the leaves of the measures and of the metric
	// filters, in order of appearance.
	var leaves []*Metric
	addLeaves := func(mt *Metric) {
		for _, l := range mt.leaves() {
			found := false
			for _, x := range leaves {
				found = found || x == l
			}
			if !found {
				leaves = append(leaves, l)
			}
		}
	}
	for _, mt := range measures {
		addLeaves(mt)
	}
	for _, h := range having {
		addLeaves(h.mt)
	}

	// Facts: the home datasets of the leaves, in order of appearance.
	var facts []*Dataset
	for _, mt := range leaves {
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
	leafPlan := map[*Metric]int{}
	for _, mt := range leaves {
		for i := range plans {
			if plans[i].fact == mt.home {
				plans[i].ms = append(plans[i].ms, mt)
				leafPlan[mt] = i
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

	leafCol := map[*Metric]string{}
	for i, p := range plans {
		var sel []string
		for j, d := range dims {
			sel = append(sel, refSQL(d.ref)+" AS "+quoteIdent(fmt.Sprintf("d%d", j)))
		}
		for _, mt := range p.ms {
			col := fmt.Sprintf("m%d", len(leafCol))
			leafCol[mt] = col
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
	colOf := func(l *Metric) string { return qn(leafPlan[l]) + "." + quoteIdent(leafCol[l]) }
	var cols []Column
	var final []string
	for j, d := range dims {
		final = append(final, dimExpr(j, len(plans))+" AS "+quoteIdent(d.alias))
		cols = append(cols, Column{Name: d.alias, Kind: "dimension", Datatype: d.ref.f.Datatype})
	}
	for _, mt := range measures { // keep the order the caller asked
		final = append(final, mt.exprSQL(colOf)+" AS "+quoteIdent(mt.Name))
		cols = append(cols, Column{Name: mt.Name, Kind: "measure", Datatype: mt.Datatype})
	}
	var from strings.Builder
	from.WriteString("FROM " + qn(0))
	for i := 1; i < len(plans); i++ {
		var on []string
		for j := range dims {
			on = append(on, dimExpr(j, i)+" IS NOT DISTINCT FROM "+qn(i)+"."+quoteIdent(fmt.Sprintf("d%d", j)))
		}
		cond := "TRUE"
		if len(on) > 0 {
			cond = strings.Join(on, " AND ")
		}
		from.WriteString("\nFULL OUTER JOIN " + qn(i) + " ON " + cond)
	}
	if len(having) == 0 {
		b.WriteString("\n")
		b.WriteString("SELECT " + strings.Join(final, ", ") + "\n" + from.String())
	} else {
		var hs []string
		for i, h := range having {
			final = append(final, h.mt.exprSQL(colOf)+" AS "+quoteIdent(fmt.Sprintf("__h%d", i)))
			hs = append(hs, havingSQL(i, h))
		}
		emit(quoteIdent("r"), "SELECT "+strings.Join(final, ", ")+"\n"+from.String())
		b.WriteString("\nSELECT " + outCols(cols) + "\nFROM " + quoteIdent("r") + "\nWHERE " + strings.Join(hs, " AND "))
	}
	if err := m.tail(&b, req, dims, measures, limit); err != nil {
		return nil, err
	}
	return &Compiled{SQL: b.String(), Columns: cols}, nil
}

func havingSQL(i int, h havingCond) string {
	probe := h.f
	probe.Field = h.mt.Name
	w, _ := filterSQL(quoteIdent(fmt.Sprintf("__h%d", i)), probe)
	return w
}

func outCols(cols []Column) string {
	var out []string
	for _, c := range cols {
		out = append(out, quoteIdent(c.Name))
	}
	return strings.Join(out, ", ")
}

// tail writes ORDER BY and LIMIT (limit 0: none).
func (m *Model) tail(b *strings.Builder, req Request, dims []dimSel, measures []*Metric, limit int) error {
	dimAlias := map[fieldRef]string{}
	for _, d := range dims {
		dimAlias[d.ref] = d.alias
	}
	measureAlias := map[*Metric]string{}
	for _, mt := range measures {
		measureAlias[mt] = mt.Name
	}
	var order []string
	if len(req.Order) == 0 {
		for _, d := range dims {
			order = append(order, quoteIdent(d.alias))
		}
	}
	for _, o := range req.Order {
		alias, err := m.orderTarget(o.By, dimAlias, measureAlias)
		if err != nil {
			return err
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
	if limit > 0 {
		b.WriteString("\nLIMIT " + strconv.Itoa(limit))
	}
	return nil
}

// exprSQL renders the metric over the aggregated columns of its base metrics
// (a derived metric) or as the aggregate itself.
func (mt *Metric) exprSQL(colOf func(*Metric) string) string {
	if !mt.derived {
		return colOf(mt)
	}
	var b strings.Builder
	for _, s := range mt.dsegs {
		if s.kind == segMetric {
			b.WriteString(colOf(s.mt))
		} else {
			b.WriteString(s.text)
		}
	}
	return b.String()
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
