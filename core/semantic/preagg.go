package semantic

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// PreaggSchema is the lake schema that holds the materialised preaggregates.
// Each role has its own tables there (PreaggTable), built from the views of
// that role, so row filters and masks are already applied in the data.
const PreaggSchema = "semantic_cache"

// Refresh modes of a preaggregate.
const (
	RefreshOnSnapshot = "on_snapshot" // rebuilt when the lake changes (default)
	RefreshManual     = "manual"      // only by `duct semantic build`
)

// Preagg is a materialised aggregate declared in the DUCT extension of the
// model:
//
//	{"preaggregate": {"name": "ventas_region", "dims": ["customers.region"],
//	                  "measures": ["revenue", "order_count"], "refresh": "on_snapshot"}}
//
// (one object or a list). A query whose dimensions, filters and measures are
// all covered is answered from it, see Scope.Fresh.
type Preagg struct {
	Name     string
	Dims     []string // dataset.field
	Measures []string
	Refresh  string

	dims    []fieldRef
	aliases []string // column of each dim in the table
	dimIdx  map[fieldRef]int
	meas    []*Metric
	measSet map[*Metric]bool
}

type preaggSpec struct {
	Name     string   `json:"name"`
	Dims     []string `json:"dims"`
	Measures []string `json:"measures"`
	Refresh  string   `json:"refresh"`
	file     string
}

func parsePreaggSpecs(file string, raw json.RawMessage) ([]preaggSpec, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var list []preaggSpec
	if raw[0] == '[' {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&list); err != nil {
			return nil, fmt.Errorf("DUCT extension: preaggregate: %w", err)
		}
	} else {
		var one preaggSpec
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&one); err != nil {
			return nil, fmt.Errorf("DUCT extension: preaggregate: %w", err)
		}
		list = []preaggSpec{one}
	}
	for i := range list {
		list[i].file = file
	}
	return list, nil
}

// Preaggs lists the preaggregates of the model.
func (m *Model) Preaggs() []*Preagg { return m.preaggs }

// Preagg returns a preaggregate by name.
func (m *Model) Preagg(name string) (*Preagg, bool) {
	for _, p := range m.preaggs {
		if p.Name == name {
			return p, true
		}
	}
	return nil, false
}

// PreaggTable is the table (inside PreaggSchema) of a preaggregate for a role.
func PreaggTable(role, name string) string { return role + "__" + name }

func (m *Model) resolvePreaggs() error {
	m.preaggs = nil
	seen := map[string]bool{}
	for _, sp := range m.preSpecs {
		p := &Preagg{Name: sp.Name, Refresh: sp.Refresh, dimIdx: map[fieldRef]int{}, measSet: map[*Metric]bool{}}
		fail := func(f string, a ...any) error {
			return fmt.Errorf("%s: preaggregate %q: %s", sp.file, p.Name, fmt.Sprintf(f, a...))
		}
		if len(sp.Measures) == 0 {
			p.Name = firstNonEmpty(p.Name, "?")
			return fail("at least one measure is required")
		}
		var refs []fieldRef
		for _, d := range sp.Dims {
			ref, err := m.resolveField(d)
			if err != nil {
				return fail("%v", err)
			}
			if _, dup := p.dimIdx[ref]; dup {
				return fail("dimension %q is repeated", d)
			}
			p.dimIdx[ref] = len(refs)
			refs = append(refs, ref)
			p.Dims = append(p.Dims, ref.ds.Name+"."+ref.f.Name)
		}
		p.dims = refs
		p.aliases = aliasesFor(refs)
		for _, n := range sp.Measures {
			mt, ok := m.Metric(n)
			if !ok || !mt.visible() {
				return fail("unknown measure %q", n)
			}
			if p.measSet[mt] {
				return fail("measure %q is repeated", n)
			}
			for _, a := range p.aliases {
				if strings.EqualFold(a, mt.Name) {
					return fail("dimension %q and measure %q have the same name", a, mt.Name)
				}
			}
			p.measSet[mt] = true
			p.meas = append(p.meas, mt)
			p.Measures = append(p.Measures, mt.Name)
		}
		if p.Name == "" {
			h := sha256.Sum256([]byte(strings.Join(p.Dims, ",") + "|" + strings.Join(p.Measures, ",")))
			p.Name = "pre_" + hex.EncodeToString(h[:4])
		}
		if !roleNameRe.MatchString(p.Name) {
			return fail("the name must be lower case letters, digits and _ (max 40)")
		}
		switch p.Refresh {
		case "":
			p.Refresh = RefreshOnSnapshot
		case RefreshOnSnapshot, RefreshManual:
		default:
			return fail("refresh must be %s or %s", RefreshOnSnapshot, RefreshManual)
		}
		if seen[p.Name] {
			return fail("defined twice")
		}
		seen[p.Name] = true
		// It must be computable at all: names, joins, no fan-out.
		if _, err := m.PreaggSQL(Scope{Role: "lint"}, p); err != nil {
			return fail("%v", err)
		}
		m.preaggs = append(m.preaggs, p)
	}
	return nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// PreaggSQL is the SELECT that computes the preaggregate over the role views
// of sc.Role (the SQL that fills the table; no LIMIT). Its columns are the
// dimensions (see Preagg column names) and the measures, by name.
func (m *Model) PreaggSQL(sc Scope, p *Preagg) (string, error) {
	c, err := m.Query(Scope{Role: sc.Role, Tables: sc.Tables}, Request{Dims: p.Dims, Measures: p.Measures, unlimited: true})
	if err != nil {
		return "", fmt.Errorf("preaggregate %s: %w", p.Name, err)
	}
	return c.SQL, nil
}

// rollup returns the SQL that re-aggregates a column holding mt: a base metric
// that is a single SUM, COUNT, MIN or MAX (without DISTINCT) can be summed up
// from finer groups; nothing else can (AVG, DISTINCT counts, ratios).
func (mt *Metric) rollup(col string) (string, bool) {
	if mt.derived {
		return "", false
	}
	ts, err := tokenize(metricSQL(mt))
	if err != nil {
		return "", false
	}
	var sig []token
	for _, t := range ts {
		if t.kind != tkSpace {
			sig = append(sig, t)
		}
	}
	if len(sig) < 4 || sig[0].kind != tkIdent || sig[1].text != "(" || sig[len(sig)-1].text != ")" {
		return "", false
	}
	depth := 0
	for i := 1; i < len(sig); i++ {
		switch {
		case sig[i].text == "(" && sig[i].kind == tkPunct:
			depth++
		case sig[i].text == ")" && sig[i].kind == tkPunct:
			depth--
			if depth == 0 && i != len(sig)-1 {
				return "", false // SUM(a) + SUM(b)
			}
		case sig[i].kind == tkIdent && strings.EqualFold(sig[i].name, "distinct"):
			return "", false
		}
	}
	switch strings.ToLower(sig[0].name) {
	case "sum":
		return "SUM(" + col + ")", true
	case "min":
		return "MIN(" + col + ")", true
	case "max":
		return "MAX(" + col + ")", true
	case "count":
		return "CAST(COALESCE(SUM(" + col + "), 0) AS BIGINT)", true
	}
	return "", false
}

// routed answers the query from the smallest fresh preaggregate that covers
// it, or returns nil when none does.
func (m *Model) routed(sc Scope, req Request, dims []dimSel, measures []*Metric, conds []cond, having []havingCond, limit int) (*Compiled, error) {
	if len(sc.Fresh) == 0 || req.unlimited {
		return nil, nil
	}
	var best *Preagg
	var bestRoll bool
	for _, p := range m.preaggs {
		if _, ok := sc.Fresh[p.Name]; !ok {
			continue
		}
		roll, ok := p.covers(dims, measures, conds, having)
		if !ok || (best != nil && len(p.dims) >= len(best.dims)) {
			continue
		}
		best, bestRoll = p, roll
	}
	if best == nil {
		return nil, nil
	}
	p := best
	col := func(r fieldRef) string { return quoteIdent(p.aliases[p.dimIdx[r]]) }
	measureSQL := func(mt *Metric) string {
		if !bestRoll {
			return quoteIdent(mt.Name)
		}
		e, _ := mt.rollup(quoteIdent(mt.Name))
		return e
	}
	var sel []string
	var cols []Column
	for _, d := range dims {
		sel = append(sel, col(d.ref)+" AS "+quoteIdent(d.alias))
		cols = append(cols, Column{Name: d.alias, Kind: "dimension", Datatype: d.ref.f.Datatype})
	}
	for _, mt := range measures {
		sel = append(sel, measureSQL(mt)+" AS "+quoteIdent(mt.Name))
		cols = append(cols, Column{Name: mt.Name, Kind: "measure", Datatype: mt.Datatype})
	}
	var hs []string
	for i, h := range having {
		sel = append(sel, measureSQL(h.mt)+" AS "+quoteIdent(fmt.Sprintf("__h%d", i)))
		hs = append(hs, havingSQL(i, h))
	}
	q := "SELECT " + strings.Join(sel, ", ") + "\nFROM " + quoteIdent(PreaggSchema) + "." + quoteIdent(sc.Fresh[p.Name])
	if len(conds) > 0 {
		var parts []string
		for _, c := range conds {
			w, _ := filterSQL(col(c.ref), c.f)
			parts = append(parts, w)
		}
		q += "\nWHERE " + strings.Join(parts, " AND ")
	}
	if bestRoll && len(dims) > 0 {
		var g []string
		for j := range dims {
			g = append(g, fmt.Sprint(j+1))
		}
		q += "\nGROUP BY " + strings.Join(g, ", ")
	}
	var b strings.Builder
	if len(having) == 0 {
		b.WriteString(q)
	} else {
		b.WriteString("WITH " + quoteIdent("r") + " AS (" + q + ")\nSELECT " + outCols(cols) + "\nFROM " + quoteIdent("r") + "\nWHERE " + strings.Join(hs, " AND "))
	}
	if err := m.tail(&b, req, dims, measures, limit); err != nil {
		return nil, err
	}
	return &Compiled{SQL: b.String(), Columns: cols, Preagg: p.Name}, nil
}

// covers reports whether the preaggregate can answer the query and whether it
// must re-aggregate (fewer dimensions than the table has).
func (p *Preagg) covers(dims []dimSel, measures []*Metric, conds []cond, having []havingCond) (roll, ok bool) {
	for _, d := range dims {
		if _, in := p.dimIdx[d.ref]; !in {
			return false, false
		}
	}
	for _, c := range conds {
		if _, in := p.dimIdx[c.ref]; !in {
			return false, false
		}
	}
	roll = len(dims) < len(p.dims)
	check := func(mt *Metric) bool {
		if !p.measSet[mt] {
			return false
		}
		if roll {
			_, can := mt.rollup("x")
			return can
		}
		return true
	}
	for _, mt := range measures {
		if !check(mt) {
			return false, false
		}
	}
	for _, h := range having {
		if !check(h.mt) {
			return false, false
		}
	}
	return roll, true
}

// RoleViewSchema is the schema the preaggregates of role are built from.
func RoleViewSchema(role string) string { return contract.RoleSchema(role) }
