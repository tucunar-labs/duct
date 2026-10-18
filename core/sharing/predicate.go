package sharing

import (
	"encoding/json"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

// Predicate prunes files by their statistics. It is a hint, never a filter:
// MayMatch answers false only when the statistics prove that no row of the file
// satisfies the predicate. Anything it does not understand (an unknown
// operator, a column without statistics, a type mismatch) keeps the file.
type Predicate struct{ roots []node }

// MayMatch reports whether the file with the given Delta statistics JSON may
// hold a row satisfying every predicate. Statistics that are missing or
// cannot be parsed keep the file.
func (p *Predicate) MayMatch(statsJSON string) bool {
	if p == nil || len(p.roots) == 0 || statsJSON == "" {
		return true
	}
	st, ok := parseStats(statsJSON)
	if !ok {
		return true
	}
	for _, r := range p.roots {
		if r.eval(st) == triFalse {
			return false
		}
	}
	return true
}

type tri int

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

// triNot: triTrue means every row satisfies the predicate and triFalse that no
// row does. Rows where it is NULL satisfy neither it nor its negation, so only
// "every row" turns into "no row".
func triNot(a tri) tri {
	if a == triTrue {
		return triFalse
	}
	return triUnknown
}

// ---- statistics ----

type stats struct {
	rows  int64
	mins  map[string]any
	maxs  map[string]any
	nulls map[string]int64
}

func parseStats(js string) (stats, bool) {
	var raw struct {
		NumRecords *int64           `json:"numRecords"`
		Min        map[string]any   `json:"minValues"`
		Max        map[string]any   `json:"maxValues"`
		Nulls      map[string]int64 `json:"nullCount"`
	}
	d := json.NewDecoder(strings.NewReader(js))
	d.UseNumber()
	if err := d.Decode(&raw); err != nil || raw.NumRecords == nil {
		return stats{}, false
	}
	return stats{rows: *raw.NumRecords, mins: raw.Min, maxs: raw.Max, nulls: raw.Nulls}, true
}

// ---- predicate tree ----

type node interface{ eval(stats) tri }

type colRef struct{ name, typ string }
type litVal struct{ value, typ string }

type cmpNode struct {
	op  string // eq lt le gt ge ne
	col colRef
	lit litVal
}
type nullNode struct {
	col     colRef
	notNull bool
}
type boolNode struct {
	op       string // and or not
	children []node
}
type unknownNode struct{}

func (unknownNode) eval(stats) tri { return triUnknown }

func (n boolNode) eval(st stats) tri {
	switch n.op {
	case "not":
		if len(n.children) != 1 {
			return triUnknown
		}
		return triNot(n.children[0].eval(st))
	case "and":
		out := triTrue
		for _, c := range n.children {
			switch c.eval(st) {
			case triFalse:
				return triFalse
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	case "or":
		out := triFalse
		for _, c := range n.children {
			switch c.eval(st) {
			case triTrue:
				return triTrue
			case triUnknown:
				out = triUnknown
			}
		}
		return out
	}
	return triUnknown
}

func (n nullNode) eval(st stats) tri {
	nc, ok := st.nulls[n.col.name]
	if !ok {
		return triUnknown
	}
	switch {
	case !n.notNull && nc == 0:
		return triFalse // no nulls in the file: IS NULL cannot hold
	case !n.notNull && st.rows > 0 && nc == st.rows:
		return triTrue
	case n.notNull && st.rows > 0 && nc == st.rows:
		return triFalse // only nulls: IS NOT NULL cannot hold
	case n.notNull && nc == 0:
		return triTrue
	}
	return triUnknown
}

func (n cmpNode) eval(st stats) tri {
	min, okMin := st.mins[n.col.name]
	max, okMax := st.maxs[n.col.name]
	if !okMin || !okMax {
		return triUnknown
	}
	lo, ok1 := compare(min, n.lit, n.col.typ)
	hi, ok2 := compare(max, n.lit, n.col.typ)
	if !ok1 || !ok2 {
		return triUnknown
	}
	// lo = sign(min - lit), hi = sign(max - lit); every value v of the file
	// satisfies min <= v <= max. A comparison with NULL is never true, so a
	// predicate that is "false for every non-null value" also prunes.
	switch n.op {
	case "eq":
		if lo > 0 || hi < 0 {
			return triFalse
		}
	case "lt": // v < lit
		if lo >= 0 {
			return triFalse
		}
	case "le":
		if lo > 0 {
			return triFalse
		}
	case "gt":
		if hi <= 0 {
			return triFalse
		}
	case "ge":
		if hi < 0 {
			return triFalse
		}
	case "ne":
		if lo == 0 && hi == 0 {
			return triFalse
		}
	}
	return triUnknown
}

// compare returns sign(stat - lit). The statistic's own type decides how both
// are read; ok is false when the literal does not fit, so the file is kept.
func compare(stat any, lit litVal, colType string) (int, bool) {
	switch s := stat.(type) {
	case json.Number:
		if !numericType(lit.typ) || colType != "" && !numericType(colType) || !numRe.MatchString(s.String()) || !numRe.MatchString(lit.value) {
			return 0, false
		}
		a, ok := new(big.Rat).SetString(s.String())
		b, ok2 := new(big.Rat).SetString(lit.value)
		if !ok || !ok2 {
			return 0, false
		}
		return a.Cmp(b), true
	case string:
		if lit.typ != "string" && lit.typ != "date" || colType != "" && colType != "string" && colType != "date" {
			return 0, false
		}
		if (lit.typ == "date" || colType == "date") && (!dateRe.MatchString(lit.value) || !dateRe.MatchString(s)) {
			return 0, false
		}
		return strings.Compare(s, lit.value), true
	}
	return 0, false
}

var (
	dateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	numRe  = regexp.MustCompile(`^-?\d{1,40}(\.\d{1,40})?([eE][+-]?\d{1,3})?$`)
)

func numericType(t string) bool {
	switch t {
	case "int", "long", "short", "byte", "float", "double":
		return true
	}
	return false
}

// ---- JSON predicates (jsonPredicateHints) ----

type jsonOp struct {
	Op        string   `json:"op"`
	Children  []jsonOp `json:"children"`
	Name      string   `json:"name"`
	Value     any      `json:"value"`
	ValueType string   `json:"valueType"`
}

const maxPredicateDepth = 32

// ParseJSONHints parses one jsonPredicateHints value. A tree that does not
// parse is ignored (nil, false): the protocol says to return every file then.
func ParseJSONHints(js string) (node, bool) {
	js = strings.TrimSpace(js)
	if js == "" {
		return nil, false
	}
	var op jsonOp
	if err := json.Unmarshal([]byte(js), &op); err != nil {
		return nil, false
	}
	n, ok := buildJSON(op, 0)
	return n, ok
}

func buildJSON(op jsonOp, depth int) (node, bool) {
	if depth > maxPredicateDepth {
		return nil, false
	}
	switch op.Op {
	case "and", "or", "not":
		if op.Op == "not" && len(op.Children) != 1 || len(op.Children) == 0 {
			return nil, false
		}
		b := boolNode{op: op.Op}
		for _, c := range op.Children {
			n, ok := buildJSON(c, depth+1)
			if !ok {
				n = unknownNode{} // still sound: an unknown child never prunes
			}
			b.children = append(b.children, n)
		}
		return b, true
	case "isNull":
		if len(op.Children) != 1 || op.Children[0].Op != "column" {
			return nil, false
		}
		return nullNode{col: colRef{op.Children[0].Name, op.Children[0].ValueType}}, true
	case "equal", "lessThan", "lessThanOrEqual", "greaterThan", "greaterThanOrEqual":
		if len(op.Children) != 2 {
			return nil, false
		}
		a, b := op.Children[0], op.Children[1]
		cmp := map[string]string{"equal": "eq", "lessThan": "lt", "lessThanOrEqual": "le", "greaterThan": "gt", "greaterThanOrEqual": "ge"}[op.Op]
		switch {
		case a.Op == "column" && b.Op == "literal":
		case a.Op == "literal" && b.Op == "column":
			a, b = b, a
			cmp = flip(cmp)
		default:
			return nil, false
		}
		return cmpNode{op: cmp, col: colRef{a.Name, a.ValueType}, lit: litVal{value: scalar(b.Value), typ: b.ValueType}}, true
	}
	return nil, false
}

func flip(op string) string {
	switch op {
	case "lt":
		return "gt"
	case "le":
		return "ge"
	case "gt":
		return "lt"
	case "ge":
		return "le"
	}
	return op
}

func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return ""
}

// ---- legacy predicateHints (a restricted SQL) ----

var (
	legacyCmpRe  = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(<=|>=|<>|!=|=|<|>)\s*(.+?)\s*$`)
	legacyRevRe  = regexp.MustCompile(`^\s*(.+?)\s*(<=|>=|<>|!=|=|<|>)\s*([A-Za-z_][A-Za-z0-9_]*)\s*$`)
	legacyNullRe = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+(NOT\s+)?NULL\s*$`)
)

// parseLegacy understands `col op literal`, `literal op col`, `col IS [NOT]
// NULL`. The literal's type is not known (the legacy format has none), so
// the statistic decides it: numbers compare as numbers, quoted text as text.
func parseLegacy(h string) (node, bool) {
	if m := legacyNullRe.FindStringSubmatch(h); m != nil {
		return nullNode{col: colRef{name: m[1]}, notNull: m[2] != ""}, true
	}
	ops := map[string]string{"=": "eq", "<": "lt", "<=": "le", ">": "gt", ">=": "ge", "<>": "ne", "!=": "ne"}
	if m := legacyCmpRe.FindStringSubmatch(h); m != nil {
		if l, ok := legacyLiteral(m[3]); ok {
			return cmpNode{op: ops[m[2]], col: colRef{name: m[1]}, lit: l}, true
		}
	}
	if m := legacyRevRe.FindStringSubmatch(h); m != nil {
		if l, ok := legacyLiteral(m[1]); ok {
			return cmpNode{op: flip(ops[m[2]]), col: colRef{name: m[3]}, lit: l}, true
		}
	}
	return nil, false
}

func legacyLiteral(s string) (litVal, bool) {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		inner := s[1 : len(s)-1]
		if strings.Contains(strings.ReplaceAll(inner, "''", ""), "'") {
			return litVal{}, false
		}
		return litVal{value: strings.ReplaceAll(inner, "''", "'"), typ: "string"}, true
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return litVal{value: s, typ: "long"}, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return litVal{value: s, typ: "double"}, true
	}
	return litVal{}, false
}

// ParseHints builds a Predicate from the request's hints: jsonPredicateHints
// (a JSON tree) and the legacy predicateHints (ANDed). Hints that cannot be
// understood are dropped, so an unreadable request prunes nothing.
func ParseHints(jsonHint string, legacy []string) *Predicate {
	p := &Predicate{}
	if n, ok := ParseJSONHints(jsonHint); ok {
		p.roots = append(p.roots, n)
	}
	for _, h := range legacy {
		if n, ok := parseLegacy(h); ok {
			p.roots = append(p.roots, n)
		}
	}
	return p
}
