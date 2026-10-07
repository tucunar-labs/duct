package semantic

import (
	"fmt"
	"strings"
	"unicode"
)

type tokKind int

const (
	tkSpace tokKind = iota
	tkIdent
	tkQuoted // "double quoted" identifier
	tkString
	tkNumber
	tkPunct
)

type token struct {
	kind tokKind
	text string // as written
	name string // identifier without quotes
}

// tokenize splits a SQL expression. It refuses what an expression of a
// semantic model has no business containing: statement separators, comments
// and dollar quoting.
func tokenize(s string) ([]token, error) {
	var out []token
	rs := []rune(s)
	for i := 0; i < len(rs); {
		c := rs[i]
		switch {
		case unicode.IsSpace(c):
			j := i
			for j < len(rs) && unicode.IsSpace(rs[j]) {
				j++
			}
			out = append(out, token{kind: tkSpace, text: string(rs[i:j])})
			i = j
		case c == '\'' || c == '"':
			j := i + 1
			for {
				if j >= len(rs) {
					return nil, fmt.Errorf("unterminated %c quote", c)
				}
				if rs[j] == c {
					if j+1 < len(rs) && rs[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			text := string(rs[i : j+1])
			t := token{kind: tkString, text: text}
			if c == '"' {
				t = token{kind: tkQuoted, text: text, name: strings.ReplaceAll(string(rs[i+1:j]), `""`, `"`)}
			}
			out = append(out, t)
			i = j + 1
		case unicode.IsDigit(c):
			j := i
			for j < len(rs) {
				switch {
				case unicode.IsDigit(rs[j]) || rs[j] == '.' || rs[j] == '_':
					j++
				case (rs[j] == 'e' || rs[j] == 'E') && j+1 < len(rs) && (unicode.IsDigit(rs[j+1]) ||
					((rs[j+1] == '-' || rs[j+1] == '+') && j+2 < len(rs) && unicode.IsDigit(rs[j+2]))):
					j += 2
				default:
					goto done
				}
			}
		done:
			out = append(out, token{kind: tkNumber, text: string(rs[i:j])})
			i = j
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(rs) && (unicode.IsLetter(rs[j]) || unicode.IsDigit(rs[j]) || rs[j] == '_' || rs[j] == '$') {
				j++
			}
			w := string(rs[i:j])
			out = append(out, token{kind: tkIdent, text: w, name: w})
			i = j
		default:
			if c == ';' || c == '`' || c == '$' || c == '\\' {
				return nil, fmt.Errorf("character %q is not allowed in an expression", c)
			}
			if c == '-' && i+1 < len(rs) && rs[i+1] == '-' || c == '/' && i+1 < len(rs) && rs[i+1] == '*' {
				return nil, fmt.Errorf("comments are not allowed in an expression")
			}
			if i+1 < len(rs) {
				two := string(rs[i : i+2])
				switch two {
				case "::", "<=", ">=", "<>", "!=", "||":
					out = append(out, token{kind: tkPunct, text: two})
					i += 2
					continue
				}
			}
			out = append(out, token{kind: tkPunct, text: string(c)})
			i++
		}
	}
	return out, nil
}

// segKind says what a piece of a parsed expression is.
type segKind int

const (
	segRaw    segKind = iota // SQL text kept as written
	segRef                   // dataset.field
	segBare                  // a bare identifier that is not a function or keyword
	segMetric                // a reference to a base metric (derived metrics only)
)

type seg struct {
	kind  segKind
	text  string  // raw text, or the bare identifier
	ds    string  // segRef: dataset name as written
	field string  // segRef: field name as written
	mt    *Metric // segMetric
}

// keywords allowed bare in an expression (everything else bare is a metric
// reference or an error).
var keywords = setOf("and", "or", "not", "in", "is", "null", "case", "when", "then", "else", "end",
	"between", "like", "ilike", "distinct", "true", "false", "as", "filter", "where", "over",
	"partition", "by", "order", "asc", "desc", "nulls", "first", "last", "rows", "range", "unbounded",
	"preceding", "following", "current", "row", "from", "interval", "year", "month", "day", "hour",
	"minute", "second", "week", "quarter", "escape", "all", "any", "date", "timestamp", "time")

var aggregates = setOf("sum", "count", "avg", "mean", "min", "max", "median", "mode", "stddev", "stddev_pop",
	"stddev_samp", "variance", "var_pop", "var_samp", "any_value", "arbitrary", "first", "last", "bool_and",
	"bool_or", "string_agg", "group_concat", "listagg", "list", "array_agg", "quantile", "quantile_cont",
	"quantile_disc", "approx_count_distinct", "approx_quantile", "arg_min", "arg_max", "min_by", "max_by",
	"product", "sumproduct", "bit_and", "bit_or", "bit_xor", "count_if", "countif", "histogram", "corr",
	"covar_pop", "covar_samp", "entropy", "kurtosis", "skewness", "fsum", "favg", "sem", "mad")

func setOf(xs ...string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// parsed is an expression split into raw text, dataset.field references and
// bare identifiers.
type parsed struct {
	segs   []seg
	hasAgg bool
}

func nextSig(ts []token, i int) (int, bool) {
	for j := i + 1; j < len(ts); j++ {
		if ts[j].kind != tkSpace {
			return j, true
		}
	}
	return 0, false
}

func prevSig(ts []token, i int) (int, bool) {
	for j := i - 1; j >= 0; j-- {
		if ts[j].kind != tkSpace {
			return j, true
		}
	}
	return 0, false
}

func isName(t token) bool { return t.kind == tkIdent || t.kind == tkQuoted }

// parseExpr tokenizes src and classifies its identifiers.
func parseExpr(src string) (*parsed, error) {
	ts, err := tokenize(src)
	if err != nil {
		return nil, err
	}
	p := &parsed{}
	var raw strings.Builder
	flush := func() {
		if raw.Len() > 0 {
			p.segs = append(p.segs, seg{kind: segRaw, text: raw.String()})
			raw.Reset()
		}
	}
	for i := 0; i < len(ts); i++ {
		t := ts[i]
		if !isName(t) {
			raw.WriteString(t.text)
			continue
		}
		// dataset.field
		if i+2 < len(ts) && ts[i+1].kind == tkPunct && ts[i+1].text == "." && isName(ts[i+2]) {
			if i+3 < len(ts) && ts[i+3].kind == tkPunct && ts[i+3].text == "." {
				return nil, fmt.Errorf("%s.%s.…: use dataset.field, without database or schema", t.name, ts[i+2].name)
			}
			flush()
			p.segs = append(p.segs, seg{kind: segRef, ds: t.name, field: ts[i+2].name})
			i += 2
			continue
		}
		if t.kind == tkIdent {
			l := strings.ToLower(t.name)
			if n, ok := nextSig(ts, i); ok && ts[n].kind == tkPunct && ts[n].text == "(" {
				if aggregates[l] {
					p.hasAgg = true
				}
				raw.WriteString(t.text)
				continue
			}
			if pv, ok := prevSig(ts, i); ok && (strings.EqualFold(ts[pv].text, "as") || ts[pv].text == "::") {
				raw.WriteString(t.text) // a type name
				continue
			}
			if keywords[l] || (i > 0 && ts[i-1].text == ".") {
				raw.WriteString(t.text)
				continue
			}
		}
		flush()
		p.segs = append(p.segs, seg{kind: segBare, text: t.name})
	}
	flush()
	return p, nil
}

// quoteIdent quotes an identifier for DuckDB.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// quoteString renders a string literal.
func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
