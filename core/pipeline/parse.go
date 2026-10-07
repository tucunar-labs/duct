// Package pipeline implements Duct declarative pipelines (ADR 0004, v1):
// parsing of CREATE MATERIALIZED VIEW / CREATE STREAMING TABLE with
// CONSTRAINT ... EXPECT, the dependency graph, and full-refresh execution
// through a contract.Engine. It has no cgo.
package pipeline

import (
	"fmt"
	"strings"
)

// Action says what happens to rows (or the update) that violate an expectation.
type Action string

// Expectation actions. Without ON VIOLATION an expectation only warns.
const (
	Warn       Action = "warn"
	DropRow    Action = "drop_row"
	FailUpdate Action = "fail_update"
)

// Expectation is one CONSTRAINT <name> EXPECT (<expr>) [ON VIOLATION ...].
type Expectation struct {
	Name   string
	Expr   string // boolean SQL expression, verbatim
	Action Action
}

// Table is one pipeline output.
type Table struct {
	Schema       string // "main" when the statement gave no schema
	Name         string
	Streaming    bool // CREATE STREAMING TABLE (full refresh in v1)
	Expectations []Expectation
	Query        string // the SELECT, verbatim, without a trailing semicolon
	File         string
	Line         int
}

// ID is the normalised key of the table: lowercase schema.name.
func (t Table) ID() string { return strings.ToLower(t.Schema) + "." + strings.ToLower(t.Name) }

// Parse reads every statement of a pipeline file. file is only used in errors.
func Parse(file, src string) ([]Table, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	var out []Table
	for _, st := range splitStatements(toks) {
		t, err := parseStatement(src, st)
		if err != nil {
			line, col := lineCol(src, st[0].start)
			var pe *posError
			if asPos(err, &pe) {
				line, col = lineCol(src, pe.off)
				err = pe.err
			}
			return nil, fmt.Errorf("%s:%d:%d: %w", file, line, col, err)
		}
		t.File = file
		t.Line, _ = lineCol(src, st[0].start)
		out = append(out, t)
	}
	return out, nil
}

type posError struct {
	off int
	err error
}

func (e *posError) Error() string { return e.err.Error() }

func asPos(err error, target **posError) bool {
	if p, ok := err.(*posError); ok { //nolint:errorlint // never wrapped
		*target = p
		return true
	}
	return false
}

func errAt(tok token, format string, a ...any) error {
	return &posError{tok.start, fmt.Errorf(format, a...)}
}

// splitStatements cuts the tokens at top-level semicolons, dropping empty statements.
func splitStatements(toks []token) [][]token {
	var out [][]token
	depth, from := 0, 0
	for i, t := range toks {
		switch {
		case t.isPunct('('):
			depth++
		case t.isPunct(')'):
			if depth > 0 {
				depth--
			}
		case t.isPunct(';') && depth == 0:
			if i > from {
				out = append(out, toks[from:i])
			}
			from = i + 1
		}
	}
	if from < len(toks) {
		out = append(out, toks[from:])
	}
	return out
}

type cursor struct {
	toks []token
	i    int
}

func (c *cursor) peek() (token, bool) {
	if c.i < len(c.toks) {
		return c.toks[c.i], true
	}
	return token{}, false
}

// accept consumes the keywords in sequence if they all match.
func (c *cursor) accept(kws ...string) bool {
	if c.i+len(kws) > len(c.toks) {
		return false
	}
	for j, kw := range kws {
		if !c.toks[c.i+j].is(kw) {
			return false
		}
	}
	c.i += len(kws)
	return true
}

func (c *cursor) here() token {
	if c.i < len(c.toks) {
		return c.toks[c.i]
	}
	last := c.toks[len(c.toks)-1]
	return token{kind: tkPunct, text: " ", start: last.end, end: last.end}
}

func (c *cursor) expect(kws ...string) error {
	if !c.accept(kws...) {
		return errAt(c.here(), "expected %s", strings.ToUpper(strings.Join(kws, " ")))
	}
	return nil
}

func (c *cursor) ident() (string, token, error) {
	t, ok := c.peek()
	if !ok || (t.kind != tkWord && t.kind != tkQIdent) {
		return "", t, errAt(c.here(), "expected an identifier")
	}
	c.i++
	if t.kind == tkQIdent {
		return strings.ReplaceAll(t.text[1:len(t.text)-1], `""`, `"`), t, nil
	}
	return t.text, t, nil
}

func parseStatement(src string, toks []token) (Table, error) {
	var t Table
	c := &cursor{toks: toks}
	if err := c.expect("create"); err != nil {
		return t, errAt(toks[0], "unsupported statement: pipeline files only accept CREATE MATERIALIZED VIEW and CREATE STREAMING TABLE")
	}
	c.accept("or", "replace")
	switch {
	case c.accept("materialized", "view"):
	case c.accept("streaming", "table"):
		t.Streaming = true
	default:
		return t, errAt(c.here(), "expected MATERIALIZED VIEW or STREAMING TABLE")
	}
	c.accept("if", "not", "exists")
	first, _, err := c.ident()
	if err != nil {
		return t, err
	}
	t.Schema, t.Name = "main", first
	if d, ok := c.peek(); ok && d.isPunct('.') {
		c.i++
		if t.Name, _, err = c.ident(); err != nil {
			return t, err
		}
		t.Schema = first
		if d, ok := c.peek(); ok && d.isPunct('.') {
			return t, errAt(d, "the table name must be <schema>.<name> (no catalog)")
		}
	}
	if p, ok := c.peek(); ok && p.isPunct('(') {
		if t.Expectations, err = parseConstraints(src, c); err != nil {
			return t, err
		}
	}
	if err := c.expect("as"); err != nil {
		return t, err
	}
	if c.i >= len(toks) {
		return t, errAt(c.here(), "missing SELECT after AS")
	}
	t.Query = strings.TrimSpace(src[toks[c.i].start:toks[len(toks)-1].end])
	return t, nil
}

// parseConstraints reads "( CONSTRAINT n EXPECT (e) [ON VIOLATION ...], ... )".
func parseConstraints(src string, c *cursor) ([]Expectation, error) {
	open := c.toks[c.i]
	c.i++
	var out []Expectation
	seen := map[string]bool{}
	for {
		if err := c.expect("constraint"); err != nil {
			return nil, err
		}
		name, nameTok, err := c.ident()
		if err != nil {
			return nil, err
		}
		if seen[strings.ToLower(name)] {
			return nil, errAt(nameTok, "duplicate constraint %q", name)
		}
		seen[strings.ToLower(name)] = true
		if err := c.expect("expect"); err != nil {
			return nil, err
		}
		p, ok := c.peek()
		if !ok || !p.isPunct('(') {
			return nil, errAt(c.here(), "expected ( after EXPECT")
		}
		closeIdx, err := matching(c.toks, c.i)
		if err != nil {
			return nil, err
		}
		expr := strings.TrimSpace(src[p.end:c.toks[closeIdx].start])
		if expr == "" {
			return nil, errAt(p, "empty expression in constraint %q", name)
		}
		c.i = closeIdx + 1
		act := Warn
		switch {
		case c.accept("on", "violation", "drop", "row"):
			act = DropRow
		case c.accept("on", "violation", "fail", "update"):
			act = FailUpdate
		case c.accept("on", "violation"):
			return nil, errAt(c.here(), "expected DROP ROW or FAIL UPDATE")
		}
		out = append(out, Expectation{Name: name, Expr: expr, Action: act})
		n, ok := c.peek()
		switch {
		case ok && n.isPunct(','):
			c.i++
		case ok && n.isPunct(')'):
			c.i++
			return out, nil
		default:
			return nil, errAt(n, "expected , or ) after constraint %q (opened at %s)", name, pos(src, open.start))
		}
	}
}

// matching returns the index of the ) that closes the ( at toks[i].
func matching(toks []token, i int) (int, error) {
	depth := 0
	for j := i; j < len(toks); j++ {
		switch {
		case toks[j].isPunct('('):
			depth++
		case toks[j].isPunct(')'):
			depth--
			if depth == 0 {
				return j, nil
			}
		}
	}
	return 0, errAt(toks[i], "unbalanced parenthesis")
}
