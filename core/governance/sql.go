package governance

import (
	"errors"
	"fmt"
	"strings"
)

// Governance SQL: the Unity-Catalog-style statements `duct sql` and the admin
// role of `duct serve` accept. The parser is hand written (tokens carry byte
// offsets, so errors name line:column) and only recognises the statements
// below; everything else is ErrNotGovernance and belongs to the engine.
//
//	CREATE ROLE r
//	DROP ROLE r
//	GRANT SELECT ON [TABLE] s.t TO [ROLE] r
//	REVOKE SELECT ON [TABLE] s.t FROM [ROLE] r
//	GRANT ROLE r TO USER u                      -- sets the (single) role of u
//	SHOW GRANTS [ON [TABLE] s.t | TO [ROLE] r]
//	ALTER TABLE s.t SET ROW FILTER <expr> FOR ROLE r
//	ALTER TABLE s.t DROP ROW FILTER FOR ROLE r
//	ALTER TABLE s.t ALTER COLUMN c SET MASK <expr> FOR ROLE r
//	ALTER TABLE s.t ALTER COLUMN c DROP MASK FOR ROLE r
//
// Divergence from Databricks: there, a row filter / mask is a SQL UDF applied
// to the table (ALTER TABLE t SET ROW FILTER fn ON (cols)) for everyone. Duct
// has no SQL UDFs in the catalog and its policies are per role, so the
// expression is written inline and names the role it applies to. The
// Databricks spelling is recognised only to answer with a clear error.

// Kind is the type of a governance statement.
type Kind string

// Statement kinds.
const (
	KindCreateRole    Kind = "create_role"
	KindDropRole      Kind = "drop_role"
	KindGrant         Kind = "grant"
	KindRevoke        Kind = "revoke"
	KindGrantRole     Kind = "grant_role"
	KindShowGrants    Kind = "show_grants"
	KindSetRowFilter  Kind = "set_row_filter"
	KindDropRowFilter Kind = "drop_row_filter"
	KindSetMask       Kind = "set_mask"
	KindDropMask      Kind = "drop_mask"
)

// Stmt is one parsed governance statement. Names are as written (bare words
// lower-cased, quoted ones verbatim); the Store validates them.
type Stmt struct {
	Kind     Kind   `json:"kind"`
	Role     string `json:"role,omitempty"`
	User     string `json:"user,omitempty"`
	Relation string `json:"relation,omitempty"` // schema.table or table
	Column   string `json:"column,omitempty"`
	Expr     string `json:"expr,omitempty"` // SET ROW FILTER / SET MASK expression, verbatim
	ExprPos  int    `json:"-"`              // byte offset of Expr in the source
	Source   string `json:"-"`              // the statement text
}

// ErrNotGovernance means the text is not a governance statement (it is not an
// error of the statement: hand it to the engine).
var ErrNotGovernance = errors.New("not a governance statement")

// ParseError is a syntax error with its position (1-based line and column).
type ParseError struct {
	Pos       int
	Line, Col int
	Msg       string
}

func (e *ParseError) Error() string { return fmt.Sprintf("%d:%d: %s", e.Line, e.Col, e.Msg) }

type tokKind int

const (
	tEOF tokKind = iota
	tWord
	tQuoted // "identifier"
	tString // 'literal'
	tNumber
	tSym
)

type token struct {
	kind tokKind
	text string // words verbatim; quoted: unquoted contents; strings: unquoted contents
	pos  int
	end  int
}

func (t token) isWord(w string) bool { return t.kind == tWord && strings.EqualFold(t.text, w) }

func (t token) describe() string {
	switch t.kind {
	case tEOF:
		return "end of statement"
	case tString:
		return "a string literal"
	case tQuoted:
		return `"` + t.text + `"`
	}
	return fmt.Sprintf("%q", t.text)
}

type lexer struct {
	src string
}

func (l lexer) errAt(pos int, format string, a ...any) *ParseError {
	line, col := 1, 1
	for i := 0; i < pos && i < len(l.src); {
		if l.src[i] == '\n' {
			line, col = line+1, 1
			i++
			continue
		}
		col++
		i++
		for i < len(l.src) && l.src[i]&0xC0 == 0x80 { // UTF-8 continuation: same column
			i++
		}
	}
	return &ParseError{Pos: pos, Line: line, Col: col, Msg: fmt.Sprintf(format, a...)}
}

func isWordStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}
func isWordChar(c byte) bool { return isWordStart(c) || c >= '0' && c <= '9' || c == '$' }

func (l lexer) tokens() ([]token, error) {
	src := l.src
	var out []token
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return nil, l.errAt(i, "unterminated /* comment")
			}
			i += end + 4
		case c == '\'' || c == '"':
			start := i
			i++
			var sb strings.Builder
			for {
				if i >= len(src) {
					what := "string"
					if c == '"' {
						what = "quoted identifier"
					}
					return nil, l.errAt(start, "unterminated %s", what)
				}
				if src[i] == c {
					if i+1 < len(src) && src[i+1] == c {
						sb.WriteByte(c)
						i += 2
						continue
					}
					i++
					break
				}
				sb.WriteByte(src[i])
				i++
			}
			k := tString
			if c == '"' {
				k = tQuoted
			}
			out = append(out, token{kind: k, text: sb.String(), pos: start, end: i})
		case isWordStart(c):
			start := i
			for i < len(src) && isWordChar(src[i]) {
				i++
			}
			out = append(out, token{kind: tWord, text: src[start:i], pos: start, end: i})
		case c >= '0' && c <= '9':
			start := i
			for i < len(src) && (src[i] >= '0' && src[i] <= '9' || src[i] == '.') {
				i++
			}
			out = append(out, token{kind: tNumber, text: src[start:i], pos: start, end: i})
		default:
			out = append(out, token{kind: tSym, text: string(c), pos: i, end: i + 1})
			i++
		}
	}
	out = append(out, token{kind: tEOF, pos: len(src), end: len(src)})
	return out, nil
}

type parser struct {
	lexer
	toks []token
	i    int
}

func (p *parser) peek() token        { return p.toks[p.i] }
func (p *parser) peekAt(n int) token { return p.toks[min(p.i+n, len(p.toks)-1)] }

func (p *parser) accept(w string) bool {
	if p.peek().isWord(w) {
		p.i++
		return true
	}
	return false
}

func (p *parser) expect(w string) error {
	if !p.accept(w) {
		t := p.peek()
		return p.errAt(t.pos, "expected %s, found %s", w, t.describe())
	}
	return nil
}

func (p *parser) expectSeq(ws ...string) error {
	for _, w := range ws {
		if err := p.expect(w); err != nil {
			return err
		}
	}
	return nil
}

// name reads one identifier: a bare word (lower-cased) or a "quoted" one.
func (p *parser) name(what string) (string, error) {
	t := p.peek()
	switch t.kind {
	case tWord:
		p.i++
		return strings.ToLower(t.text), nil
	case tQuoted:
		p.i++
		return t.text, nil
	}
	return "", p.errAt(t.pos, "expected %s, found %s", what, t.describe())
}

// relation reads name or schema.name.
func (p *parser) relation() (string, error) {
	first, err := p.name("a table (schema.table)")
	if err != nil {
		return "", err
	}
	if t := p.peek(); t.kind == tSym && t.text == "." {
		p.i++
		second, err := p.name("a table name after the dot")
		if err != nil {
			return "", err
		}
		if t := p.peek(); t.kind == tSym && t.text == "." {
			return "", p.errAt(t.pos, "use schema.table (no catalog part)")
		}
		return first + "." + second, nil
	}
	return first, nil
}

// optionalKeyword consumes an optional noun (TABLE, ROLE) only when what
// follows is a name, so a role called "role" still works.
func (p *parser) optionalKeyword(w string) {
	if p.peek().isWord(w) {
		if n := p.peekAt(1); n.kind == tWord || n.kind == tQuoted {
			p.i++
		}
	}
}

func (p *parser) end() error {
	if t := p.peek(); t.kind != tEOF {
		if t.kind == tSym && t.text == ";" {
			return p.errAt(t.pos, "only one statement per call")
		}
		return p.errAt(t.pos, "unexpected %s after the end of the statement", t.describe())
	}
	return nil
}

// Parse parses one governance statement (an optional final ";" is fine). It
// returns ErrNotGovernance when the text is something else, so callers can tell
// "not mine" from "mine, but wrong". A *ParseError is returned for statements
// that start like a governance statement and then go wrong.
func Parse(sql string) (*Stmt, error) {
	l := lexer{src: sql}
	toks, err := l.tokens()
	if err != nil {
		// A lexical error is only ours if the statement starts with our words;
		// peek at the first words without the failing token.
		if !leadsGovernance(sql) {
			return nil, ErrNotGovernance
		}
		return nil, err
	}
	// Drop one trailing semicolon.
	if n := len(toks); n >= 2 && toks[n-2].kind == tSym && toks[n-2].text == ";" {
		toks = append(toks[:n-2], toks[n-1])
	}
	p := &parser{lexer: l, toks: toks}
	st, err := p.statement()
	if err != nil {
		return nil, err
	}
	if err := p.end(); err != nil {
		return nil, err
	}
	st.Source = sql
	return st, nil
}

// leadsGovernance is a cheap first-words check used when the text does not
// even tokenise.
func leadsGovernance(sql string) bool {
	f := strings.Fields(strings.ToUpper(sql))
	if len(f) == 0 {
		return false
	}
	switch f[0] {
	case "GRANT", "REVOKE":
		return true
	case "CREATE", "DROP":
		return len(f) > 1 && f[1] == "ROLE"
	case "SHOW":
		return len(f) > 1 && strings.HasPrefix(f[1], "GRANTS")
	case "ALTER":
		return len(f) > 1 && f[1] == "TABLE" && (strings.Contains(sql, "ROW FILTER") || strings.Contains(strings.ToUpper(sql), "MASK"))
	}
	return false
}

func (p *parser) statement() (*Stmt, error) {
	first := p.peek()
	if first.kind != tWord {
		return nil, ErrNotGovernance
	}
	switch strings.ToUpper(first.text) {
	case "CREATE", "DROP":
		if !p.peekAt(1).isWord("ROLE") {
			return nil, ErrNotGovernance
		}
		p.i += 2
		role, err := p.name("a role name")
		if err != nil {
			return nil, err
		}
		k := KindCreateRole
		if strings.EqualFold(first.text, "DROP") {
			k = KindDropRole
		}
		return &Stmt{Kind: k, Role: role}, nil
	case "GRANT":
		p.i++
		return p.grant()
	case "REVOKE":
		p.i++
		return p.revoke()
	case "SHOW":
		if !p.peekAt(1).isWord("GRANTS") {
			return nil, ErrNotGovernance
		}
		p.i += 2
		return p.showGrants()
	case "ALTER":
		return p.alter()
	}
	return nil, ErrNotGovernance
}

func (p *parser) grant() (*Stmt, error) {
	if p.peek().isWord("ROLE") && p.peekAt(1).kind != tEOF && !p.peekAt(1).isWord("TO") {
		p.i++
		role, err := p.name("a role name")
		if err != nil {
			return nil, err
		}
		if err := p.expectSeq("TO", "USER"); err != nil {
			return nil, err
		}
		user, err := p.userName()
		if err != nil {
			return nil, err
		}
		return &Stmt{Kind: KindGrantRole, Role: role, User: user}, nil
	}
	return p.privilege(KindGrant, "TO")
}

func (p *parser) revoke() (*Stmt, error) {
	if t := p.peek(); t.isWord("ROLE") {
		return nil, p.errAt(t.pos, "REVOKE ROLE is not supported: a user always has exactly one role; use GRANT ROLE <role> TO USER <user> to change it")
	}
	return p.privilege(KindRevoke, "FROM")
}

// privilege parses SELECT ON [TABLE] rel <prep> [ROLE] r.
func (p *parser) privilege(k Kind, prep string) (*Stmt, error) {
	t := p.peek()
	if !t.isWord("SELECT") {
		if t.kind == tWord {
			return nil, p.errAt(t.pos, "only the SELECT privilege exists, found %s", strings.ToUpper(t.text))
		}
		return nil, p.errAt(t.pos, "expected SELECT or ROLE, found %s", t.describe())
	}
	p.i++
	if err := p.expect("ON"); err != nil {
		return nil, err
	}
	p.optionalKeyword("TABLE")
	rel, err := p.relation()
	if err != nil {
		return nil, err
	}
	if err := p.expect(prep); err != nil {
		return nil, err
	}
	p.optionalKeyword("ROLE")
	role, err := p.name("a role name")
	if err != nil {
		return nil, err
	}
	return &Stmt{Kind: k, Relation: rel, Role: role}, nil
}

func (p *parser) userName() (string, error) {
	t := p.peek()
	switch t.kind {
	case tWord, tNumber: // user names are case sensitive: letters, digits, _.@-
		p.i++
		name := t.text
		prev := t
		for n := p.peek(); n.pos == prev.end && (n.kind == tWord || n.kind == tNumber || n.kind == tSym && strings.Contains(".@-", n.text)); n = p.peek() {
			name += n.text
			prev = n
			p.i++
		}
		return name, nil
	case tQuoted, tString:
		p.i++
		return t.text, nil
	}
	return "", p.errAt(t.pos, "expected a user name, found %s", t.describe())
}

func (p *parser) showGrants() (*Stmt, error) {
	st := &Stmt{Kind: KindShowGrants}
	switch {
	case p.accept("ON"):
		p.optionalKeyword("TABLE")
		rel, err := p.relation()
		if err != nil {
			return nil, err
		}
		st.Relation = rel
	case p.accept("TO"):
		p.optionalKeyword("ROLE")
		role, err := p.name("a role name")
		if err != nil {
			return nil, err
		}
		st.Role = role
	}
	return st, nil
}

func (p *parser) alter() (*Stmt, error) {
	if !p.peekAt(1).isWord("TABLE") {
		return nil, ErrNotGovernance
	}
	save := p.i
	p.i += 2
	rel, err := p.relation()
	if err != nil {
		// ALTER TABLE <garbage>: the engine words the error.
		p.i = save
		return nil, ErrNotGovernance
	}
	switch {
	case p.peek().isWord("SET") && p.peekAt(1).isWord("ROW"):
		p.i += 2
		if err := p.expect("FILTER"); err != nil {
			return nil, err
		}
		return p.policy(KindSetRowFilter, rel, "")
	case p.peek().isWord("DROP") && p.peekAt(1).isWord("ROW"):
		p.i += 2
		if err := p.expect("FILTER"); err != nil {
			return nil, err
		}
		return p.forRole(&Stmt{Kind: KindDropRowFilter, Relation: rel})
	case p.peek().isWord("ALTER") && p.peekAt(1).isWord("COLUMN"):
		p.i += 2
		col, err := p.name("a column name")
		if err != nil {
			p.i = save
			return nil, ErrNotGovernance
		}
		switch {
		case p.peek().isWord("SET") && p.peekAt(1).isWord("MASK"):
			p.i += 2
			return p.policy(KindSetMask, rel, col)
		case p.peek().isWord("DROP") && p.peekAt(1).isWord("MASK"):
			p.i += 2
			return p.forRole(&Stmt{Kind: KindDropMask, Relation: rel, Column: col})
		}
	}
	p.i = save
	return nil, ErrNotGovernance
}

// forRole parses the closing FOR ROLE r.
func (p *parser) forRole(st *Stmt) (*Stmt, error) {
	if err := p.expectSeq("FOR", "ROLE"); err != nil {
		return nil, err
	}
	role, err := p.name("a role name")
	if err != nil {
		return nil, err
	}
	st.Role = role
	return st, nil
}

// policy parses "<expr> FOR ROLE r": the expression is the source text up to
// the last FOR ROLE <name> that closes the statement (an expression may itself
// contain FOR, as in substring(x FROM 1 FOR 2)).
func (p *parser) policy(k Kind, rel, col string) (*Stmt, error) {
	start := p.peek()
	// Databricks spellings that rely on SQL UDFs.
	if k == KindSetRowFilter && start.kind == tWord && p.peekAt(1).isWord("ON") && p.peekAt(2).kind == tSym && p.peekAt(2).text == "(" {
		return nil, p.errAt(start.pos, "Databricks SQL-UDF row filters (SET ROW FILTER fn ON (cols)) are not supported: write the condition inline, SET ROW FILTER <expression> FOR ROLE <role>")
	}
	if k == KindSetMask && start.kind == tWord && p.peekAt(1).isWord("USING") && p.peekAt(2).isWord("COLUMNS") {
		return nil, p.errAt(start.pos, "Databricks SQL-UDF masks (SET MASK fn USING COLUMNS ...) are not supported: write the expression inline, SET MASK <expression> FOR ROLE <role>")
	}
	// Terminator FOR ROLE <name> EOF, searched from the end.
	n := len(p.toks) // last is EOF
	forAt := -1
	if n >= 4 && p.toks[n-4].isWord("FOR") && p.toks[n-3].isWord("ROLE") &&
		(p.toks[n-2].kind == tWord || p.toks[n-2].kind == tQuoted) {
		forAt = n - 4
	}
	if forAt < p.i {
		forAt = -1
	}
	if forAt < 0 {
		// Report where the role clause should be.
		last := p.toks[n-1]
		what := "row filter"
		if k == KindSetMask {
			what = "mask"
		}
		if start.kind == tEOF {
			return nil, p.errAt(start.pos, "expected an expression for the %s", what)
		}
		return nil, p.errAt(last.pos, "expected FOR ROLE <role> at the end of the %s, found %s", what, last.describe())
	}
	if forAt == p.i {
		return nil, p.errAt(p.toks[forAt].pos, "expected an expression before FOR ROLE")
	}
	exprEnd := p.toks[forAt].pos
	expr := strings.TrimSpace(p.src[start.pos:exprEnd])
	role := p.toks[forAt+2]
	st := &Stmt{Kind: k, Relation: rel, Column: col, Expr: expr, ExprPos: start.pos}
	if role.kind == tWord {
		st.Role = strings.ToLower(role.text)
	} else {
		st.Role = role.text
	}
	p.i = n - 1 // at EOF
	return st, nil
}
