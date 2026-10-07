package pipeline

import (
	"fmt"
	"strings"
)

type tokKind int

const (
	tkWord   tokKind = iota // identifier, keyword or number
	tkQIdent                // "quoted identifier"
	tkString                // 'string' or $tag$...$tag$
	tkPunct                 // any other single character
)

type token struct {
	kind       tokKind
	text       string // raw source text
	start, end int    // byte offsets in the source
}

// is reports whether the token is the (case-insensitive) keyword kw.
func (t token) is(kw string) bool { return t.kind == tkWord && strings.EqualFold(t.text, kw) }

func (t token) isPunct(c byte) bool { return t.kind == tkPunct && t.text[0] == c }

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// lex splits SQL into tokens, dropping whitespace and comments (-- and nested
// /* */). Strings, quoted identifiers and dollar-quoted strings are single
// tokens, so their contents never confuse the parser.
func lex(src string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end, err := skipBlockComment(src, i)
			if err != nil {
				return nil, err
			}
			i = end
		case c == '\'' || c == '"':
			end, err := skipQuoted(src, i, c)
			if err != nil {
				return nil, err
			}
			kind := tkString
			if c == '"' {
				kind = tkQIdent
			}
			toks = append(toks, token{kind, src[i:end], i, end})
			i = end
		case c == '$':
			if end, ok := skipDollar(src, i); ok {
				toks = append(toks, token{tkString, src[i:end], i, end})
				i = end
				continue
			}
			toks = append(toks, token{tkPunct, "$", i, i + 1})
			i++
		case isWordByte(c):
			j := i
			for j < len(src) && isWordByte(src[j]) {
				j++
			}
			toks = append(toks, token{tkWord, src[i:j], i, j})
			i = j
		default:
			toks = append(toks, token{tkPunct, string(c), i, i + 1})
			i++
		}
	}
	return toks, nil
}

func skipBlockComment(src string, i int) (int, error) {
	depth := 0
	start := i
	for i < len(src) {
		switch {
		case strings.HasPrefix(src[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(src[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i, nil
			}
		default:
			i++
		}
	}
	return 0, fmt.Errorf("unterminated /* comment starting at %s", pos(src, start))
}

// skipQuoted returns the offset after the closing quote; a doubled quote is an escape.
func skipQuoted(src string, i int, q byte) (int, error) {
	start := i
	i++
	for i < len(src) {
		if src[i] == q {
			if i+1 < len(src) && src[i+1] == q {
				i += 2
				continue
			}
			return i + 1, nil
		}
		i++
	}
	what := "string"
	if q == '"' {
		what = "quoted identifier"
	}
	return 0, fmt.Errorf("unterminated %s starting at %s", what, pos(src, start))
}

// skipDollar recognises $$...$$ and $tag$...$tag$.
func skipDollar(src string, i int) (int, bool) {
	j := i + 1
	for j < len(src) && isWordByte(src[j]) && (src[j] < '0' || src[j] > '9' || j > i+1) {
		j++
	}
	if j >= len(src) || src[j] != '$' {
		return 0, false
	}
	tag := src[i : j+1]
	k := strings.Index(src[j+1:], tag)
	if k < 0 {
		return 0, false
	}
	return j + 1 + k + len(tag), true
}

// pos renders a byte offset as line:col.
func pos(src string, off int) string {
	line, col := lineCol(src, off)
	return fmt.Sprintf("%d:%d", line, col)
}

func lineCol(src string, off int) (int, int) {
	line, col := 1, 1
	for i := 0; i < off && i < len(src); i++ {
		if src[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	return line, col
}
