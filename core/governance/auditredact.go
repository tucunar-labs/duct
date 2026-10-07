package governance

import "regexp"

const redacted = "***"

var redactors = []struct {
	re   *regexp.Regexp
	repl string
}{
	// CREATE [OR REPLACE] [TEMPORARY|PERSISTENT] SECRET ...: the whole body
	// carries credentials.
	{regexp.MustCompile(`(?is)\b(create\s+(?:or\s+replace\s+)?(?:(?:temporary|persistent)\s+)?secret\b).*`), "$1 " + redacted},
	// Authorization headers and bearer tokens.
	{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), "$1 " + redacted},
	// key = 'value' / key: "value" / key=value for credential-looking keys.
	{regexp.MustCompile(`(?i)\b((?:[a-z0-9_]*(?:password|passwd|pwd|secret|token|apikey|api_key|api-key|access_key|secret_key|private_key|credential|authorization))\w*)(\s*[=:]\s*)('(?:[^']|'')*'|"[^"]*"|[^\s,;)&]+)`), "$1$2" + redacted},
	// PASSWORD 'x' / TOKEN 'x' / IDENTIFIED BY 'x' (ALTER USER ... PASSWORD 'x').
	{regexp.MustCompile(`(?i)\b(password|passwd|secret|token|identified\s+by)\s+('(?:[^']|'')*')`), "$1 '" + redacted + "'"},
	// user:password@host in connection URLs.
	{regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^\s:/@'"]+):[^\s@/'"]+@`), "$1:" + redacted + "@"},
	// Duct API tokens.
	{regexp.MustCompile(`\bdct_[A-Za-z0-9_-]{20,}`), redacted},
}

// RedactSQL removes credentials from statement text before it is stored:
// CREATE SECRET bodies, password=/token=/api_key=-style assignments, PASSWORD
// 'x' clauses, bearer tokens, user:password@ in URLs and Duct API tokens. It
// is a safety net, not a parser: it errs on the side of hiding too much.
func RedactSQL(s string) string {
	for _, r := range redactors {
		s = r.re.ReplaceAllString(s, r.repl)
	}
	return s
}
