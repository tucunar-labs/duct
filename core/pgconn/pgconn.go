// Package pgconn parses and rewrites PostgreSQL connection strings for the
// Postgres catalog (ADR 0001) without depending on a driver. Accepted forms:
//
//	postgres://user:pass@host:5432/db?sslmode=require
//	postgresql://...
//	postgres:dbname=duct host=db user=duct password=...   (DuckLake style)
//
// Passwords are secrets: nothing here logs one, and Redact removes them from
// text that may echo a connection string.
package pgconn

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// Params are connection parameters keyed by their DuckDB postgres-secret name:
// host, port, database, user, password and the ssl*/passfile options.
type Params map[string]string

// allowed is the set of keys the DuckDB postgres secret understands.
var allowed = map[string]bool{
	"host": true, "port": true, "database": true, "user": true, "password": true,
	"passfile": true, "sslmode": true, "sslcert": true, "sslkey": true,
	"sslrootcert": true, "sslcrl": true, "sslpassword": true,
	"ssl_min_protocol_version": true, "ssl_max_protocol_version": true,
}

var aliases = map[string]string{"dbname": "database", "username": "user"}

// IsDSN reports whether s looks like a Postgres connection string.
func IsDSN(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://") || strings.HasPrefix(s, "postgres:")
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
}

// Parse reads a connection string in any accepted form. Errors never include
// the input (it may hold a password).
func Parse(s string) (Params, error) {
	s = strings.TrimSpace(s)
	if !IsDSN(s) {
		return nil, errors.New("pgconn: not a postgres connection string (want postgres://... or postgres:key=value ...)")
	}
	p := Params{}
	if isURL(s) {
		u, err := url.Parse(s)
		if err != nil {
			return nil, errors.New("pgconn: malformed postgres URL")
		}
		if h := u.Hostname(); h != "" {
			p["host"] = h
		}
		if u.Port() != "" {
			p["port"] = u.Port()
		}
		if u.User != nil {
			p["user"] = u.User.Username()
			if pw, ok := u.User.Password(); ok {
				p["password"] = pw
			}
		}
		if db := strings.TrimPrefix(u.Path, "/"); db != "" {
			p["database"] = db
		}
		for k, v := range u.Query() {
			if err := p.set(k, v[len(v)-1]); err != nil {
				return nil, err
			}
		}
	} else {
		kv, err := parseKeywords(strings.TrimPrefix(s, "postgres:"))
		if err != nil {
			return nil, err
		}
		for _, e := range kv {
			if err := p.set(e[0], e[1]); err != nil {
				return nil, err
			}
		}
	}
	if p["database"] == "" {
		return nil, errors.New("pgconn: the connection string needs a database (path or dbname=)")
	}
	return p, nil
}

func (p Params) set(k, v string) error {
	if a, ok := aliases[k]; ok {
		k = a
	}
	if !allowed[k] {
		return fmt.Errorf("pgconn: unsupported connection parameter %q", k)
	}
	p[k] = v
	return nil
}

// parseKeywords splits "k=v k2='v 2'" (libpq rules: single quotes, backslash escapes).
func parseKeywords(s string) ([][2]string, error) {
	var out [][2]string
	i := 0
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n') {
			i++
		}
		if i >= len(s) {
			return out, nil
		}
		j := i
		for j < len(s) && s[j] != '=' && s[j] != ' ' {
			j++
		}
		if j >= len(s) || s[j] != '=' || j == i {
			return nil, errors.New("pgconn: malformed key=value connection string")
		}
		key := s[i:j]
		i = j + 1
		var val strings.Builder
		if i < len(s) && s[i] == '\'' {
			i++
			closed := false
			for i < len(s) {
				c := s[i]
				if c == '\\' && i+1 < len(s) {
					val.WriteByte(s[i+1])
					i += 2
					continue
				}
				if c == '\'' {
					i++
					closed = true
					break
				}
				val.WriteByte(c)
				i++
			}
			if !closed {
				return nil, errors.New("pgconn: unterminated quote in connection string")
			}
		} else {
			for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				val.WriteByte(s[i])
				i++
			}
		}
		out = append(out, [2]string{key, val.String()})
	}
}

// Format renders p as a URL (asURL) or as "postgres:k=v ..." with keys sorted.
func (p Params) Format(asURL bool) string {
	if asURL {
		u := url.URL{Scheme: "postgres", Path: "/" + p["database"]}
		host := p["host"]
		if p["port"] != "" {
			host += ":" + p["port"]
		}
		u.Host = host
		if p["user"] != "" {
			if pw, ok := p["password"]; ok {
				u.User = url.UserPassword(p["user"], pw)
			} else {
				u.User = url.User(p["user"])
			}
		}
		q := url.Values{}
		for _, k := range sortedKeys(p) {
			switch k {
			case "host", "port", "database", "user", "password":
			default:
				q.Set(k, p[k])
			}
		}
		u.RawQuery = q.Encode()
		return u.String()
	}
	var b strings.Builder
	b.WriteString("postgres:")
	for n, k := range sortedKeys(p) {
		if n > 0 {
			b.WriteByte(' ')
		}
		k2 := k
		if k == "database" {
			k2 = "dbname"
		}
		b.WriteString(k2 + "=" + quoteKW(p[k]))
	}
	return b.String()
}

func quoteKW(v string) string {
	if v != "" && !strings.ContainsAny(v, " \t\n'\\") {
		return v
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

func sortedKeys(p Params) []string {
	ks := make([]string, 0, len(p))
	for k := range p {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// WithPassword returns the connection string s with the password set, when s
// carries none. The form (URL or keywords) of s is kept.
func WithPassword(s, password string) (string, error) {
	p, err := Parse(s)
	if err != nil {
		return s, err
	}
	if _, has := p["password"]; has || password == "" {
		return s, nil
	}
	p["password"] = password
	return p.Format(isURL(s)), nil
}

// StripPassword returns s without its password and whether it had one.
func StripPassword(s string) (string, bool, error) {
	p, err := Parse(s)
	if err != nil {
		return s, false, err
	}
	if _, had := p["password"]; !had {
		return s, false, nil
	}
	delete(p, "password")
	return p.Format(isURL(s)), true, nil
}

// Describe is a log-safe "user@host:port/database".
func (p Params) Describe() string {
	h := p["host"]
	if h == "" {
		h = "localhost"
	}
	if p["port"] != "" {
		h += ":" + p["port"]
	}
	if p["user"] != "" {
		h = p["user"] + "@" + h
	}
	return h + "/" + p["database"]
}

// Redact removes the password of p from text: always where a connection
// string would put it (":pw@", "password=pw"), and anywhere else when the
// password is long enough not to mangle ordinary words.
func (p Params) Redact(text string) string {
	pw := p["password"]
	if pw == "" {
		return text
	}
	for _, v := range []string{pw, url.PathEscape(pw), url.QueryEscape(pw)} {
		text = strings.ReplaceAll(text, ":"+v+"@", ":***@")
		text = strings.ReplaceAll(text, "password="+v, "password=***")
		text = strings.ReplaceAll(text, "password='"+v+"'", "password='***'")
		if len(v) >= 8 {
			text = strings.ReplaceAll(text, v, "***")
		}
	}
	return text
}

// SecretSQL is the DuckDB statement creating a temporary postgres secret named
// name from p. Temporary secrets work with allow_persistent_secrets=false; a
// plain connection string does not (DuckDB looks the secret up in the disabled
// local_file storage). The statement carries the password: never log it.
func (p Params) SecretSQL(name string) string {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TEMPORARY SECRET " + name + " (TYPE postgres")
	for _, k := range sortedKeys(p) {
		b.WriteString(", " + strings.ToUpper(k) + " '" + strings.ReplaceAll(p[k], "'", "''") + "'")
	}
	b.WriteString(")")
	return b.String()
}
