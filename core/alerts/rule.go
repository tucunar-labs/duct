// Package alerts evaluates SQL alert rules (alerts/*.sql) over the last
// minutes of Kafka topics kept in an in-memory DuckDB, tracks each alert as
// firing -> resolved with deduplication per (rule, key), notifies the
// AlertChannel modules and records every transition in bronze.duct_alerts
// (ADR 0009). It has no cgo and does not import any concrete module.
package alerts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Dir is the folder of a project that holds the rules.
const Dir = "alerts"

// Rule defaults.
const (
	DefaultEvery    = time.Second
	MinEvery        = 10 * time.Millisecond
	DefaultSeverity = "warning"
)

// ChannelRef is "type:name" in a CHANNELS directive: name selects the
// [alerts.channels.<name>] table of duct.toml and type must match its type.
type ChannelRef struct {
	Type string // empty if the rule wrote just the name
	Name string
}

func (c ChannelRef) String() string {
	if c.Type == "" {
		return c.Name
	}
	return c.Type + ":" + c.Name
}

// Rule is one alerts/<name>.sql file. Its leading comment lines are
// directives:
//
//	-- EVERY 200ms                       evaluation period (default 1s)
//	-- KEY sensor_id                     column identifying the alert (default "key"; none = one global alert)
//	-- SEVERITY critical                 info | warning | error | critical (default warning)
//	-- CHANNELS ntfy:fire, webhook:ops   where to notify (none = only recorded in Bronze)
//	-- MESSAGE {sensor_id} at {temp} C   template; {column} comes from the row, plus {rule} and {key}
//
// The statement is a SELECT whose rows are the alerts firing right now, one
// per key. Streams with a window are tables named after the stream.
type Rule struct {
	Name     string
	File     string
	SQL      string
	Every    time.Duration
	Key      string
	Severity string
	Channels []ChannelRef
	Message  string
}

var (
	ruleNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	identRe    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	firstWord  = regexp.MustCompile(`(?i)^\s*(select|with|from|values)\b`)
	severities = map[string]bool{"info": true, "warning": true, "error": true, "critical": true}
)

// ParseRule reads one rule; name comes from the file name.
func ParseRule(name, src string) (Rule, error) {
	r := Rule{Name: name, Every: DefaultEvery, Severity: DefaultSeverity}
	if !ruleNameRe.MatchString(name) {
		return r, fmt.Errorf("rule name %q must match %s", name, ruleNameRe)
	}
	seen := map[string]bool{}
	for _, line := range strings.Split(src, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "--") {
			break
		}
		word, arg, _ := strings.Cut(strings.TrimSpace(strings.TrimPrefix(line, "--")), " ")
		arg = strings.TrimSpace(arg)
		switch word {
		case "EVERY", "KEY", "SEVERITY", "CHANNELS", "MESSAGE":
		default:
			continue // an ordinary comment
		}
		if seen[word] {
			return r, fmt.Errorf("directive %s given twice", word)
		}
		seen[word] = true
		if arg == "" {
			return r, fmt.Errorf("directive %s needs a value", word)
		}
		switch word {
		case "EVERY":
			d, err := time.ParseDuration(arg)
			if err != nil || d < MinEvery {
				return r, fmt.Errorf("EVERY %q must be a duration of at least %s (e.g. 200ms, 5s)", arg, MinEvery)
			}
			r.Every = d
		case "KEY":
			if !identRe.MatchString(arg) {
				return r, fmt.Errorf("KEY %q must be a plain column name", arg)
			}
			r.Key = arg
		case "SEVERITY":
			if !severities[strings.ToLower(arg)] {
				return r, fmt.Errorf("SEVERITY %q must be info, warning, error or critical", arg)
			}
			r.Severity = strings.ToLower(arg)
		case "CHANNELS":
			for _, p := range strings.Split(arg, ",") {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				typ, nm, ok := strings.Cut(p, ":")
				if !ok {
					typ, nm = "", p
				}
				if (ok && !ruleNameRe.MatchString(typ)) || !ruleNameRe.MatchString(nm) {
					return r, fmt.Errorf("CHANNELS entry %q must be type:name (lowercase letters, digits, _)", p)
				}
				r.Channels = append(r.Channels, ChannelRef{Type: typ, Name: nm})
			}
		case "MESSAGE":
			r.Message = arg
		}
	}
	r.SQL = strings.TrimSpace(src)
	for strings.HasSuffix(r.SQL, ";") {
		r.SQL = strings.TrimSpace(strings.TrimSuffix(r.SQL, ";"))
	}
	body := stripComments(r.SQL)
	if !firstWord.MatchString(body) {
		return r, errors.New("the rule must be a SELECT statement")
	}
	if strings.Contains(body, ";") {
		return r, errors.New("the rule must be a single statement")
	}
	if r.Message == "" {
		r.Message = "{rule} {key}"
	}
	return r, nil
}

func stripComments(s string) string {
	var b strings.Builder
	for _, l := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(l); strings.HasPrefix(t, "--") {
			continue
		}
		b.WriteString(l)
		b.WriteByte('\n')
	}
	return b.String()
}

// LoadRules reads every alerts/*.sql of the project root, sorted by name. A
// missing folder yields no rules.
func LoadRules(root string) ([]Rule, error) {
	dir := filepath.Join(root, Dir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Rule
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".sql" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path) //nolint:gosec // inside the project
		if err != nil {
			return nil, err
		}
		r, err := ParseRule(strings.TrimSuffix(e.Name(), ".sql"), string(data))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(Dir, e.Name()), err)
		}
		r.File = path
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Render fills {placeholders} of a template from vars; unknown ones stay.
func Render(tmpl string, vars map[string]string) string {
	var b strings.Builder
	for i := 0; i < len(tmpl); {
		if tmpl[i] == '{' {
			if j := strings.IndexByte(tmpl[i:], '}'); j > 0 {
				if v, ok := vars[tmpl[i+1:i+j]]; ok {
					b.WriteString(v)
					i += j + 1
					continue
				}
			}
		}
		b.WriteByte(tmpl[i])
		i++
	}
	return b.String()
}
