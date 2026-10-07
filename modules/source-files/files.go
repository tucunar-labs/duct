// Package sourcefiles ingests CSV, Parquet and JSON files with DuckDB's native
// readers. It never touches the engine: it returns a SELECT (contract.SQLSource)
// that the core executes and loads into Bronze.
package sourcefiles

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindSource,
		Name: "files",
		New:  func() contract.Module { return &Source{} },
	})
}

// Source is the files source module.
type Source struct {
	paths     []string
	format    string
	reader    map[string]any
	watermark string
}

var _ contract.SQLSource = (*Source)(nil)

// Name implements contract.Module.
func (*Source) Name() string { return "files" }

var readers = map[string]string{"csv": "read_csv", "parquet": "read_parquet", "json": "read_json"}

var paramRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Init reads the ingestion config: root (project dir), watermark, and
// options {path, format, reader}.
func (s *Source) Init(_ context.Context, cfg contract.Config) error {
	root, _ := cfg["root"].(string)
	s.watermark, _ = cfg["watermark"].(string)
	opts, _ := cfg["options"].(map[string]any)

	switch p := opts["path"].(type) {
	case string:
		s.paths = []string{p}
	case []any:
		for _, v := range p {
			str, ok := v.(string)
			if !ok {
				return errors.New("source-files: path entries must be strings")
			}
			s.paths = append(s.paths, str)
		}
	default:
		return errors.New("source-files: options.path is required (string or list of paths/globs)")
	}
	if len(s.paths) == 0 {
		return errors.New("source-files: options.path is empty")
	}
	for i, p := range s.paths {
		if strings.Contains(p, "://") {
			return fmt.Errorf("source-files: remote path %q not supported yet (local paths and globs only)", p)
		}
		if !filepath.IsAbs(p) {
			s.paths[i] = filepath.Join(root, p)
		}
	}
	s.format, _ = opts["format"].(string)
	if s.format == "" {
		s.format = strings.TrimPrefix(strings.ToLower(filepath.Ext(strings.TrimSuffix(s.paths[0], ".gz"))), ".")
		if s.format == "ndjson" || s.format == "jsonl" {
			s.format = "json"
		}
	}
	if _, ok := readers[s.format]; !ok {
		return fmt.Errorf("source-files: format %q must be csv, parquet or json", s.format)
	}
	if r, ok := opts["reader"]; ok {
		m, ok := r.(map[string]any)
		if !ok {
			return errors.New("source-files: options.reader must be a map of reader parameters")
		}
		s.reader = m
	}
	for k := range s.reader {
		if !paramRe.MatchString(k) {
			return fmt.Errorf("source-files: invalid reader parameter %q", k)
		}
	}
	return nil
}

// Plan implements contract.SQLSource.
func (s *Source) Plan(_ context.Context, st contract.IngestState) (contract.SQLPlan, error) {
	args := []string{pathArg(s.paths)}
	params := map[string]string{"union_by_name": "true"}
	for k, v := range s.reader {
		lit, err := literal(v)
		if err != nil {
			return contract.SQLPlan{}, fmt.Errorf("source-files: reader.%s: %w", k, err)
		}
		params[k] = lit
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, k+" = "+params[k])
	}
	sel := fmt.Sprintf("SELECT * FROM %s(%s)", readers[s.format], strings.Join(args, ", "))
	if s.watermark != "" && st.Watermark != "" {
		// DuckDB casts the quoted literal to the column type.
		sel += fmt.Sprintf(` WHERE "%s" > %s`, s.watermark, quote(st.Watermark))
	}
	return contract.SQLPlan{Select: sel}, nil
}

// Close implements contract.Module.
func (*Source) Close() error { return nil }

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func pathArg(paths []string) string {
	if len(paths) == 1 {
		return quote(paths[0])
	}
	q := make([]string, len(paths))
	for i, p := range paths {
		q[i] = quote(p)
	}
	return "[" + strings.Join(q, ", ") + "]"
}

func literal(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return quote(x), nil
	case bool:
		return fmt.Sprint(x), nil
	case int, int64, float64:
		return fmt.Sprint(x), nil
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			p, err := literal(e)
			if err != nil {
				return "", err
			}
			parts[i] = p
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			p, err := literal(x[k])
			if err != nil {
				return "", err
			}
			parts[i] = quote(k) + ": " + p
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	}
	return "", fmt.Errorf("unsupported value %v", v)
}
