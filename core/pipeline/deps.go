package pipeline

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Ref is a table referenced by a SELECT, with schema defaulted to "main" and
// lowercased, like Table.ID.
type Ref struct{ Schema, Name string }

// ID is the lowercase schema.name key.
func (r Ref) ID() string { return r.Schema + "." + r.Name }

const lakeCatalogName = "lake"

// RefsFromAST extracts the base tables referenced by a statement serialised
// with DuckDB's json_serialize_sql. It walks the whole tree, so CTEs,
// subqueries (in FROM, WHERE, SELECT), joins and set operations are all
// covered. CTE names are not tables and are dropped; table functions
// (read_csv, range...) are not tables either. References into another
// catalog (e.g. duct.*) are ignored. Result is sorted and unique.
func RefsFromAST(astJSON string) ([]Ref, error) {
	var root any
	if err := json.Unmarshal([]byte(astJSON), &root); err != nil {
		return nil, fmt.Errorf("invalid AST from json_serialize_sql: %w", err)
	}
	if m, ok := root.(map[string]any); ok {
		if e, _ := m["error"].(bool); e {
			msg, _ := m["error_message"].(string)
			if msg == "" {
				msg = "unknown error"
			}
			return nil, fmt.Errorf("DuckDB cannot parse the SELECT: %s", msg)
		}
	}
	ctes := map[string]bool{}
	var tables []Ref
	var walk func(n any)
	walk = func(n any) {
		switch v := n.(type) {
		case map[string]any:
			if cm, ok := v["cte_map"].(map[string]any); ok {
				entries, _ := cm["map"].([]any)
				for _, e := range entries {
					if em, ok := e.(map[string]any); ok {
						if k, ok := em["key"].(string); ok {
							ctes[strings.ToLower(k)] = true
						}
					}
				}
			}
			if v["type"] == "BASE_TABLE" {
				cat, _ := v["catalog_name"].(string)
				sch, _ := v["schema_name"].(string)
				name, _ := v["table_name"].(string)
				if name != "" && (cat == "" || strings.EqualFold(cat, lakeCatalogName)) {
					tables = append(tables, Ref{strings.ToLower(sch), strings.ToLower(name)})
				}
			}
			for _, c := range v {
				walk(c)
			}
		case []any:
			for _, c := range v {
				walk(c)
			}
		}
	}
	walk(root)
	seen := map[string]bool{}
	var out []Ref
	for _, r := range tables {
		if r.Schema == "" {
			if ctes[r.Name] {
				continue // a CTE reference, not a table
			}
			r.Schema = "main"
		}
		if seen[r.ID()] {
			continue
		}
		seen[r.ID()] = true
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out, nil
}
