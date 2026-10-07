package governance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// DeniedError is returned when a statement is not allowed for the principal.
type DeniedError struct{ Reason string }

func (e *DeniedError) Error() string { return "permission denied: " + e.Reason }

func denied(format string, a ...any) error { return &DeniedError{Reason: fmt.Sprintf(format, a...)} }

// Catalog name of the lake in the engine (contract.Engine).
const lakeCatalog = "lake"

// deniedFuncPrefixes and deniedFuncs are scalar functions that reach outside
// the role's views (files, settings, other catalogs, sequences). Table
// functions are refused as a class, so the list only has to cover scalars (and
// is belt and braces for anything DuckDB lets in expression position).
var deniedFuncPrefixes = []string{
	"read_", "write_", "sqlite_", "postgres_", "mysql_", "duckdb_", "pragma_", "parquet_", "ducklake_",
	"iceberg_", "delta_", "http_", "json_serialize", "json_deserialize", "which_",
}

var deniedFuncs = map[string]bool{
	"glob": true, "query": true, "query_table": true, "getenv": true, "current_setting": true,
	"set_config": true, "nextval": true, "currval": true, "setval": true, "getvariable": true,
	"sniff_csv": true, "st_read": true, "load_extension": true, "current_database": false,
}

// allowedRefTypes are the FROM-clause node types a role may use.
var allowedRefTypes = map[string]bool{
	"BASE_TABLE": true, "JOIN": true, "SUBQUERY": true, "EMPTY": true, "EXPRESSION_LIST": true,
}

// parsedSQL is the DuckDB parse tree (json_serialize_sql) of one SELECT.
type parsedSQL struct {
	root map[string]any
}

// parseAST decodes the output of json_serialize_sql and requires exactly one
// statement. Non-SELECT statements make json_serialize_sql itself fail.
func parseAST(js string) (*parsedSQL, error) {
	dec := json.NewDecoder(strings.NewReader(js))
	dec.UseNumber() // query_location is a uint64 sentinel: keep it exact
	var root map[string]any
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("cannot parse statement: %w", err)
	}
	if e, _ := root["error"].(bool); e {
		msg, _ := root["error_message"].(string)
		if strings.Contains(msg, "Only SELECT statements") {
			return nil, denied("only SELECT statements are allowed")
		}
		return nil, fmt.Errorf("syntax error: %s", msg)
	}
	stmts, _ := root["statements"].([]any)
	if len(stmts) != 1 {
		return nil, denied("exactly one SELECT statement is allowed")
	}
	return &parsedSQL{root: root}, nil
}

// rewriter checks every reference of a parse tree and pins it to the role's
// schema in the lake catalog.
type rewriter struct {
	schema string          // role_<name>
	views  map[string]bool // views existing in the role schema
	udf    map[string]bool // user-defined functions this role may not call (udf.go)
}

// Rewrite validates p for role and returns the JSON of the statement with all
// relations qualified as lake.role_<role>.<view>. The caller turns it back
// into SQL with json_deserialize_sql, so what runs is exactly what was checked
// (no gap between two parsers). deniedFuncs is a set of function names (lower
// case) the role may not call: the user-defined functions, which run user code
// (ADR 0005).
func (p *parsedSQL) rewrite(role string, views, deniedFuncs map[string]bool) (string, error) {
	rw := &rewriter{schema: contract.RoleSchema(role), views: views, udf: deniedFuncs}
	if err := rw.walk(p.root["statements"], nil); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(p.root); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func withNames(scope map[string]bool, names ...string) map[string]bool {
	out := make(map[string]bool, len(scope)+len(names))
	for k := range scope {
		out[k] = true
	}
	for _, n := range names {
		out[strings.ToLower(n)] = true
	}
	return out
}

func (rw *rewriter) walk(n any, scope map[string]bool) error {
	switch v := n.(type) {
	case []any:
		for _, x := range v {
			if err := rw.walk(x, scope); err != nil {
				return err
			}
		}
	case map[string]any:
		return rw.walkObject(v, scope)
	}
	return nil
}

func (rw *rewriter) walkObject(v map[string]any, scope map[string]bool) error {
	// A recursive CTE sees its own name in its body.
	if name, ok := v["cte_name"].(string); ok && name != "" {
		scope = withNames(scope, name)
	}
	// CTEs: each definition sees only the CTEs before it (an out-of-order or
	// out-of-scope name resolves to a catalog table in DuckDB, so it must not
	// be taken for a CTE); the rest of the node sees all of them.
	bodyScope := scope
	if cm, ok := v["cte_map"].(map[string]any); ok {
		entries, _ := cm["map"].([]any)
		var names []string
		for _, e := range entries {
			em, _ := e.(map[string]any)
			key, _ := em["key"].(string)
			if err := rw.walk(em["value"], withNames(scope, names...)); err != nil {
				return err
			}
			names = append(names, key)
		}
		bodyScope = withNames(scope, names...)
	}
	if err := rw.check(v, bodyScope); err != nil {
		return err
	}
	for k, x := range v {
		if k == "cte_map" {
			continue
		}
		if err := rw.walk(x, bodyScope); err != nil {
			return err
		}
	}
	return nil
}

// check applies the rules for the node kind of v.
func (rw *rewriter) check(v map[string]any, scope map[string]bool) error {
	if class, ok := v["class"].(string); ok {
		if class == "FUNCTION" || class == "WINDOW" {
			if err := rw.checkUDF(v); err != nil {
				return err
			}
			return checkFunction(v)
		}
		return nil
	}
	typ, isStr := v["type"].(string)
	_, hasAlias := v["alias"]
	_, hasLoc := v["query_location"]
	if !isStr || !hasAlias || !hasLoc {
		return nil // not a table reference
	}
	if typ == "TABLE_FUNCTION" {
		return denied("table functions are not allowed")
	}
	if !allowedRefTypes[typ] {
		return denied("%s is not allowed in FROM", strings.ToLower(typ))
	}
	if typ != "BASE_TABLE" {
		return nil
	}
	if v["at_clause"] != nil {
		return denied("time travel (AT) is not allowed")
	}
	return rw.pinTable(v, scope)
}

func checkFunction(v map[string]any) error {
	name, _ := v["function_name"].(string)
	if cat, _ := v["catalog"].(string); cat != "" {
		return denied("function %s.%s is not allowed", cat, name)
	}
	if sch, _ := v["schema"].(string); sch != "" {
		return denied("function %s.%s is not allowed", sch, name)
	}
	l := strings.ToLower(name)
	if deniedFuncs[l] {
		return denied("function %s is not allowed", name)
	}
	for _, p := range deniedFuncPrefixes {
		if strings.HasPrefix(l, p) {
			return denied("function %s is not allowed", name)
		}
	}
	return nil
}

func (rw *rewriter) pinTable(v map[string]any, scope map[string]bool) error {
	cat := strings.ToLower(str(v["catalog_name"]))
	sch := strings.ToLower(str(v["schema_name"]))
	tbl := strings.ToLower(str(v["table_name"]))
	switch {
	case cat == "" && sch == "":
		if scope[tbl] {
			return nil // a CTE of the statement
		}
		if !rw.views[tbl] {
			return denied("relation %q does not exist in %s", str(v["table_name"]), rw.schema)
		}
	case cat != "" && cat != lakeCatalog && cat != "ducklake":
		return denied("catalog %q is not allowed", str(v["catalog_name"]))
	case sch != rw.schema:
		return denied("only schema %s may be queried, not %q", rw.schema, str(v["schema_name"]))
	}
	v["catalog_name"] = lakeCatalog
	v["schema_name"] = rw.schema
	return nil
}

func str(x any) string { s, _ := x.(string); return s }

var errEmptyIdentity = errors.New("identity has no role")
