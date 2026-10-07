package governance

import (
	"fmt"
	"strings"
)

// checkPolicyExpr validates the user-written expression of a row filter or a
// mask. js is json_serialize_sql of `SELECT (<expr>) FROM "schema"."table"`.
//
// The expression ends up inside a view definition made with the privileges of
// Duct, so it must be a plain scalar expression over the columns of that one
// table. Rules (documented in docs/adr/0007-gobierno.md):
//   - the statement is exactly one SELECT of one expression from that table,
//     with no WHERE/GROUP/HAVING/ORDER/LIMIT/CTE/set operation (a ")" that
//     tries to break out of the wrapper changes this shape and is refused);
//   - no subquery, no table function, no star/COLUMNS(), no parameter;
//   - functions follow the same deny list as role queries (no read_*, glob,
//     getenv, current_setting, sqlite_*, ...);
//   - column references name a column of the table (optionally table.column),
//     never another schema or table.
func checkPolicyExpr(js, schema, table string) error {
	p, err := parseAST(js)
	if err != nil {
		return err
	}
	stmts, _ := p.root["statements"].([]any)
	st, _ := stmts[0].(map[string]any)
	node, _ := st["node"].(map[string]any)
	shape := func(msg string) error {
		return fmt.Errorf("must be a single scalar expression over the columns of %s.%s (%s)", schema, table, msg)
	}
	if str(node["type"]) != "SELECT_NODE" {
		return shape("no set operations")
	}
	if l, _ := node["modifiers"].([]any); len(l) > 0 {
		return shape("no ORDER BY/LIMIT/DISTINCT")
	}
	if cm, _ := node["cte_map"].(map[string]any); cm != nil {
		if m, _ := cm["map"].([]any); len(m) > 0 {
			return shape("no WITH")
		}
	}
	for _, k := range []string{"where_clause", "having", "sample", "qualify"} {
		if node[k] != nil {
			return shape("no " + strings.ToUpper(strings.ReplaceAll(k, "_clause", "")))
		}
	}
	if l, _ := node["group_expressions"].([]any); len(l) > 0 {
		return shape("no GROUP BY")
	}
	if l, _ := node["group_sets"].([]any); len(l) > 0 {
		return shape("no GROUP BY")
	}
	list, _ := node["select_list"].([]any)
	if len(list) != 1 {
		return shape("exactly one expression")
	}
	from, _ := node["from_table"].(map[string]any)
	if str(from["type"]) != "BASE_TABLE" || str(from["catalog_name"]) != "" ||
		!strings.EqualFold(str(from["schema_name"]), schema) || !strings.EqualFold(str(from["table_name"]), table) {
		return shape("no other tables")
	}
	return walkPolicyExpr(list[0], table)
}

func walkPolicyExpr(n any, table string) error {
	switch v := n.(type) {
	case []any:
		for _, x := range v {
			if err := walkPolicyExpr(x, table); err != nil {
				return err
			}
		}
	case map[string]any:
		if _, ok := v["select_list"]; ok {
			return denied("subqueries are not allowed in a row filter or mask")
		}
		switch str(v["class"]) {
		case "SUBQUERY":
			return denied("subqueries are not allowed in a row filter or mask")
		case "STAR", "PARAMETER", "LAMBDA":
			return denied("%s is not allowed in a row filter or mask", strings.ToLower(str(v["class"])))
		case "FUNCTION", "WINDOW":
			if err := checkFunction(v); err != nil {
				return err
			}
		case "COLUMN_REF":
			names, _ := v["column_names"].([]any)
			if len(names) > 2 || len(names) == 2 && !strings.EqualFold(str(names[0]), table) {
				return denied("column reference to another table is not allowed")
			}
		}
		if str(v["type"]) == "TABLE_FUNCTION" {
			return denied("table functions are not allowed in a row filter or mask")
		}
		for _, x := range v {
			if err := walkPolicyExpr(x, table); err != nil {
				return err
			}
		}
	}
	return nil
}
