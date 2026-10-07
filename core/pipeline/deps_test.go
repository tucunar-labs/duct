package pipeline

import (
	"reflect"
	"strings"
	"testing"
)

// ASTs are trimmed versions of what json_serialize_sql emits; the real thing
// is exercised end to end in cmd/duct (TestE2EPipelines) and in
// modules/engine-duckdb-backed tests there.
func TestRefsFromAST(t *testing.T) {
	ast := `{"error":false,"statements":[{"node":{"type":"SELECT_NODE",
	  "cte_map":{"map":[{"key":"Tmp","value":{"query":{"node":{"from_table":{"type":"BASE_TABLE","schema_name":"","table_name":"raw","catalog_name":""}}}}}]},
	  "from_table":{"type":"JOIN","left":{"type":"BASE_TABLE","schema_name":"","table_name":"tmp","catalog_name":""},
	    "right":{"type":"SUBQUERY","subquery":{"node":{"type":"SET_OPERATION_NODE","children":[
	      {"from_table":{"type":"BASE_TABLE","schema_name":"Silver","table_name":"A","catalog_name":"lake"}},
	      {"from_table":{"type":"BASE_TABLE","schema_name":"silver","table_name":"a","catalog_name":""}}]}}}},
	  "where_clause":{"type":"SUBQUERY","subquery":{"node":{"from_table":{"type":"BASE_TABLE","schema_name":"duct","table_name":"x","catalog_name":"duct"}}}},
	  "select_list":[{"type":"SUBQUERY","subquery":{"node":{"from_table":{"type":"TABLE_FUNCTION","function":{"function_name":"range"}}}}}]}}]}`
	got, err := RefsFromAST(ast)
	if err != nil {
		t.Fatal(err)
	}
	want := []Ref{{"main", "raw"}, {"silver", "a"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("refs = %v, want %v", got, want)
	}
}

func TestRefsFromASTErrors(t *testing.T) {
	if _, err := RefsFromAST(`{"error":true,"error_message":"Parser Error: syntax error"}`); err == nil || !strings.Contains(err.Error(), "syntax error") {
		t.Errorf("error AST: %v", err)
	}
	if _, err := RefsFromAST(`not json`); err == nil {
		t.Error("expected invalid JSON error")
	}
}
