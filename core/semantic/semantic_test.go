package semantic

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func shop(t *testing.T) *Model {
	t.Helper()
	m, err := LoadFiles(filepath.Join("testdata", "shop.ossie.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

var allTables = TableSet("customers", "orders", "order_items")

func golden(t *testing.T, name, got string) {
	t.Helper()
	p := filepath.Join("testdata", name+".sql")
	if *update {
		if err := os.WriteFile(p, []byte(got+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(want)) != strings.TrimSpace(got) {
		t.Errorf("%s differs (run go test -update):\n--- got\n%s\n--- want\n%s", name, got, want)
	}
}

func TestGolden(t *testing.T) {
	m := shop(t)
	paid := Filter{Field: "orders.status", Op: "=", Value: "paid"}
	cases := []struct {
		name string
		req  Request
	}{
		{"single_fact", Request{Dims: []string{"region"}, Measures: []string{"revenue", "avg_order"},
			Filters: []Filter{paid}, Order: []Order{{By: "revenue", Desc: true}}, Limit: 10}},
		// Two facts (orders, order_items) sharing the customer dimension:
		// each is aggregated apart and the results are joined on the dims.
		{"fan_out_two_facts", Request{Dims: []string{"customers.region", "month"}, Measures: []string{"revenue", "units"}}},
		{"no_dims", Request{Measures: []string{"order_count"}}},
		{"chain_join", Request{Dims: []string{"customers.name"}, Measures: []string{"item_revenue"},
			Filters: []Filter{{Field: "region", Op: "in", Value: []any{"CL", "AR"}}}}},
		{"filters", Request{Measures: []string{"revenue"}, Filters: []Filter{
			{Field: "created", Op: "between", Value: []any{"2026-01-01", "2026-03-31"}},
			{Field: "customers.name", Op: "like", Value: "O'Brien%"},
			{Field: "orders.amount", Op: ">=", Value: float64(100)},
			{Field: "customers.region", Op: "is_not_null"},
			{Field: "status", Op: "not_in", Value: []any{"void", "'; DROP TABLE x; --"}},
		}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := m.Query(Scope{Role: "analistas", Tables: allTables}, c.req)
			if err != nil {
				t.Fatal(err)
			}
			golden(t, c.name, got.SQL)
		})
	}
}

func TestQueryReadsRoleViewsOnly(t *testing.T) {
	m := shop(t)
	got, err := m.Query(Scope{Role: "ventas"}, Request{Dims: []string{"region"}, Measures: []string{"revenue"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`"gold"`, `"lake"`, "gold."} {
		if strings.Contains(got.SQL, bad) {
			t.Errorf("SQL mentions %s:\n%s", bad, got.SQL)
		}
	}
	if !strings.Contains(got.SQL, `"role_ventas"."orders"`) || !strings.Contains(got.SQL, `"role_ventas"."customers"`) {
		t.Errorf("SQL does not read the role views:\n%s", got.SQL)
	}
}

func TestRoleWithoutGrant(t *testing.T) {
	m := shop(t)
	// The role sees orders but not customers.
	sc := Scope{Role: "r", Tables: TableSet("orders")}
	if _, err := m.Query(sc, Request{Measures: []string{"revenue"}}); err != nil {
		t.Fatalf("orders alone should work: %v", err)
	}
	_, err := m.Query(sc, Request{Dims: []string{"region"}, Measures: []string{"revenue"}})
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "customers") {
		t.Errorf("dimension without grant: %v", err)
	}
	_, err = m.Query(Scope{Role: "r", Tables: TableSet("customers")}, Request{Measures: []string{"revenue"}})
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("measure without grant: %v", err)
	}
	// A path through an unreadable dataset is refused too.
	_, err = m.Query(Scope{Role: "r", Tables: TableSet("order_items", "customers")},
		Request{Dims: []string{"region"}, Measures: []string{"item_revenue"}})
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "orders") {
		t.Errorf("path through ungranted dataset: %v", err)
	}
	// Describe only shows what the role can read.
	info := m.Describe(sc)
	if len(info.Datasets) != 1 || info.Datasets[0].Name != "orders" {
		t.Errorf("datasets = %+v", info.Datasets)
	}
	names := []string{}
	for _, x := range info.Metrics {
		names = append(names, x.Name)
	}
	if strings.Join(names, ",") != "revenue,order_count,avg_order" {
		t.Errorf("metrics = %v", names)
	}
	for _, d := range info.Metrics[0].Dimensions {
		if strings.HasPrefix(d, "customers.") {
			t.Errorf("dimension %s offered without grant", d)
		}
	}
}

func TestFanOutRefused(t *testing.T) {
	m := shop(t)
	sc := Scope{Role: "r", Tables: allTables}
	// revenue lives on orders; sku is on order_items (the many side): grouping
	// would repeat each order once per line.
	_, err := m.Query(sc, Request{Dims: []string{"order_items.sku"}, Measures: []string{"revenue"}})
	if !errors.Is(err, ErrRequest) || !strings.Contains(err.Error(), "many side") {
		t.Errorf("group by many side: %v", err)
	}
	_, err = m.Query(sc, Request{Measures: []string{"revenue"}, Filters: []Filter{{Field: "order_items.sku", Op: "=", Value: "A"}}})
	if !errors.Is(err, ErrRequest) || !strings.Contains(err.Error(), "many side") {
		t.Errorf("filter on many side: %v", err)
	}
	// The other direction is fine: item_revenue by a dimension of orders.
	if _, err := m.Query(sc, Request{Dims: []string{"orders.status"}, Measures: []string{"item_revenue"}}); err != nil {
		t.Errorf("many to one: %v", err)
	}
}

func TestRequestErrors(t *testing.T) {
	m := shop(t)
	sc := Scope{Role: "r", Tables: allTables}
	cases := map[string]Request{
		"unknown measure":    {Measures: []string{"nope"}},
		"no measures":        {Dims: []string{"region"}},
		"unknown field":      {Dims: []string{"zip"}, Measures: []string{"revenue"}},
		"hidden field":       {Dims: []string{"orders.secret_note"}, Measures: []string{"revenue"}},
		"ambiguous":          {Dims: []string{"id"}, Measures: []string{"revenue"}},
		"bad operator":       {Measures: []string{"revenue"}, Filters: []Filter{{Field: "region", Op: "; DROP", Value: "x"}}},
		"missing value":      {Measures: []string{"revenue"}, Filters: []Filter{{Field: "region", Op: "="}}},
		"object value":       {Measures: []string{"revenue"}, Filters: []Filter{{Field: "region", Op: "=", Value: map[string]any{"a": 1}}}},
		"empty in":           {Measures: []string{"revenue"}, Filters: []Filter{{Field: "region", Op: "in", Value: []any{}}}},
		"limit over max":     {Measures: []string{"revenue"}, Limit: 5001},
		"order not selected": {Measures: []string{"revenue"}, Order: []Order{{By: "units"}}},
		"repeated dimension": {Dims: []string{"region", "customers.region"}, Measures: []string{"revenue"}},
	}
	for name, req := range cases {
		if _, err := m.Query(sc, req); !errors.Is(err, ErrRequest) {
			t.Errorf("%s: got %v, want ErrRequest", name, err)
		}
	}
	if _, err := m.Query(Scope{Role: "Robert'; DROP"}, Request{Measures: []string{"revenue"}}); err == nil {
		t.Error("role name is not validated")
	}
}

func TestDefaultLimitFromExtension(t *testing.T) {
	m := shop(t)
	got, err := m.Query(Scope{Role: "r"}, Request{Measures: []string{"revenue"}})
	if err != nil || !strings.HasSuffix(got.SQL, "LIMIT 500") {
		t.Errorf("err=%v sql=%s", err, got)
	}
}

func TestParseErrors(t *testing.T) {
	base, err := os.ReadFile(filepath.Join("testdata", "shop.ossie.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	doc := string(base)
	cases := []struct{ name, old, new, want string }{
		{"version", "version: 0.2.0.dev0", "version: \"9.9.0\"", "unsupported Ossie version"},
		{"schema", "    source: gold.orders\n", "    sauce: gold.orders\n", "Ossie schema"},
		{"cross dataset", "SUM(orders.amount)}]}\n    ai_context", "SUM(orders.amount) + SUM(customers.id)}]}\n    ai_context", "spans datasets"},
		{"no aggregate", "SUM(order_items.qty)", "order_items.qty", "no aggregate"},
		{"unknown field", "SUM(order_items.qty)", "SUM(order_items.nope)", "no field"},
		{"cycle", "expression: revenue / order_count", "expression: avg_order + SUM(orders.amount)", "cycle"},
		{"bare column", "SUM(order_items.qty)", "SUM(qty)", "neither a function"},
		{"query source", "source: gold.orders", "source: SELECT 1", "table reference"},
		{"statement separator", "SUM(order_items.qty)", "SUM(order_items.qty); DROP TABLE x", "not allowed"},
		{"fan-out key", "primary_key: [id]\n    description: Clientes", "primary_key: [name]\n    description: Clientes", "duplicate rows"},
		{"duct extension", `{"hidden": true}`, `{"hide": true}`, "unknown field"},
	}
	for _, c := range cases {
		if !strings.Contains(doc, c.old) {
			t.Fatalf("%s: pattern not in the model", c.name)
		}
		_, err := Parse("x.ossie.yaml", []byte(strings.Replace(doc, c.old, c.new, 1)))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
	}
}

func TestMergeDuplicates(t *testing.T) {
	p := filepath.Join("testdata", "shop.ossie.yaml")
	if _, err := LoadFiles(p, p); err == nil || !strings.Contains(err.Error(), "defined twice") {
		t.Errorf("got %v", err)
	}
}

func TestTokenizeRefusesComments(t *testing.T) {
	for _, s := range []string{"a -- x", "a /* x */", "a; b", "a `b`", "$$x$$"} {
		if _, err := tokenize(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}
