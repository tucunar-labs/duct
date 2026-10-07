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

func TestDerivedAcrossFacts(t *testing.T) {
	m := shop(t)
	sc := Scope{Role: "analistas", Tables: allTables}
	mt, _ := m.Metric("revenue_per_unit")
	if !mt.Derived() || len(mt.Homes()) != 2 {
		t.Fatalf("derived=%v homes=%d", mt.Derived(), len(mt.Homes()))
	}
	c, err := m.Query(sc, Request{Dims: []string{"customers.region"}, Measures: []string{"revenue_per_unit"}, Order: []Order{{By: "revenue_per_unit", Desc: true}}})
	if err != nil {
		t.Fatal(err)
	}
	// Each base metric is aggregated on its own fact: no fan-out.
	for _, want := range []string{`FROM "orders"`, `FROM "order_items"`, "FULL OUTER JOIN", `("q0"."m0") / ("q1"."m1")`} {
		if !strings.Contains(c.SQL, want) {
			t.Errorf("SQL lacks %s:\n%s", want, c.SQL)
		}
	}
	golden(t, "derived_ratio", c.SQL)
	// Describe offers only the dimensions both facts share, and only to roles
	// that can read both.
	info := m.Describe(Scope{Role: "r", Tables: TableSet("orders")})
	for _, x := range info.Metrics {
		if x.Name == "revenue_per_unit" {
			t.Errorf("derived metric offered without a grant on order_items")
		}
	}
	if _, err := m.Query(Scope{Role: "r", Tables: TableSet("orders", "customers")}, Request{Measures: []string{"revenue_per_unit"}}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("derived without grant on a part: %v", err)
	}
	// A dimension only one fact reaches is refused (many side).
	if _, err := m.Query(sc, Request{Dims: []string{"order_items.sku"}, Measures: []string{"revenue_per_unit"}}); !errors.Is(err, ErrRequest) {
		t.Errorf("non conformed dimension: %v", err)
	}
}

func TestDerivedErrors(t *testing.T) {
	base, _ := os.ReadFile(filepath.Join("testdata", "shop.ossie.yaml"))
	doc := string(base)
	cases := []struct{ name, old, new, want string }{
		{"derived plus field", "revenue / units", "revenue / SUM(order_items.qty)", "spans datasets"},
		{"derived plus aggregate", "revenue / units", "SUM(revenue) / units", "spans datasets"},
	}
	for _, c := range cases {
		_, err := Parse("x.ossie.yaml", []byte(strings.Replace(doc, c.old, c.new, 1)))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
	}
}

func TestMetricFilters(t *testing.T) {
	m := shop(t)
	sc := Scope{Role: "analistas", Tables: allTables}
	c, err := m.Query(sc, Request{Dims: []string{"region"}, Measures: []string{"units"},
		Filters: []Filter{{Metric: "revenue", Op: ">=", Value: float64(1000)}, {Field: "status", Op: "=", Value: "paid"}}})
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "having", c.SQL)
	if got := outColumns(c); got != "region,units" {
		t.Errorf("columns = %s (helper columns must not leak)", got)
	}
	// Validation.
	bad := map[string]Filter{
		"both":         {Field: "region", Metric: "revenue", Op: "=", Value: 1.0},
		"unknown":      {Metric: "nope", Op: ">", Value: 1.0},
		"like":         {Metric: "revenue", Op: "like", Value: "1%"},
		"missing":      {Metric: "revenue", Op: ">"},
		"bad operator": {Metric: "revenue", Op: "; DROP", Value: 1.0},
		"neither":      {Op: "=", Value: 1.0},
	}
	for name, f := range bad {
		if _, err := m.Query(sc, Request{Measures: []string{"revenue"}, Filters: []Filter{f}}); !errors.Is(err, ErrRequest) {
			t.Errorf("%s: got %v, want ErrRequest", name, err)
		}
	}
	// A role without the metric's dataset cannot filter by it.
	if _, err := m.Query(Scope{Role: "r", Tables: TableSet("orders", "customers")}, Request{Measures: []string{"revenue"},
		Filters: []Filter{{Metric: "units", Op: ">", Value: 1.0}}}); !errors.Is(err, ErrUnavailable) {
		t.Errorf("filter by ungranted metric: %v", err)
	}
}

func outColumns(c *Compiled) string {
	var n []string
	for _, x := range c.Columns {
		n = append(n, x.Name)
	}
	return strings.Join(n, ",")
}

func TestPreaggregateRouting(t *testing.T) {
	m := shop(t)
	p, ok := m.Preagg("ventas_region")
	if !ok || len(p.Dims) != 2 || p.Refresh != RefreshOnSnapshot {
		t.Fatalf("preagg = %+v", p)
	}
	fresh := func() Scope {
		return Scope{Role: "ventas", Tables: allTables, Fresh: map[string]string{"ventas_region": PreaggTable("ventas", "ventas_region")}}
	}
	// Exact dimensions: read the table as it is.
	c, err := m.Query(fresh(), Request{Dims: []string{"customers.region", "orders.status"}, Measures: []string{"revenue", "avg_order"}})
	if err != nil || c.Preagg != "ventas_region" {
		t.Fatalf("exact: err=%v preagg=%q", err, c.Preagg)
	}
	golden(t, "preagg_exact", c.SQL)
	// Fewer dimensions with an additive measure: re-aggregate.
	c, err = m.Query(fresh(), Request{Dims: []string{"region"}, Measures: []string{"revenue"},
		Filters: []Filter{{Field: "orders.status", Op: "=", Value: "paid"}, {Metric: "revenue", Op: ">", Value: 10.0}}})
	if err != nil || c.Preagg == "" {
		t.Fatalf("rollup: err=%v", err)
	}
	golden(t, "preagg_rollup", c.SQL)
	if strings.Contains(c.SQL, `"role_ventas"`) || !strings.Contains(c.SQL, "SUM(") {
		t.Errorf("a routed query must read the table, not the views:\n%s", c.SQL)
	}
	// Not routable: non additive measure on fewer dimensions, a dimension the
	// table lacks, a measure it lacks, or the table is not fresh for the role.
	notRouted := map[string]Request{
		"distinct count rolled up": {Dims: []string{"region"}, Measures: []string{"order_count"}},
		"dimension not in table":   {Dims: []string{"month"}, Measures: []string{"revenue"}},
		"measure not in table":     {Dims: []string{"region"}, Measures: []string{"units"}},
		"filter on other field":    {Dims: []string{"region"}, Measures: []string{"revenue"}, Filters: []Filter{{Field: "created", Op: ">", Value: "2026-01-01"}}},
		"derived across facts":     {Dims: []string{"region"}, Measures: []string{"revenue_per_unit"}},
	}
	for name, req := range notRouted {
		c, err := m.Query(fresh(), req)
		if err != nil || c.Preagg != "" {
			t.Errorf("%s: err=%v preagg=%q", name, err, c.Preagg)
		}
	}
	stale := Scope{Role: "ventas", Tables: allTables}
	if c, _ := m.Query(stale, Request{Dims: []string{"customers.region", "orders.status"}, Measures: []string{"revenue"}}); c.Preagg != "" || !strings.Contains(c.SQL, `"role_ventas"."orders"`) {
		t.Errorf("not fresh: still routed\n%s", c.SQL)
	}
	// The build SQL reads the role views (so policies apply to the data).
	sql, err := m.PreaggSQL(Scope{Role: "ventas", Tables: allTables}, p)
	if err != nil || !strings.Contains(sql, `"role_ventas"."customers"`) || strings.Contains(sql, "LIMIT") {
		t.Errorf("build sql: %v\n%s", err, sql)
	}
}

func TestPreaggregateErrors(t *testing.T) {
	base, _ := os.ReadFile(filepath.Join("testdata", "shop.ossie.yaml"))
	doc := string(base)
	cases := []struct{ name, old, new, want string }{
		{"unknown dim", `"customers.region", "orders.status"`, `"customers.zip"`, "no field"},
		{"unknown measure", `"revenue", "order_count"`, `"nope"`, "unknown measure"},
		{"bad refresh", `"on_snapshot"`, `"hourly"`, "refresh must be"},
		{"no measures", `"measures": ["revenue", "order_count", "avg_order"]`, `"measures": []`, "at least one measure"},
		{"fan out", `"customers.region", "orders.status"`, `"order_items.sku"`, "many side"},
		{"unknown key", `"refresh"`, `"every"`, "unknown field"},
		{"bad name", `"name": "ventas_region"`, `"name": "Ventas Region"`, "the name must be"},
	}
	for _, c := range cases {
		if !strings.Contains(doc, c.old) {
			t.Fatalf("%s: pattern missing", c.name)
		}
		_, err := Parse("x.ossie.yaml", []byte(strings.Replace(doc, c.old, c.new, 1)))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
	}
}

func TestLiveReload(t *testing.T) {
	dir := t.TempDir()
	src, _ := os.ReadFile(filepath.Join("testdata", "shop.ossie.yaml"))
	write := func(s string) {
		if err := os.WriteFile(filepath.Join(dir, "shop.ossie.yaml"), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	l, err := NewLive(dir)
	if err != nil || l.Model() != nil {
		t.Fatalf("empty folder: model=%v err=%v", l.Model(), err)
	}
	var seen int
	l.OnChange(func(*Model) { seen++ })
	write(string(src))
	if ch, err := l.Reload(); !ch || err != nil || l.Model() == nil {
		t.Fatalf("first load: changed=%v err=%v", ch, err)
	}
	good := l.Model()
	if ch, _ := l.Reload(); ch {
		t.Error("unchanged content reported as a change")
	}
	// An invalid edit is rejected and the previous model keeps serving.
	write(strings.Replace(string(src), "SUM(orders.amount)}]}\n    ai_context", "SUM(orders.nope)}]}\n    ai_context", 1))
	if ch, err := l.Reload(); ch || err == nil {
		t.Fatalf("invalid model accepted: changed=%v err=%v", ch, err)
	}
	if l.Model() != good {
		t.Error("the invalid model replaced the good one")
	}
	if _, err := l.Reload(); err != nil {
		t.Errorf("the same broken content is reported again: %v", err)
	}
	// A valid edit swaps it in.
	write(strings.Replace(string(src), "name: revenue_per_unit", "name: rev_per_unit", 1))
	if ch, err := l.Reload(); !ch || err != nil {
		t.Fatalf("valid edit: changed=%v err=%v", ch, err)
	}
	if _, ok := l.Model().Metric("rev_per_unit"); !ok || l.Model() == good {
		t.Error("the new model is not live")
	}
	if seen != 2 {
		t.Errorf("subscribers called %d times, want 2", seen)
	}
}
