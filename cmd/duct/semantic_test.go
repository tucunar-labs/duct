package main

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/alonsoburon/duct/core/project"
)

const shopModel = `version: 0.2.0.dev0
name: shop
description: Tienda
datasets:
  - name: customers
    source: gold.customers
    primary_key: [id]
    fields:
      - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
      - {name: region, description: Región, expression: {dialects: [{dialect: ANSI_SQL, expression: region}]}}
  - name: orders
    source: gold.orders
    primary_key: [id]
    fields:
      - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
      - {name: status, expression: {dialects: [{dialect: ANSI_SQL, expression: status}]}}
      - {name: amount, expression: {dialects: [{dialect: ANSI_SQL, expression: amount}]}}
  - name: order_items
    source: gold.order_items
    fields:
      - {name: id, expression: {dialects: [{dialect: ANSI_SQL, expression: id}]}}
      - {name: qty, expression: {dialects: [{dialect: ANSI_SQL, expression: qty}]}}
relationships:
  - {name: orders_customer, from: orders, to: customers, from_columns: [customer_id], to_columns: [id]}
  - {name: items_order, from: order_items, to: orders, from_columns: [order_id], to_columns: [id]}
metrics:
  - name: revenue
    description: Ventas
    expression: {dialects: [{dialect: ANSI_SQL, expression: SUM(orders.amount)}]}
    ai_context: {synonyms: [ventas]}
  - name: units
    expression: {dialects: [{dialect: ANSI_SQL, expression: SUM(order_items.qty)}]}
`

// semanticProject: gold tables, a model, and roles with different views.
func semanticProject(t *testing.T) string {
	t.Helper()
	root := newProject(t)
	q := func(sql string) { mustDuct(t, "query", "--project", root, sql) }
	q("CREATE SCHEMA gold")
	q("CREATE TABLE gold.customers AS SELECT * FROM (VALUES (1,'CL'),(2,'AR'),(3,'CL')) t(id, region)")
	q("CREATE TABLE gold.orders AS SELECT * FROM (VALUES (10,1,100,'paid'),(11,1,50,'paid'),(12,2,200,'paid'),(13,3,300,'void')) t(id, customer_id, amount, status)")
	// Order 10 has two lines and 11 one: a plain join would count 100 twice.
	q("CREATE TABLE gold.order_items AS SELECT * FROM (VALUES (1,10,1),(2,10,2),(3,11,5),(4,12,1),(5,13,7)) t(id, order_id, qty)")
	writeFile(t, filepath.Join(root, "semantic", "shop.ossie.yaml"), shopModel)

	g := func(args ...string) { mustDuct(t, append(args, "--project", root)...) }
	for _, r := range []string{"jefes", "analistas", "limitado"} {
		g("role", "create", r)
	}
	for _, r := range []string{"jefes", "analistas"} {
		for _, tb := range []string{"customers", "orders", "order_items"} {
			g("grant", "select", "on", "gold."+tb, "to", r)
		}
	}
	g("grant", "select", "on", "gold.orders", "to", "limitado")
	g("policy", "row-filter", "gold.orders", "--role", "analistas", "--where", "status = 'paid'")
	g("user", "create", "jefa", "--role", "jefes", "--password", "pw-jefa-1234")
	g("user", "create", "ana", "--role", "analistas", "--password", "pw-ana-12345")
	g("user", "create", "lim", "--role", "limitado", "--password", "pw-lim-12345")
	return root
}

func TestSemanticCLI(t *testing.T) {
	root := semanticProject(t)
	if out := mustDuct(t, "semantic", "validate", "--project", root); !strings.Contains(out, "ok: 1 documents, 3 datasets, 2 relationships, 2 metrics") {
		t.Errorf("validate: %q", out)
	}
	// Invalid model: the CLI fails with the schema error.
	bad := filepath.Join(t.TempDir(), "bad.ossie.yaml")
	writeFile(t, bad, "version: 0.2.0.dev0\nname: x\n")
	if _, errs, code := duct(t, "semantic", "validate", bad); code == 0 || !strings.Contains(errs, "datasets") {
		t.Errorf("bad model: code=%d %q", code, errs)
	}
	list := mustDuct(t, "semantic", "list", "--project", root, "--role", "limitado")
	if !strings.Contains(list, "revenue") || strings.Contains(list, "units") {
		t.Errorf("list for limitado: %q", list)
	}
	// Two facts, shared dimension: 450/15 for CL, 200/1 for AR (not inflated by the item lines).
	out := mustDuct(t, "semantic", "query", "--project", root, "--role", "jefes", "--dims", "region",
		"--measures", "revenue,units", "--format", "csv")
	if !strings.Contains(out, "CL,450,15") || !strings.Contains(out, "AR,200,1") {
		t.Errorf("fan-out safe result: %q", out)
	}
	// Row filter of the role applies: only paid orders.
	out = mustDuct(t, "semantic", "query", "--project", root, "--role", "analistas", "--dims", "status",
		"--measures", "revenue", "--format", "csv")
	if strings.Contains(out, "void") || !strings.Contains(out, "paid,350") {
		t.Errorf("role filter: %q", out)
	}
	out = mustDuct(t, "semantic", "query", "--project", root, "--role", "jefes", "--measures", "revenue",
		"--filter", "region=CL", "--filter", "status in paid,void", "--format", "csv")
	if !strings.Contains(out, "450") {
		t.Errorf("filters: %q", out)
	}
	// A role without the views cannot compile the query.
	_, errs, code := duct(t, "semantic", "query", "--project", root, "--role", "limitado", "--dims", "region", "--measures", "revenue")
	if code == 0 || !strings.Contains(errs, "not granted") {
		t.Errorf("role without grant: code=%d %q", code, errs)
	}
	if out := mustDuct(t, "semantic", "build", "--project", root, "--role", "jefes"); !strings.Contains(out, `"role_jefes"."orders"`) {
		t.Errorf("build: %q", out)
	}
}

type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if b.token != "" {
		r.Header.Set("Authorization", "Bearer "+b.token)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func mcpConnect(ctx context.Context, addr, token string) (*sdk.ClientSession, error) {
	c := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "0"}, nil)
	return c.Connect(ctx, &sdk.StreamableClientTransport{
		Endpoint:   "http://" + addr + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{token}},
		MaxRetries: -1,
	}, nil)
}

func callTool(t *testing.T, ctx context.Context, s *sdk.ClientSession, name string, args any) (map[string]any, string, bool) {
	t.Helper()
	res, err := s.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	var text string
	for _, c := range res.Content {
		if tc, ok := c.(*sdk.TextContent); ok {
			text += tc.Text
		}
	}
	var out map[string]any
	if res.StructuredContent != nil {
		b, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(b, &out)
	}
	return out, text, res.IsError
}

func TestE2EMCP(t *testing.T) {
	root := semanticProject(t)
	tok := func(user string) string {
		return strings.TrimSpace(mustDuct(t, "token", "create", "--user", user, "--project", root))
	}
	tokJefa, tokAna, tokLim := tok("jefa"), tok("ana"), tok("lim")

	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	p.Config.Serve.Modules = map[string]map[string]any{"mcp": {"sql": true}}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{Protocols: []string{"mcp"}, Listen: map[string]string{"mcp": "127.0.0.1:0"},
			Ready: func(a map[string]string) { ready <- a }})
	}()
	var addr string
	select {
	case a := <-ready:
		addr = a["mcp"]
	case err := <-done:
		t.Fatalf("serve exited: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("serve did not start")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("serve did not shut down")
		}
	}
	t.Cleanup(stop)
	cctx, ccancel := context.WithTimeout(ctx, 60*time.Second)
	defer ccancel()

	t.Run("unauthenticated", func(t *testing.T) {
		for _, tk := range []string{"", "not-a-token"} {
			if s, err := mcpConnect(cctx, addr, tk); err == nil {
				_ = s.Close()
				t.Errorf("token %q connected", tk)
			}
		}
		req, _ := http.NewRequest("POST", "http://"+addr+"/mcp", strings.NewReader(`{}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("no token: %d", resp.StatusCode)
		}
	})

	connect := func(t *testing.T, token string) *sdk.ClientSession {
		t.Helper()
		s, err := mcpConnect(cctx, addr, token)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	t.Run("tools and describe", func(t *testing.T) {
		s := connect(t, tokJefa)
		tl, err := s.ListTools(cctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, x := range tl.Tools {
			names = append(names, x.Name)
		}
		if strings.Join(names, ",") != "describe,list_metrics,query_metrics,sql" {
			t.Errorf("tools = %v", names)
		}
		out, _, isErr := callTool(t, cctx, s, "list_metrics", map[string]any{})
		if isErr || !strings.Contains(toJSON(out), "ventas") || !strings.Contains(toJSON(out), "customers.region") {
			t.Errorf("list_metrics: %v", out)
		}
		out, _, isErr = callTool(t, cctx, s, "describe", map[string]any{"name": "revenue"})
		if isErr || !strings.Contains(toJSON(out), `"datasets"`) {
			t.Errorf("describe: %v", out)
		}
	})

	t.Run("query_metrics no fan-out", func(t *testing.T) {
		s := connect(t, tokJefa)
		out, text, isErr := callTool(t, cctx, s, "query_metrics", map[string]any{
			"dims": []string{"region"}, "measures": []string{"revenue", "units"}})
		if isErr {
			t.Fatalf("error: %s", text)
		}
		rows := toJSON(out["rows"])
		if rows != `[["AR",200,1],["CL",450,15]]` {
			t.Errorf("rows = %s", rows)
		}
	})

	t.Run("row filter applies", func(t *testing.T) {
		s := connect(t, tokAna)
		out, text, isErr := callTool(t, cctx, s, "query_metrics", map[string]any{
			"dims": []string{"status"}, "measures": []string{"revenue"}})
		if isErr {
			t.Fatalf("error: %s", text)
		}
		if rows := toJSON(out["rows"]); rows != `[["paid",350]]` {
			t.Errorf("analistas must see only paid orders, got %s", rows)
		}
		// The structured filter works and cannot smuggle SQL.
		out, text, isErr = callTool(t, cctx, s, "query_metrics", map[string]any{
			"measures": []string{"revenue"},
			"filters":  []map[string]any{{"field": "region", "op": "=", "value": "CL' OR 1=1 --"}}})
		if isErr {
			t.Fatalf("error: %s", text)
		}
		if rows := toJSON(out["rows"]); rows != `[[null]]` {
			t.Errorf("injection attempt returned %s", rows)
		}
	})

	t.Run("role without grant", func(t *testing.T) {
		s := connect(t, tokLim)
		out, _, _ := callTool(t, cctx, s, "list_metrics", map[string]any{})
		if j := toJSON(out); !strings.Contains(j, "revenue") || strings.Contains(j, "units") || strings.Contains(j, "customers.") {
			t.Errorf("limitado sees %s", j)
		}
		_, text, isErr := callTool(t, cctx, s, "query_metrics", map[string]any{"dims": []string{"region"}, "measures": []string{"revenue"}})
		if !isErr || !strings.Contains(text, "not granted") {
			t.Errorf("isErr=%v %q", isErr, text)
		}
		_, text, isErr = callTool(t, cctx, s, "query_metrics", map[string]any{"measures": []string{"units"}})
		if !isErr || !strings.Contains(text, "not granted") {
			t.Errorf("isErr=%v %q", isErr, text)
		}
	})

	t.Run("fan-out request refused", func(t *testing.T) {
		s := connect(t, tokJefa)
		_, text, isErr := callTool(t, cctx, s, "query_metrics", map[string]any{"dims": []string{"order_items.qty"}, "measures": []string{"revenue"}})
		if !isErr || !strings.Contains(text, "many side") {
			t.Errorf("isErr=%v %q", isErr, text)
		}
	})

	t.Run("sql tool goes through the guard", func(t *testing.T) {
		s := connect(t, tokAna)
		out, text, isErr := callTool(t, cctx, s, "sql", map[string]any{"query": "SELECT count(*) AS n FROM orders"})
		if isErr || toJSON(out["rows"]) != `[[3]]` {
			t.Errorf("filtered count: isErr=%v %q %v", isErr, text, out)
		}
		for _, bad := range []string{"DROP TABLE orders", "SELECT * FROM gold.orders", "SELECT * FROM read_csv('/etc/passwd')"} {
			if _, text, isErr := callTool(t, cctx, s, "sql", map[string]any{"query": bad}); !isErr {
				t.Errorf("%q was allowed: %s", bad, text)
			}
		}
	})

	// Every call is in the audit log, as protocol mcp, with the right user.
	stop()
	rows := queryRows(t, root, "SELECT user_name, status, sql_text FROM duct.duct_audit WHERE protocol = 'mcp' ORDER BY occurred_at")
	var sawList, sawQuery, sawDenied bool
	for _, r := range rows {
		sql, _ := r["sql_text"].(string)
		switch {
		case strings.HasPrefix(sql, "mcp list_metrics") && r["user_name"] == "jefa":
			sawList = true
		case strings.Contains(sql, "role_ana") && r["status"] == "ok":
			sawQuery = true
		case strings.HasPrefix(sql, "mcp query_metrics") && r["user_name"] == "lim" && r["status"] == "error":
			sawDenied = true
		}
	}
	if !sawList || !sawQuery || !sawDenied {
		t.Errorf("audit incomplete (list=%v query=%v denied=%v): %v", sawList, sawQuery, sawDenied, rows)
	}
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
