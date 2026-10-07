package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writePipeline(t *testing.T, root, name, sql string) {
	t.Helper()
	p := filepath.Join(root, "pipelines", name)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(sql), 0o600); err != nil {
		t.Fatal(err)
	}
}

// queryRows runs `duct query --format json` and decodes the rows.
func queryRows(t *testing.T, root, sql string) []map[string]any {
	t.Helper()
	out := mustDuct(t, "query", "--project", root, "--format", "json", sql)
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("query %q: %v (%s)", sql, err, out)
	}
	return rows
}

func count(t *testing.T, root, table string) float64 {
	t.Helper()
	return queryRows(t, root, "SELECT count(*) AS n FROM "+table)[0]["n"].(float64)
}

// TestE2EPipelines: bronze -> silver -> gold with a DROP ROW, a warn and a
// FAIL UPDATE that triggers on the second run.
func TestE2EPipelines(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	q := func(sql string) { mustDuct(t, "query", "--project", root, sql) }
	q("CREATE SCHEMA bronze")
	q("CREATE TABLE bronze.orders (id INTEGER, customer VARCHAR, amount DOUBLE)")
	q("INSERT INTO bronze.orders VALUES (1,'ana',10),(2,'ana',-5),(3,'bob',20),(4,'bob',NULL),(5,'cy',7)")

	writePipeline(t, root, "silver.sql", `-- clean orders
CREATE MATERIALIZED VIEW silver.orders (
  CONSTRAINT amount_positive EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT customer_short EXPECT (length(customer) < 3)
) AS
WITH src AS (SELECT * FROM bronze.orders)
SELECT id, customer, amount FROM src;
`)
	// Alphabetically first on purpose: dependencies, not file order, decide.
	writePipeline(t, root, "a/gold.sql", `CREATE MATERIALIZED VIEW gold.revenue (
  CONSTRAINT has_rows EXPECT (total < 1000) ON VIOLATION FAIL UPDATE
) AS SELECT customer, sum(amount) AS total FROM silver.orders GROUP BY customer;
CREATE STREAMING TABLE gold.stream_copy AS SELECT * FROM silver.orders;
`)

	out := mustDuct(t, "run", "--project", root)
	t.Logf("duct run:\n%s", out)
	for _, want := range []string{"SUCCESS  silver.orders", "3 rows (2 dropped)", "amount_positive [drop_row]: 2 violations",
		"customer_short [warn]: 4 violations", "SUCCESS  gold.revenue", "SUCCESS  gold.stream_copy", "3 refreshed, 0 failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("run output lacks %q", want)
		}
	}
	if strings.Index(out, "silver.orders") > strings.Index(out, "gold.revenue") {
		t.Error("silver must run before gold")
	}
	if n := count(t, root, "silver.orders"); n != 3 { // NULL and negative dropped; the warn keeps rows
		t.Errorf("silver rows = %v", n)
	}
	if n := count(t, root, "gold.revenue"); n != 3 {
		t.Errorf("gold rows = %v", n)
	}

	// New bronze data makes a total exceed the FAIL UPDATE limit.
	q("INSERT INTO bronze.orders VALUES (6,'ana',5000)")
	out, errs, code := duct(t, "run", "--project", root)
	t.Logf("duct run (failing):\n%s\nstderr: %s", out, errs)
	if code != 1 {
		t.Fatalf("code = %d", code)
	}
	// stream_copy does not depend on gold.revenue, so it is refreshed.
	for _, want := range []string{"FAILED   gold.revenue", "has_rows [fail_update]: 1 violations", "previous version kept", "SUCCESS  gold.stream_copy", "2 refreshed, 1 failed"} {
		if !strings.Contains(out, want) {
			t.Errorf("run output lacks %q", want)
		}
	}
	if !strings.Contains(errs, "failures") {
		t.Errorf("stderr = %q", errs)
	}
	if n := count(t, root, "silver.orders"); n != 4 {
		t.Errorf("silver rows after = %v", n)
	}
	rows := queryRows(t, root, "SELECT total FROM gold.revenue WHERE customer = 'ana'")
	if rows[0]["total"].(float64) != 10 { // gold keeps its previous version
		t.Errorf("gold was updated despite FAIL UPDATE: %v", rows)
	}
	if n := count(t, root, "gold.stream_copy"); n != 4 {
		t.Errorf("stream_copy rows = %v", n)
	}

	// Recorded in the Duct table.
	rec := queryRows(t, root, "SELECT target, status, rows_written, rows_dropped FROM duct.duct_pipeline_runs WHERE status <> 'success'")
	if len(rec) != 1 || rec[0]["target"] != "gold.revenue" || rec[0]["status"] != "failed" {
		t.Errorf("runs = %v", rec)
	}
	if n := count(t, root, "duct.duct_pipeline_runs"); n != 6 {
		t.Errorf("duct_pipeline_runs rows = %v", n)
	}
	exp := queryRows(t, root, "SELECT expectations FROM duct.duct_pipeline_runs WHERE target = 'silver.orders' ORDER BY run_id LIMIT 1")
	if !strings.Contains(exp[0]["expectations"].(string), `"count":2`) {
		t.Errorf("expectations json = %v", exp)
	}

	// Targets: with upstream by default, --only skips it.
	q("DROP TABLE gold.stream_copy")
	out = mustDuct(t, "run", "--project", root, "--only", "gold.stream_copy")
	if strings.Contains(out, "silver.orders") || !strings.Contains(out, "1 refreshed") {
		t.Errorf("--only output: %s", out)
	}
	out = mustDuct(t, "run", "--project", root, "gold.stream_copy")
	if !strings.Contains(out, "silver.orders") || !strings.Contains(out, "2 refreshed") {
		t.Errorf("upstream output: %s", out)
	}
	if _, errs, code := duct(t, "run", "--project", root, "nope.x"); code != 1 || !strings.Contains(errs, "unknown pipeline target") {
		t.Errorf("unknown target: %d %q", code, errs)
	}
}

func TestE2EPipelineCycleAndSyntax(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	writePipeline(t, root, "c.sql", `CREATE MATERIALIZED VIEW s.a AS SELECT * FROM s.b;
CREATE MATERIALIZED VIEW s.b AS SELECT * FROM s.a;`)
	if _, errs, code := duct(t, "run", "--project", root); code != 1 || !strings.Contains(errs, "dependency cycle: s.a -> s.b -> s.a") {
		t.Errorf("cycle: code=%d err=%q", code, errs)
	}
	writePipeline(t, root, "c.sql", "CREATE MATERIALIZED VIEW s.a AS SELEC 1;")
	if _, errs, code := duct(t, "run", "--project", root); code != 1 || !strings.Contains(errs, "c.sql:1") {
		t.Errorf("syntax: code=%d err=%q", code, errs)
	}
	writePipeline(t, root, "c.sql", "CREATE MATERIALIZED VIEW s.a AS SELECT 1;")
	writePipeline(t, root, "d.sql", "CREATE MATERIALIZED VIEW s.a AS SELECT 2;")
	if _, errs, code := duct(t, "run", "--project", root); code != 1 || !strings.Contains(errs, "already defined") {
		t.Errorf("dup: code=%d err=%q", code, errs)
	}
}

func TestE2EPipelineSkipsDownstream(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	writePipeline(t, root, "p.sql", `CREATE MATERIALIZED VIEW s.a (CONSTRAINT never EXPECT (false) ON VIOLATION FAIL UPDATE) AS SELECT 1 AS x;
CREATE MATERIALIZED VIEW s.b AS SELECT * FROM s.a;
CREATE MATERIALIZED VIEW s.c AS SELECT 1 AS y;`)
	out, _, code := duct(t, "run", "--project", root)
	if code != 1 || !strings.Contains(out, "FAILED   s.a") || !strings.Contains(out, "SKIPPED  s.b") ||
		!strings.Contains(out, "upstream s.a did not refresh") || !strings.Contains(out, "SUCCESS  s.c") {
		t.Errorf("code=%d out=%s", code, out)
	}
	if rows := queryRows(t, root, "SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = 's'"); rows[0]["n"].(float64) != 1 {
		t.Errorf("only s.c should exist: %v", rows)
	}
}
