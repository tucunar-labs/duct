package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const revenueContract = `apiVersion: v3.2.0
kind: DataContract
id: 6f1c2a3e-0000-4000-8000-000000000002
name: gold_revenue
version: 1.0.0
status: active
servers:
  - server: lake
    type: custom
    customProperties:
      - property: ducklake_table
        value: gold.revenue
schema:
  - name: revenue
    physicalType: table
    properties:
      - name: customer
        logicalType: string
        primaryKey: true
      - name: total
        logicalType: number
        quality:
          - metric: nullValues
            mustBe: 0
            severity: warning
    quality:
      - metric: rowCount
        mustBeGreaterThan: 0
        severity: critical
      - type: sql
        name: no_negative_totals
        query: SELECT count(*) FROM {table} WHERE total < 0
        mustBe: 0
        severity: critical
roles:
  - role: analistas
    access: read
    customProperties:
      - property: duct.row_filter
        value: customer <> 'cy'
      - property: duct.mask.customer
        value: "left(customer, 1) || '***'"
slaProperties:
  - property: latency
    value: 1
    unit: d
`

func writeContractFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestE2EContracts: a Gold table governed by a contract: lint, enforcement in
// `duct run` (schema, critical rule keeps the previous version, warning is
// recorded), roles applied, test, freshness and breaking-change diff.
func TestE2EContracts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	q := func(sql string) { mustDuct(t, "query", "--project", root, sql) }
	q("CREATE SCHEMA bronze")
	q("CREATE TABLE bronze.orders (id INTEGER, customer VARCHAR, amount DOUBLE)")
	q("INSERT INTO bronze.orders VALUES (1,'ana',10),(2,'ana',5),(3,'bob',20),(4,'cy',7)")
	writePipeline(t, root, "gold.sql", `CREATE MATERIALIZED VIEW gold.revenue AS
SELECT customer, sum(amount) AS total FROM bronze.orders GROUP BY customer;`)
	writeContractFile(t, root, "contracts/revenue.odcs.yaml", revenueContract)

	// Lint before the first run: the table is a pipeline target, not in the lake yet.
	out := mustDuct(t, "contract", "lint", "--project", root)
	if !strings.Contains(out, "not materialized yet") || !strings.Contains(out, "1 contracts checked") {
		t.Errorf("lint: %s", out)
	}

	// compile prints without applying.
	out = mustDuct(t, "contract", "compile", "--project", root)
	for _, want := range []string{"gold.revenue", "customer.unique", "no_negative_totals", "GRANT SELECT ON gold.revenue TO analistas"} {
		if !strings.Contains(out, want) {
			t.Errorf("compile lacks %q:\n%s", want, out)
		}
	}
	if roles := mustDuct(t, "role", "list", "--project", root); strings.Contains(roles, "analistas") {
		t.Errorf("compile without --apply created a role: %s", roles)
	}

	// First run: refreshed, roles applied.
	out = mustDuct(t, "run", "--project", root)
	t.Logf("run:\n%s", out)
	if !strings.Contains(out, "SUCCESS  gold.revenue") || !strings.Contains(out, "grant select on gold.revenue to analistas") {
		t.Errorf("run: %s", out)
	}
	rows := queryRows(t, root, "SELECT customer FROM role_analistas.revenue ORDER BY customer")
	if len(rows) != 2 || rows[0]["customer"] != "a***" { // filter hides cy, mask hides names
		t.Errorf("role view = %v", rows)
	}
	if out = mustDuct(t, "contract", "lint", "--project", root, "--strict"); strings.Contains(out, "error") {
		t.Errorf("lint after run: %s", out)
	}
	out = mustDuct(t, "contract", "test", "--project", root)
	if !strings.Contains(out, "PASS  [critical] no_negative_totals") || strings.Contains(out, "FAIL") {
		t.Errorf("test: %s", out)
	}
	out = mustDuct(t, "contract", "freshness", "--project", root)
	if !strings.HasPrefix(out, "FRESH ") {
		t.Errorf("freshness: %s", out)
	}

	// Stale: the last refresh was two days ago.
	q("UPDATE duct.duct_pipeline_runs SET finished_at = CAST(CAST(finished_at AS TIMESTAMP) - INTERVAL 2 DAY AS VARCHAR)")
	out, _, code := duct(t, "contract", "freshness", "--project", root)
	if code != 1 || !strings.HasPrefix(out, "STALE ") {
		t.Errorf("stale freshness: code=%d %s", code, out)
	}
	out, _, code = duct(t, "contract", "freshness", "--project", root, "--json")
	if code != 1 || !strings.Contains(out, `"status": "stale"`) {
		t.Errorf("stale json: code=%d %s", code, out)
	}

	// A warning rule (NULL total) is recorded and the refresh goes on.
	q("INSERT INTO bronze.orders VALUES (5,'dan',NULL)")
	out = mustDuct(t, "run", "--project", root)
	if !strings.Contains(out, "SUCCESS  gold.revenue") || !strings.Contains(out, "contract:nullValues(total) [warn]") {
		t.Errorf("warning run: %s", out)
	}
	if n := count(t, root, "gold.revenue"); n != 4 {
		t.Errorf("gold rows = %v", n)
	}
	out, _, code = duct(t, "contract", "test", "--project", root)
	if code != 0 || !strings.Contains(out, "WARN  [warning] nullValues(total)") {
		t.Errorf("test with warning: code=%d %s", code, out)
	}

	// A critical rule fails: previous version kept, run exits 1, recorded.
	q("INSERT INTO bronze.orders VALUES (6,'bob',-100)")
	out, errs, code := duct(t, "run", "--project", root)
	t.Logf("critical run:\n%s", out)
	if code != 1 || !strings.Contains(out, "FAILED   gold.revenue") || !strings.Contains(out, "no_negative_totals = 1, must be = 0") ||
		!strings.Contains(out, "previous version kept") || !strings.Contains(errs, "failures") {
		t.Errorf("critical: code=%d out=%s err=%s", code, out, errs)
	}
	rows = queryRows(t, root, "SELECT total FROM gold.revenue WHERE customer = 'bob'")
	if rows[0]["total"].(float64) != 20 {
		t.Errorf("gold changed despite the critical rule: %v", rows)
	}
	rec := queryRows(t, root, "SELECT error FROM duct.duct_pipeline_runs WHERE status = 'failed'")
	if len(rec) != 1 || !strings.Contains(rec[0]["error"].(string), "contract contracts/revenue.odcs.yaml violated") {
		t.Errorf("recorded = %v", rec)
	}
	if _, _, code = duct(t, "contract", "test", "--project", root); code != 0 {
		t.Errorf("lake still satisfies the contract, code=%d", code)
	}
	q("DELETE FROM bronze.orders WHERE id = 6")

	// Schema drift in the pipeline: a column the contract does not declare.
	writePipeline(t, root, "gold.sql", `CREATE MATERIALIZED VIEW gold.revenue AS
SELECT customer, sum(amount) AS total, count(*) AS n FROM bronze.orders GROUP BY customer;`)
	out, _, code = duct(t, "run", "--project", root)
	if code != 1 || !strings.Contains(out, `column "n" (BIGINT) is in the table but not in the contract`) {
		t.Errorf("schema drift: code=%d %s", code, out)
	}
	writePipeline(t, root, "gold.sql", `CREATE MATERIALIZED VIEW gold.revenue AS
SELECT customer, CAST(sum(amount) AS VARCHAR) AS total FROM bronze.orders GROUP BY customer;`)
	out, _, code = duct(t, "run", "--project", root)
	if code != 1 || !strings.Contains(out, `column "total" has type VARCHAR, the contract says number`) {
		t.Errorf("type drift: code=%d %s", code, out)
	}

	// Diff against git: a breaking change exits 1, a compatible one does not.
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root, "-c", "user.name=t", "-c", "user.email=t@t"}, args...)...)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Skipf("git unavailable: %v %s", err, b)
		}
	}
	git("init", "-q")
	git("add", "contracts")
	git("commit", "-q", "-m", "contract")
	if out, _, code := duct(t, "contract", "diff", "--project", root); code != 0 || !strings.Contains(out, "unchanged") {
		t.Errorf("diff clean: code=%d %s", code, out)
	}
	writeContractFile(t, root, "contracts/revenue.odcs.yaml", strings.Replace(revenueContract, "        logicalType: number\n", "        logicalType: integer\n", 1))
	out, _, code = duct(t, "contract", "diff", "--project", root)
	if code != 1 || !strings.Contains(out, "BREAKING") || !strings.Contains(out, "logicalType") {
		t.Errorf("diff breaking: code=%d %s", code, out)
	}
	writeContractFile(t, root, "contracts/revenue.odcs.yaml", strings.Replace(revenueContract, "        logicalType: number\n", "        logicalType: number\n        description: Net revenue\n", 1))
	if out, _, code = duct(t, "contract", "diff", "--project", root, "--base", "HEAD"); code != 0 || !strings.Contains(out, "changed") {
		t.Errorf("diff compatible: code=%d %s", code, out)
	}
	if err := os.Remove(filepath.Join(root, "contracts/revenue.odcs.yaml")); err != nil {
		t.Fatal(err)
	}
	if out, _, code = duct(t, "contract", "diff", "--project", root); code != 1 || !strings.Contains(out, "removed") {
		t.Errorf("diff removed: code=%d %s", code, out)
	}
}

// TestE2EContractLintErrors: duct-specific lint findings.
func TestE2EContractLintErrors(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	mustDuct(t, "query", "--project", root, "CREATE SCHEMA gold")
	mustDuct(t, "query", "--project", root, "CREATE TABLE gold.revenue (customer VARCHAR, total VARCHAR)")
	writeContractFile(t, root, "contracts/revenue.odcs.yaml", revenueContract)
	out, _, code := duct(t, "contract", "lint", "--project", root)
	if code != 1 || !strings.Contains(out, `column "total" has type VARCHAR, the contract says number`) {
		t.Errorf("lint type: code=%d %s", code, out)
	}
	writeContractFile(t, root, "contracts/ghost.odcs.yaml", strings.ReplaceAll(strings.Replace(revenueContract, "id: 6f1c2a3e-0000-4000-8000-000000000002", "id: 6f1c2a3e-0000-4000-8000-000000000003", 1), "gold.revenue", "gold.ghost"))
	out, _, code = duct(t, "contract", "lint", "--project", root, "contracts/ghost.odcs.yaml")
	if code != 1 || !strings.Contains(out, "neither a pipeline target nor a table in the lake") {
		t.Errorf("lint ghost: code=%d %s", code, out)
	}
	writeContractFile(t, root, "contracts/bad.odcs.yaml", "apiVersion: v3.2.0\nkind: DataContract\n")
	if out, _, code = duct(t, "contract", "lint", "--project", root, "contracts/bad.odcs.yaml"); code != 1 || !strings.Contains(out, "error") {
		t.Errorf("lint invalid: code=%d %s", code, out)
	}
}
