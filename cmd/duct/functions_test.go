package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// pythonEnv points DUCT_PYTHON at the workspace venv of python/ (duct, pyarrow,
// marimo, psycopg installed); the test is skipped without it.
func pythonEnv(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	py := filepath.Join(filepath.Dir(file), "..", "..", "python", ".venv", "bin", "python")
	if v := os.Getenv("DUCT_TEST_PYTHON"); v != "" {
		py = v
	}
	if _, err := os.Stat(py); err != nil {
		t.Skip("python/.venv not found: run `cd python && uv sync` (or set DUCT_TEST_PYTHON)")
	}
	t.Setenv("DUCT_PYTHON", py)
	return py
}

const testFunctionsPy = `
import os
import duct

@duct.function
def inc(x: int) -> int:
    return x + 1

@duct.function(vectorized=True, types=["DOUBLE"], returns="DOUBLE")
def vsq(a):
    import pyarrow.compute as pc
    return pc.multiply(a, a)

@duct.function(returns="VARCHAR")
def leak(x: int):
    return ",".join(sorted(k for k in os.environ if k.startswith(("DUCT_", "AWS_"))))

@duct.function
def boom(x: int) -> int:
    raise ValueError("kapow")
`

func writeFunctions(t *testing.T, root string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "functions", "f.py"), testFunctionsPy)
	writeFile(t, filepath.Join(root, "functions", "up.toml"), "command = [\"tr\", \"a-z\", \"A-Z\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\n")
}

func TestE2EFunctions(t *testing.T) {
	pythonEnv(t)
	root := newProject(t)
	writeFunctions(t, root)

	out := mustDuct(t, "functions", "list", "--project", root)
	for _, want := range []string{"inc", "python (vectorized)", "vsq", "up", "exec", "functions/f.py", "functions/up.toml"} {
		if !strings.Contains(out, want) {
			t.Errorf("functions list lacks %q:\n%s", want, out)
		}
	}
	if got := queryCSV(t, root, "SELECT inc(41) AS a, vsq(1.5) AS b, up('hi') AS c, inc(NULL) AS d"); got != "a,b,c,d\n42,2.25,HI," {
		t.Fatalf("query = %q", got)
	}
	// Many chunks, NULLs, parallel scans.
	mustDuct(t, "query", "--project", root, "CREATE TABLE nums AS SELECT i, CASE WHEN i % 10 = 0 THEN NULL ELSE i END AS j FROM range(50000) t(i)")
	if got := queryCSV(t, root, "SELECT count(inc(j)) AS n, sum(inc(j)) AS s FROM nums"); got != "n,s\n45000,1125045000" {
		t.Fatalf("aggregate = %q", got)
	}
	// A function error reaches the user with its reason.
	if _, errs, code := duct(t, "query", "--project", root, "SELECT boom(1)"); code == 0 || !strings.Contains(errs, "ValueError: kapow") {
		t.Fatalf("boom: code=%d err=%q", code, errs)
	}
	// Pipelines can use them.
	writePipeline(t, root, "gold.sql", "CREATE MATERIALIZED VIEW gold.plus AS SELECT i, inc(i) AS i1, up('x') AS u FROM nums WHERE i < 3;\n")
	mustDuct(t, "run", "--project", root)
	if got := queryCSV(t, root, "SELECT i1 FROM gold.plus ORDER BY i"); got != "i1\n1\n2\n3" {
		t.Fatalf("pipeline = %q", got)
	}
	// Overhead numbers (reported by the bench; here only that it runs).
	bench := mustDuct(t, "functions", "bench", "--project", root, "--rows", "20000", "inc", "vsq", "up")
	for _, want := range []string{"PER ROW", "inc", "vsq", "up"} {
		if !strings.Contains(bench, want) {
			t.Errorf("bench lacks %q:\n%s", want, bench)
		}
	}
	// duct gc leaves the project's other folders alone (MLflow artifacts live in lake/_mlflow).
	writeFile(t, filepath.Join(root, "lake", "_mlflow", "1", "art.txt"), "keep me")
	mustDuct(t, "gc", "--project", root, "--keep-snapshots", "1s")
	if b, err := os.ReadFile(filepath.Join(root, "lake", "_mlflow", "1", "art.txt")); err != nil || string(b) != "keep me" {
		t.Fatalf("gc touched lake/_mlflow: %v %q", err, b)
	}
}

func TestFunctionsBrokenDoesNotBlockQueries(t *testing.T) {
	root := newProject(t)
	writeFile(t, filepath.Join(root, "functions", "f.py"), "import duct\n")
	t.Setenv("DUCT_PYTHON", "false") // cannot load anything
	out, errs, code := duct(t, "query", "--project", root, "--format", "csv", "SELECT 1 AS x")
	if code != 0 || !strings.Contains(out, "x\n1") { // the warning goes to the process stderr
		t.Fatalf("code=%d out=%q err=%q", code, out, errs)
	}
	if _, errs, code := duct(t, "functions", "list", "--project", root); code == 0 || !strings.Contains(errs, "cannot load functions") {
		t.Fatalf("functions list should fail loudly: code=%d err=%q", code, errs)
	}
}

// TestE2EServeFunctionsAdminOnly: user code runs in duct serve only for duct_admin; the
// guard refuses it for every other role in any position of the statement.
func TestE2EServeFunctionsAdminOnly(t *testing.T) {
	pythonEnv(t)
	t.Setenv("DUCT_S3_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("DUCT_CATALOG_PASSWORD", "super-secret")
	root := governedProject(t)
	writeFunctions(t, root)
	g := func(args ...string) string { return mustDuct(t, append(args, "--project", root)...) }
	g("role", "create", "duct_admin")
	g("grant", "select", "on", "gold.customers", "to", "duct_admin")
	g("user", "create", "root", "--role", "duct_admin", "--password", "pw-root-123")
	tokAna := strings.TrimSpace(g("token", "create", "--user", "ana"))
	tokRoot := strings.TrimSpace(g("token", "create", "--user", "root"))
	srv := startServe(t, root, nil)

	post := func(sql, token string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest("POST", "http://"+srv.http+"/query", strings.NewReader(sql))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	for _, q := range []string{
		"SELECT inc(amt) FROM customers",
		"SELECT INC(1)",
		`SELECT "Inc"(1)`,
		"SELECT name FROM customers WHERE inc(amt) > 0",
		"SELECT * FROM (SELECT inc(amt) AS v FROM customers) s",
		"WITH c AS (SELECT inc(1) AS v) SELECT v FROM c",
		"SELECT sum(inc(amt)) OVER () FROM customers",
		"SELECT list_transform([1, 2], x -> inc(x))",
		"SELECT up(name) FROM customers",
		"SELECT vsq(1.0)",
		"SELECT leak(1)",
	} {
		code, body := post(q, tokAna)
		if code != 403 || !strings.Contains(body, "reserved to the duct_admin role") && !strings.Contains(body, "denied") {
			t.Errorf("analistas %q: %d %s (want 403)", q, code, body)
		}
	}
	// Plain queries of the same role still work.
	if code, body := post("SELECT name FROM customers ORDER BY name", tokAna); code != 200 || !strings.Contains(body, "Ana") {
		t.Fatalf("plain query: %d %s", code, body)
	}
	// The admin may.
	code, body := post("SELECT inc(amt) AS v, up(name) AS n, vsq(1.5) AS q FROM customers ORDER BY v FORMAT JSON", tokRoot)
	if code != 200 {
		t.Fatalf("admin: %d %s", code, body)
	}
	var js struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &js); err != nil || len(js.Data) != 3 || js.Data[0]["v"] != float64(101) || js.Data[0]["n"] != "ANA" || js.Data[0]["q"] != 2.25 {
		t.Fatalf("admin rows: %s (%v)", body, err)
	}
	// User code does not see the credentials of the server process.
	if code, body := post("SELECT leak(1) AS l FROM customers LIMIT 1 FORMAT JSON", tokRoot); code != 200 || !strings.Contains(body, `"l":""`) {
		t.Fatalf("worker environment leaks: %d %s", code, body)
	}
	// An error in user code is a clean error, and the next call works.
	if code, body := post("SELECT boom(amt) FROM customers", tokRoot); code == 200 || !strings.Contains(body, "kapow") {
		t.Fatalf("boom: %d %s", code, body)
	}
	if code, _ := post("SELECT inc(1)", tokRoot); code != 403 && code != 200 { // admin without grants may still compute
		t.Fatalf("after error: %d", code)
	}
	// Over the Postgres wire the functions do not exist at all (own DuckDB session).
	c, err := pgConnect(t, srv.pg, "root", "pw-root-123", "sslmode=disable")
	if err != nil {
		t.Fatalf("pg connect: %v", err)
	}
	defer func() { _ = c.Close(context.Background()) }()
	if rows, err := pgRows(t, c, "SELECT inc(1)"); err == nil {
		t.Errorf("pg must not run user functions, got %v", rows)
	}
}
