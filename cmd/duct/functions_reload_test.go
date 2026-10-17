package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const typesFunctionsPy = `
import datetime
import pyarrow as pa
import duct

@duct.function
def next_day(d: datetime.date) -> datetime.date:
    return d + datetime.timedelta(days=1)

@duct.function
def plus_hour(t: datetime.datetime) -> datetime.datetime:
    return t + datetime.timedelta(hours=1)

@duct.function(types=["DECIMAL(10,2)"], returns="DECIMAL(12,2)")
def double_price(p):
    return p * 2

@duct.function
def doubled(xs: list[int]) -> list[int]:
    return [None if x is None else x * 2 for x in xs]

@duct.function(types=["STRUCT(a BIGINT, b VARCHAR)"], returns="STRUCT(n BIGINT, s VARCHAR[])")
def reshape(r):
    return {"n": r["a"] + 1, "s": [r["b"], r["b"].upper()]}

@duct.function(vectorized=True, types=["DECIMAL(10,2)", "DATE"], returns="VARCHAR")
def vdesc(p, d):
    return pa.array([f"{a}@{b}" for a, b in zip(p.to_pylist(), d.to_pylist(), strict=True)])

@duct.table_function(columns={"n": "BIGINT", "sq": "BIGINT", "d": "DATE"})
def squares(upto: int):
    for lo in range(0, upto, 1000):
        hi = min(lo + 1000, upto)
        yield pa.record_batch({
            "n": list(range(lo, hi)),
            "sq": [i * i for i in range(lo, hi)],
            "d": [datetime.date(2026, 1, 1) + datetime.timedelta(days=i % 3) for i in range(lo, hi)],
        })

@duct.table_function(columns=[("k", "VARCHAR"), ("v", "DOUBLE[]")], types=["VARCHAR"])
def kv(prefix):
    return [{"k": prefix + "1", "v": [1.5, 2.5]}, {"k": prefix + "2", "v": []}]
`

// TestE2EFunctionsTypesAndTables: DATE, TIMESTAMP, DECIMAL, lists and structs in
// Python (scalar and vectorized) and in executables, and Python table functions,
// from the CLI.
func TestE2EFunctionsTypesAndTables(t *testing.T) {
	t.Parallel()
	pythonEnv(t)
	root := newProject(t)
	writeFile(t, filepath.Join(root, "functions", "t.py"), typesFunctionsPy)
	writeFile(t, filepath.Join(root, "functions", "stamp.toml"),
		"command = [\"sed\", \"s/$/ UTC/\"]\nargs = [\"TIMESTAMP\"]\nreturns = \"VARCHAR\"\n")
	writeFile(t, filepath.Join(root, "functions", "summary.toml"),
		"command = [\"python3\", \"-c\", \"import sys, json\\nfor l in sys.stdin:\\n d, xs, s = json.loads(l)\\n print(json.dumps({'day': d, 'n': len(xs), 'p': s['p']}))\"]\n"+
			"args = [\"DATE\", \"DECIMAL(10,2)[]\", \"STRUCT(p DECIMAL(10,2))\"]\nreturns = \"STRUCT(day DATE, n BIGINT, p DECIMAL(10,2))\"\nformat = \"json\"\n")

	list := mustDuct(t, "functions", "list", "--project", root)
	for _, want := range []string{"next_day", "(DATE)", "DECIMAL(12,2)", "BIGINT[]", "STRUCT(n BIGINT, s VARCHAR[])", "python (table)", "TABLE(n BIGINT, sq BIGINT, d DATE)", "summary"} {
		if !strings.Contains(list, want) {
			t.Errorf("functions list lacks %q:\n%s", want, list)
		}
	}
	for q, want := range map[string]string{
		"SELECT next_day(DATE '2026-01-31') AS v":                                                                      "v\n2026-02-01",
		"SELECT plus_hour(TIMESTAMP '2026-01-31 23:30:00') AS v":                                                       "v\n2026-02-01T00:30:00",
		"SELECT double_price(12.34::DECIMAL(10,2)) AS v":                                                               "v\n24.68",
		"SELECT CAST(doubled([1, NULL, 3]) AS VARCHAR) AS v":                                                           "v\n\"[2, NULL, 6]\"",
		"SELECT CAST(reshape({'a': 1, 'b': 'x'}) AS VARCHAR) AS v":                                                     "v\n\"{'n': 2, 's': [x, X]}\"",
		"SELECT vdesc(1.50::DECIMAL(10,2), DATE '2026-03-04') AS v":                                                    "v\n1.50@2026-03-04",
		"SELECT stamp(TIMESTAMP '2026-01-31 12:30:00.5') AS v":                                                         "v\n2026-01-31 12:30:00.5 UTC",
		"SELECT CAST(summary(DATE '2026-01-31', [1.5::DECIMAL(10,2), 2], {'p': 3.25::DECIMAL(10,2)}) AS VARCHAR) AS v": "v\n\"{'day': 2026-01-31, 'n': 2, 'p': 3.25}\"",
		"SELECT count(*) AS n, sum(sq) AS s, count(DISTINCT d) AS days FROM squares(5000)":                             "n,s,days\n5000,41654167500,3",
		"SELECT k, CAST(v AS VARCHAR) AS v FROM kv('a') ORDER BY k":                                                    "k,v\na1,\"[1.5, 2.5]\"\na2,[]",
		"SELECT count(*) AS n FROM squares(10) s JOIN squares(5) USING (n)":                                            "n\n5",
	} {
		if got := queryCSV(t, root, q); got != want {
			t.Errorf("%s\n got %q\nwant %q", q, got, want)
		}
	}
	// Table functions are not served to roles (the guard denies them in duct serve) but a
	// pipeline can use them.
	writePipeline(t, root, "gold.sql", "CREATE MATERIALIZED VIEW gold.sq AS SELECT n, sq FROM squares(4);\n")
	mustDuct(t, "run", "--project", root)
	if got := queryCSV(t, root, "SELECT sum(sq) AS s FROM gold.sq"); got != "s\n14" {
		t.Fatalf("pipeline over a table function = %q", got)
	}
}

func postSQL(t *testing.T, addr, sql, token string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://"+addr+"/query", strings.NewReader(sql))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestServeFunctionsHotReload: duct serve follows functions/. A new body and a new
// name apply without a restart (and the new name is admin-only from its first
// call), a broken or incompatible edit is rejected and the old functions keep
// working, a removed one fails clearly, and a call in flight finishes on the old code.
func testServeFunctionsHotReload(t *testing.T) {
	pythonEnv(t)
	old := functionsReloadEvery
	functionsReloadEvery = 200 * time.Millisecond
	t.Cleanup(func() { functionsReloadEvery = old })
	root := governedProject(t)
	g := func(args ...string) string { return mustDuct(t, append(args, "--project", root)...) }
	g("role", "create", "duct_admin")
	g("user", "create", "root", "--role", "duct_admin", "--password", "pw-root-123")
	tokAna := strings.TrimSpace(g("token", "create", "--user", "ana"))
	tokRoot := strings.TrimSpace(g("token", "create", "--user", "root"))
	py := filepath.Join(root, "functions", "f.py")
	src := func(body string) string { return "import time, duct\n" + body }
	writeFile(t, py, src(`
@duct.function
def inc(x: int) -> int:
    return x + 1

@duct.function
def gone(x: int) -> int:
    return x

@duct.function
def nap(x: int) -> int:
    time.sleep(x)
    return 1
`))
	writeFile(t, filepath.Join(root, "functions", "up.toml"), "command = [\"tr\", \"a-z\", \"A-Z\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\n")
	srv := startServe(t, root, nil)
	defer srv.stop()
	val := func(sql, tok string) (int, string) {
		code, body := postSQL(t, srv.http, sql+" AS v FORMAT JSON", tok)
		return code, body
	}
	waitVal := func(what, sql, want string) {
		t.Helper()
		var last string
		waitFor(t, what, 60*time.Second, func() bool {
			code, body := val(sql, tokRoot)
			last = body
			return code == 200 && strings.Contains(body, `"v":`+want)
		})
		_ = last
	}
	if code, body := val("SELECT inc(1)", tokRoot); code != 200 || !strings.Contains(body, `"v":2`) {
		t.Fatalf("inc before: %d %s", code, body)
	}

	// 1. New body, new name, a removed function; the executable stays.
	writeFile(t, py, src(`
@duct.function
def inc(x: int) -> int:
    return x + 100

@duct.function
def dbl(x: int) -> int:
    return 2 * x

@duct.function
def nap(x: int) -> int:
    time.sleep(x)
    return 2
`))
	waitVal("new body", "SELECT inc(1)", "101")
	waitVal("new name", "SELECT dbl(4)", "8")
	waitVal("executable untouched", "SELECT up('x')", `"X"`)
	if code, body := val("SELECT gone(1)", tokRoot); code == 200 || !strings.Contains(body, "was removed") {
		t.Fatalf("removed function: %d %s", code, body)
	}
	// The governance guard covers the new name for other roles.
	for _, q := range []string{"SELECT dbl(1)", "SELECT inc(1)", "SELECT up('a')"} {
		if code, body := val(q, tokAna); code != 403 {
			t.Errorf("analistas %q after reload: %d %s (want 403)", q, code, body)
		}
	}

	// 2. A call in flight during a reload finishes on the old code.
	type res struct {
		code int
		body string
	}
	inflight := make(chan res, 1)
	go func() {
		c, b := val("SELECT nap(3)", tokRoot)
		inflight <- res{c, b}
	}()
	time.Sleep(1500 * time.Millisecond)
	writeFile(t, py, src(`
@duct.function
def inc(x: int) -> int:
    return x + 200

@duct.function
def dbl(x: int) -> int:
    return 2 * x

@duct.function
def nap(x: int) -> int:
    time.sleep(x)
    return 3
`))
	waitVal("reload while a call is running", "SELECT inc(1)", "201")
	if r := <-inflight; r.code != 200 || !strings.Contains(r.body, `"v":2`) {
		t.Fatalf("in-flight call: %d %s (want it to finish on the code it started with)", r.code, r.body)
	}
	waitVal("new calls use the new code", "SELECT nap(0)", "3")

	// 3. Broken edits: rejected, everything keeps working; then a fix applies.
	writeFile(t, py, src("\n@duct.function\ndef inc(x: int) -> int:\n    return x +\n"))
	time.Sleep(1500 * time.Millisecond)
	waitVal("after a syntax error", "SELECT inc(1)", "201")
	writeFile(t, py, src("\n@duct.function\ndef inc(x: float) -> float:\n    return x\n")) // signature change
	time.Sleep(1500 * time.Millisecond)
	waitVal("after a signature change", "SELECT inc(1)", "201")
	writeFile(t, py, src("\n@duct.function\ndef inc(x: int) -> int:\n    return x + 300\n"))
	waitVal("after the fix", "SELECT inc(1)", "301")

	// 4. A first function file in a project that is already serving others (toml only).
	writeFile(t, filepath.Join(root, "functions", "lo.toml"), "command = [\"tr\", \"A-Z\", \"a-z\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\n")
	waitVal("new executable", "SELECT lo('ABC')", `"abc"`)
	if err := os.Remove(filepath.Join(root, "functions", "lo.toml")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "removed executable fails", 60*time.Second, func() bool {
		code, body := val("SELECT lo('ABC')", tokRoot)
		return code != 200 && strings.Contains(body, "was removed")
	})
}

// TestServeFunctionsHotReloadFirstFile: a server that started without a
// functions/ folder picks up the first function and serves it to the admin only.
func testServeFunctionsHotReloadFirstFile(t *testing.T) {
	pythonEnv(t)
	old := functionsReloadEvery
	functionsReloadEvery = 200 * time.Millisecond
	t.Cleanup(func() { functionsReloadEvery = old })
	root := governedProject(t)
	g := func(args ...string) string { return mustDuct(t, append(args, "--project", root)...) }
	g("role", "create", "duct_admin")
	g("user", "create", "root", "--role", "duct_admin", "--password", "pw-root-123")
	tokAna := strings.TrimSpace(g("token", "create", "--user", "ana"))
	tokRoot := strings.TrimSpace(g("token", "create", "--user", "root"))
	srv := startServe(t, root, nil)
	defer srv.stop()
	writeFile(t, filepath.Join(root, "functions", "f.py"), "import duct\n\n@duct.function\ndef inc(x: int) -> int:\n    return x + 1\n")
	waitFor(t, "first function", 60*time.Second, func() bool {
		code, body := postSQL(t, srv.http, "SELECT inc(1) AS v FORMAT JSON", tokRoot)
		return code == 200 && strings.Contains(body, `"v":2`)
	})
	if code, body := postSQL(t, srv.http, "SELECT inc(1) AS v", tokAna); code != 403 {
		t.Fatalf("analistas: %d %s", code, body)
	}
	// Table functions are never served over the wire, not even to the admin.
	writeFile(t, filepath.Join(root, "functions", "t.py"),
		"import duct\n\n@duct.table_function(columns={'n': 'BIGINT'})\ndef three():\n    return {'n': [1, 2, 3]}\n")
	time.Sleep(2 * time.Second)
	if code, body := postSQL(t, srv.http, "SELECT * FROM three()", tokRoot); code == 200 || !strings.Contains(body, "table functions are not allowed") {
		t.Fatalf("table function over HTTP: %d %s", code, body)
	}
}
