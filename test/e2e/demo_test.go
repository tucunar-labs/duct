//go:build e2e

// Package e2e runs examples/demo with the real `duct` binary: the five steps
// of the v0.1 goal (init, ingest, run, serve as a governed role, web).
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const demoPassword = "demo-pw-123"

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if parent := filepath.Dir(dir); parent != dir {
			dir = parent
			continue
		}
		t.Fatal("go.mod not found")
	}
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		b, err := os.ReadFile(p) //nolint:gosec // test fixture
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}

type cli struct {
	t    *testing.T
	bin  string
	root string
	env  []string
}

// run executes `duct <args> --project root` (or without it for init) and returns
// stdout, stderr and the exit code.
func (c *cli) run(args ...string) (string, string, int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.bin, args...) //nolint:gosec // test binary
	cmd.Dir = c.root
	cmd.Env = append(os.Environ(), c.env...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	code := 0
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			c.t.Fatalf("duct %v: %v", args, err)
		}
		code = ee.ExitCode()
	}
	return out.String(), errb.String(), code
}

func (c *cli) must(args ...string) string {
	c.t.Helper()
	out, errs, code := c.run(args...)
	if code != 0 {
		c.t.Fatalf("duct %v: code=%d\nstdout: %s\nstderr: %s", args, code, out, errs)
	}
	return out
}

func (c *cli) rows(sql string) []map[string]any {
	c.t.Helper()
	var rows []map[string]any
	out := c.must("query", "--format", "json", sql)
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		c.t.Fatalf("query %q: %v (%s)", sql, err, out)
	}
	return rows
}

func (c *cli) n(table string) float64 {
	c.t.Helper()
	return c.rows("SELECT count(*) AS n FROM " + table)[0]["n"].(float64)
}

func podman(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("podman", args...).CombinedOutput() //nolint:gosec // test
	if err != nil {
		t.Fatalf("podman %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// odbcSupport reports whether the ODBC step can run, or why not.
func odbcSupport() (driver, why string) {
	if os.Getenv("DUCT_E2E_NO_ODBC") != "" {
		return "", "DUCT_E2E_NO_ODBC is set"
	}
	for _, d := range []string{
		"/usr/lib/psqlodbcw.so",
		"/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so",
		"/usr/lib/aarch64-linux-gnu/odbc/psqlodbcw.so",
	} {
		if _, err := os.Stat(d); err == nil {
			driver = d
			break
		}
	}
	if driver == "" {
		return "", "psqlODBC driver not found"
	}
	found := false
	for _, l := range []string{"/usr/lib/libodbc.so.2", "/usr/lib/x86_64-linux-gnu/libodbc.so.2", "/usr/lib/aarch64-linux-gnu/libodbc.so.2"} {
		if _, err := os.Stat(l); err == nil {
			found = true
		}
	}
	if !found {
		return "", "unixODBC (libodbc.so.2) not installed"
	}
	if _, err := exec.LookPath("podman"); err != nil {
		return "", "podman not available"
	}
	return driver, ""
}

func startPostgres(t *testing.T) (port int) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port = l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	id := podman(t, "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=pw",
		"-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "docker.io/library/postgres:17-bookworm")
	t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", id).Run() })
	deadline := time.Now().Add(150 * time.Second)
	for exec.Command("podman", "exec", id, "psql", "-U", "postgres", "-c", "select 1").Run() != nil {
		if time.Now().After(deadline) {
			t.Fatal("postgres did not start")
		}
		time.Sleep(time.Second)
	}
	podman(t, "exec", id, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
		"CREATE TABLE clientes (id int, nombre text, region text); INSERT INTO clientes VALUES (1,'Ana','CL'),(2,'Beto','AR'),(3,'Cami','CL')")
	return port
}

func TestDemo(t *testing.T) {
	root := repoRoot(t)
	work := t.TempDir()
	bin := filepath.Join(work, "duct")
	{
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "build", "-tags", "duckdb_arrow", "-o", bin, "./cmd/duct") //nolint:gosec // test
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build: %v\n%s", err, out)
		}
	}

	demo := filepath.Join(work, "demo")
	copyDir(t, filepath.Join(root, "examples", "demo"), demo)
	c := &cli{t: t, bin: bin, root: demo}

	driver, why := odbcSupport()
	odbc := why == ""
	if odbc {
		port := startPostgres(t)
		c.env = []string{"PG_ODBC_DRIVER=" + driver, fmt.Sprintf("PGPORT=%d", port), "PGPW=pw"}
	} else {
		// Without ODBC the Postgres-dependent files are not part of the project.
		_ = os.Remove(filepath.Join(demo, "ingest", "clientes.yaml"))
		_ = os.Remove(filepath.Join(demo, "pipelines", "clientes.sql"))
	}

	t.Run("1 init", func(t *testing.T) {
		out := c.must("init", ".")
		if !strings.Contains(out, "Created Duct project") {
			t.Fatalf("init: %s", out)
		}
		for _, p := range []string{"duct.toml", "lake", ".duct/catalog.sqlite", "data/ventas.csv", "setup.sh"} {
			if _, err := os.Stat(filepath.Join(demo, p)); err != nil {
				t.Errorf("missing %s: %v", p, err)
			}
		}
	})

	t.Run("2 ingest", func(t *testing.T) {
		out := c.must("ingest")
		if !strings.Contains(out, "bronze.ventas") || !strings.Contains(out, "6 rows") {
			t.Fatalf("ingest: %s", out)
		}
		if n := c.n("bronze.ventas"); n != 6 {
			t.Errorf("bronze.ventas = %v", n)
		}
		t.Run("clientes via ODBC", func(t *testing.T) {
			if !odbc {
				t.Skip("SKIPPED ODBC step: " + why)
			}
			if !strings.Contains(out, "bronze.clientes") {
				t.Fatalf("ingest output lacks bronze.clientes: %s", out)
			}
			if n := c.n("bronze.clientes"); n != 3 {
				t.Errorf("bronze.clientes = %v", n)
			}
		})
	})

	t.Run("3 run", func(t *testing.T) {
		out := c.must("run")
		t.Logf("duct run:\n%s", out)
		for _, want := range []string{"SUCCESS  silver.ventas", "4 rows (2 dropped)", "monto_positivo [drop_row]: 2 violations",
			"SUCCESS  gold.ventas", "SUCCESS  gold.ventas_por_region"} {
			if !strings.Contains(out, want) {
				t.Errorf("run output lacks %q", want)
			}
		}
		if n := c.n("silver.ventas"); n != 4 {
			t.Errorf("silver.ventas = %v", n)
		}
		if odbc && c.n("silver.clientes") != 3 {
			t.Errorf("silver.clientes rows")
		}

		// A big sale breaks the FAIL UPDATE of gold.ventas_por_region only.
		f, err := os.OpenFile(filepath.Join(demo, "data", "ventas.csv"), os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString("7,2026-01-11,AR,22.222.222-2,5000\n")
		_ = f.Close()
		c.must("ingest", "ventas")
		out, errs, code := c.run("run")
		t.Logf("duct run (FAIL UPDATE):\n%s\nstderr: %s", out, errs)
		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		for _, want := range []string{"FAILED   gold.ventas_por_region", "total_bajo_tope [fail_update]: 1 violations", "previous version kept", "SUCCESS  gold.ventas"} {
			if !strings.Contains(out, want) {
				t.Errorf("run output lacks %q", want)
			}
		}
		if n := c.n("gold.ventas"); n != 5 {
			t.Errorf("gold.ventas must follow silver (5), got %v", n)
		}
		for _, r := range c.rows("SELECT region, total FROM gold.ventas_por_region") {
			if r["region"] == "AR" && fmt.Sprint(r["total"]) != "200" {
				t.Errorf("gold.ventas_por_region was updated despite FAIL UPDATE: %v", r)
			}
		}
	})

	var user = "ana"
	t.Run("4 governance and serve", func(t *testing.T) {
		cmd := exec.Command("bash", "setup.sh") //nolint:gosec // test
		cmd.Dir = demo
		cmd.Env = append(os.Environ(), "DUCT="+bin)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("setup.sh: %v\n%s", err, out)
		}
		token := strings.TrimSpace(c.must("token", "create", "--user", user, "--ttl", "1h"))

		ctx, cancel := context.WithCancel(context.Background())
		srv := exec.CommandContext(ctx, bin, "serve", "--project", demo, //nolint:gosec // test
			"--listen", "pg=127.0.0.1:0", "--listen", "http=127.0.0.1:0", "--listen", "web=127.0.0.1:0")
		srv.Dir = demo
		pipe, err := srv.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		var srvErr bytes.Buffer
		srv.Stderr = &srvErr
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cancel()
			done := make(chan struct{})
			go func() { _ = srv.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				_ = srv.Process.Kill()
			}
		})
		addrs := map[string]string{}
		ready := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(pipe)
			for sc.Scan() {
				f := strings.Fields(sc.Text())
				if len(f) == 4 && f[0] == "serving" {
					addrs[f[1]] = f[3]
					if len(addrs) == 3 {
						close(ready)
						break
					}
				}
			}
			_, _ = io.Copy(io.Discard, pipe)
		}()
		select {
		case <-ready:
		case <-time.After(90 * time.Second):
			t.Fatalf("serve did not report 3 protocols (got %v)\nstderr: %s", addrs, srvErr.String())
		}
		t.Logf("serving: %v", addrs)

		t.Run("pg", func(t *testing.T) {
			cctx, ccancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer ccancel()
			conn, err := pgx.Connect(cctx, fmt.Sprintf("postgres://%s:%s@%s/duct?sslmode=disable", user, demoPassword, addrs["pg"]))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close(cctx) }()
			q := func(sql string) ([][]any, error) {
				rs, err := conn.Query(cctx, sql, pgx.QueryExecModeSimpleProtocol)
				if err != nil {
					return nil, err
				}
				defer rs.Close()
				var out [][]any
				for rs.Next() {
					v, err := rs.Values()
					if err != nil {
						return nil, err
					}
					out = append(out, v)
				}
				return out, rs.Err()
			}
			rows, err := q("SELECT id, rut, monto FROM ventas ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			// CL only: ids 1, 3, 6 (the negative one was dropped in Silver).
			if len(rows) != 3 || rows[0][1] != "***" || rows[2][1] != "***" {
				t.Fatalf("filtered+masked rows = %v", rows)
			}
			for _, sql := range []string{"SELECT * FROM gold.ventas", "SELECT * FROM silver.ventas", "SELECT * FROM bronze.ventas", "SELECT * FROM duct.duct_users"} {
				if r, err := q(sql); err == nil {
					t.Errorf("%q must be denied, got %v", sql, r)
				}
			}
		})

		t.Run("http", func(t *testing.T) {
			do := func(sql, tok string) (int, string) {
				req, _ := http.NewRequest("POST", "http://"+addrs["http"]+"/query", strings.NewReader(sql))
				req.Header.Set("Authorization", "Bearer "+tok)
				cl := http.Client{Timeout: 60 * time.Second}
				resp, err := cl.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = resp.Body.Close() }()
				b, _ := io.ReadAll(resp.Body)
				return resp.StatusCode, string(b)
			}
			code, body := do("SELECT id, rut, monto FROM ventas ORDER BY id", token)
			var js struct {
				Data []map[string]any `json:"data"`
				Rows int              `json:"rows"`
			}
			if err := json.Unmarshal([]byte(body), &js); code != 200 || err != nil {
				t.Fatalf("http: %d %v %s", code, err, body)
			}
			if js.Rows != 3 || js.Data[0]["rut"] != "***" || js.Data[2]["rut"] != "***" {
				t.Fatalf("http rows: %s", body)
			}
			if code, _ := do("SELECT * FROM gold.ventas", token); code != 403 && code != 400 {
				t.Errorf("base table over http: %d", code)
			}
		})

		t.Run("5 web", func(t *testing.T) {
			base := "http://" + addrs["web"]
			jar, _ := cookiejar.New(nil)
			cl := &http.Client{Jar: jar, Timeout: 30 * time.Second}
			resp, err := cl.Get(base + "/login")
			if err != nil {
				t.Fatal(err)
			}
			page, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			m := regexp.MustCompile(`name="csrf"[^>]*value="([^"]+)"`).FindSubmatch(page)
			if resp.StatusCode != 200 || m == nil {
				t.Fatalf("GET /login: %d, csrf found=%v", resp.StatusCode, m != nil)
			}
			form := url.Values{"user": {user}, "password": {demoPassword}, "csrf": {string(m[1])}}
			req, _ := http.NewRequest("POST", base+"/login", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Origin", base)
			resp, err = cl.Do(req) // follows the redirect to /catalog
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || !strings.Contains(string(body), "ventas") {
				t.Fatalf("login -> %s: %d\n%s", resp.Request.URL.Path, resp.StatusCode, body)
			}
			resp, err = cl.Get(base + "/catalog")
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || !strings.Contains(string(body), "ventas") || strings.Contains(string(body), "bronze") {
				t.Fatalf("GET /catalog: %d\n%s", resp.StatusCode, body)
			}
		})
	})
}
