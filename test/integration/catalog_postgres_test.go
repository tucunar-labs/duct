//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// These tests drive the real `duct` binary against Postgres 17 in rootless
// Podman: the catalog, Duct state, migration from SQLite and concurrent
// writers (ADR 0001). They remove their containers at the end.

const pgPassword = "pw-catalog"

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

// ductBinary builds cmd/duct once per test run.
func ductBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "duct-bin-*")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "duct")
		cmd := exec.Command("go", "build", "-tags", "duckdb_arrow", "-o", binPath, "github.com/alonsoburon/duct/cmd/duct")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

type pgServer struct {
	id   string
	port int
}

// startPG runs Postgres 17 in Podman, waits for it and removes it at the end.
func startPG(t *testing.T) *pgServer {
	t.Helper()
	if _, err := exec.LookPath("podman"); err != nil {
		t.Skip("podman not available")
	}
	port := freePort(t)
	id := podman(t, "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD="+pgPassword,
		"-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "docker.io/library/postgres:17-bookworm",
		"-c", "max_connections=300")
	t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", id).Run() })
	// The image restarts once after initdb: require a few consecutive successes.
	ok := 0
	for i := 0; ok < 3; i++ {
		if exec.Command("podman", "exec", id, "psql", "-U", "postgres", "-c", "select 1").Run() == nil {
			ok++
		} else {
			ok = 0
		}
		if i > 120 {
			t.Fatal("postgres did not start")
		}
		time.Sleep(time.Second)
	}
	return &pgServer{id: id, port: port}
}

func (s *pgServer) createDB(t *testing.T, name string) {
	t.Helper()
	podman(t, "exec", s.id, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE "+name)
}

// url is the connection string without password (it goes to duct.toml).
func (s *pgServer) url(db string) string {
	return fmt.Sprintf("postgres://postgres@127.0.0.1:%d/%s", s.port, db)
}

// sql runs a query as superuser and returns its unaligned output.
func (s *pgServer) sql(t *testing.T, db, q string) string {
	t.Helper()
	return podman(t, "exec", s.id, "psql", "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-At", "-c", q)
}

type ductRun struct {
	stdout, stderr string
	err            error
}

func duct(t *testing.T, root string, env []string, args ...string) ductRun {
	t.Helper()
	cmd := exec.Command(ductBinary(t), args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), env...)
	var o, e bytes.Buffer
	cmd.Stdout, cmd.Stderr = &o, &e
	err := cmd.Run()
	return ductRun{o.String(), e.String(), err}
}

func mustDuct(t *testing.T, root string, env []string, args ...string) string {
	t.Helper()
	r := duct(t, root, env, args...)
	if r.err != nil {
		t.Fatalf("duct %v: %v\nstdout: %s\nstderr: %s", args, r.err, r.stdout, r.stderr)
	}
	return r.stdout
}

func csvQuery(t *testing.T, root string, env []string, q string) string {
	t.Helper()
	return strings.TrimSpace(mustDuct(t, root, env, "query", "--format", "csv", q))
}

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

var pwEnv = []string{"DUCT_CATALOG_PASSWORD=" + pgPassword}

// startServe runs `duct serve` as a subprocess and returns the pg address.
func startServe(t *testing.T, root string, env []string) string {
	t.Helper()
	cmd := exec.Command(ductBinary(t), "serve", "--protocols", "pg,http", "--listen", "pg=127.0.0.1:0", "--listen", "http=127.0.0.1:0")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGINT)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	addr := make(chan string, 1)
	re := regexp.MustCompile(`serving pg\s+on (\S+)`)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if m := re.FindStringSubmatch(sc.Text()); m != nil {
				addr <- m[1]
			}
		}
	}()
	select {
	case a := <-addr:
		return a
	case <-time.After(120 * time.Second):
		t.Fatalf("duct serve did not start: %s", errb.String())
		return ""
	}
}

func pgQuery(t *testing.T, addr, user, pass, q string) ([][]any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, "postgres://"+user+":"+pass+"@"+addr+"/duct")
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close(ctx) }()
	rows, err := c.Query(ctx, q, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// TestPostgresCatalogEndToEnd: init with a Postgres catalog -> ingest -> run
// -> governed query through `duct serve`; Duct state lives in schema duct.
func TestPostgresCatalogEndToEnd(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "lake1")
	root := filepath.Join(t.TempDir(), "proj")

	out := mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("lake1"))
	if !strings.Contains(out, "schema duct") {
		t.Fatalf("init output: %s", out)
	}
	toml, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	if strings.Contains(string(toml), pgPassword) || !strings.Contains(string(toml), "postgres://postgres@") {
		t.Fatalf("duct.toml must hold the URL without password:\n%s", toml)
	}
	// DuckLake metadata in public, Duct state in schema duct.
	if got := pg.sql(t, "lake1", "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'ducklake_%'"); got == "0" {
		t.Fatal("no ducklake_* tables in Postgres")
	}
	if got := pg.sql(t, "lake1", "SELECT count(*) FROM information_schema.tables WHERE table_schema='duct' AND table_name LIKE 'duct_%'"); got != "11" {
		t.Fatalf("duct tables in schema duct = %s, want 11", got)
	}
	if got := pg.sql(t, "lake1", "SELECT data_type FROM information_schema.columns WHERE table_schema='duct' AND table_name='duct_users' AND column_name='disabled'"); got != "boolean" {
		t.Fatalf("duct_users.disabled type = %q", got)
	}

	// Without the password the open fails and the message never leaks it.
	r := duct(t, root, nil, "query", "select 1")
	if r.err == nil || strings.Contains(r.stderr, pgPassword) {
		t.Fatalf("no password: err=%v stderr=%s", r.err, r.stderr)
	}
	r = duct(t, root, []string{"DUCT_CATALOG_PASSWORD=wrong-pw-xyz"}, "query", "select 1")
	if r.err == nil || strings.Contains(r.stderr, "wrong-pw-xyz") {
		t.Fatalf("wrong password: err=%v stderr=%s", r.err, r.stderr)
	}
	// DUCT_CATALOG_URL carries everything.
	full := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/lake1", pgPassword, pg.port)
	if got := csvQuery(t, root, []string{"DUCT_CATALOG_URL=" + full}, "select 42 as n"); got != "n\n42" {
		t.Fatalf("DUCT_CATALOG_URL: %q", got)
	}

	// Ingest -> run.
	write(t, filepath.Join(root, "data", "customers.csv"),
		"region,rut,name,amt\nCL,11.111.111-1,Ana,100\nAR,22.222.222-2,Beto,200\nCL,33.333.333-3,Cami,300\n")
	write(t, filepath.Join(root, "ingest", "customers.yaml"), "name: customers\nsource: files\noptions:\n  path: data/customers.csv\n")
	if out := mustDuct(t, root, pwEnv, "ingest"); !strings.Contains(out, "bronze.customers") {
		t.Fatalf("ingest: %s", out)
	}
	write(t, filepath.Join(root, "pipelines", "gold.sql"), "CREATE MATERIALIZED VIEW gold.customers AS SELECT * FROM bronze.customers;\n")
	if out := mustDuct(t, root, pwEnv, "run"); !strings.Contains(out, "SUCCESS  gold.customers") {
		t.Fatalf("run: %s", out)
	}
	if got := csvQuery(t, root, pwEnv, "SELECT count(*) AS n FROM gold.customers"); got != "n\n3" {
		t.Fatalf("gold.customers: %q", got)
	}
	if got := pg.sql(t, "lake1", "SELECT count(*) FROM duct.duct_ingest_state"); got != "1" {
		t.Fatalf("ingest state rows in Postgres = %s", got)
	}
	if got := pg.sql(t, "lake1", "SELECT count(*) FROM duct.duct_pipeline_runs"); got != "1" {
		t.Fatalf("pipeline runs in Postgres = %s", got)
	}
	mustDuct(t, root, pwEnv, "gc")

	// Governance (users, grants, filters, audit) is shared through Postgres.
	g := func(args ...string) { mustDuct(t, root, pwEnv, args...) }
	g("role", "create", "analistas")
	g("grant", "select", "on", "gold.customers", "to", "analistas")
	g("policy", "row-filter", "gold.customers", "--role", "analistas", "--where", "region = 'CL'")
	g("policy", "mask", "gold.customers.rut", "--role", "analistas", "--expr", "'***'")
	g("user", "create", "ana", "--role", "analistas", "--password", "pw-ana-123")
	g("user", "create", "baja", "--role", "analistas", "--password", "pw-baja-123")
	g("user", "disable", "baja")
	if got := pg.sql(t, "lake1", "SELECT name || ':' || disabled FROM duct.duct_users ORDER BY name"); got != "ana:false\nbaja:true" {
		t.Fatalf("users in Postgres: %q", got)
	}

	addr := startServe(t, root, pwEnv)
	rows, err := pgQuery(t, addr, "ana", "pw-ana-123", "SELECT name, rut FROM customers ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0] != "Ana" || rows[0][1] != "***" || rows[1][0] != "Cami" {
		t.Fatalf("served rows (row filter + mask): %v", rows)
	}
	if _, err := pgQuery(t, addr, "baja", "pw-baja-123", "SELECT 1"); err == nil {
		t.Fatal("disabled user logged in")
	}
	if _, err := pgQuery(t, addr, "ana", "wrong", "SELECT 1"); err == nil {
		t.Fatal("wrong password accepted")
	}
	// The audit trail lands in Postgres.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if got := pg.sql(t, "lake1", "SELECT count(*) FROM duct.duct_audit WHERE user_name='ana'"); got != "0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no audit rows in Postgres")
		}
		time.Sleep(time.Second)
	}
}

// TestCatalogMigrateSQLiteToPostgres: a SQLite project with inlined rows,
// Parquet files, schemas, users and a disabled user moves to Postgres; counts
// are verified and duct.toml switches only afterwards.
func TestCatalogMigrateSQLiteToPostgres(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "dest")
	pg.createDB(t, "busy")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), nil, "init", root)

	q := func(sql string) { mustDuct(t, root, nil, "query", sql) }
	q("CREATE SCHEMA bronze")
	q("CREATE TABLE bronze.big AS SELECT i AS id, 'v' || i AS v FROM range(50000) r(i)") // Parquet
	q("CREATE TABLE bronze.small (id INTEGER, ts TIMESTAMP, ok BOOLEAN)")
	q("INSERT INTO bronze.small VALUES (1, TIMESTAMP '2024-05-01 10:00:00', true), (2, TIMESTAMP '2024-05-02 11:00:00', false)") // inlined
	q("DELETE FROM bronze.big WHERE id < 10")
	write(t, filepath.Join(root, "pipelines", "gold.sql"), "CREATE MATERIALIZED VIEW gold.small_ok AS SELECT id FROM bronze.small WHERE ok;\n")
	mustDuct(t, root, nil, "run")
	mustDuct(t, root, nil, "role", "create", "analistas")
	mustDuct(t, root, nil, "grant", "select", "on", "gold.small_ok", "to", "analistas")
	mustDuct(t, root, nil, "user", "create", "ana", "--role", "analistas", "--password", "pw-ana-123")
	mustDuct(t, root, nil, "user", "create", "baja", "--role", "analistas", "--password", "pw-baja-123")
	mustDuct(t, root, nil, "user", "disable", "baja")
	mustDuct(t, root, nil, "token", "create", "--user", "ana", "--label", "ci")

	tomlBefore, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	target := pg.url("dest")
	withPw := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/dest", pgPassword, pg.port)

	// Dry run: reports, writes nothing.
	r := duct(t, root, nil, "catalog", "migrate", "--to", withPw, "--dry-run")
	if r.err != nil {
		t.Fatalf("dry run: %v\n%s\n%s", r.err, r.stdout, r.stderr)
	}
	if strings.Contains(r.stdout+r.stderr, pgPassword) {
		t.Fatal("dry run printed the password")
	}
	for _, want := range []string{"rows bronze.big", "state duct_users", "Dry run"} {
		if !strings.Contains(r.stdout, want) {
			t.Errorf("dry run output lacks %q:\n%s", want, r.stdout)
		}
	}
	if got := pg.sql(t, "dest", "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'ducklake_%'"); got != "0" {
		t.Fatalf("dry run wrote to the target (%s tables)", got)
	}
	if after, _ := os.ReadFile(filepath.Join(root, "duct.toml")); !bytes.Equal(after, tomlBefore) {
		t.Fatal("dry run changed duct.toml")
	}

	// A target that already holds a lake is refused and left intact.
	busyRoot := filepath.Join(t.TempDir(), "busy")
	mustDuct(t, t.TempDir(), pwEnv, "init", busyRoot, "--catalog", pg.url("busy"))
	mustDuct(t, busyRoot, pwEnv, "query", "CREATE TABLE keep AS SELECT 1 AS x")
	r = duct(t, root, nil, "catalog", "migrate", "--to", fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/busy", pgPassword, pg.port))
	if r.err == nil || !strings.Contains(r.stderr, "refusing to overwrite") {
		t.Fatalf("busy target: err=%v stderr=%s", r.err, r.stderr)
	}
	if got := csvQuery(t, busyRoot, pwEnv, "SELECT x FROM keep"); got != "x\n1" {
		t.Fatalf("busy target damaged: %q", got)
	}

	// Real run.
	want := csvQuery(t, root, nil, "SELECT (SELECT count(*) FROM bronze.big) AS big, (SELECT count(*) FROM bronze.small) AS small, (SELECT sum(id) FROM bronze.big) AS s")
	r = duct(t, root, nil, "catalog", "migrate", "--to", withPw)
	if r.err != nil {
		t.Fatalf("migrate: %v\n%s\n%s", r.err, r.stdout, r.stderr)
	}
	if strings.Contains(r.stdout+r.stderr, pgPassword) {
		t.Fatal("migrate printed the password")
	}
	for _, want := range []string{"ok   rows bronze.big", "ok   snapshots", "ok   state duct_users", "Verified", "DUCT_CATALOG_PASSWORD"} {
		if !strings.Contains(r.stdout, want) {
			t.Errorf("migrate output lacks %q:\n%s", want, r.stdout)
		}
	}
	if strings.Contains(r.stdout, "FAIL") {
		t.Fatalf("a count differs:\n%s", r.stdout)
	}
	toml, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	if strings.Contains(string(toml), pgPassword) || !strings.Contains(string(toml), target) || !strings.Contains(string(toml), "# was: catalog") {
		t.Fatalf("duct.toml after migrate:\n%s", toml)
	}

	// Same data through Postgres; history kept; writes keep working.
	got := csvQuery(t, root, pwEnv, "SELECT (SELECT count(*) FROM bronze.big) AS big, (SELECT count(*) FROM bronze.small) AS small, (SELECT sum(id) FROM bronze.big) AS s")
	if got != want {
		t.Fatalf("after migrate: %q, before: %q", got, want)
	}
	if got := csvQuery(t, root, pwEnv, "SELECT id FROM gold.small_ok"); got != "id\n1" {
		t.Fatalf("gold.small_ok: %q", got)
	}
	if got := csvQuery(t, root, pwEnv, "SELECT count(*) > 8 AS history FROM ducklake_snapshots('lake')"); got != "history\ntrue" {
		t.Fatalf("snapshot history: %q", got)
	}
	mustDuct(t, root, pwEnv, "query", "INSERT INTO bronze.small VALUES (3, now(), true)")
	mustDuct(t, root, pwEnv, "query", "INSERT INTO bronze.small SELECT 4, now(), false")
	if got := csvQuery(t, root, pwEnv, "SELECT count(*) AS n FROM bronze.small"); got != "n\n4" {
		t.Fatalf("writes after migrate: %q", got)
	}
	mustDuct(t, root, pwEnv, "gc")
	// SQLite files are untouched (rollback = restore the toml line).
	if _, err := os.Stat(filepath.Join(root, ".duct", "catalog.sqlite")); err != nil {
		t.Fatal(err)
	}
	// A second migrate is refused.
	if r := duct(t, root, pwEnv, "catalog", "migrate", "--to", withPw); r.err == nil || !strings.Contains(r.stderr, "already Postgres") {
		t.Fatalf("second migrate: err=%v stderr=%s", r.err, r.stderr)
	}

	// Users, flags and tokens survived: booleans arrived as booleans.
	if got := pg.sql(t, "dest", "SELECT name || ':' || disabled FROM duct.duct_users ORDER BY name"); got != "ana:false\nbaja:true" {
		t.Fatalf("users after migrate: %q", got)
	}
	if got := pg.sql(t, "dest", "SELECT revoked FROM duct.duct_tokens"); got != "f" {
		t.Fatalf("token revoked flag: %q", got)
	}
	addr := startServe(t, root, pwEnv)
	rows, err := pgQuery(t, addr, "ana", "pw-ana-123", "SELECT id FROM small_ok")
	if err != nil || len(rows) != 1 {
		t.Fatalf("served query after migrate: %v %v", rows, err)
	}
	if _, err := pgQuery(t, addr, "baja", "pw-baja-123", "SELECT 1"); err == nil {
		t.Fatal("disabled user can log in after migrate")
	}
}

// TestCatalogConcurrentWriters: several processes write through the same
// Postgres catalog at once. Appends never fail (DuckLake retries snapshot
// races); a logical conflict (same table created twice) fails cleanly for the
// loser, and the retry then succeeds.
func TestCatalogConcurrentWriters(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "conc")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("conc"))
	mustDuct(t, root, pwEnv, "query", "CREATE TABLE ev (w INTEGER, i INTEGER)")

	const writers, perWriter = 8, 4
	var wg sync.WaitGroup
	errs := make(chan string, writers*perWriter)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				r := duct(t, root, pwEnv, "query", fmt.Sprintf("INSERT INTO ev SELECT %d, %d + i FROM range(1000) r(i)", w, i*1000))
				if r.err != nil {
					errs <- fmt.Sprintf("writer %d insert %d: %v: %s", w, i, r.err, r.stderr)
				}
			}
		}()
	}
	// Two ingests of different sources run in parallel with the writers.
	for _, name := range []string{"alpha", "beta"} {
		write(t, filepath.Join(root, "data", name+".csv"), "id,v\n1,a\n2,b\n3,c\n")
		write(t, filepath.Join(root, "ingest", name+".yaml"), fmt.Sprintf("name: %s\nsource: files\noptions:\n  path: data/%s.csv\n", name, name))
	}
	for _, name := range []string{"alpha", "beta"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r := duct(t, root, pwEnv, "ingest", name); r.err != nil {
				errs <- fmt.Sprintf("ingest %s: %v: %s %s", name, r.err, r.stdout, r.stderr)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	want := writers * perWriter * 1000
	if got := csvQuery(t, root, pwEnv, "SELECT count(*) AS n, count(DISTINCT (w, i)) AS d FROM ev"); got != fmt.Sprintf("n,d\n%d,%d", want, want) {
		t.Fatalf("ev rows: %q, want %d distinct", got, want)
	}
	for _, name := range []string{"alpha", "beta"} {
		if got := csvQuery(t, root, pwEnv, "SELECT count(*) AS n FROM bronze."+name); got != "n\n3" {
			t.Errorf("bronze.%s: %q", name, got)
		}
	}
	if got := pg.sql(t, "conc", "SELECT count(*) FROM duct.duct_ingest_state"); got != "2" {
		t.Errorf("ingest state rows = %s, want 2", got)
	}

	// Logical conflict: four processes create the same table.
	var okN, conflictN int
	var mu sync.Mutex
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := duct(t, root, pwEnv, "query", "CREATE TABLE only_one (a INTEGER)")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case r.err == nil:
				okN++
			case strings.Contains(r.stderr, "Transaction conflict") || strings.Contains(r.stderr, "already exists"):
				conflictN++
			default:
				t.Errorf("unexpected failure: %s", r.stderr)
			}
		}()
	}
	wg.Wait()
	if okN != 1 || conflictN != 3 {
		t.Errorf("create-table race: %d won, %d conflicted (want 1 and 3)", okN, conflictN)
	}
	// Retry after a conflict works (the winner's snapshot is visible).
	mustDuct(t, root, pwEnv, "query", "INSERT INTO only_one VALUES (1)")
}
