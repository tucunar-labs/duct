//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These tests drive the real `duct` binary against RustFS in rootless Podman
// (storage = "s3://bucket/prefix", ADR 0001): init -> ingest -> run -> gc ->
// serve (pg + http) as a role, with DuckDB locked down to the lake prefix
// (ADR 0007). They remove their containers at the end.
//
// CI: needs only podman (the default image is docker.io/rustfs/rustfs:latest;
// DUCT_TEST_RUSTFS_IMAGE pins it). A job with an S3 service container can set
// DUCT_TEST_S3_ENDPOINT / DUCT_TEST_S3_ACCESS_KEY / DUCT_TEST_S3_SECRET_KEY
// and skip Podman. The `httpfs` DuckDB extension is downloaded the first time.

const secretCSV = "id,v\n1,top-secret\n"

// s3Flags are the init flags that point a project at s.
func (s *s3Server) initFlags(storage string) []string {
	return []string{"--storage", storage, "--s3-endpoint", s.endpoint, "--s3-no-ssl"}
}

// startServeAddrs runs `duct serve` and returns the pg and http addresses.
func startServeAddrs(t *testing.T, root string, env []string) (pgAddr, httpAddr string) {
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
	addrs := make(chan [2]string, 4)
	re := regexp.MustCompile(`serving (pg|http)\s+on (\S+)`)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if m := re.FindStringSubmatch(sc.Text()); m != nil {
				addrs <- [2]string{m[1], m[2]}
			}
		}
	}()
	deadline := time.After(120 * time.Second)
	for pgAddr == "" || httpAddr == "" {
		select {
		case a := <-addrs:
			if a[0] == "pg" {
				pgAddr = a[1]
			} else {
				httpAddr = a[1]
			}
		case <-deadline:
			t.Fatalf("duct serve did not start: %s", errb.String())
		}
	}
	return pgAddr, httpAddr
}

func httpQuery(t *testing.T, addr, user, pass, sql string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+addr+"/query?default_format=CSVWithNames", strings.NewReader(sql))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// TestS3RustFSEndToEnd: the whole flow on s3://lake/proj for a SQLite catalog
// and for a Postgres catalog.
func TestS3RustFSEndToEnd(t *testing.T) {
	s3 := startRustFS(t)
	for _, tc := range []struct {
		name     string
		postgres bool
	}{{"sqlite", false}, {"postgres", true}} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := "lake-" + tc.name
			s3.createBucket(t, bucket)
			s3.put(t, bucket, "other/secret.csv", secretCSV) // same bucket, another prefix
			storage := "s3://" + bucket + "/proj"
			env := s3.env()
			root := filepath.Join(t.TempDir(), "proj")
			args := append([]string{"init", root}, s3.initFlags(storage)...)
			if tc.postgres {
				pg := startPG(t)
				pg.createDB(t, "lake1")
				args = append(args, "--catalog", pg.url("lake1"))
				env = append(env, pwEnv...)
			}

			// init: duct.toml carries endpoint and storage, never a credential.
			out := mustDuct(t, t.TempDir(), env, args...)
			if !strings.Contains(out, storage) {
				t.Fatalf("init output: %s", out)
			}
			toml, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
			for _, bad := range []string{s3.secret, s3.access} {
				if strings.Contains(string(toml), bad) {
					t.Fatalf("duct.toml holds a credential:\n%s", toml)
				}
			}
			if !strings.Contains(string(toml), `storage = "`+storage+`"`) || !strings.Contains(string(toml), "[s3]") {
				t.Fatalf("duct.toml:\n%s", toml)
			}
			if _, err := os.Stat(filepath.Join(root, "lake")); err == nil {
				t.Fatal("init created a local lake/ for s3 storage")
			}
			// Without credentials the failure is explicit and leaks nothing.
			noCreds := []string{"DUCT_S3_ENDPOINT=" + s3.endpoint, "DUCT_S3_USE_SSL=false", "AWS_ACCESS_KEY_ID=", "AWS_SECRET_ACCESS_KEY="}
			if r := duct(t, root, append(noCreds, tcEnv(env)...), "query", "select 1"); r.err == nil || !strings.Contains(r.stderr, "no credentials") {
				t.Fatalf("no credentials: err=%v stderr=%s", r.err, r.stderr)
			}

			// ingest -> run -> query: the Parquet files land in the bucket.
			write(t, filepath.Join(root, "data", "customers.csv"),
				"region,rut,name,amt\nCL,11.111.111-1,Ana,100\nAR,22.222.222-2,Beto,200\nCL,33.333.333-3,Cami,300\n")
			write(t, filepath.Join(root, "ingest", "customers.yaml"), "name: customers\nsource: files\noptions:\n  path: data/customers.csv\n")
			if out := mustDuct(t, root, env, "ingest"); !strings.Contains(out, "bronze.customers") {
				t.Fatalf("ingest: %s", out)
			}
			write(t, filepath.Join(root, "pipelines", "gold.sql"), "CREATE MATERIALIZED VIEW gold.customers AS SELECT * FROM bronze.customers;\n")
			if out := mustDuct(t, root, env, "run"); !strings.Contains(out, "SUCCESS  gold.customers") {
				t.Fatalf("run: %s", out)
			}
			if got := csvQuery(t, root, env, "SELECT count(*) AS n FROM gold.customers"); got != "n\n3" {
				t.Fatalf("gold.customers: %q", got)
			}
			if _, err := os.Stat(filepath.Join(root, "lake")); err == nil {
				t.Fatal("data was written to a local lake/")
			}
			// gc: an orphan file (not referenced by any snapshot) is deleted.
			s3.put(t, bucket, "proj/orphan-file.parquet", "not a parquet file")
			time.Sleep(2 * time.Second) // older_than = now - keep-snapshots
			gc := mustDuct(t, root, env, "gc", "--keep-snapshots", "1s")
			if !strings.Contains(gc, "gc done") {
				t.Fatalf("gc: %s", gc)
			}
			for _, k := range s3.keys(t, bucket, "proj/") {
				if strings.Contains(k, "orphan-file") {
					t.Fatalf("gc left the orphan: %s", gc)
				}
			}
			// Small tables are inlined in the catalog; gc flushes them to Parquet in the bucket.
			parquet := 0
			for _, k := range s3.keys(t, bucket, "proj/") {
				if strings.HasSuffix(k, ".parquet") {
					parquet++
				}
			}
			if parquet == 0 {
				t.Fatalf("no parquet under s3://%s/proj/: %v", bucket, s3.keys(t, bucket, ""))
			}

			if got := csvQuery(t, root, env, "SELECT count(*) AS n FROM gold.customers"); got != "n\n3" {
				t.Fatalf("data after gc: %q", got)
			}

			// Locked CLI: the lake works, the other prefix of the same bucket does not.
			if got := csvQuery(t, root, env, "SELECT count(*) AS n FROM bronze.customers"); got != "n\n3" {
				t.Fatal(got)
			}
			other := "SELECT * FROM read_csv('s3://" + bucket + "/other/secret.csv')"
			if got := csvQuery(t, root, env, other); !strings.Contains(got, "top-secret") {
				t.Fatalf("unlocked admin must see the other prefix (control): %q", got)
			}
			r := duct(t, root, env, "query", "--locked", other)
			if r.err == nil || strings.Contains(r.stdout+r.stderr, "top-secret") {
				t.Fatalf("locked query read another prefix: err=%v\n%s%s", r.err, r.stdout, r.stderr)
			}
			if got := csvQuery(t, root, env, "SELECT count(*) AS n FROM gold.customers"); got != "n\n3" {
				t.Fatal(got)
			}
			if r := duct(t, root, env, "query", "--locked", "SELECT count(*) AS n FROM gold.customers"); r.err != nil || strings.TrimSpace(r.stdout) == "" {
				t.Fatalf("locked read of the lake: %v %s", r.err, r.stderr)
			}

			// serve: pg and http as a role with row filter + mask.
			g := func(a ...string) { mustDuct(t, root, env, a...) }
			g("role", "create", "analistas")
			g("grant", "select", "on", "gold.customers", "to", "analistas")
			g("policy", "row-filter", "gold.customers", "--role", "analistas", "--where", "region = 'CL'")
			g("policy", "mask", "gold.customers.rut", "--role", "analistas", "--expr", "'***'")
			g("user", "create", "ana", "--role", "analistas", "--password", "pw-ana-123")
			pgAddr, httpAddr := startServeAddrs(t, root, env)

			rows, err := pgQuery(t, pgAddr, "ana", "pw-ana-123", "SELECT name, rut FROM customers ORDER BY name")
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 2 || rows[0][0] != "Ana" || rows[0][1] != "***" || rows[1][0] != "Cami" {
				t.Fatalf("pg rows (row filter + mask over s3): %v", rows)
			}
			code, body := httpQuery(t, httpAddr, "ana", "pw-ana-123", "SELECT name, rut FROM customers ORDER BY name")
			if code != 200 || body != "name,rut\nAna,***\nCami,***" {
				t.Fatalf("http: %d %q", code, body)
			}
			// The role cannot reach the other prefix, the base table, nor the bucket.
			for _, q := range []string{other, "SELECT * FROM bronze.customers", "SELECT * FROM glob('s3://" + bucket + "/**')"} {
				if res, err := pgQuery(t, pgAddr, "ana", "pw-ana-123", q); err == nil {
					t.Errorf("pg allowed %q: %v", q, res)
				}
				if code, body := httpQuery(t, httpAddr, "ana", "pw-ana-123", q); code == 200 || strings.Contains(body, "top-secret") {
					t.Errorf("http allowed %q: %d %s", q, code, body)
				}
			}
		})
	}
}

func tcEnv(env []string) []string {
	// Keep only what is not a credential/endpoint variable of the S3 server.
	var out []string
	for _, e := range env {
		if !strings.HasPrefix(e, "DUCT_S3_") {
			out = append(out, e)
		}
	}
	return out
}

// TestS3STSScopedCredentials: with [s3] role_arn the process trades the root
// key for STS credentials limited by a session policy to the lake prefix.
// RustFS enforces it: even unlocked, the project cannot read another prefix.
func TestS3STSScopedCredentials(t *testing.T) {
	s3 := startRustFS(t)
	const bucket = "lake-sts"
	s3.createBucket(t, bucket)
	s3.put(t, bucket, "other/secret.csv", secretCSV)
	root := filepath.Join(t.TempDir(), "proj")
	env := s3.env()
	args := append([]string{"init", root}, s3.initFlags("s3://"+bucket+"/proj")...)
	mustDuct(t, t.TempDir(), env, append(args, "--s3-role-arn", "arn:aws:iam::duct:role/lake")...)

	mustDuct(t, root, env, "query", "CREATE TABLE t AS SELECT i FROM range(1000) r(i)")
	if got := csvQuery(t, root, env, "SELECT count(*) AS n FROM t"); got != "n\n1000" {
		t.Fatalf("lake through STS credentials: %q", got)
	}
	other := "SELECT * FROM read_csv('s3://" + bucket + "/other/secret.csv')"
	r := duct(t, root, env, "query", other)
	if r.err == nil || strings.Contains(r.stdout, "top-secret") {
		t.Fatalf("STS credentials read outside the prefix: err=%v\n%s%s", r.err, r.stdout, r.stderr)
	}
	for _, leak := range []string{s3.secret} {
		if strings.Contains(r.stderr, leak) {
			t.Fatal("error leaked a secret")
		}
	}
	// Control: the same project without role_arn (root key) does read it.
	toml, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	noRole := strings.ReplaceAll(string(toml), "role_arn", "# role_arn")
	if err := os.WriteFile(filepath.Join(root, "duct.toml"), []byte(noRole), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := csvQuery(t, root, env, other); !strings.Contains(got, "top-secret") {
		t.Fatalf("control without role_arn: %q", got)
	}
}

// TestS3MigrateFromLocal: the documented path from ./lake to s3://: copy the
// folder (rclone sync ./lake s3:bucket/prefix; here, plain PUTs), change
// `storage`, and the same catalog reads the same data.
func TestS3MigrateFromLocal(t *testing.T) {
	s3 := startRustFS(t)
	const bucket = "lake-migrate"
	s3.createBucket(t, bucket)
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), nil, "init", root)
	mustDuct(t, root, nil, "query", "CREATE SCHEMA bronze")
	mustDuct(t, root, nil, "query", "CREATE TABLE bronze.big AS SELECT i AS id FROM range(50000) r(i)") // Parquet
	mustDuct(t, root, nil, "query", "CREATE TABLE bronze.small AS SELECT 1 AS id")                      // inlined
	mustDuct(t, root, nil, "gc")                                                                        // flush inlined to Parquet
	want := csvQuery(t, root, nil, "SELECT (SELECT count(*) FROM bronze.big) AS big, (SELECT count(*) FROM bronze.small) AS small")

	files := 0
	lake := filepath.Join(root, "lake")
	err := filepath.WalkDir(lake, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(lake, path)
		body, err := os.ReadFile(path) //nolint:gosec // test path
		if err != nil {
			return err
		}
		s3.must(t, http.MethodPut, "/"+bucket+"/proj/"+filepath.ToSlash(rel), body)
		files++
		return nil
	})
	if err != nil || files == 0 {
		t.Fatalf("copy: %v (%d files)", err, files)
	}
	toml, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	moved := strings.Replace(string(toml), `storage = "./lake"`, `storage = "s3://`+bucket+`/proj"`, 1) +
		"\n[s3]\nendpoint = \"" + s3.endpoint + "\"\nuse_ssl = false\n"
	if err := os.WriteFile(filepath.Join(root, "duct.toml"), []byte(moved), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(lake); err != nil {
		t.Fatal(err)
	}
	env := s3.env()
	if got := csvQuery(t, root, env, "SELECT (SELECT count(*) FROM bronze.big) AS big, (SELECT count(*) FROM bronze.small) AS small"); got != want {
		t.Fatalf("after moving the lake to s3: %q, want %q", got, want)
	}
	// New writes go to the bucket.
	mustDuct(t, root, env, "query", "INSERT INTO bronze.big SELECT i FROM range(50000, 100000) r(i)")
	if got := csvQuery(t, root, env, "SELECT count(*) AS n FROM bronze.big"); got != "n\n100000" {
		t.Fatalf("append after migration: %q", got)
	}
	if _, err := os.Stat(lake); err == nil {
		t.Fatal("a local lake/ reappeared")
	}
}
