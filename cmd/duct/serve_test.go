package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/jackc/pgx/v5"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
)

// selfSigned writes a throwaway certificate for 127.0.0.1 and returns its paths.
func selfSigned(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "duct-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: nil,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	dir := t.TempDir()
	certPath, keyPath = filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	writeFile(t, certPath, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeFile(t, keyPath, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})))
	return certPath, keyPath
}

type served struct {
	pg, http string
	stop     func()
}

// startServe runs `duct serve` in-process on random ports and waits until bound.
func startServe(t *testing.T, root string, extra map[string]map[string]any) served {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range extra {
		if p.Config.Serve.Modules == nil {
			p.Config.Serve.Modules = map[string]map[string]any{}
		}
		p.Config.Serve.Modules[k] = v
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{
			Protocols: []string{"pg", "http"},
			Listen:    map[string]string{"pg": "127.0.0.1:0", "http": "127.0.0.1:0"},
			Ready:     func(a map[string]string) { ready <- a },
		})
	}()
	var addrs map[string]string
	select {
	case addrs = <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("serve exited early: %v", err)
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("serve did not start")
	}
	var once bool
	stop := func() {
		if once {
			return
		}
		once = true
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
	return served{pg: addrs["pg"], http: addrs["http"], stop: stop}
}

// governedProject builds: init -> ingest CSV -> run pipeline -> roles/users.
func governedProject(t *testing.T) string {
	t.Helper()
	root := newProject(t)
	writeFile(t, filepath.Join(root, "data", "customers.csv"),
		"region,rut,name,amt\nCL,11.111.111-1,Ana,100\nAR,22.222.222-2,Beto,200\nCL,33.333.333-3,Cami,300\n")
	writeFile(t, filepath.Join(root, "ingest", "customers.yaml"), "name: customers\nsource: files\noptions:\n  path: data/customers.csv\n")
	mustDuct(t, "ingest", "--project", root)
	writePipeline(t, root, "gold.sql", "CREATE MATERIALIZED VIEW gold.customers AS SELECT * FROM bronze.customers;\n")
	mustDuct(t, "run", "--project", root)
	mustDuct(t, "query", "--project", root, "CREATE TABLE main.secret AS SELECT 'top-secret' AS v")

	g := func(args ...string) string { return mustDuct(t, append(args, "--project", root)...) }
	g("role", "create", "analistas")
	g("role", "create", "vacio")
	g("grant", "select", "on", "gold.customers", "to", "analistas")
	g("policy", "row-filter", "gold.customers", "--role", "analistas", "--where", "region = 'CL'")
	g("policy", "mask", "gold.customers.rut", "--role", "analistas", "--expr", "'***'")
	g("user", "create", "ana", "--role", "analistas", "--password", "pw-ana-123")
	g("user", "create", "nadie", "--role", "vacio", "--password", "pw-nadie-123")
	g("user", "create", "baja", "--role", "analistas", "--password", "pw-baja-123")
	g("user", "disable", "baja")
	return root
}

func pgConnect(t *testing.T, addr, user, pass, params string) (*pgx.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return pgx.Connect(ctx, "postgres://"+user+":"+pass+"@"+addr+"/duct?"+params)
}

func pgRows(t *testing.T, c *pgx.Conn, sql string) ([][]any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rows, err := c.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
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

func TestE2EServe(t *testing.T) {
	t.Parallel()
	root := governedProject(t)
	cert, key := selfSigned(t)
	srv := startServe(t, root, map[string]map[string]any{"pg": {"tls_cert": cert, "tls_key": key}})

	t.Run("pg filtered and masked", func(t *testing.T) {
		for _, mode := range []string{"default", "simple"} {
			c, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=require")
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = c.Close(context.Background()) }()
			sql := "SELECT name, rut, amt FROM customers ORDER BY amt"
			var rows [][]any
			if mode == "simple" {
				rows, err = pgRows(t, c, sql)
			} else {
				var rs pgx.Rows
				rs, err = c.Query(context.Background(), sql)
				if err == nil {
					for rs.Next() {
						v, verr := rs.Values()
						if verr != nil {
							err = verr
							break
						}
						rows = append(rows, v)
					}
					rs.Close()
					if err == nil {
						err = rs.Err()
					}
				}
			}
			if err != nil {
				t.Fatalf("%s: %v", mode, err)
			}
			if len(rows) != 2 || rows[0][0] != "Ana" || rows[0][1] != "***" || rows[1][0] != "Cami" || rows[1][1] != "***" {
				t.Fatalf("%s: rows = %v (want Ana and Cami only, rut masked)", mode, rows)
			}
		}
		c, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=require")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(context.Background()) }()
		rows, err := pgRows(t, c, "SELECT count(*) FROM role_analistas.customers")
		if err != nil || len(rows) != 1 || rows[0][0].(int64) != 2 {
			t.Fatalf("qualified count: %v %v", rows, err)
		}
		if rows, err := pgRows(t, c, "SELECT current_user"); err != nil || rows[0][0] != "ana" {
			t.Fatalf("current_user: %v %v", rows, err)
		}
	})

	t.Run("pg denies everything else", func(t *testing.T) {
		c, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=require")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(context.Background()) }()
		sqliteState := filepath.Join(root, project.StateFile)
		for _, q := range []string{
			"SELECT * FROM gold.customers",
			"SELECT * FROM main.secret",
			"SELECT * FROM secret",
			"SELECT * FROM ducklake.main.secret",
			"SELECT * FROM bronze.customers",
			"SELECT * FROM duct.duct_users",
			"SELECT * FROM read_csv('/etc/passwd')",
			"SELECT * FROM read_text('/etc/passwd')",
			"SELECT * FROM glob('/*')",
			"SELECT * FROM sqlite_scan('" + sqliteState + "', 'duct_users')",
			"SELECT * FROM duckdb_secrets()",
			"SELECT getenv('HOME')",
			"ATTACH '/tmp/duct-evil.db' AS evil",
			"ATTACH '" + sqliteState + "' AS st (TYPE sqlite)",
			"SET enable_external_access = true",
			"SET lock_configuration = false",
			"INSTALL httpfs",
			"LOAD httpfs",
			"CREATE SECRET s (TYPE s3)",
			"COPY (SELECT 1) TO '/tmp/duct-evil.csv'",
			"CREATE TABLE role_analistas.x (i INT)",
			"DROP VIEW role_analistas.customers",
			"INSERT INTO role_analistas.customers VALUES ('a','b','c',1)",
			"PRAGMA database_list",
		} {
			rows, err := pgRows(t, c, q)
			if err == nil {
				t.Errorf("%q must be refused, got rows %v", q, rows)
			}
			// The session survives a refusal.
			if rows, err := pgRows(t, c, "SELECT 1"); err != nil || len(rows) != 1 {
				t.Fatalf("session broken after %q: %v", q, err)
			}
		}
		for _, f := range []string{"/tmp/duct-evil.db", "/tmp/duct-evil.csv"} {
			if _, err := os.Stat(f); err == nil {
				t.Errorf("%s was created", f)
			}
		}
	})

	t.Run("pg auth", func(t *testing.T) {
		if _, err := pgConnect(t, srv.pg, "ana", "wrong", "sslmode=require"); err == nil {
			t.Error("wrong password accepted")
		}
		if _, err := pgConnect(t, srv.pg, "baja", "pw-baja-123", "sslmode=require"); err == nil {
			t.Error("disabled user accepted")
		}
		if _, err := pgConnect(t, srv.pg, "nobody", "x", "sslmode=require"); err == nil {
			t.Error("unknown user accepted")
		}
		// TLS is configured, so a plain connection is refused (no cleartext password).
		if _, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=disable"); err == nil {
			t.Error("plaintext connection accepted although TLS is configured")
		}
		// A role with no grants sees nothing.
		c, err := pgConnect(t, srv.pg, "nadie", "pw-nadie-123", "sslmode=require")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(context.Background()) }()
		if _, err := pgRows(t, c, "SELECT * FROM customers"); err == nil {
			t.Error("role without grants read customers")
		}
		if _, err := pgRows(t, c, "SELECT * FROM role_analistas.customers"); err == nil {
			t.Error("role vacio read another role's schema")
		}
	})

	tok := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "ana", "--label", "test", "--ttl", "1h", "--project", root))
	tokNadie := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "nadie", "--project", root))
	hdo := func(method, sql, token string, hdr map[string]string) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(method, "http://"+srv.http+"/query", strings.NewReader(sql))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header
	}

	t.Run("http formats", func(t *testing.T) {
		code, body, _ := hdo("POST", "SELECT name, rut, amt FROM customers ORDER BY amt", tok, nil)
		if code != 200 {
			t.Fatalf("JSON: %d %s", code, body)
		}
		var js struct {
			Meta []map[string]string `json:"meta"`
			Data []map[string]any    `json:"data"`
			Rows int                 `json:"rows"`
		}
		if err := json.Unmarshal([]byte(body), &js); err != nil {
			t.Fatalf("%v: %s", err, body)
		}
		if js.Rows != 2 || js.Data[0]["name"] != "Ana" || js.Data[0]["rut"] != "***" || js.Data[1]["name"] != "Cami" || len(js.Meta) != 3 {
			t.Fatalf("JSON body: %s", body)
		}
		_, csvBody, _ := hdo("POST", "SELECT name, rut FROM customers ORDER BY amt FORMAT CSVWithNames", tok, nil)
		if csvBody != "name,rut\nAna,***\nCami,***\n" {
			t.Errorf("CSV: %q", csvBody)
		}
		_, nd, _ := hdo("POST", "SELECT name FROM customers ORDER BY amt", tok, map[string]string{})
		_ = nd
		req, _ := http.NewRequest("POST", "http://"+srv.http+"/?default_format=JSONEachRow", strings.NewReader("SELECT name FROM customers ORDER BY amt"))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(b) != "{\"name\":\"Ana\"}\n{\"name\":\"Cami\"}\n" {
			t.Errorf("JSONEachRow: %q", b)
		}

		code, ab, h := hdo("POST", "SELECT name, amt FROM customers ORDER BY amt FORMAT ArrowStream", tok, nil)
		if code != 200 || !strings.Contains(h.Get("Content-Type"), "arrow") {
			t.Fatalf("ArrowStream: %d %v", code, h)
		}
		rd, err := ipc.NewReader(bytes.NewReader([]byte(ab)))
		if err != nil {
			t.Fatal(err)
		}
		defer rd.Release()
		n := 0
		for rd.Next() {
			n += int(rd.RecordBatch().NumRows())
		}
		if n != 2 || len(rd.Schema().Fields()) != 2 {
			t.Errorf("arrow rows=%d schema=%v", n, rd.Schema())
		}
		code, pq, _ := hdo("POST", "SELECT name FROM customers FORMAT Parquet", tok, nil)
		if code != 200 || !strings.HasPrefix(pq, "PAR1") || !strings.HasSuffix(pq, "PAR1") {
			t.Errorf("Parquet: %d len=%d", code, len(pq))
		}
		// Basic auth with a password works as well.
		req, _ = http.NewRequest("POST", "http://"+srv.http+"/query", strings.NewReader("SELECT 1 AS one"))
		req.SetBasicAuth("ana", "pw-ana-123")
		resp, err = http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Errorf("basic auth: %v %v", resp, err)
		}
		_ = resp.Body.Close()
	})

	t.Run("http denies", func(t *testing.T) {
		if code, _, _ := hdo("POST", "SELECT 1", "", nil); code != 401 {
			t.Errorf("no token: %d", code)
		}
		if code, _, _ := hdo("POST", "SELECT 1", "dct_nope", nil); code != 401 {
			t.Errorf("bad token: %d", code)
		}
		if code, _, _ := hdo("POST", "SELECT * FROM customers", tokNadie, nil); code != 403 {
			t.Errorf("role without grants: %d", code)
		}
		sqliteState := filepath.Join(root, project.StateFile)
		for _, q := range []string{
			"SELECT * FROM gold.customers",
			"SELECT * FROM main.secret",
			"SELECT * FROM secret",
			"SELECT * FROM lake.main.secret",
			"SELECT * FROM ducklake.main.secret",
			"SELECT * FROM duct.duct_users",
			"SELECT * FROM duct.main.duct_users",
			"SELECT * FROM role_vacio.customers",
			"SELECT * FROM read_csv('/etc/passwd')",
			"SELECT * FROM sqlite_scan('" + sqliteState + "', 'duct_users')",
			"SELECT * FROM glob('/*')",
			"SELECT * FROM range(3)",
			"SELECT * FROM duckdb_tables()",
			"SELECT getenv('HOME')",
			"SELECT current_setting('s3_secret_access_key')",
			"SELECT (SELECT v FROM main.secret)",
			"SELECT * FROM customers WHERE name IN (SELECT v FROM main.secret)",
			"WITH c AS (SELECT * FROM main.secret) SELECT * FROM c",
			"WITH a AS (SELECT * FROM b), b AS (SELECT 1) SELECT * FROM a",
			"SELECT * FROM (SELECT * FROM main.secret) s",
			"SELECT * FROM customers AT (VERSION => 1)",
			"SELECT * FROM customers; SELECT * FROM main.secret",
			"ATTACH '/tmp/duct-evil.db' AS evil",
			"SET enable_external_access = true",
			"INSTALL httpfs",
			"COPY (SELECT 1) TO '/tmp/duct-evil.csv'",
			"CREATE TABLE x (i INT)",
			"DROP VIEW role_analistas.customers",
			"INSERT INTO role_analistas.customers VALUES ('a','b','c',1)",
			"PRAGMA database_list",
			"SHOW TABLES",
			"DESCRIBE customers",
		} {
			code, body, _ := hdo("POST", q, tok, nil)
			if code != 403 && code != 400 {
				t.Errorf("%q: status %d body %q (want refusal)", q, code, body)
			}
			if code == 200 {
				t.Errorf("%q was executed", q)
			}
		}
		// CTE named like a role view still reads the CTE, not the view.
		_, body, _ := hdo("POST", "WITH customers AS (SELECT 'cte' AS name) SELECT name FROM customers", tok, nil)
		if !strings.Contains(body, `"cte"`) {
			t.Errorf("CTE shadowing: %s", body)
		}
	})

	t.Run("http misc", func(t *testing.T) {
		resp, err := http.Get("http://" + srv.http + "/healthz")
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("healthz: %v %v", resp, err)
		}
		_ = resp.Body.Close()
		resp, err = http.Get("http://" + srv.http + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		for _, want := range []string{"duct_http_requests_total", "duct_http_denied_total", "duct_http_rows_returned_total"} {
			if !strings.Contains(string(b), want) {
				t.Errorf("metrics lacks %s:\n%s", want, b)
			}
		}
		if strings.Contains(string(b), "duct_http_denied_total 0\n") {
			t.Errorf("denied counter did not move:\n%s", b)
		}
		if code, _, _ := hdo("POST", "SELECT * FROM customers", tok, nil); code != 200 {
			t.Errorf("token must keep working: %d", code)
		}
	})

	// Revoking a token stops it at once; a disabled user's tokens stop too.
	t.Run("token revocation", func(t *testing.T) {
		tk := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "ana", "--project", root))
		if code, _, _ := hdo("POST", "SELECT 1", tk, nil); code != 200 {
			t.Fatalf("fresh token: %d", code)
		}
		out := mustDuct(t, "query", "--project", root, "--format", "csv", "SELECT id FROM duct.duct_tokens ORDER BY created_at DESC LIMIT 1")
		id := strings.TrimSpace(strings.Split(out, "\n")[1])
		mustDuct(t, "token", "revoke", id, "--project", root)
		if code, _, _ := hdo("POST", "SELECT 1", tk, nil); code != 401 {
			t.Errorf("revoked token: %d", code)
		}
	})

	srv.stop() // flushes the audit log

	t.Run("audit", func(t *testing.T) {
		rows := queryRows(t, root, "SELECT user_name, role, protocol, sql_text, status, rows_returned FROM duct.duct_audit")
		var pgOK, pgDenied, httpOK, httpDenied int
		for _, r := range rows {
			if r["role"] == "" || r["user_name"] == "" {
				t.Errorf("audit row without identity: %v", r)
			}
			switch {
			case r["protocol"] == "pg" && r["status"] == "ok":
				pgOK++
			case r["protocol"] == "pg" && r["status"] == "denied":
				pgDenied++
			case r["protocol"] == "http" && r["status"] == "ok":
				httpOK++
			case r["protocol"] == "http" && r["status"] == "denied":
				httpDenied++
			}
			for _, k := range []string{"sql_text"} {
				if s, _ := r[k].(string); strings.Contains(s, "pw-ana-123") || strings.Contains(s, "dct_") {
					t.Errorf("audit leaks a secret: %v", r)
				}
			}
		}
		if pgOK < 4 || pgDenied < 20 || httpOK < 5 || httpDenied < 20 {
			t.Errorf("audit counts pg ok=%d denied=%d http ok=%d denied=%d (of %d rows)", pgOK, pgDenied, httpOK, httpDenied, len(rows))
		}
		got := queryRows(t, root, "SELECT rows_returned AS n FROM duct.duct_audit WHERE protocol = 'http' AND sql_text LIKE 'SELECT name, rut, amt FROM customers%' AND status = 'ok'")
		if len(got) != 1 || got[0]["n"].(float64) != 2 {
			t.Errorf("rows_returned for the JSON query: %v", got)
		}
	})
}

var _ = tls.VersionTLS12

// TestHandlerCatalog: the optional Catalog method lists only the role's views.
func TestHandlerCatalog(t *testing.T) {
	t.Parallel()
	root := governedProject(t)
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := openEngine(context.Background(), p, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	h := governance.NewHandler(eng, governance.HandlerOptions{})
	defer func() { _ = h.Close() }()
	cat, ok := h.ForProtocol("web").(interface {
		Catalog(context.Context, contract.Identity) (contract.Stream, error)
	})
	if !ok {
		t.Fatal("protocol handler lacks Catalog")
	}
	list := func(role string) []string {
		st, err := cat.Catalog(context.Background(), contract.Identity{User: "x", Role: role})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Release()
		var out []string
		for st.Next() {
			r := st.RecordBatch()
			for i := range int(r.NumRows()) {
				out = append(out, r.Column(0).ValueStr(i)+"."+r.Column(1).ValueStr(i)+"."+r.Column(2).ValueStr(i)+":"+r.Column(3).ValueStr(i))
			}
		}
		return out
	}
	got := list("analistas")
	if len(got) != 4 || got[0] != "role_analistas.customers.region:VARCHAR" {
		t.Errorf("analistas catalog: %v", got)
	}
	if got := list("vacio"); len(got) != 0 {
		t.Errorf("vacio must see nothing: %v", got)
	}
}
