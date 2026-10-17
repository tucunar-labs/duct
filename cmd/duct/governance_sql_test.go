package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestE2EGovernanceSQL: `duct sql` runs the Unity-style statements with the
// same effect as the role/grant/policy commands.
func TestE2EGovernanceSQL(t *testing.T) {
	t.Parallel()
	root := governedProject(t)
	s := func(sql string) string { return mustDuct(t, "sql", "--project", root, sql) }

	s("CREATE ROLE lectores")
	s("GRANT SELECT ON TABLE gold.customers TO ROLE lectores;")
	s("ALTER TABLE gold.customers SET ROW FILTER region = 'AR' FOR ROLE lectores")
	s("ALTER TABLE gold.customers ALTER COLUMN name SET MASK upper(left(name, 1)) || '.' FOR ROLE lectores")
	if got := queryRows(t, root, "SELECT name, region FROM role_lectores.customers"); len(got) != 1 || got[0]["name"] != "B." || got[0]["region"] != "AR" {
		t.Fatalf("view of lectores: %v", got)
	}
	show := s("SHOW GRANTS TO ROLE lectores")
	for _, want := range []string{"lectores", "gold.customers", "region = 'AR'", "name = upper(left(name, 1)) || '.'"} {
		if !strings.Contains(show, want) {
			t.Errorf("SHOW GRANTS lacks %q:\n%s", want, show)
		}
	}
	if on := s("SHOW GRANTS ON gold.customers"); !strings.Contains(on, "analistas") || !strings.Contains(on, "lectores") {
		t.Errorf("SHOW GRANTS ON: %s", on)
	}
	// The CLI and the SQL see the same state.
	if list := mustDuct(t, "role", "list", "--project", root); !strings.Contains(list, "where region = 'AR'") {
		t.Errorf("role list: %s", list)
	}

	s("ALTER TABLE gold.customers ALTER COLUMN name DROP MASK FOR ROLE lectores")
	s("ALTER TABLE gold.customers DROP ROW FILTER FOR ROLE lectores")
	if got := queryRows(t, root, "SELECT name FROM role_lectores.customers"); len(got) != 3 {
		t.Fatalf("after dropping filter and mask: %v", got)
	}
	s("GRANT ROLE lectores TO USER nadie")
	if u := mustDuct(t, "user", "list", "--project", root); !strings.Contains(u, "nadie\tlectores") {
		t.Errorf("user list: %s", u)
	}
	s("REVOKE SELECT ON gold.customers FROM lectores")
	s("GRANT ROLE vacio TO USER nadie")
	s("DROP ROLE lectores")
	if got := queryRows(t, root, "SELECT schema_name FROM duckdb_schemas() WHERE schema_name = 'role_lectores'"); len(got) != 0 {
		t.Errorf("role schema survived DROP ROLE: %v", got)
	}

	// A trailing line comment cannot swallow the closing parenthesis of the view.
	s("ALTER TABLE gold.customers SET ROW FILTER region = 'CL' -- solo Chile\n FOR ROLE analistas")
	if got := queryRows(t, root, "SELECT count(*) AS n FROM role_analistas.customers"); got[0]["n"].(float64) != 2 {
		t.Errorf("filter with a comment: %v", got)
	}
	s("ALTER TABLE gold.customers SET ROW FILTER region = 'CL' FOR ROLE analistas")

	// Anything else goes to the engine.
	if out := s("SELECT count(*) AS n FROM gold.customers"); !strings.Contains(out, "3") {
		t.Errorf("passthrough: %s", out)
	}
	s("ALTER TABLE main.secret ADD COLUMN extra INTEGER")

	// Errors: syntax with position, semantic, and the unsafe expressions.
	for _, c := range []struct{ sql, want string }{
		{"GRANT SELECT gold.customers TO analistas", `1:14: expected ON`},
		{"GRANT SELECT ON gold.nada TO analistas", "does not exist in the lake"},
		{"GRANT SELECT ON gold.customers TO fantasma", `role "fantasma" does not exist`},
		{"CREATE ROLE Mal-Nombre", "unexpected"},
		{"CREATE ROLE analistas", "already exists"},
		{"ALTER TABLE gold.customers SET ROW FILTER fn ON (region)", "Databricks SQL-UDF"},
		{"ALTER TABLE gold.customers SET ROW FILTER region IN (SELECT region FROM main.secret) FOR ROLE analistas", "subqueries are not allowed"},
		{"ALTER TABLE gold.customers SET ROW FILTER region IN (SELECT 'x') FOR ROLE analistas", "subqueries are not allowed"},
		{"ALTER TABLE gold.customers SET ROW FILTER 1=1) UNION SELECT (1 FOR ROLE analistas", "single scalar expression"},
		{"ALTER TABLE gold.customers SET ROW FILTER (1=1) FROM main.secret --  FOR ROLE analistas", "expected FOR ROLE"},
		{"ALTER TABLE gold.customers SET ROW FILTER 1=1 UNION SELECT true FOR ROLE analistas", "syntax error"},
		{"ALTER TABLE gold.customers SET ROW FILTER (1=1) FROM main.secret FOR ROLE analistas", "syntax error"},
		{"ALTER TABLE gold.customers SET ROW FILTER true) FROM gold.customers WHERE (true FOR ROLE analistas", "syntax error"},
		{"ALTER TABLE gold.customers ALTER COLUMN rut SET MASK 'x') AS rut, (rut FROM gold.customers -- FOR ROLE analistas", "expected FOR ROLE"},
		{"ALTER TABLE gold.customers ALTER COLUMN rut SET MASK 'x') AS rut, (rut FOR ROLE analistas", "single scalar expression"},
		{"ALTER TABLE gold.customers SET ROW FILTER region = read_text('/etc/hostname') FOR ROLE analistas", "function read_text is not allowed"},
		{"ALTER TABLE gold.customers SET ROW FILTER main.secret.v = 'x' FOR ROLE analistas", "another table"},
		{"ALTER TABLE gold.customers SET ROW FILTER ghost = 1 FOR ROLE analistas", "ghost"},
		{"ALTER TABLE gold.customers ALTER COLUMN rut SET MASK (SELECT v FROM main.secret) FOR ROLE analistas", "subqueries are not allowed"},
		{"ALTER TABLE gold.customers ALTER COLUMN rut SET MASK getenv('HOME') FOR ROLE analistas", "function getenv is not allowed"},
		{"ALTER TABLE gold.customers ALTER COLUMN zzz SET MASK 'x' FOR ROLE analistas", `no column "zzz"`},
		{"ALTER TABLE gold.customers SET ROW FILTER region = 'CL' FOR ROLE vacio", "has no grant"},
	} {
		_, errs, code := duct(t, "sql", "--project", root, c.sql)
		if code == 0 || !strings.Contains(errs, c.want) {
			t.Errorf("%q: code=%d stderr=%q, want %q", c.sql, code, errs, c.want)
		}
	}
	// None of the refused statements changed the filter of analistas.
	if got := queryRows(t, root, "SELECT predicate FROM duct.duct_row_filters WHERE role = 'analistas'"); len(got) != 1 || got[0]["predicate"] != "region = 'CL'" {
		t.Errorf("filter of analistas changed: %v", got)
	}
}

// TestE2EServeAdmin: the duct_admin role runs governance SQL over the Postgres
// protocol and over HTTP; a normal user is refused and audited.
func TestE2EServeAdmin(t *testing.T) {
	t.Parallel()
	root := governedProject(t)
	mustDuct(t, "sql", "--project", root, "CREATE ROLE duct_admin")
	mustDuct(t, "user", "create", "jefa", "--role", "duct_admin", "--password", "pw-jefa-123", "--project", root)
	cert, key := selfSigned(t)
	srv := startServe(t, root, map[string]map[string]any{"pg": {"tls_cert": cert, "tls_key": key}})

	connect := func(user, pw string) *testConn {
		c, err := pgConnect(t, srv.pg, user, pw, "sslmode=require")
		if err != nil {
			t.Fatalf("connect %s: %v", user, err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		return &testConn{t: t, c: c}
	}
	admin, ana := connect("jefa", "pw-jefa-123"), connect("ana", "pw-ana-123")

	// Ana is connected from the start and keeps her session throughout.
	if rows := ana.rows("SELECT count(*) FROM customers"); rows[0][0].(int64) != 2 {
		t.Fatalf("ana before: %v", rows)
	}

	// Admin: role, grant, filter and mask; a user moved into the role sees it at once.
	admin.exec("CREATE ROLE ventas")
	admin.exec("GRANT SELECT ON TABLE gold.customers TO ROLE ventas")
	admin.exec("ALTER TABLE gold.customers SET ROW FILTER region = 'AR' FOR ROLE ventas")
	admin.exec("ALTER TABLE gold.customers ALTER COLUMN name SET MASK 'x-' || name FOR ROLE ventas;")
	admin.exec("GRANT ROLE ventas TO USER nadie")
	nadie := connect("nadie", "pw-nadie-123")
	rows := nadie.rows("SELECT name, region FROM customers")
	if len(rows) != 1 || rows[0][0] != "x-Beto" || rows[0][1] != "AR" {
		t.Fatalf("nadie after GRANT ROLE: %v", rows)
	}
	show := admin.rows("SHOW GRANTS TO ROLE ventas")
	if len(show) != 1 || show[0][2] != "gold.customers" || show[0][3] != "region = 'AR'" {
		t.Fatalf("SHOW GRANTS over pg: %v", show)
	}
	if none := admin.rows("SHOW GRANTS TO ROLE vacio"); len(none) != 0 {
		t.Fatalf("SHOW GRANTS of a role without grants: %v", none)
	}

	// A change on a role already in use reaches the open session.
	admin.exec("ALTER TABLE gold.customers ALTER COLUMN rut SET MASK 'oculto' FOR ROLE analistas")
	if rows := ana.rows("SELECT rut FROM customers LIMIT 1"); rows[0][0] != "oculto" {
		t.Errorf("ana's open session after SET MASK: %v", rows)
	}
	admin.exec("ALTER TABLE gold.customers DROP ROW FILTER FOR ROLE analistas")
	if rows := ana.rows("SELECT count(*) FROM customers"); rows[0][0].(int64) != 3 {
		t.Errorf("ana after DROP ROW FILTER: %v", rows)
	}

	// The extended protocol (pgx default mode) works too.
	var n int64
	if err := admin.c.QueryRow(context.Background(), "ALTER TABLE gold.customers SET ROW FILTER region = 'CL' FOR ROLE analistas").Scan(new(string)); err != nil {
		t.Errorf("extended protocol: %v", err)
	}
	if err := ana.c.QueryRow(context.Background(), "SELECT count(*) FROM customers").Scan(&n); err != nil || n != 2 {
		t.Errorf("ana after re-filter: %d %v", n, err)
	}

	// Admin errors keep the session alive and say what is wrong.
	for _, c := range []struct{ sql, want string }{
		{"GRANT SELECT ON TABLE gold.customers TO fantasma", "does not exist"},
		{"ALTER TABLE gold.customers SET ROW FILTER region IN (SELECT 1) FOR ROLE ventas", "subqueries are not allowed"},
		{"ALTER TABLE gold.customers SET ROW FILTER fn ON (region)", "Databricks SQL-UDF"},
		{"GRANT SELECT", "expected ON"},
	} {
		if _, err := admin.try(c.sql); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("admin %q: err = %v, want %q", c.sql, err, c.want)
		}
	}
	if r := admin.rows("SELECT 1"); len(r) != 1 {
		t.Fatal("admin session broke")
	}

	// Non-admins: every governance statement is refused, none has an effect.
	attempts := []string{
		"CREATE ROLE intruso",
		"DROP ROLE ventas",
		"GRANT SELECT ON TABLE main.secret TO ROLE analistas",
		"GRANT ROLE duct_admin TO USER ana",
		"REVOKE SELECT ON TABLE gold.customers FROM analistas",
		"SHOW GRANTS",
		"ALTER TABLE gold.customers DROP ROW FILTER FOR ROLE analistas",
		"ALTER TABLE gold.customers SET ROW FILTER true FOR ROLE analistas",
		"ALTER TABLE gold.customers ALTER COLUMN rut DROP MASK FOR ROLE analistas",
		"ALTER TABLE gold.customers ALTER COLUMN rut SET MASK rut FOR ROLE analistas",
	}
	for _, q := range attempts {
		if rows, err := ana.try(q); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("ana %q: rows=%v err=%v, want permission denied", q, rows, err)
		}
	}
	if rows := ana.rows("SELECT rut, count(*) FROM customers GROUP BY rut"); len(rows) != 1 || rows[0][0] != "oculto" || rows[0][1].(int64) != 2 {
		t.Errorf("ana's view changed after her attempts: %v", rows)
	}

	// HTTP: the same rule with bearer tokens.
	tokAna := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "ana", "--project", root))
	tokJefa := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "jefa", "--project", root))
	hpost := func(token, sql string) (int, string) {
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
	if code, body := hpost(tokAna, "CREATE ROLE http_intruso"); code != 403 || !strings.Contains(body, "permission denied") {
		t.Errorf("http non-admin: %d %s", code, body)
	}
	if code, body := hpost(tokJefa, "CREATE ROLE http_ok"); code != 200 {
		t.Errorf("http admin: %d %s", code, body)
	}
	if code, body := hpost(tokJefa, "SHOW GRANTS TO ROLE ventas FORMAT CSVWithNames"); code != 200 || !strings.Contains(body, "gold.customers") {
		t.Errorf("http SHOW GRANTS: %d %s", code, body)
	}

	srv.stop() // flushes the audit log
	roles := queryRows(t, root, "SELECT name FROM duct.duct_roles WHERE name IN ('intruso', 'http_intruso', 'http_ok', 'ventas')")
	got := map[string]bool{}
	for _, r := range roles {
		got[r["name"].(string)] = true
	}
	if got["intruso"] || got["http_intruso"] || !got["http_ok"] || !got["ventas"] {
		t.Errorf("roles after the run: %v", got)
	}
	if rows := queryRows(t, root, "SELECT role FROM duct.duct_users WHERE name = 'ana'"); rows[0]["role"] != "analistas" {
		t.Errorf("ana escalated: %v", rows)
	}

	// Audit: refusals by the non-admin and the admin's statements, original text, no replacement SELECTs.
	audit := queryRows(t, root, "SELECT user_name, role, protocol, sql_text, status FROM duct.duct_audit")
	count := func(user, sqlPrefix, status string) int {
		n := 0
		for _, r := range audit {
			if r["user_name"] == user && strings.HasPrefix(r["sql_text"].(string), sqlPrefix) && r["status"] == status {
				n++
			}
		}
		return n
	}
	for _, q := range attempts {
		if count("ana", q, "denied") != 1 {
			t.Errorf("audit lacks the denial of ana's %q", q)
		}
	}
	for _, q := range []string{"CREATE ROLE ventas", "GRANT SELECT ON TABLE gold.customers TO ROLE ventas", "GRANT ROLE ventas TO USER nadie", "SHOW GRANTS TO ROLE ventas"} {
		if count("jefa", q, "ok") < 1 {
			t.Errorf("audit lacks the admin statement %q", q)
		}
	}
	if count("jefa", "ALTER TABLE gold.customers SET ROW FILTER region = 'AR'", "ok") != 1 ||
		count("jefa", "ALTER TABLE gold.customers SET ROW FILTER fn ON", "denied")+count("jefa", "ALTER TABLE gold.customers SET ROW FILTER fn ON", "error") < 1 {
		t.Error("audit lacks the filter statements of the admin")
	}
	if count("jefa", "CREATE ROLE http_ok", "ok") != 1 || count("ana", "CREATE ROLE http_intruso", "denied") != 1 {
		t.Error("audit lacks the http statements")
	}
	for _, r := range audit {
		if s := r["sql_text"].(string); strings.Contains(s, "duct:admin") {
			t.Errorf("audit holds an internal replacement statement: %v", r)
		}
	}
}

// testConn is a pgx connection that runs statements in simple-protocol mode.
type testConn struct {
	t *testing.T
	c *pgx.Conn
}

func (tc *testConn) try(sql string) ([][]any, error) {
	tc.t.Helper()
	return pgRows(tc.t, tc.c, sql)
}

func (tc *testConn) rows(sql string) [][]any {
	tc.t.Helper()
	rows, err := tc.try(sql)
	if err != nil {
		tc.t.Fatalf("%q: %v", sql, err)
	}
	return rows
}

func (tc *testConn) exec(sql string) { tc.t.Helper(); tc.rows(sql) }
