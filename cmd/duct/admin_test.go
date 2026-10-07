package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
)

// api is a tiny client of /admin/v1.
type api struct {
	t    *testing.T
	base string
}

func (a api) do(user, pass, token, method, path, body string) (int, map[string]any, string) {
	a.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+a.base+path, rd)
	if err != nil {
		a.t.Fatal(err)
	}
	switch {
	case token != "":
		req.Header.Set("Authorization", "Bearer "+token)
	case user != "":
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, string(raw)
}

func (a api) query(token, user, pass, sql string) (int, string) {
	a.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+a.base+"/", strings.NewReader(sql))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else {
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestE2EAdminAPI(t *testing.T) {
	root := governedProject(t)
	mustDuct(t, "sql", "--project", root, "CREATE ROLE duct_admin")
	mustDuct(t, "user", "create", "jefa", "--role", "duct_admin", "--password", "pw-jefa-123", "--project", root)
	srv := startServe(t, root, nil)
	c := api{t: t, base: srv.http}

	// Authentication and authorisation: 401 without credentials, 403 for a
	// role that is not duct_admin, on every verb.
	for _, rt := range [][2]string{{"GET", "/admin/v1/users"}, {"POST", "/admin/v1/users"}, {"GET", "/admin/v1/status"}, {"DELETE", "/admin/v1/quotas/analistas"}} {
		if code, _, _ := c.do("", "", "", rt[0], rt[1], ""); code != 401 {
			t.Errorf("%s %s without credentials: %d, want 401", rt[0], rt[1], code)
		}
		if code, _, body := c.do("ana", "pw-ana-123", "", rt[0], rt[1], `{"name":"x","role":"analistas"}`); code != 403 {
			t.Errorf("%s %s as a non-admin: %d %s, want 403", rt[0], rt[1], code, body)
		}
	}
	if code, _, _ := c.do("jefa", "wrong", "", "GET", "/admin/v1/users", ""); code != 401 {
		t.Errorf("bad password: %d, want 401", code)
	}
	admin := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		code, m, raw := c.do("jefa", "pw-jefa-123", "", method, path, body)
		if code >= 500 {
			t.Fatalf("%s %s: %d %s", method, path, code, raw)
		}
		return code, m
	}

	// Status.
	code, st := admin("GET", "/admin/v1/status", "")
	if code != 200 || st["catalog"].(map[string]any)["type"] != "sqlite" || st["storage"].(map[string]any)["kind"] != "local" {
		t.Fatalf("status: %d %v", code, st)
	}
	mig := st["migrations"].(map[string]any)
	if int(mig["latest"].(float64)) != len(project.Migrations) || len(mig["pending"].([]any)) != 0 {
		t.Errorf("status migrations: %v", mig)
	}
	if _, ok := st["snapshot"].(map[string]any)["id"]; !ok || st["components"].(map[string]any)["jobs"] == nil {
		t.Errorf("status lacks snapshot or components: %v", st)
	}

	// Users: create with and without a password, list, get, disable, enable, reset.
	if code, m := admin("POST", "/admin/v1/users", `{"name":"lucia","role":"analistas","password":"pw-lucia-123"}`); code != 201 || m["temporary_password"] != nil {
		t.Fatalf("create user: %d %v", code, m)
	}
	if code, _ := admin("POST", "/admin/v1/users", `{"name":"lucia","role":"analistas"}`); code != 409 {
		t.Errorf("duplicate user: %d, want 409", code)
	}
	if code, _ := admin("POST", "/admin/v1/users", `{"name":"x","role":"nope"}`); code != 404 {
		t.Errorf("user with unknown role: %d, want 404", code)
	}
	if code, _ := admin("POST", "/admin/v1/users", `{"name":"x","role":"analistas","bogus":1}`); code != 400 {
		t.Errorf("unknown field: %d, want 400", code)
	}
	code, m := admin("POST", "/admin/v1/users", `{"name":"mario","role":"analistas"}`)
	tmp, _ := m["temporary_password"].(string)
	if code != 201 || tmp == "" {
		t.Fatalf("create user without password: %d %v", code, m)
	}
	if code, _ := c.query("", "mario", tmp, "SELECT count(*) AS n FROM customers"); code != 200 {
		t.Errorf("mario with the temporary password: %d", code)
	}
	if code, m := admin("GET", "/admin/v1/users", ""); code != 200 || len(m["users"].([]any)) < 5 {
		t.Errorf("list users: %d %v", code, m)
	}
	if code, m := admin("GET", "/admin/v1/users/lucia", ""); code != 200 || m["role"] != "analistas" || m["disabled"] != false {
		t.Errorf("get user: %d %v", code, m)
	}
	if code, _ := admin("GET", "/admin/v1/users/ghost", ""); code != 404 {
		t.Errorf("get unknown user: %d, want 404", code)
	}
	if code, _ := admin("POST", "/admin/v1/users/lucia/disable", ""); code != 200 {
		t.Fatalf("disable: %d", code)
	}
	if code, _ := c.query("", "lucia", "pw-lucia-123", "SELECT 1"); code != 401 {
		t.Errorf("disabled user can still log in: %d", code)
	}
	admin("POST", "/admin/v1/users/lucia/enable", "")
	if code, _ := c.query("", "lucia", "pw-lucia-123", "SELECT count(*) AS n FROM customers"); code != 200 {
		t.Errorf("enabled user cannot log in: %d", code)
	}
	code, m = admin("POST", "/admin/v1/users/lucia/password-reset", "")
	reset, _ := m["temporary_password"].(string)
	if code != 200 || reset == "" || reset == "pw-lucia-123" {
		t.Fatalf("password reset: %d %v", code, m)
	}
	if code, _ := c.query("", "lucia", "pw-lucia-123", "SELECT 1"); code != 401 {
		t.Errorf("old password still works after reset: %d", code)
	}
	if code, _ := c.query("", "lucia", reset, "SELECT count(*) AS n FROM customers"); code != 200 {
		t.Errorf("reset password does not work: %d", code)
	}
	if code, _ := admin("POST", "/admin/v1/users/jefa/disable", ""); code != 409 {
		t.Errorf("an admin disabling themselves: %d, want 409", code)
	}

	// Roles and grants.
	if code, _ := admin("POST", "/admin/v1/roles", `{"name":"finanzas"}`); code != 201 {
		t.Fatalf("create role: %d", code)
	}
	if code, _ := admin("PUT", "/admin/v1/roles/finanzas/grants/gold.customers", ""); code != 200 {
		t.Fatalf("grant: %d", code)
	}
	if code, _ := admin("PUT", "/admin/v1/roles/finanzas/grants/gold.customers/row-filter", `{"predicate":"region = 'AR'"}`); code != 200 {
		t.Fatalf("row filter: %d", code)
	}
	if code, _ := admin("PUT", "/admin/v1/roles/finanzas/grants/gold.customers/masks/name", `{"expr":"'x-' || name"}`); code != 200 {
		t.Fatalf("mask: %d", code)
	}
	code, m = admin("GET", "/admin/v1/roles/finanzas/grants", "")
	if code != 200 || len(m["grants"].([]any)) != 1 || m["row_filters"].(map[string]any)["gold.customers"] != "region = 'AR'" {
		t.Errorf("role grants: %d %v", code, m)
	}
	admin("PUT", "/admin/v1/users/mario/role", `{"role":"finanzas"}`)
	if code, body := c.query("", "mario", tmp, "SELECT name, region FROM customers"); code != 200 || !strings.Contains(body, "x-Beto") || strings.Contains(body, "Ana") {
		t.Errorf("mario in finanzas: %d %s", code, body)
	}
	if code, _ := admin("DELETE", "/admin/v1/roles/finanzas", ""); code != 409 {
		t.Errorf("deleting a role with users: %d, want 409", code)
	}
	if code, _ := admin("DELETE", "/admin/v1/roles/duct_admin", ""); code != 409 {
		t.Errorf("deleting duct_admin: %d, want 409", code)
	}
	if code, _ := admin("DELETE", "/admin/v1/roles/finanzas/grants/gold.customers", ""); code != 204 {
		t.Errorf("revoke: %d", code)
	}

	// Tokens: the secret is returned once and never listed.
	code, m = admin("POST", "/admin/v1/tokens", `{"user":"lucia","label":"ci","ttl_seconds":600}`)
	tok, _ := m["token"].(string)
	tokID, _ := m["id"].(string)
	if code != 201 || !strings.HasPrefix(tok, "dct_") || tokID == "" {
		t.Fatalf("create token: %d %v", code, m)
	}
	if code, body := c.query(tok, "", "", "SELECT count(*) AS n FROM customers"); code != 200 {
		t.Errorf("bearer token: %d %s", code, body)
	}
	code, _, raw := c.do("jefa", "pw-jefa-123", "", "GET", "/admin/v1/tokens?user=lucia", "")
	if code != 200 || strings.Contains(raw, tok) || strings.Contains(raw, "token_hash") || !strings.Contains(raw, tokID) {
		t.Errorf("token list leaks the secret or lacks the id: %d %s", code, raw)
	}
	if code, _ := admin("DELETE", "/admin/v1/tokens/"+tokID, ""); code != 200 {
		t.Errorf("revoke token: %d", code)
	}
	if code, _ := c.query(tok, "", "", "SELECT 1"); code != 401 {
		t.Errorf("revoked token still works: %d", code)
	}
	if code, _ := admin("DELETE", "/admin/v1/tokens/nope", ""); code != 404 {
		t.Errorf("revoke unknown token: %d, want 404", code)
	}

	// Quotas: max_rows cuts a statement with 429; removing the quota lifts it.
	if code, _ := admin("PUT", "/admin/v1/quotas/analistas", `{}`); code != 400 {
		t.Errorf("empty quota: %d, want 400", code)
	}
	if code, _ := admin("PUT", "/admin/v1/quotas/ghost", `{"max_rows":1}`); code != 404 {
		t.Errorf("quota of an unknown role: %d, want 404", code)
	}
	if code, m := admin("PUT", "/admin/v1/quotas/analistas", `{"max_rows":1,"max_concurrent":3}`); code != 200 || m["max_rows"].(float64) != 1 {
		t.Fatalf("set quota: %d %v", code, m)
	}
	if code, body := c.query("", "ana", "pw-ana-123", "SELECT name FROM customers"); code != 429 || !strings.Contains(body, "quota exceeded") {
		t.Errorf("2 rows (ana's filter) over max_rows=1: %d %s", code, body)
	}
	if code, body := c.query("", "ana", "pw-ana-123", "SELECT name FROM customers LIMIT 1"); code != 200 {
		t.Errorf("1 row within max_rows=1: %d %s", code, body)
	}
	if code, m := admin("GET", "/admin/v1/quotas", ""); code != 200 || len(m["quotas"].([]any)) != 1 {
		t.Errorf("list quotas: %d %v", code, m)
	}
	if code, _ := admin("DELETE", "/admin/v1/quotas/analistas", ""); code != 204 {
		t.Errorf("delete quota: %d", code)
	}
	if code, body := c.query("", "ana", "pw-ana-123", "SELECT name FROM customers"); code != 200 {
		t.Errorf("after removing the quota: %d %s", code, body)
	}

	// Probes and metrics.
	resp, err := http.Get("http://" + srv.http + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(rb), `"catalog":"ok"`) || !strings.Contains(string(rb), `"storage":"ok"`) {
		t.Errorf("/readyz: %d %s", resp.StatusCode, rb)
	}
	resp, err = http.Get("http://" + srv.http + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	mb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	for _, want := range []string{
		`duct_queries_total{protocol="http",status="ok"}`,
		`duct_queries_total{protocol="http",status="quota"}`,
		`duct_queries_total{protocol="admin",status="denied"}`,
		`duct_query_duration_seconds_bucket{protocol="http",le="+Inf"}`,
		`duct_rows_returned_total{protocol="http"}`,
		`duct_auth_failures_total{scheme="password"}`,
		`duct_auth_failures_total{scheme="bearer"}`,
		`duct_quota_rejections_total{kind="rows"}`,
		`duct_pipeline_runs_total{status="success"}`,
		"duct_lake_snapshot_id ",
		`duct_build_info{version="`,
		"duct_http_requests_total",
	} {
		if !strings.Contains(string(mb), want) {
			t.Errorf("/metrics lacks %q; got:\n%s", want, grep(string(mb), "duct_pipeline|duct_collect|duct_metrics|duct_job"))
		}
	}

	// Every call is audited, and no secret reaches the audit log.
	srv.stop()
	out := mustDuct(t, "query", "--project", root, "--format", "csv",
		"SELECT status, count(*) AS n FROM duct.duct_audit WHERE protocol = 'admin' GROUP BY status ORDER BY status")
	for _, want := range []string{"denied", "error", "ok"} {
		if !strings.Contains(out, want) {
			t.Errorf("admin audit lacks status %q:\n%s", want, out)
		}
	}
	leak := mustDuct(t, "query", "--project", root, "--format", "csv",
		"SELECT count(*) AS n FROM duct.duct_audit WHERE sql_text LIKE '%dct_%' OR sql_text LIKE '%"+tmp+"%' OR sql_text LIKE '%"+reset+"%' OR sql_text LIKE '%pw-lucia-123%'")
	if strings.TrimSpace(leak) != "n\n0" {
		t.Errorf("secrets in the audit log: %q", leak)
	}
	post := mustDuct(t, "query", "--project", root, "--format", "csv",
		"SELECT count(*) AS n FROM duct.duct_audit WHERE protocol = 'admin' AND user_name = 'jefa' AND sql_text LIKE 'POST /admin/v1/users name=lucia%'")
	if strings.TrimSpace(post) != "n\n1" { // only the successful create names the user (failures are audited without a note)
		t.Errorf("audit of POST /users: %q", post)
	}
}

// TestQuotaConcurrent: max_concurrent counts statements still being read.
func TestQuotaConcurrent(t *testing.T) {
	root := governedProject(t)
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	h := governance.NewHandler(eng, governance.HandlerOptions{})
	defer func() { _ = h.Close() }()
	one := 1
	if err := governance.NewStore(eng).SetQuota(ctx, "analistas", &one, nil); err != nil {
		t.Fatal(err)
	}
	h.InvalidateQuotas()
	ana := contract.Identity{User: "ana", Role: "analistas"}
	first, err := h.Query(ctx, ana, "SELECT name FROM customers")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Query(ctx, ana, "SELECT name FROM customers"); !governance.IsQuotaError(err) {
		t.Fatalf("second concurrent statement: %v, want a quota error", err)
	}
	// Another role is unaffected.
	if _, err := h.Query(ctx, contract.Identity{User: "nadie", Role: "vacio"}, "SELECT 1"); err != nil && governance.IsQuotaError(err) {
		t.Errorf("quota leaked to another role: %v", err)
	}
	first.Release()
	third, err := h.Query(ctx, ana, "SELECT name FROM customers")
	if err != nil {
		t.Fatalf("after the first statement finished: %v", err)
	}
	third.Release()
}

// grep returns the lines of text that match any of the |-separated words.
func grep(text, words string) string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		for _, w := range strings.Split(words, "|") {
			if strings.Contains(l, w) {
				out = append(out, l)
				break
			}
		}
	}
	return strings.Join(out, "\n")
}
