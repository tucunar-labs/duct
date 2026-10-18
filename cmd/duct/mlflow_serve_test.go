package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// callModel POSTs (or GETs) a /models/ path with an optional bearer token.
func callModel(t *testing.T, addr, method, path, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestModelProxyAuth: /models/<name>/invocations is only reachable with a Duct token of an
// allowed role (401 / 403 / 200), never leaks the credential to the backend, caps the body,
// only forwards the two routes and audits each call. The backend is a stand-in for
// `mlflow models serve` (the real one runs in TestMLflowIntegration).
func TestModelProxyAuth(t *testing.T) {
	t.Parallel()
	var sawAuth, sawPath string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawAuth, sawPath = r.Header.Get("Authorization"), r.URL.Path
		if r.URL.Path == "/ping" {
			w.WriteHeader(200)
			return
		}
		_, _ = io.WriteString(w, `{"predictions":[1.5]}`)
	}))
	defer backend.Close()
	portOf := func(addr string) string { return addr[strings.LastIndex(addr, ":")+1:] }
	port, otherPort := portOf(backend.Listener.Addr().String()), portOf(freePort(t))

	root := newProject(t)
	cfg, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	writeFile(t, filepath.Join(root, "duct.toml"), string(cfg)+"\n[mlflow]\n[mlflow.serving]\nroles = [\"scoring\"]\nmax_body_mb = 1\n"+
		"[mlflow.serving.models.churn]\nport = "+port+"\n[mlflow.serving.models.restricted]\nport = "+otherPort+"\nroles = [\"nobody\"]\n")
	mustDuct(t, "role", "create", "scoring", "--project", root)
	mustDuct(t, "role", "create", "otro", "--project", root)
	mustDuct(t, "sql", "--project", root, "CREATE ROLE duct_admin")
	mustDuct(t, "user", "create", "sam", "--role", "scoring", "--password", "pw-sam-123", "--project", root)
	mustDuct(t, "user", "create", "ota", "--role", "otro", "--password", "pw-ota-123", "--project", root)
	mustDuct(t, "user", "create", "jefa", "--role", "duct_admin", "--password", "pw-jefa-123", "--project", root)
	tok := func(u string) string {
		return strings.TrimSpace(mustDuct(t, "token", "create", "--user", u, "--project", root))
	}
	sam, ota, jefa := tok("sam"), tok("ota"), tok("jefa")
	srv := startServe(t, root, nil)

	const inv = "/models/churn/invocations"
	body := `{"dataframe_split":{"columns":["a"],"data":[[1]]}}`
	if code, _ := callModel(t, srv.http, "POST", inv, "", body); code != 401 {
		t.Errorf("no token: %d, want 401", code)
	}
	if code, _ := callModel(t, srv.http, "POST", inv, "not-a-token", body); code != 401 {
		t.Errorf("bad token: %d, want 401", code)
	}
	if code, _ := callModel(t, srv.http, "POST", inv, ota, body); code != 403 {
		t.Errorf("wrong role: %d, want 403", code)
	}
	sawAuth, sawPath = "x", ""
	if code, out := callModel(t, srv.http, "POST", inv, sam, body); code != 200 || !strings.Contains(out, "predictions") {
		t.Errorf("allowed role: %d %s", code, out)
	}
	if sawAuth != "" || sawPath != "/invocations" {
		t.Errorf("backend saw Authorization=%q path=%q: credentials must not reach it", sawAuth, sawPath)
	}
	if code, _ := callModel(t, srv.http, "POST", inv, jefa, body); code != 200 {
		t.Errorf("duct_admin: %d, want 200", code)
	}
	if code, _ := callModel(t, srv.http, "POST", "/models/restricted/invocations", sam, body); code != 403 {
		t.Errorf("per-model roles replace the table roles: %d, want 403", code)
	}
	if code, _ := callModel(t, srv.http, "GET", "/models/churn/health", sam, ""); code != 200 {
		t.Errorf("health: %d, want 200", code)
	}
	if code, _ := callModel(t, srv.http, "GET", "/models/churn/health", "", ""); code != 401 {
		t.Errorf("health without token: %d, want 401", code)
	}
	if code, _ := callModel(t, srv.http, "GET", inv, sam, ""); code != 405 {
		t.Errorf("GET invocations: %d, want 405", code)
	}
	for _, p := range []string{"/models/churn/version", "/models/churn/metadata", "/models/nope/invocations", "/models/churn"} {
		if code, _ := callModel(t, srv.http, "POST", p, sam, body); code != 404 {
			t.Errorf("%s: %d, want 404", p, code)
		}
	}
	if code, _ := callModel(t, srv.http, "POST", inv, sam, strings.Repeat("x", 2<<20)); code != 413 {
		t.Errorf("2 MiB body with a 1 MiB cap: %d, want 413", code)
	}
	if m := scrapeMetrics(t, srv.http); !strings.Contains(m, `duct_model_requests_total{model="churn",status="denied"}`) {
		t.Errorf("/metrics lacks the denied counter")
	}

	srv.stop()
	out := mustDuct(t, "query", "--project", root, "--format", "csv",
		"SELECT status, count(*) FROM duct.duct_audit WHERE protocol = 'models' AND sql_text LIKE '%churn%' GROUP BY status ORDER BY status")
	for _, want := range []string{"denied", "ok"} {
		if !strings.Contains(out, want) {
			t.Errorf("audit lacks %q for protocol models:\n%s", want, out)
		}
	}
	if strings.Contains(out, sam) || strings.Contains(mustDuct(t, "query", "--project", root, "SELECT sql_text FROM duct.duct_audit WHERE protocol = 'models'"), sam) {
		t.Error("a token reached the audit log")
	}
}

func TestModelNameFromURI(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{"models:/churn/1": "churn", "models:/churn@champion": "churn", "models:/a-b.c/Production": "a-b.c"} {
		if got, err := modelNameFromURI(in); err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"runs:/abc/model", "models:/", "churn"} {
		if _, err := modelNameFromURI(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
