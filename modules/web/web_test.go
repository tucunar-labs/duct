package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// ---- fakes ----

type fakeAuth struct {
	fail bool
	role string // "" = "analyst"
}

func (*fakeAuth) Name() string                                { return "fake" }
func (*fakeAuth) Init(context.Context, contract.Config) error { return nil }
func (*fakeAuth) Close() error                                { return nil }
func (f *fakeAuth) Authenticate(_ context.Context, c contract.Credentials) (contract.Identity, error) {
	if f.fail {
		return contract.Identity{}, errors.New("backend down")
	}
	if c.Scheme == "password" && c.User == "ana" && c.Secret == "pw" {
		role := f.role
		if role == "" {
			role = "analyst"
		}
		return contract.Identity{User: "ana", Role: role}, nil
	}
	return contract.Identity{}, fmt.Errorf("bad login: %w", contract.ErrUnauthenticated)
}

type call struct {
	who contract.Identity
	sql string
}

// fakeQ answers information_schema with a fixed catalog and everything else
// from a per-statement table; it records who asked.
type fakeQ struct {
	mu    sync.Mutex
	calls []call
	rows  int    // rows returned by "SELECT n"
	fail  string // statement that errors
}

func (q *fakeQ) Query(_ context.Context, id contract.Identity, sql string) (contract.Stream, error) {
	q.mu.Lock()
	q.calls = append(q.calls, call{id, sql})
	q.mu.Unlock()
	if sql == q.fail {
		return nil, errors.New("Catalog Error: table \"nope\" does not exist")
	}
	mem := memory.NewGoAllocator()
	str := func(vs ...string) arrow.Array {
		b := array.NewStringBuilder(mem)
		for _, v := range vs {
			if v == "<null>" {
				b.AppendNull()
			} else {
				b.Append(v)
			}
		}
		return b.NewArray()
	}
	var names []string
	var cols []arrow.Array
	if strings.Contains(sql, "information_schema.columns") {
		names = []string{"table_schema", "table_name", "column_name", "data_type", "is_nullable"}
		cols = []arrow.Array{
			str("gold", "gold", "gold", "silver"),
			str("sales", "sales", "sales", "orders"),
			str("id", "<b>name</b>", "amount", "id"),
			str("BIGINT", "VARCHAR", "DOUBLE", "BIGINT"),
			str("NO", "YES", "YES", "NO"),
		}
	} else if strings.Contains(sql, "count(*)") {
		names = []string{"count_star()"}
		cols = []arrow.Array{str("42")}
	} else {
		n := q.rows
		if n == 0 {
			n = 2
		}
		ib := array.NewInt64Builder(mem)
		var vs []string
		for i := 0; i < n; i++ {
			ib.Append(int64(i))
			switch i {
			case 0:
				vs = append(vs, "<script>alert(1)</script>")
			case 1:
				vs = append(vs, "<null>")
			default:
				vs = append(vs, "v")
			}
		}
		names = []string{"n", "label"}
		cols = []arrow.Array{ib.NewArray(), str(vs...)}
	}
	fields := make([]arrow.Field, len(names))
	for i, n := range names {
		fields[i] = arrow.Field{Name: n, Type: cols[i].DataType(), Nullable: true}
	}
	sc := arrow.NewSchema(fields, nil)
	rec := array.NewRecordBatch(sc, cols, int64(cols[0].Len()))
	rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
	return rdr, err
}

// ---- helpers ----

type env struct {
	t   *testing.T
	srv *httptest.Server
	q   *fakeQ
	c   *http.Client
}

func newEnv(t *testing.T, cfg appConfig, auth *fakeAuth) *env {
	t.Helper()
	if cfg.rowLimit == 0 {
		cfg.rowLimit = 1000
	}
	cfg.queryTimeout, cfg.sessionTTL = 5*time.Second, time.Hour
	q := &fakeQ{}
	srv := httptest.NewServer(newApp(q, auth, cfg).routes())
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &env{t: t, srv: srv, q: q, c: c}
}

func (e *env) do(req *http.Request) (*http.Response, string) {
	e.t.Helper()
	resp, err := e.c.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func (e *env) get(path string) (*http.Response, string) {
	e.t.Helper()
	req, _ := http.NewRequest("GET", e.srv.URL+path, nil)
	return e.do(req)
}

func (e *env) cookie(name string) string {
	u, _ := url.Parse(e.srv.URL)
	for _, c := range e.c.Jar.Cookies(u) {
		if c.Name == name {
			return c.Value
		}
	}
	return ""
}

func (e *env) postForm(path string, v url.Values) (*http.Response, string) {
	e.t.Helper()
	req, _ := http.NewRequest("POST", e.srv.URL+path, strings.NewReader(v.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return e.do(req)
}

// login performs the real flow: GET the form (csrf cookie), POST credentials.
func (e *env) login() {
	e.t.Helper()
	_, body := e.get("/login")
	if !strings.Contains(body, `name="csrf"`) {
		e.t.Fatalf("login form without csrf field: %s", body)
	}
	resp, _ := e.postForm("/login", url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {"ana"}, "password": {"pw"}})
	if resp.StatusCode != http.StatusSeeOther {
		e.t.Fatalf("login status = %d", resp.StatusCode)
	}
}

// csrfToken scrapes the logout form token, as the browser would hold it.
func (e *env) csrfToken() string {
	e.t.Helper()
	_, body := e.get("/catalog")
	const marker = `name="csrf" value="`
	i := strings.Index(body, marker)
	if i < 0 {
		e.t.Fatalf("no csrf token in page: %s", body)
	}
	rest := body[i+len(marker):]
	return rest[:strings.Index(rest, `"`)]
}

func (e *env) run(sql, token string) (*http.Response, string) {
	e.t.Helper()
	body := fmt.Sprintf(`{"sql":%q,"_csrf":"x"}`, sql)
	req, _ := http.NewRequest("POST", e.srv.URL+"/sql/run", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Datastar-Request", "true")
	if token != "" {
		req.Header.Set("X-CSRF-Token", token)
	}
	return e.do(req)
}

// ---- tests ----

func TestUnauthorizedRedirects(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	for _, p := range []string{"/catalog", "/sql"} {
		resp, _ := e.get(p)
		if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/login?next=") {
			t.Errorf("%s: status %d location %q", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	resp, _ := e.postForm("/logout", nil)
	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("logout without session: %d", resp.StatusCode)
	}
	resp, body := e.run("select 1", "whatever")
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, "/login") {
		t.Errorf("datastar unauth: %d %q", resp.StatusCode, body)
	}
	if len(e.q.calls) != 0 {
		t.Errorf("query handler reached without session: %v", e.q.calls)
	}
}

func TestLoginFlow(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})

	// CSRF: no cookie / wrong token are rejected before the provider is asked.
	resp, _ := e.postForm("/login", url.Values{"user": {"ana"}, "password": {"pw"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("login without csrf: %d", resp.StatusCode)
	}
	e.get("/login")
	resp, _ = e.postForm("/login", url.Values{"csrf": {"forged"}, "user": {"ana"}, "password": {"pw"}})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("login with forged csrf: %d", resp.StatusCode)
	}

	// Bad password: 401, message, no session.
	resp, body := e.postForm("/login", url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {"ana"}, "password": {"nope"}})
	if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, "incorrectos") {
		t.Errorf("bad password: %d", resp.StatusCode)
	}
	if e.cookie(sessionCookie) != "" {
		t.Error("session cookie set after failed login")
	}

	// Good login: redirect to next, hardened cookie.
	resp, _ = e.postForm("/login", url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {"ana"}, "password": {"pw"}, "next": {"/sql"}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/sql" {
		t.Fatalf("good login: %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	var sc *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie {
			sc = c
		}
	}
	if sc == nil || !sc.HttpOnly || sc.SameSite != http.SameSiteStrictMode || sc.Value == "" || sc.Path != "/" {
		t.Fatalf("session cookie not hardened: %+v", sc)
	}
	if resp, _ := e.get("/sql"); resp.StatusCode != http.StatusOK {
		t.Errorf("/sql after login: %d", resp.StatusCode)
	}

	// Logout needs the CSRF token, then kills the session.
	resp, _ = e.postForm("/logout", nil)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("logout without csrf: %d", resp.StatusCode)
	}
	resp, _ = e.postForm("/logout", url.Values{"csrf": {e.csrfToken()}})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Errorf("logout: %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if resp, _ := e.get("/catalog"); resp.StatusCode != http.StatusSeeOther {
		t.Errorf("catalog after logout: %d", resp.StatusCode)
	}
}

func TestLoginProviderFailureAndOpenRedirect(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{fail: true})
	e.get("/login")
	resp, body := e.postForm("/login", url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {"ana"}, "password": {"pw"}})
	if resp.StatusCode != http.StatusBadGateway || strings.Contains(body, "backend down") {
		t.Errorf("provider failure: %d (must not leak the error)", resp.StatusCode)
	}

	for _, next := range []string{"//evil.example", "https://evil.example", `/\evil.example`} {
		e = newEnv(t, appConfig{}, &fakeAuth{})
		e.get("/login")
		resp, _ := e.postForm("/login", url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {"ana"}, "password": {"pw"}, "next": {next}})
		if loc := resp.Header.Get("Location"); loc != "/catalog" {
			t.Errorf("next=%q redirected to %q", next, loc)
		}
	}
}

func TestSecureCookieBehindProxy(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.get("/login")
	req, _ := http.NewRequest("POST", e.srv.URL+"/login", strings.NewReader(url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {"ana"}, "password": {"pw"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Forwarded-Proto", "https")
	resp, _ := e.do(req)
	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie && !c.Secure {
			t.Error("session cookie not Secure behind https proxy")
		}
	}
}

func TestCatalogRender(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	resp, body := e.get("/catalog")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	for _, want := range []string{"gold", "silver", "sales", "orders", "BIGINT", "amount", "analyst", "ana", "3 columnas", "/sql?q=SELECT"} {
		if !strings.Contains(body, want) {
			t.Errorf("catalog missing %q", want)
		}
	}
	if strings.Contains(body, "<b>name</b>") || !strings.Contains(body, "&lt;b&gt;name&lt;/b&gt;") {
		t.Error("column name not HTML-escaped")
	}
	last := e.q.calls[len(e.q.calls)-1]
	if last.who != (contract.Identity{User: "ana", Role: "analyst"}) || !strings.Contains(last.sql, "information_schema") {
		t.Errorf("catalog ran as %+v", last)
	}
}

func TestSQLPageIsPrefilled(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	_, body := e.get("/sql?q=" + url.QueryEscape(`SELECT '</textarea><script>x</script>'`))
	if strings.Contains(body, "<script>x</script>") {
		t.Error("query parameter injected unescaped")
	}
	for _, want := range []string{"data-sql-editor", "data-bind:sql", "/sql/run", "editor.js", "grid.js", "datastar.js", "wordmark"} {
		if !strings.Contains(body, want) {
			t.Errorf("editor page missing %q", want)
		}
	}
}

func TestRunQuery(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	tok := e.csrfToken()

	resp, body := e.run("SELECT n, label FROM t", tok)
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	for _, want := range []string{
		"datastar-patch-elements", "mode replace", `id="results"`, // replaced, not morphed
		"2 filas", `data-rows="2"`, // meta line for the history recorder
		"<duct-grid data-spec='", // virtualized island fed with compact JSON
		`"rows":[`,               // raw JSON in a single-quoted attribute
		`"name":"label"`,
		`\u003cscript\u003ealert(1)\u003c/script\u003e`, // < > & are \u-escaped by encoding/json
	} {
		if !strings.Contains(body, want) {
			t.Errorf("result missing %q in %s", want, body)
		}
	}
	if strings.Contains(body, "<script>alert(1)") {
		t.Error("cell not escaped")
	}
	last := e.q.calls[len(e.q.calls)-1]
	if last.who.User != "ana" || last.who.Role != "analyst" || last.sql != "SELECT n, label FROM t" {
		t.Errorf("ran as %+v", last)
	}
}

func TestRunQueryTruncates(t *testing.T) {
	e := newEnv(t, appConfig{rowLimit: 5}, &fakeAuth{})
	e.q.rows = 50
	e.login()
	_, body := e.run("SELECT n FROM big", e.csrfToken())
	if !strings.Contains(body, "5 filas") || !strings.Contains(body, "truncado al límite de 5") {
		t.Errorf("no truncation notice: %s", body)
	}
}

func TestRunQueryErrorAndEmpty(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.q.fail = "SELECT * FROM nope"
	e.login()
	tok := e.csrfToken()
	_, body := e.run("SELECT * FROM nope", tok)
	if !strings.Contains(body, `role="alert"`) || !strings.Contains(body, "does not exist") {
		t.Errorf("error not shown inline: %s", body)
	}
	_, body = e.run("   ", tok)
	if !strings.Contains(body, "Escribe una consulta") {
		t.Errorf("empty query: %s", body)
	}
}

func TestCSRFAndOriginRejected(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	tok := e.csrfToken()
	before := len(e.q.calls)

	for name, token := range map[string]string{"missing": "", "wrong": "deadbeef", "login-cookie-token": e.cookie(sessionCookie)} {
		if resp, _ := e.run("SELECT 1", token); resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s token: status %d", name, resp.StatusCode)
		}
	}
	for h, v := range map[string]string{"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"} {
		req, _ := http.NewRequest("POST", e.srv.URL+"/sql/run", strings.NewReader(`{"sql":"SELECT 1"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-CSRF-Token", tok)
		req.Header.Set(h, v)
		if resp, _ := e.do(req); resp.StatusCode != http.StatusForbidden {
			t.Errorf("%s=%s: status %d", h, v, resp.StatusCode)
		}
	}
	if len(e.q.calls) != before {
		t.Error("rejected requests reached the query handler")
	}
}

func TestStaticAndHeaders(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	resp, body := e.get("/login")
	if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'self'") || resp.Header.Get("Cache-Control") != "no-store" {
		t.Error("missing security headers")
	}
	// Pages reference hashed asset URLs; fetch every one of them.
	for _, name := range []string{"app.css", "datastar.js", "editor.js", "brand/wordmark.svg", "brand/wordmark-dark.svg"} {
		u := newAssets().URL(name)
		if !strings.Contains(u, "?v=") {
			t.Errorf("%s has no content hash", name)
		}
		r, b := e.get(u)
		if r.StatusCode != 200 || len(b) == 0 {
			t.Errorf("%s: %d, %d bytes", name, r.StatusCode, len(b))
		}
	}
	if !strings.Contains(body, "/static/app.css?v=") || !strings.Contains(body, "wordmark-dark.svg") {
		t.Error("login page does not link branded assets")
	}
}

func TestServeLifecycle(t *testing.T) {
	w := &Web{}
	if err := w.Init(context.Background(), contract.Config{"listen": "127.0.0.1:0", "row_limit": float64(10)}); err != nil {
		t.Fatal(err)
	}
	addr := w.Addr()
	if strings.HasSuffix(addr, ":0") {
		t.Fatalf("Addr() did not report the bound port: %s", addr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Serve(ctx, &fakeQ{}, &fakeAuth{}) }()
	var resp *http.Response
	var err error
	for i := 0; i < 50; i++ {
		if resp, err = http.Get("http://" + addr + "/login"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("serve: %v %v", err, resp)
	}
	_ = resp.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned %v on clean cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not stop")
	}
	if err := w.Close(); err != nil || w.Close() != nil {
		t.Error("Close not idempotent")
	}
	if w.Name() != "web" {
		t.Error("name")
	}
}

func TestInitValidation(t *testing.T) {
	for _, cfg := range []contract.Config{{"session_ttl": "banana"}, {"row_limit": float64(0)}, {"row_limit": "x"}, {"query_timeout": "-1s"}} {
		if err := (&Web{}).Init(context.Background(), cfg); err == nil {
			t.Errorf("Init(%v) accepted", cfg)
		}
	}
}

// governedQ mimics governance.Handler: information_schema is denied, the
// catalog comes from the optional Catalog method.
type governedQ struct{ *fakeQ }

func (g governedQ) Query(ctx context.Context, id contract.Identity, sql string) (contract.Stream, error) {
	if strings.Contains(sql, "information_schema") {
		return nil, errors.New("permission denied: only schema role_analyst may be queried")
	}
	return g.fakeQ.Query(ctx, id, sql)
}

func (g governedQ) Catalog(ctx context.Context, id contract.Identity) (contract.Stream, error) {
	return g.fakeQ.Query(ctx, id, catalogSQL)
}

func TestCatalogUsesOptionalCatalogLister(t *testing.T) {
	q := &fakeQ{}
	srv := httptest.NewServer(newApp(governedQ{q}, &fakeAuth{}, appConfig{rowLimit: 10, queryTimeout: time.Second, sessionTTL: time.Hour}).routes())
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	e := &env{t: t, srv: srv, q: q, c: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	e.login()
	_, body := e.get("/catalog")
	if !strings.Contains(body, "silver") || strings.Contains(body, "permission denied") {
		t.Errorf("catalog via Catalog(): %s", body)
	}
}

func TestCatalogErrorIsShown(t *testing.T) {
	q := &fakeQ{fail: catalogSQL}
	srv := httptest.NewServer(newApp(q, &fakeAuth{}, appConfig{rowLimit: 10, queryTimeout: time.Second, sessionTTL: time.Hour}).routes())
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	e := &env{t: t, srv: srv, q: q, c: &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	e.login()
	resp, body := e.get("/catalog")
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "No se pudo leer el catálogo") {
		t.Errorf("catalog error: %d", resp.StatusCode)
	}
}

// ---- redesigned UI: shell, table detail, SQL selection, admin nav ----

func TestAppShellMarkers(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	_, body := e.get("/catalog")
	for _, want := range []string{
		"shell.js", "data-theme-toggle", "data-sidebar-toggle", "data-menu-toggle",
		"Migas de pan", "data-menu-panel", "Salir", "Catálogo", "Editor SQL", "Dashboards",
		"Trabajos",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("shell missing %q", want)
		}
	}
	// The classic shell script must not be inline (CSP).
	if strings.Contains(body, "<script>") || strings.Contains(body, "<script >") {
		t.Error("inline script in the page")
	}
}

func TestAdminNavOnlyForAdmin(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	if _, body := e.get("/catalog"); strings.Contains(body, ">Admin<") {
		t.Error("Admin placeholder shown to a non-admin role")
	}
	e2 := newEnv(t, appConfig{}, &fakeAuth{role: "duct_admin"})
	e2.login()
	_, body := e2.get("/catalog")
	if !strings.Contains(body, ">Admin<") || !strings.Contains(body, "duct_admin") {
		t.Errorf("Admin placeholder missing for duct_admin: %s", body)
	}
}

func TestCatalogTableDetail(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	resp, body := e.get("/catalog/gold/sales")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		"Vista previa", "Detalles", "Abrir en editor SQL", // tabs + action
		"&lt;b&gt;name&lt;/b&gt;", // columns tab, escaped
		">No<", ">Sí<",            // nullable badges
		"42",            // row count through QueryHandler
		"2 filas",       // preview
		"<duct-grid",    // preview rendered by the virtualized island
		"grid.js",       // island loaded on this page
		"/sql?q=SELECT", // open in editor
		"data-copy-text",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("table detail missing %q", want)
		}
	}
	// The preview and the count run as the session identity.
	for _, c := range e.q.calls {
		if c.who.User != "ana" {
			t.Errorf("detail query ran as %+v", c)
		}
	}
	if !strings.Contains(body, `href="/catalog"`) {
		t.Error("no breadcrumb to /catalog")
	}
}

func TestCatalogTableNotFound(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	resp, body := e.get("/catalog/gold/nope")
	if resp.StatusCode != http.StatusNotFound || !strings.Contains(body, "No existe la tabla") {
		t.Errorf("unknown table: %d %s", resp.StatusCode, body)
	}
}

func TestSQLRunSelection(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	tok := e.csrfToken()
	body := fmt.Sprintf(`{"sql":"SELECT 1","sel":"SELECT 2","_csrf":"%s"}`, tok)
	req, _ := http.NewRequest("POST", e.srv.URL+"/sql/run", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Datastar-Request", "true")
	req.Header.Set("X-CSRF-Token", tok)
	resp, out := e.do(req)
	if resp.StatusCode != 200 || !strings.Contains(out, "id=\"results\"") {
		t.Fatalf("run selection: %d %s", resp.StatusCode, out)
	}
	last := e.q.calls[len(e.q.calls)-1]
	if last.sql != "SELECT 2" {
		t.Errorf("ran %q, want the selection", last.sql)
	}
}

func TestSQLEditorHasSchemaBrowser(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{})
	e.login()
	_, body := e.get("/sql")
	for _, want := range []string{
		"data-sql-editor", "data-bind:sql", "data-bind:sel", "/sql/run",
		"data-worksheets", "data-history", "data-splitter", "data-insert",
		"editor.js", "wordmark", "gold",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("editor page missing %q", want)
		}
	}
}

func TestErrPos(t *testing.T) {
	for _, tc := range []struct {
		msg       string
		line, col int
	}{
		{`Parser Error: syntax error at or near "SELCT"`, 0, 0},
		{"LINE 3: SELCT 1", 3, 0},
		{"Binder Error: at line 2, column 9", 2, 9},
	} {
		l, c := errPos(tc.msg)
		if l != tc.line || c != tc.col {
			t.Errorf("errPos(%q) = %d,%d want %d,%d", tc.msg, l, c, tc.line, tc.col)
		}
	}
}

// TestGridSpecEscaping pins the single-quoted data-spec contract: raw JSON
// (encoding/json escapes <, > and &), apostrophes entity-encoded, and the
// payload parses back to the original cells.
func TestGridSpecEscaping(t *testing.T) {
	r := &queryResult{
		Columns: []resultColumn{{Name: "c", Type: "TEXT"}},
		Rows:    [][]cell{{{Text: `it's </script> "q" & <b>`}}, {{Null: true}}},
	}
	var sb strings.Builder
	if err := (gridSpecElem{spec: resultsSpec(r)}).Render(context.Background(), &sb); err != nil {
		t.Fatal(err)
	}
	out := sb.String()
	if n := strings.Count(out, "'"); n != 2 {
		t.Errorf("raw apostrophe outside the delimiters: %q", out)
	}
	if strings.Contains(out, "<b>") || strings.Contains(out, "</script>") {
		t.Errorf("HTML not neutralised: %q", out)
	}
	spec := out[strings.IndexByte(out, '\'')+1 : strings.LastIndex(out, "'")]
	var parsed struct {
		Rows [][]any `json:"rows"`
	}
	if err := json.Unmarshal([]byte(strings.ReplaceAll(spec, "&#39;", "'")), &parsed); err != nil {
		t.Fatalf("payload does not parse back: %v (%q)", err, spec)
	}
	if len(parsed.Rows) != 2 || parsed.Rows[0][0] != `it's </script> "q" & <b>` || parsed.Rows[1][0] != nil {
		t.Errorf("round trip lost data: %#v", parsed.Rows)
	}
}
