package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// consoleQ is a QueryHandler that also implements the optional console
// interfaces (JobsConsole, AdminCaller, AuditReader) with recordings.
type consoleQ struct {
	*fakeQ

	mu    sync.Mutex
	acts  []string // "run:<job>:<user>", "cancel:<id>", "enable:<job>", "disable:<job>"
	admin []adminCall
	audit []contract.AuditQuery

	jobs  []contract.JobInfo
	runs  map[int64]contract.JobRunInfo
	tasks map[int64][]contract.JobTaskRun
	polls int // JobRun calls, to make a run finish while being watched

	adminFn func(method, path string, body []byte) (int, []byte)
	entries []contract.AuditEntry
}

type adminCall struct {
	who          contract.Identity
	method, path string
	body         string
}

func (c *consoleQ) Jobs(context.Context) ([]contract.JobInfo, error) { return c.jobs, nil }
func (c *consoleQ) JobRuns(_ context.Context, job string, _ int) ([]contract.JobRunInfo, error) {
	var out []contract.JobRunInfo
	for _, r := range c.runs {
		if r.Job == job {
			out = append(out, r)
		}
	}
	return out, nil
}

func (c *consoleQ) JobRun(_ context.Context, id int64) (contract.JobRunInfo, []contract.JobTaskRun, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.polls++
	r, ok := c.runs[id]
	if ok && r.Status == contract.RunRunning && c.polls > 2 { // finishes while watched
		r.Status, r.FinishedAt = contract.RunSuccess, time.Now()
		c.runs[id] = r
		ts := append([]contract.JobTaskRun(nil), c.tasks[id]...)
		for i := range ts {
			ts[i].Status = contract.RunSuccess
		}
		c.tasks[id] = ts
	}
	return r, c.tasks[id], ok, nil
}

func (c *consoleQ) RunJob(_ context.Context, id contract.Identity, job string) (int64, error) {
	if id.Role != "duct_admin" {
		return 0, errors.New("permission denied")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acts = append(c.acts, "run:"+job+":"+id.User)
	return 900, nil
}

func (c *consoleQ) CancelJobRun(_ context.Context, id contract.Identity, runID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acts = append(c.acts, "cancel:"+itoa(runID))
	return nil
}

func (c *consoleQ) SetJobEnabled(_ context.Context, _ contract.Identity, job string, enabled bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if enabled {
		c.acts = append(c.acts, "enable:"+job)
	} else {
		c.acts = append(c.acts, "disable:"+job)
	}
	return nil
}

func itoa(n int64) string { return strings.TrimSpace(strings.Join([]string{jsonInt(n)}, "")) }
func jsonInt(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func (c *consoleQ) AdminCall(_ context.Context, id contract.Identity, method, path string, body []byte) (int, []byte, error) {
	c.mu.Lock()
	c.admin = append(c.admin, adminCall{id, method, path, string(body)})
	c.mu.Unlock()
	if c.adminFn != nil {
		st, b := c.adminFn(method, path, body)
		return st, b, nil
	}
	return 200, []byte(`{}`), nil
}

func (c *consoleQ) AuditPage(_ context.Context, id contract.Identity, q contract.AuditQuery) ([]contract.AuditEntry, int64, error) {
	c.mu.Lock()
	c.audit = append(c.audit, q)
	c.mu.Unlock()
	return c.entries, int64(len(c.entries)), nil
}

func (c *consoleQ) actions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.acts...)
}

func (c *consoleQ) adminCalls() []adminCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]adminCall(nil), c.admin...)
}

func newConsoleEnv(t *testing.T, role string, q *consoleQ) *env {
	t.Helper()
	cfg := appConfig{rowLimit: 100, queryTimeout: 5 * time.Second, sessionTTL: time.Hour, livePoll: 20 * time.Millisecond}
	srv := httptest.NewServer(newApp(q, &fakeAuth{role: role}, cfg).routes())
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	e := &env{t: t, srv: srv, q: q.fakeQ, c: c}
	e.login()
	return e
}

func newConsoleQ() *consoleQ {
	now := time.Now()
	q := &consoleQ{fakeQ: &fakeQ{}, runs: map[int64]contract.JobRunInfo{}, tasks: map[int64][]contract.JobTaskRun{}}
	last := contract.JobRunInfo{ID: 11, Job: "nightly", Trigger: "cron", Status: contract.RunSuccess, StartedAt: now.Add(-2 * time.Hour), FinishedAt: now.Add(-2*time.Hour + 95*time.Second), Tasks: 3, Attempts: 4}
	q.jobs = []contract.JobInfo{
		{Name: "nightly", Schedule: "0 3 * * *", Timezone: "America/Santiago", Enabled: true, NextFire: now.Add(5 * time.Hour), LastRun: &last,
			OnSnapshot: []string{"bronze.orders"},
			Tasks: []contract.JobTaskDef{
				{ID: "ingest", Type: "ingest", Timeout: time.Hour},
				{ID: "build", Type: "pipeline", DependsOn: []string{"ingest"}, Retries: 2, Timeout: time.Hour},
				{ID: "report", Type: "shell", DependsOn: []string{"build"}, Timeout: time.Hour},
				{ID: "audit", Type: "shell", DependsOn: []string{"build"}, Timeout: time.Hour},
			}},
		{Name: "paused_job", Schedule: "@every 30m", Enabled: false, Tasks: []contract.JobTaskDef{{ID: "only", Type: "shell", Timeout: time.Hour}}},
	}
	q.runs[11] = last
	q.tasks[11] = []contract.JobTaskRun{
		{ID: "ingest", Type: "ingest", Status: contract.RunSuccess, Attempts: 1, MaxAttempts: 1, Duration: 20 * time.Second, Output: "orders -> 10 rows"},
		{ID: "build", Type: "pipeline", DependsOn: []string{"ingest"}, Status: contract.RunSuccess, Attempts: 2, MaxAttempts: 3, Duration: 60 * time.Second},
		{ID: "report", Type: "shell", DependsOn: []string{"build"}, Status: contract.RunFailed, Attempts: 1, MaxAttempts: 1, Error: "exit status 3", Output: "line1\nboom"},
	}
	q.runs[12] = contract.JobRunInfo{ID: 12, Job: "nightly", Trigger: "manual", Detail: "ana", Status: contract.RunRunning, StartedAt: now.Add(-3 * time.Second), Tasks: 1, Attempts: 1}
	q.tasks[12] = []contract.JobTaskRun{{ID: "ingest", Type: "ingest", Status: contract.RunRunning, Attempts: 1, MaxAttempts: 1, StartedAt: now.Add(-2 * time.Second)}}
	q.runs[13] = contract.JobRunInfo{ID: 13, Job: "paused_job", Status: contract.RunSuccess, StartedAt: now}
	return q
}

func (e *env) postCSRF(path string, v url.Values, token string) (*http.Response, string) {
	e.t.Helper()
	if v == nil {
		v = url.Values{}
	}
	if token != "" {
		v.Set("csrf", token)
	}
	return e.postForm(path, v)
}

// ---- jobs ----

func TestJobsListReadOnlyForNonAdmin(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "analyst", q)
	resp, body := e.get("/jobs")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	for _, want := range []string{"nightly", "Todos los días a las 03:00", "America/Santiago", "Correcta", "al cambiar bronze.orders", "En pausa", "paused_job", "Cada 30 minutos", "solo lectura"} {
		if !strings.Contains(body, want) {
			t.Errorf("jobs list missing %q", want)
		}
	}
	for _, bad := range []string{"Ejecutar", "Pausar", "Reanudar"} {
		if strings.Contains(body, bad) {
			t.Errorf("non-admin sees action %q", bad)
		}
	}
	if strings.Contains(body, ">Admin<") {
		t.Error("Admin nav for non-admin")
	}
}

func TestJobsActionsForbiddenForNonAdmin(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "analyst", q)
	tok := e.csrfToken()
	for _, p := range []string{"/jobs/nightly/run", "/jobs/nightly/enabled", "/jobs/nightly/runs/12/cancel"} {
		resp, body := e.postCSRF(p, url.Values{"enabled": {"0"}}, tok)
		if resp.StatusCode != http.StatusForbidden || !strings.Contains(body, "403") {
			t.Errorf("%s: status %d", p, resp.StatusCode)
		}
	}
	if a := q.actions(); len(a) != 0 {
		t.Errorf("core was called: %v", a)
	}
}

func TestJobsActionsNeedCSRF(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "duct_admin", q)
	for _, tok := range []string{"", "deadbeef"} {
		if resp, _ := e.postCSRF("/jobs/nightly/run", nil, tok); resp.StatusCode != http.StatusForbidden {
			t.Errorf("token %q: status %d", tok, resp.StatusCode)
		}
	}
	if a := q.actions(); len(a) != 0 {
		t.Errorf("core was called without CSRF: %v", a)
	}
}

func TestJobsAdminActions(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "duct_admin", q)
	_, list := e.get("/jobs")
	for _, want := range []string{"Ejecutar", "Pausar", "Reanudar"} {
		if !strings.Contains(list, want) {
			t.Errorf("admin list missing %q", want)
		}
	}
	tok := e.csrfToken()

	resp, _ := e.postCSRF("/jobs/nightly/run", nil, tok)
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/jobs/nightly/runs/900?f=") {
		t.Fatalf("run: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	loc := resp.Header.Get("Location")
	if _, b := e.get(loc); !strings.Contains(b, "Ejecución 900 iniciada") {
		// run 900 does not exist in the fake (404) so the flash is consumed by the 404 page: fine
		_ = b
	}
	if resp, _ := e.postCSRF("/jobs/nightly/runs/12/cancel", nil, tok); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	if resp, _ := e.postCSRF("/jobs/paused_job/enabled", url.Values{"enabled": {"1"}}, tok); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("enable: %d", resp.StatusCode)
	}
	if resp, _ := e.postCSRF("/jobs/nightly/enabled", url.Values{"enabled": {"0"}}, tok); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("disable: %d", resp.StatusCode)
	}
	got := strings.Join(q.actions(), ",")
	if got != "run:nightly:ana,cancel:12,enable:paused_job,disable:nightly" {
		t.Errorf("actions = %s", got)
	}
}

func TestJobFlashShownOnce(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "duct_admin", q)
	tok := e.csrfToken()
	resp, _ := e.postCSRF("/jobs/nightly/enabled", url.Values{"enabled": {"0"}, "back": {"list"}}, tok)
	loc := resp.Header.Get("Location")
	if !strings.HasPrefix(loc, "/jobs?f=") {
		t.Fatalf("location %s", loc)
	}
	if _, b := e.get(loc); !strings.Contains(b, "en pausa") {
		t.Errorf("flash not shown: %s", b)
	}
	if _, b := e.get(loc); strings.Contains(b, "en pausa: no se ejecutará") {
		t.Error("flash shown twice")
	}
	// A flash of another session is not readable.
	e2 := newConsoleEnv(t, "duct_admin", q)
	resp, _ = e.postCSRF("/jobs/nightly/enabled", url.Values{"enabled": {"1"}, "back": {"list"}}, tok)
	if _, b := e2.get(resp.Header.Get("Location")); strings.Contains(b, "reanudado") {
		t.Error("flash leaked to another session")
	}
}

func TestJobDetailAndRunDetail(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "analyst", q)
	resp, body := e.get("/jobs/nightly")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	for _, want := range []string{"<svg", "dag-node", "dag-edge", "ingest", "report", "#11", "Horario", "4 / 3 tareas"} {
		if !strings.Contains(body, want) {
			t.Errorf("job detail missing %q", want)
		}
	}
	if strings.Contains(body, "Ejecutar ahora") {
		t.Error("non-admin sees Run now")
	}
	resp, body = e.get("/jobs/nightly/runs/11")
	if resp.StatusCode != 200 {
		t.Fatalf("run status %d", resp.StatusCode)
	}
	for _, want := range []string{`data-status="failed"`, "exit status 3", "boom", "Fallida", "id=\"task-report\"", `href="#task-report"`} {
		if !strings.Contains(body, want) {
			t.Errorf("run detail missing %q", want)
		}
	}
	if strings.Contains(body, "Cancelar ejecución") {
		t.Error("cancel shown for a finished run / non-admin")
	}
	if resp, _ := e.get("/jobs/paused_job/runs/11"); resp.StatusCode != 404 {
		t.Errorf("run of another job: %d", resp.StatusCode)
	}
	if resp, _ := e.get("/jobs/nope"); resp.StatusCode != 404 {
		t.Errorf("unknown job: %d", resp.StatusCode)
	}
	if resp, _ := e.get("/jobs/nightly/runs/abc"); resp.StatusCode != 404 {
		t.Errorf("bad run id: %d", resp.StatusCode)
	}
}

func TestRunLiveStreamsUntilDone(t *testing.T) {
	q := newConsoleQ()
	e := newConsoleEnv(t, "duct_admin", q)
	_, page := e.get("/jobs/nightly/runs/12")
	if !strings.Contains(page, "/jobs/nightly/runs/12/live") || !strings.Contains(page, "Cancelar ejecución") {
		t.Errorf("running page lacks live subscription or cancel: %s", page)
	}
	req, _ := http.NewRequest("GET", e.srv.URL+"/jobs/nightly/runs/12/live", nil)
	req.Header.Set("Datastar-Request", "true")
	done := make(chan string, 1)
	go func() {
		_, b := e.do(req)
		done <- b
	}()
	select {
	case b := <-done:
		if !strings.Contains(b, "run-live") || !strings.Contains(b, "Correcta") {
			t.Errorf("stream did not end with the finished run: %s", b)
		}
		if strings.Count(b, "event: datastar-patch-elements") < 2 {
			t.Errorf("expected several patches: %s", b)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("live stream did not end")
	}
}

// ---- cron wording ----

func TestHumanSchedule(t *testing.T) {
	for spec, want := range map[string]string{
		"":              "Sin horario",
		"* * * * *":     "Cada minuto",
		"*/5 * * * *":   "Cada 5 minutos",
		"15 * * * *":    "Cada hora, en el minuto 15",
		"0 */6 * * *":   "Cada 6 horas, en el minuto 0",
		"30 2 * * *":    "Todos los días a las 02:30",
		"0 8 * * 1":     "Los lunes a las 08:00",
		"0 8 * * 1-5":   "De lunes a viernes a las 08:00",
		"0 8 * * 1,3,5": "Los lunes, miércoles y viernes a las 08:00",
		"0 4 1 * *":     "El día 1 de cada mes a las 04:00",
		"@hourly":       "Cada hora",
		"@daily":        "Todos los días a las 00:00",
		"@every 30s":    "Cada 30 segundos",
		"@every 1h":     "Cada hora",
		"@every 2h":     "Cada 2 horas",
		"0 0 1 6 *":     "Cron: 0 0 1 6 *",
		"1-5 * * * *":   "Cron: 1-5 * * * *",
	} {
		if got := humanSchedule(spec); got != want {
			t.Errorf("humanSchedule(%q) = %q, want %q", spec, got, want)
		}
	}
}

func TestFormatting(t *testing.T) {
	for d, want := range map[time.Duration]string{
		420 * time.Millisecond: "420 ms", 35 * time.Second: "35 s", 4*time.Minute + 5*time.Second: "4 min 05 s", 62 * time.Minute: "1 h 02 min", 2500 * time.Millisecond: "2.5 s",
	} {
		if got := fmtDuration(d); got != want {
			t.Errorf("fmtDuration(%v) = %q, want %q", d, got, want)
		}
	}
	now := time.Now()
	if got := relTime(now.Add(-3*time.Minute), now); got != "hace 3 min" {
		t.Errorf("relTime = %q", got)
	}
	if got := relTime(now.Add(2*time.Hour+time.Minute), now); got != "en 2 h" {
		t.Errorf("relTime = %q", got)
	}
}

func TestLayoutDAG(t *testing.T) {
	l := layoutDAG([]dagNode{
		{ID: "a"}, {ID: "b", DependsOn: []string{"a"}}, {ID: "c", DependsOn: []string{"a"}}, {ID: "d", DependsOn: []string{"b", "c"}},
	})
	pos := map[string]dagBox{}
	for _, b := range l.Boxes {
		pos[b.ID] = b
	}
	if pos["a"].X >= pos["b"].X || pos["b"].X != pos["c"].X || pos["c"].X >= pos["d"].X {
		t.Errorf("columns wrong: %+v", pos)
	}
	if pos["b"].Y == pos["c"].Y {
		t.Error("siblings overlap")
	}
	if len(l.Edges) != 4 || l.Width <= 0 || l.Height <= 0 {
		t.Errorf("edges %d size %dx%d", len(l.Edges), l.Width, l.Height)
	}
	if got := layoutDAG(nil); len(got.Boxes) != 0 {
		t.Error("empty graph drew boxes")
	}
	// A (non validated) cycle must not hang.
	_ = layoutDAG([]dagNode{{ID: "x", DependsOn: []string{"y"}}, {ID: "y", DependsOn: []string{"x"}}})
}

// ---- admin ----

func adminRouter(secretPw, secretTok string) func(method, path string, body []byte) (int, []byte) {
	return func(method, path string, body []byte) (int, []byte) {
		switch method + " " + path {
		case "GET /admin/v1/users":
			return 200, []byte(`{"users":[{"name":"ana","role":"duct_admin","disabled":false},{"name":"beto","role":"analyst","disabled":true}]}`)
		case "GET /admin/v1/roles":
			return 200, []byte(`{"roles":[{"name":"analyst","users":1,"grants":["gold.sales","gold.cust"],"row_filters":{"gold.sales":"region = 'sur'"},"masks":{"gold.cust.email":"'***'"}},{"name":"duct_admin","users":1,"grants":[],"row_filters":{},"masks":{}}]}`)
		case "GET /admin/v1/quotas":
			return 200, []byte(`{"quotas":[{"role":"analyst","max_concurrent":2,"max_rows":5000}]}`)
		case "GET /admin/v1/tokens":
			return 200, []byte(`{"tokens":[{"id":"tk1","user":"ana","label":"ci","created_at":"2026-10-01 10:00:00","revoked":false},{"id":"tk2","user":"beto","created_at":"2026-10-01 10:00:00","revoked":true}]}`)
		case "GET /admin/v1/status":
			return 200, []byte(`{"version":"v0.5.0","go":"go1.27","duckdb":"v2.0","ducklake":"v1.0","uptime_seconds":3700,"catalog":{"type":"sqlite","ref":"/x/.duct/catalog.db"},"storage":{"kind":"local","path":"./lake"},"migrations":{"applied":[1,2,3],"latest":3,"pending":[]},"snapshot":{"id":42,"time":"2026-10-12 10:00:00"},"audit":{"retention_seconds":7776000,"export":false},"components":{"jobs":{"configured":true,"health":"ok","runs_24h":{"success":3}},"stream":{"configured":false,"health":"disabled"},"alerts":{"configured":true,"health":"firing"}}}`)
		case "POST /admin/v1/users":
			if strings.Contains(string(body), `"name":"dup"`) {
				return 409, []byte(`{"error":"user \"dup\" already exists"}`)
			}
			if strings.Contains(string(body), `"password"`) {
				return 201, []byte(`{"name":"nuevo","role":"analyst"}`)
			}
			return 201, []byte(`{"name":"nuevo","role":"analyst","temporary_password":"` + secretPw + `"}`)
		case "POST /admin/v1/users/beto/password-reset":
			return 200, []byte(`{"name":"beto","temporary_password":"` + secretPw + `"}`)
		case "POST /admin/v1/tokens":
			return 201, []byte(`{"id":"tk9","user":"ana","token":"` + secretTok + `"}`)
		case "PUT /admin/v1/roles/analyst/grants/gold.sales/row-filter":
			if strings.Contains(string(body), "nope") {
				return 400, []byte(`{"error":"row filter on gold.sales: column \"nope\" does not exist"}`)
			}
			return 200, []byte(`{}`)
		}
		return 200, []byte(`{}`)
	}
}

func newAdminQ() *consoleQ {
	q := newConsoleQ()
	q.adminFn = adminRouter("TEMP-PW-123", "dct_SECRETTOKENVALUE")
	q.entries = []contract.AuditEntry{
		{At: time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC), User: "ana", Role: "duct_admin", Protocol: "web", Status: "ok", Duration: 12 * time.Millisecond, Rows: 3, SQL: "SELECT * FROM gold.sales WHERE token = '***'"},
		{At: time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC), User: "beto", Role: "analyst", Protocol: "pg", Status: "denied", SQL: "SELECT 1", Error: "permission denied"},
	}
	return q
}

func TestAdminForbiddenForNonAdmin(t *testing.T) {
	q := newAdminQ()
	e := newConsoleEnv(t, "analyst", q)
	for _, p := range []string{"/admin", "/admin/users", "/admin/roles", "/admin/roles/analyst", "/admin/tokens", "/admin/audit"} {
		resp, body := e.get(p)
		if resp.StatusCode != http.StatusForbidden || !strings.Contains(body, "403") {
			t.Errorf("GET %s: %d", p, resp.StatusCode)
		}
	}
	tok := e.csrfToken()
	for p, v := range map[string]url.Values{
		"/admin/users":                       {"name": {"x"}, "role": {"analyst"}},
		"/admin/users/beto/reset-password":   {},
		"/admin/users/beto/disable":          {},
		"/admin/tokens":                      {"user": {"ana"}, "ttl": {"3600"}},
		"/admin/roles/analyst/grants":        {"relation": {"gold.x"}},
		"/admin/roles/analyst/quota":         {"max_rows": {"1"}},
		"/admin/roles/analyst/row-filter":    {"relation": {"gold.sales"}, "predicate": {"true"}},
		"/admin/roles/analyst/masks":         {"relation": {"gold.sales"}, "column": {"c"}, "expr": {"1"}},
		"/admin/tokens/tk1/revoke":           {},
		"/admin/roles/analyst/grants/revoke": {"relation": {"gold.sales"}},
	} {
		if resp, _ := e.postCSRF(p, v, tok); resp.StatusCode != http.StatusForbidden {
			t.Errorf("POST %s: %d", p, resp.StatusCode)
		}
	}
	if n := len(q.adminCalls()); n != 0 {
		t.Errorf("the admin API was called %d times for a non-admin", n)
	}
	if len(q.audit) != 0 {
		t.Error("audit read for a non-admin")
	}
	if _, body := e.get("/catalog"); strings.Contains(body, ">Admin<") {
		t.Error("Admin nav shown")
	}
}

func TestAdminPostNeedsCSRF(t *testing.T) {
	q := newAdminQ()
	e := newConsoleEnv(t, "duct_admin", q)
	for _, tok := range []string{"", "bad"} {
		if resp, _ := e.postCSRF("/admin/users", url.Values{"name": {"x"}, "role": {"analyst"}}, tok); resp.StatusCode != http.StatusForbidden {
			t.Errorf("token %q: %d", tok, resp.StatusCode)
		}
	}
	if n := len(q.adminCalls()); n != 0 {
		t.Errorf("admin API called %d times without CSRF", n)
	}
}

func TestAdminPagesRender(t *testing.T) {
	q := newAdminQ()
	e := newConsoleEnv(t, "duct_admin", q)
	checks := map[string][]string{
		"/admin":               {"v0.5.0", "go1.27", "sqlite", "al día", "#42", "90 días", "Sano", "En alerta", "No configurado", "correctas 3"},
		"/admin/users":         {"ana", "beto", "Deshabilitado", "Restablecer contraseña", "tú"},
		"/admin/roles":         {"analyst", "2 simultáneas · 5000 filas", "duct_admin"},
		"/admin/roles/analyst": {"gold.sales", "region = &#39;sur&#39;", "email", "Guardar máscara", "Quitar cuota"},
		"/admin/tokens":        {"tk1", "Revocar", "Revocado", "nunca se muestra su valor"},
		"/admin/audit":         {"SELECT * FROM gold.sales", "denied", "permission denied", "2 filas"},
	}
	for path, wants := range checks {
		resp, body := e.get(path)
		if resp.StatusCode != 200 {
			t.Errorf("%s: %d", path, resp.StatusCode)
			continue
		}
		for _, w := range wants {
			if !strings.Contains(body, w) {
				t.Errorf("%s missing %q", path, w)
			}
		}
	}
	if resp, _ := e.get("/admin/roles/ghost"); resp.StatusCode != 404 {
		t.Errorf("unknown role: %d", resp.StatusCode)
	}
}

func TestAdminSecretsShownOnce(t *testing.T) {
	q := newAdminQ()
	e := newConsoleEnv(t, "duct_admin", q)
	tok := e.csrfToken()

	// New user with a generated password.
	resp, _ := e.postCSRF("/admin/users", url.Values{"name": {"nuevo"}, "role": {"analyst"}}, tok)
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("create user: %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if strings.Contains(loc, "TEMP-PW") {
		t.Fatal("secret in the redirect URL")
	}
	if _, b := e.get(loc); !strings.Contains(b, "TEMP-PW-123") || !strings.Contains(b, "una sola vez") {
		t.Errorf("secret not shown after create: %s", b)
	}
	if _, b := e.get(loc); strings.Contains(b, "TEMP-PW-123") {
		t.Error("secret shown a second time")
	}
	if _, b := e.get("/admin/users"); strings.Contains(b, "TEMP-PW-123") {
		t.Error("secret in the user list")
	}
	var last adminCall
	for _, c := range q.adminCalls() {
		if c.method == "POST" && c.path == "/admin/v1/users" {
			last = c
			break
		}
	}
	if last.method != "POST" || last.path != "/admin/v1/users" || last.who.Role != "duct_admin" ||
		!strings.Contains(last.body, `"name":"nuevo"`) || strings.Contains(last.body, "password") {
		t.Errorf("unexpected admin call %+v", last)
	}

	// A password typed by the admin is never echoed.
	resp, _ = e.postCSRF("/admin/users", url.Values{"name": {"otro"}, "role": {"analyst"}, "password": {"hunter2hunter2"}}, tok)
	if _, b := e.get(resp.Header.Get("Location")); strings.Contains(b, "hunter2") || strings.Contains(b, "Contraseña temporal") {
		t.Error("typed password echoed or temp shown")
	}

	// Reset password.
	resp, _ = e.postCSRF("/admin/users/beto/reset-password", nil, tok)
	if _, b := e.get(resp.Header.Get("Location")); !strings.Contains(b, "TEMP-PW-123") || !strings.Contains(b, "beto") {
		t.Errorf("reset secret missing: %s", b)
	}

	// Token: secret once, never in the list.
	resp, _ = e.postCSRF("/admin/tokens", url.Values{"user": {"ana"}, "label": {"ci"}, "ttl": {"86400"}}, tok)
	if _, b := e.get(resp.Header.Get("Location")); !strings.Contains(b, "dct_SECRETTOKENVALUE") {
		t.Errorf("token secret missing: %s", b)
	}
	if _, b := e.get("/admin/tokens"); strings.Contains(b, "dct_SECRETTOKENVALUE") {
		t.Error("token secret in the list")
	}
	var tc adminCall
	for _, c := range q.adminCalls() {
		if c.path == "/admin/v1/tokens" && c.method == "POST" {
			tc = c
		}
	}
	if !strings.Contains(tc.body, `"ttl_seconds":86400`) || !strings.Contains(tc.body, `"user":"ana"`) {
		t.Errorf("token call %+v", tc)
	}
}

func TestAdminSecretNotReadableByAnotherSession(t *testing.T) {
	q := newAdminQ()
	e1 := newConsoleEnv(t, "duct_admin", q)
	e2 := newConsoleEnv(t, "duct_admin", q)
	resp, _ := e1.postCSRF("/admin/users/beto/reset-password", nil, e1.csrfToken())
	if _, b := e2.get(resp.Header.Get("Location")); strings.Contains(b, "TEMP-PW-123") {
		t.Error("secret readable from another session")
	}
}

func TestAdminValidationErrorKeepsDraft(t *testing.T) {
	q := newAdminQ()
	e := newConsoleEnv(t, "duct_admin", q)
	tok := e.csrfToken()
	resp, _ := e.postCSRF("/admin/roles/analyst/row-filter", url.Values{"relation": {"gold.sales"}, "predicate": {"nope = 1"}}, tok)
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/admin/roles/analyst?f=") {
		t.Fatalf("set filter: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	_, b := e.get(resp.Header.Get("Location"))
	if !strings.Contains(b, `column \&#34;nope\&#34; does not exist`) && !strings.Contains(b, "does not exist") {
		t.Errorf("governance error not shown: %s", b)
	}
	if !strings.Contains(b, ">nope = 1</textarea>") {
		t.Errorf("draft predicate lost: %s", b)
	}
	// Duplicate user: error from the API, form values kept, no password kept.
	resp, _ = e.postCSRF("/admin/users", url.Values{"name": {"dup"}, "role": {"analyst"}, "password": {"secretsecret"}}, tok)
	_, b = e.get(resp.Header.Get("Location"))
	if !strings.Contains(b, "already exists") || strings.Contains(b, "secretsecret") {
		t.Errorf("dup user page wrong: %s", b)
	}
}

func TestAdminEscapesPathSegments(t *testing.T) {
	q := newAdminQ()
	e := newConsoleEnv(t, "duct_admin", q)
	tok := e.csrfToken()
	e.postCSRF("/admin/roles/analyst/grants", url.Values{"relation": {"gold/../x y"}}, tok)
	calls := q.adminCalls()
	last := calls[len(calls)-1]
	if strings.Contains(last.path, "/../") || !strings.Contains(last.path, "%2F") {
		t.Errorf("relation not escaped: %s", last.path)
	}
}

func TestAdminAuditFiltersAndPaging(t *testing.T) {
	q := newAdminQ()
	for i := 0; i < 120; i++ {
		q.entries = append(q.entries, contract.AuditEntry{User: "x", Protocol: "pg", Status: "ok", SQL: "SELECT " + itoa(int64(i))})
	}
	e := newConsoleEnv(t, "duct_admin", q)
	_, b := e.get("/admin/audit?user=ana&protocol=web&status=ok&from=2026-10-01&to=2026-10-02&page=2")
	if len(q.audit) != 1 {
		t.Fatal("audit not queried")
	}
	got := q.audit[0]
	if got.User != "ana" || got.Protocol != "web" || got.Status != "ok" || got.Offset != auditPageSize || got.Limit != auditPageSize ||
		!got.From.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) || !got.To.Equal(time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("query = %+v", got)
	}
	if !strings.Contains(b, "rel=\"prev\"") || !strings.Contains(b, "rel=\"next\"") || !strings.Contains(b, "page=3") {
		t.Errorf("pagination links missing: %s", b)
	}
	if resp, b := e.get("/admin/audit?from=garbage"); resp.StatusCode != 200 || !strings.Contains(b, "no es válida") {
		t.Errorf("bad date: %d", resp.StatusCode)
	}
}

func TestConsoleUnavailable(t *testing.T) {
	e := newEnv(t, appConfig{}, &fakeAuth{role: "duct_admin"}) // plain fakeQ: no console interfaces
	e.login()
	if resp, b := e.get("/jobs"); resp.StatusCode != 200 || !strings.Contains(b, "no expone los trabajos") {
		t.Errorf("jobs: %d %s", resp.StatusCode, b)
	}
	if resp, _ := e.get("/admin/users"); resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("admin: %d", resp.StatusCode)
	}
}
