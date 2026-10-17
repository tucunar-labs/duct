package main

import (
	"context"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/project"
)

// webConsole is a browser-like client of the web protocol of an in-process `duct serve`.
type webConsole struct {
	t    *testing.T
	base string
	c    *http.Client
}

func startServeWeb(t *testing.T, root string) (addr string) {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{
			Protocols: []string{"http", "web"},
			Listen:    map[string]string{"http": "127.0.0.1:0", "web": "127.0.0.1:0"},
			Ready:     func(a map[string]string) { ready <- a },
		})
	}()
	select {
	case a := <-ready:
		addr = a["web"]
	case err := <-done:
		cancel()
		t.Fatalf("serve exited early: %v", err)
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("serve did not start")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("serve did not shut down")
		}
	})
	return addr
}

func (w *webConsole) do(method, path string, form url.Values) (*http.Response, string) {
	w.t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, _ := http.NewRequest(method, w.base+path, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := w.c.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func (w *webConsole) login(user, pass string) bool {
	w.t.Helper()
	jar, _ := cookiejar.New(nil)
	w.c = &http.Client{Jar: jar, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	_, page := w.do("GET", "/login", nil)
	m := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(page)
	if m == nil {
		w.t.Fatalf("no csrf in login page")
	}
	resp, _ := w.do("POST", "/login", url.Values{"csrf": {m[1]}, "user": {user}, "password": {pass}})
	return resp.StatusCode == http.StatusSeeOther
}

// csrf scrapes the session token from any authenticated page.
func (w *webConsole) csrf() string {
	w.t.Helper()
	_, page := w.do("GET", "/jobs", nil)
	m := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(page)
	if m == nil {
		w.t.Fatalf("no csrf in page: %s", page)
	}
	return m[1]
}

func (w *webConsole) post(path string, v url.Values) (*http.Response, string) {
	w.t.Helper()
	if v == nil {
		v = url.Values{}
	}
	v.Set("csrf", w.csrf())
	return w.do("POST", path, v)
}

// follow GETs the Location of a redirect response.
func (w *webConsole) follow(resp *http.Response) string {
	w.t.Helper()
	loc := resp.Header.Get("Location")
	if loc == "" {
		w.t.Fatalf("no redirect (status %d)", resp.StatusCode)
	}
	_, b := w.do("GET", loc, nil)
	return b
}

// TestE2EWebConsole drives the Jobs and Admin pages of a real server: the
// actions reach the orchestrator and the governance store, are audited, and
// non-admins are refused.
func TestE2EWebConsole(t *testing.T) {
	t.Parallel()
	root := newProject(t)
	mustDuct(t, "sql", "--project", root, "CREATE ROLE duct_admin")
	mustDuct(t, "role", "create", "analistas", "--project", root)
	mustDuct(t, "user", "create", "jefa", "--role", "duct_admin", "--password", "pw-jefa-123", "--project", root)
	mustDuct(t, "user", "create", "ana", "--role", "analistas", "--password", "pw-ana-123", "--project", root)
	writeJob(t, root, "quick", "name: quick\ntasks:\n  - {id: a, type: shell, params: {command: [sh, -c, \"echo hola\"]}}\n  - {id: b, type: shell, depends_on: [a], params: {command: [sh, -c, \"echo chao\"]}}\n")
	writeJob(t, root, "slow", "name: slow\ntasks:\n  - {id: wait, type: shell, params: {command: [sleep, \"60\"]}}\n  - {id: after, type: shell, depends_on: [wait], params: {command: [echo, x]}}\n")
	writeJob(t, root, "tick", "name: tick\nschedule: \"@every 1s\"\ntasks:\n  - {id: t, type: shell, params: {command: [echo, tick]}}\n")
	addr := startServeWeb(t, root)

	// Non-admin: read-only jobs, 403 everywhere else.
	ana := &webConsole{t: t, base: "http://" + addr}
	if !ana.login("ana", "pw-ana-123") {
		t.Fatal("ana cannot log in")
	}
	if resp, b := ana.do("GET", "/jobs", nil); resp.StatusCode != 200 || !strings.Contains(b, "quick") || strings.Contains(b, "Ejecutar") {
		t.Errorf("non-admin jobs page: %d", resp.StatusCode)
	}
	if resp, _ := ana.do("GET", "/admin", nil); resp.StatusCode != 403 {
		t.Errorf("non-admin /admin: %d", resp.StatusCode)
	}
	if resp, _ := ana.post("/jobs/quick/run", nil); resp.StatusCode != 403 {
		t.Errorf("non-admin run: %d", resp.StatusCode)
	}
	if n := len(runsOf(t, root, "job = 'quick'")); n != 0 {
		t.Fatalf("a refused action started %d runs", n)
	}

	jefa := &webConsole{t: t, base: "http://" + addr}
	if !jefa.login("jefa", "pw-jefa-123") {
		t.Fatal("jefa cannot log in")
	}
	// Pause the schedule before anything else; it must stop producing runs.
	waitFor(t, "first cron tick", 60*time.Second, func() bool { return len(runsOf(t, root, "job = 'tick'")) >= 1 })
	if resp, _ := jefa.post("/jobs/tick/enabled", url.Values{"enabled": {"0"}}); resp.StatusCode != 303 {
		t.Fatalf("pause: %d", resp.StatusCode)
	}
	time.Sleep(2 * time.Second) // runs in flight finish
	before := len(runsOf(t, root, "job = 'tick'"))
	time.Sleep(4 * time.Second)
	if after := len(runsOf(t, root, "job = 'tick'")); after != before {
		t.Errorf("a paused job kept firing: %d -> %d runs", before, after)
	}
	if _, b := jefa.do("GET", "/jobs", nil); !strings.Contains(b, "En pausa") {
		t.Error("list does not show the pause")
	}
	if resp, _ := jefa.post("/jobs/tick/enabled", url.Values{"enabled": {"1"}}); resp.StatusCode != 303 {
		t.Fatalf("resume: %d", resp.StatusCode)
	}
	waitFor(t, "ticks resume", 30*time.Second, func() bool { return len(runsOf(t, root, "job = 'tick'")) > before })

	// Run now: manual run by jefa, executed by the server's workers.
	resp, _ := jefa.post("/jobs/quick/run", nil)
	if resp.StatusCode != 303 || !strings.HasPrefix(resp.Header.Get("Location"), "/jobs/quick/runs/") {
		t.Fatalf("run now: %d %s", resp.StatusCode, resp.Header.Get("Location"))
	}
	runPage := resp.Header.Get("Location")
	waitFor(t, "manual run success", 60*time.Second, func() bool {
		return len(runsOf(t, root, "job = 'quick' AND trigger_kind = 'manual' AND status = 'success'")) == 1
	})
	if d := queryRows(t, root, "SELECT detail FROM duct.duct_jobs_runs WHERE job = 'quick'"); len(d) != 1 || d[0]["detail"] != "jefa" {
		t.Errorf("run detail = %v", d)
	}
	_, page := jefa.do("GET", strings.SplitN(runPage, "?", 2)[0], nil)
	for _, want := range []string{"Correcta", "hola", "chao", "dag-node"} {
		if !strings.Contains(page, want) {
			t.Errorf("run page lacks %q", want)
		}
	}

	// Cancel a running run.
	resp, _ = jefa.post("/jobs/slow/run", nil)
	slowPage := strings.SplitN(resp.Header.Get("Location"), "?", 2)[0]
	waitFor(t, "slow task running", 60*time.Second, func() bool {
		return len(queryRows(t, root, "SELECT 1 AS x FROM duct.duct_task_runs WHERE job = 'slow' AND status = 'running'")) == 1
	})
	resp, _ = jefa.post(slowPage+"/cancel", nil)
	if resp.StatusCode != 303 {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	waitFor(t, "run cancelled", 30*time.Second, func() bool {
		return len(runsOf(t, root, "job = 'slow' AND status = 'cancelled'")) == 1
	})
	tasks := queryRows(t, root, "SELECT task_id, status FROM duct.duct_task_runs WHERE job = 'slow' ORDER BY task_id")
	if len(tasks) != 2 || tasks[0]["status"] != "cancelled" || tasks[1]["status"] != "cancelled" {
		t.Errorf("slow tasks after cancel = %v", tasks)
	}

	// Admin: a user gets a temporary password (shown once) that works.
	resp, _ = jefa.post("/admin/users", url.Values{"name": {"nuevo"}, "role": {"analistas"}})
	b := jefa.follow(resp)
	m := regexp.MustCompile(`data-secret>([^<]+)<`).FindStringSubmatch(b)
	if m == nil {
		t.Fatalf("no temporary password shown: %s", b)
	}
	if !(&webConsole{t: t, base: "http://" + addr}).login("nuevo", m[1]) {
		t.Error("the temporary password does not work")
	}
	if _, b := jefa.do("GET", "/admin/users", nil); strings.Contains(b, m[1]) {
		t.Error("the temporary password is listed")
	}
	// Governance validation reaches the form.
	resp, _ = jefa.post("/admin/roles/analistas/grants", url.Values{"relation": {"gold.nope"}})
	if b := jefa.follow(resp); !strings.Contains(b, "role=\"alert\"") {
		t.Errorf("grant of a missing table did not report an error")
	}
	// Audit viewer shows the console's own calls and the job actions.
	_, aud := jefa.do("GET", "/admin/audit?protocol=web", nil)
	for _, want := range []string{"POST /admin/v1/users", "JOBS run job=quick", "JOBS cancel run=", "JOBS disable job=tick"} {
		if !strings.Contains(aud, want) {
			t.Errorf("audit lacks %q", want)
		}
	}
	if strings.Contains(aud, m[1]) {
		t.Error("the temporary password reached the audit log")
	}
	// Status panel.
	if _, st := jefa.do("GET", "/admin", nil); !strings.Contains(st, "sqlite") || !strings.Contains(st, "al día") {
		t.Errorf("status panel: %s", st)
	}
}
