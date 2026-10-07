package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeJob(t *testing.T, root, name, body string) {
	t.Helper()
	writeFile(t, filepath.Join(root, "jobs", name+".yaml"), body)
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func runsOf(t *testing.T, root, where string) []map[string]any {
	t.Helper()
	return queryRows(t, root, "SELECT id, job, trigger_kind, status, scheduled_for, error FROM duct.duct_jobs_runs WHERE "+where+" ORDER BY id")
}

// chainProject: ingest -> pipeline -> shell, the demo shape.
func chainProject(t *testing.T) string {
	t.Helper()
	root := newProject(t)
	writeFile(t, filepath.Join(root, "data", "orders.csv"), "id,customer,amount\n1,ana,10\n2,bob,20\n3,ana,5\n")
	writeFile(t, filepath.Join(root, "ingest", "orders.yaml"), "name: orders\nsource: files\noptions:\n  path: data/orders.csv\n")
	writePipeline(t, root, "gold.sql", `CREATE MATERIALIZED VIEW gold.revenue AS
SELECT customer, sum(amount) AS total FROM bronze.orders GROUP BY customer;`)
	return root
}

const chainJob = `name: nightly
tasks:
  - id: load
    type: ingest
    params: {names: [orders]}
  - id: build
    type: pipeline
    depends_on: [load]
    params: {targets: [gold.revenue]}
  - id: stamp
    type: shell
    depends_on: [build]
    params: {command: [sh, -c, "echo done $DUCT_JOB $DUCT_TASK > stamp.txt; env > env.txt"]}
`

// TestE2EJobsCLIChain: `duct jobs run` drives ingest -> pipeline -> shell in order,
// records runs and tasks, and the shell task sees no lake credentials.
func TestE2EJobsCLIChain(t *testing.T) {
	root := chainProject(t)
	writeJob(t, root, "nightly", chainJob)
	t.Setenv("DUCT_S3_SECRET_ACCESS_KEY", "lake-secret")
	t.Setenv("DUCT_TOKEN", "tok-123")

	out := mustDuct(t, "jobs", "list", "--project", root)
	if !strings.Contains(out, "nightly") {
		t.Errorf("list: %s", out)
	}
	out = mustDuct(t, "jobs", "run", "nightly", "--project", root)
	t.Logf("jobs run:\n%s", out)
	for _, want := range []string{"SUCCESS (manual)", "load", "build", "stamp"} {
		if !strings.Contains(out, want) {
			t.Errorf("run output lacks %q", want)
		}
	}
	if n := count(t, root, "gold.revenue"); n != 2 {
		t.Errorf("gold rows = %v", n)
	}
	if b, _ := os.ReadFile(filepath.Join(root, "stamp.txt")); strings.TrimSpace(string(b)) != "done nightly stamp" {
		t.Errorf("stamp = %q", b)
	}
	env, _ := os.ReadFile(filepath.Join(root, "env.txt"))
	if strings.Contains(string(env), "lake-secret") || !strings.Contains(string(env), "DUCT_TOKEN=tok-123") {
		t.Errorf("task environment wrong:\n%s", env)
	}
	tasks := queryRows(t, root, "SELECT task_id, status, attempts FROM duct.duct_task_runs ORDER BY started_at")
	if len(tasks) != 3 || tasks[0]["task_id"] != "load" || tasks[2]["task_id"] != "stamp" {
		t.Errorf("tasks = %v", tasks)
	}
	for _, r := range tasks {
		if r["status"] != "success" || r["attempts"].(float64) != 1 {
			t.Errorf("task %v", r)
		}
	}
	if h := mustDuct(t, "jobs", "history", "nightly", "--project", root); !strings.Contains(h, "success") || !strings.Contains(h, "manual") {
		t.Errorf("history: %s", h)
	}
}

// TestE2EJobsFailureSkipsDownstream: a failing task fails the run, skips what
// depends on it, and independent tasks still run.
func TestE2EJobsFailureSkipsDownstream(t *testing.T) {
	root := newProject(t)
	writeJob(t, root, "j", `name: j
tasks:
  - {id: bad, type: shell, params: {command: [sh, -c, "echo nope >&2; exit 3"]}}
  - {id: after, type: shell, depends_on: [bad], params: {command: [touch, after.txt]}}
  - {id: side, type: shell, params: {command: [touch, side.txt]}}
`)
	out, _, code := duct(t, "jobs", "run", "j", "--project", root)
	if code != 1 || !strings.Contains(out, "FAILED") || !strings.Contains(out, "nope") {
		t.Errorf("code=%d out=%s", code, out)
	}
	if _, err := os.Stat(filepath.Join(root, "after.txt")); err == nil {
		t.Error("downstream task ran")
	}
	if _, err := os.Stat(filepath.Join(root, "side.txt")); err != nil {
		t.Error("independent task did not run")
	}
	rows := queryRows(t, root, "SELECT task_id, status FROM duct.duct_task_runs ORDER BY task_id")
	got := map[string]any{}
	for _, r := range rows {
		got[r["task_id"].(string)] = r["status"]
	}
	if got["bad"] != "failed" || got["after"] != "skipped" || got["side"] != "success" {
		t.Errorf("statuses %v", got)
	}
}

// TestE2EJobsRetriesAndTimeout: backoff retries until success; a timeout is a failed attempt.
func TestE2EJobsRetriesAndTimeout(t *testing.T) {
	root := newProject(t)
	writeJob(t, root, "j", `name: j
tasks:
  - id: flaky
    type: shell
    retries: 2
    backoff: 200ms
    params: {command: [sh, -c, "test -f ok.marker || { touch ok.marker; exit 1; }"]}
  - id: slow
    type: shell
    timeout: 300ms
    params: {command: [sleep, "30"]}
`)
	start := time.Now()
	out, _, code := duct(t, "jobs", "run", "j", "--project", root)
	t.Logf("%s", out)
	if code != 1 || time.Since(start) > 20*time.Second {
		t.Errorf("code=%d after %s", code, time.Since(start))
	}
	rows := queryRows(t, root, "SELECT task_id, status, attempts, error FROM duct.duct_task_runs ORDER BY task_id")
	by := map[string]map[string]any{}
	for _, r := range rows {
		by[r["task_id"].(string)] = r
	}
	if by["flaky"]["status"] != "success" || by["flaky"]["attempts"].(float64) != 2 {
		t.Errorf("flaky = %v", by["flaky"])
	}
	if by["slow"]["status"] != "failed" || !strings.Contains(by["slow"]["error"].(string), "timeout after 300ms") {
		t.Errorf("slow = %v", by["slow"])
	}
}

// TestE2EJobsPipelineHonoursContracts: a scheduled pipeline task is gated by
// the data contract exactly like `duct run` (critical rule keeps the previous
// version, the run fails, downstream is skipped) and syncs the contract roles.
func TestE2EJobsPipelineHonoursContracts(t *testing.T) {
	root := newProject(t)
	q := func(sql string) { mustDuct(t, "query", "--project", root, sql) }
	q("CREATE SCHEMA bronze")
	q("CREATE TABLE bronze.orders (id INTEGER, customer VARCHAR, amount DOUBLE)")
	q("INSERT INTO bronze.orders VALUES (1,'ana',10),(2,'ana',5),(3,'bob',20),(4,'cy',7)")
	writePipeline(t, root, "gold.sql", `CREATE MATERIALIZED VIEW gold.revenue AS
SELECT customer, sum(amount) AS total FROM bronze.orders GROUP BY customer;`)
	writeContractFile(t, root, "contracts/revenue.odcs.yaml", revenueContract)
	writeJob(t, root, "refresh", `name: refresh
tasks:
  - {id: build, type: pipeline}
  - {id: after, type: shell, depends_on: [build], params: {command: [touch, after.txt]}}
`)
	mustDuct(t, "jobs", "run", "refresh", "--project", root)
	rows := queryRows(t, root, "SELECT customer FROM role_analistas.revenue ORDER BY customer")
	if len(rows) != 2 { // roles synced from the contract by the scheduled run
		t.Fatalf("role view after job run = %v", rows)
	}
	_ = os.Remove(filepath.Join(root, "after.txt"))

	q("INSERT INTO bronze.orders VALUES (6,'bob',-100)")
	out, _, code := duct(t, "jobs", "run", "refresh", "--project", root)
	if code != 1 || !strings.Contains(out, "no_negative_totals") {
		t.Errorf("code=%d out=%s", code, out)
	}
	if rows := queryRows(t, root, "SELECT total FROM gold.revenue WHERE customer = 'bob'"); rows[0]["total"].(float64) != 20 {
		t.Errorf("gold changed despite the critical rule: %v", rows)
	}
	if _, err := os.Stat(filepath.Join(root, "after.txt")); err == nil {
		t.Error("downstream ran after a blocked contract")
	}
}

// TestE2EJobsFailureAlert: on_failure reaches a webhook through the channel
// module (firing), and the next good run sends resolved.
func TestE2EJobsFailureAlert(t *testing.T) {
	var mu sync.Mutex
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		mu.Lock()
		got = append(got, m)
		mu.Unlock()
	}))
	defer srv.Close()
	root := newProject(t)
	writeJob(t, root, "j", `name: j
tasks:
  - {id: t, type: shell, params: {command: [sh, -c, "test -f good"]}}
alerts:
  on_failure:
    - channel: webhook
      config: {url: "`+srv.URL+`"}
`)
	if _, _, code := duct(t, "jobs", "run", "j", "--project", root); code != 1 {
		t.Fatalf("code %d", code)
	}
	mu.Lock()
	if len(got) != 1 || got[0]["state"] != "firing" || got[0]["rule"] != "job:j" || !strings.Contains(got[0]["message"].(string), "failed") {
		t.Fatalf("alerts after failure: %v", got)
	}
	mu.Unlock()
	writeFile(t, filepath.Join(root, "good"), "")
	mustDuct(t, "jobs", "run", "j", "--project", root)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[1]["state"] != "resolved" {
		t.Fatalf("alerts after recovery: %v", got)
	}
}

// TestE2EJobsServeSchedulerAndTriggers: inside `duct serve`, a cron job ticks,
// a snapshot trigger fires after an ingest, a file trigger fires on a new file,
// and shutdown records a running task as cancelled.
func TestE2EJobsServeSchedulerAndTriggers(t *testing.T) {
	root := chainProject(t)
	mustDuct(t, "ingest", "--project", root) // bronze.orders exists before the baseline
	writeJob(t, root, "tick", `name: tick
schedule: "@every 1s"
tasks:
  - {id: t, type: shell, params: {command: [sh, -c, "echo x >> ticks.txt"]}}
`)
	writeJob(t, root, "onsnap", `name: onsnap
triggers:
  on_snapshot: [bronze.orders]
tasks:
  - {id: build, type: pipeline, params: {targets: [gold.revenue]}}
  - {id: stamp, type: shell, depends_on: [build], params: {command: [touch, snap.txt]}}
`)
	writeJob(t, root, "onfile", `name: onfile
triggers:
  on_file: data/in/*.csv
tasks:
  - {id: t, type: shell, params: {command: [touch, file.txt]}}
`)
	writeJob(t, root, "long", `name: long
triggers:
  on_file: data/long/*.go
tasks:
  - {id: t, type: shell, params: {command: [sleep, "60"]}}
`)
	s := startServe(t, root, nil)

	waitFor(t, "two cron ticks", 20*time.Second, func() bool {
		return len(runsOf(t, root, "job = 'tick' AND status = 'success'")) >= 2
	})
	rs := runsOf(t, root, "job = 'tick'")
	seen := map[any]bool{}
	for _, r := range rs {
		if seen[r["scheduled_for"]] {
			t.Errorf("tick %v ran twice", r["scheduled_for"])
		}
		seen[r["scheduled_for"]] = true
	}
	if n := len(runsOf(t, root, "job = 'onsnap'")); n != 0 {
		t.Errorf("snapshot job fired on the baseline: %d runs", n)
	}

	// New snapshot of bronze.orders (a second ingest, full mode replaces it).
	writeFile(t, filepath.Join(root, "data", "orders.csv"), "id,customer,amount\n1,ana,10\n2,bob,20\n3,ana,5\n4,cy,1\n")
	mustDuct(t, "ingest", "--project", root)
	waitFor(t, "snapshot-triggered run", 30*time.Second, func() bool {
		return len(runsOf(t, root, "job = 'onsnap' AND trigger_kind = 'snapshot' AND status = 'success'")) == 1
	})
	if _, err := os.Stat(filepath.Join(root, "snap.txt")); err != nil {
		t.Error("snapshot job tasks did not run")
	}
	if n := count(t, root, "gold.revenue"); n != 3 {
		t.Errorf("gold rows after triggered refresh = %v", n)
	}
	time.Sleep(5 * time.Second) // the job's own writes must not retrigger it (gold is not watched)
	if n := len(runsOf(t, root, "job = 'onsnap'")); n != 1 {
		t.Errorf("snapshot job ran %d times", n)
	}

	// New file.
	writeFile(t, filepath.Join(root, "data", "in", "a.csv"), "x\n1\n")
	waitFor(t, "file-triggered run", 30*time.Second, func() bool {
		return len(runsOf(t, root, "job = 'onfile' AND trigger_kind = 'file' AND status = 'success'")) == 1
	})

	// Shutdown while a task runs: recorded as cancelled, process group gone.
	writeFile(t, filepath.Join(root, "data", "long", "a.go"), "x")
	waitFor(t, "long task running", 30*time.Second, func() bool {
		return len(queryRows(t, root, "SELECT 1 AS x FROM duct.duct_task_runs WHERE job = 'long' AND status = 'running'")) == 1
	})
	s.stop()
	rows := queryRows(t, root, "SELECT status FROM duct.duct_task_runs WHERE job = 'long'")
	runs := runsOf(t, root, "job = 'long'")
	if len(rows) != 1 || rows[0]["status"] != "cancelled" || runs[0]["status"] != "cancelled" {
		t.Errorf("after shutdown: tasks=%v runs=%v", rows, runs)
	}
}
