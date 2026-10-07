//go:build integration

package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestJobsTwoServersFireOnce: two `duct serve` processes share one Postgres
// catalog; a cron job fires exactly once per tick (one run per tick, no tick
// twice), and every task is claimed by exactly one of them (attempts = 1).
func TestJobsTwoServersFireOnce(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "jobs")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("jobs"))
	write(t, filepath.Join(root, "jobs", "tick.yaml"), `name: tick
schedule: "@every 2s"
tasks:
  - {id: a, type: shell, params: {command: [sh, -c, "echo a >> a.log"]}}
  - {id: b, type: shell, depends_on: [a], params: {command: [sh, -c, "echo b >> b.log"]}}
`)
	mustDuct(t, root, pwEnv, "query", "CREATE SCHEMA bronze")
	mustDuct(t, root, pwEnv, "query", "CREATE TABLE bronze.t (i INTEGER)")
	write(t, filepath.Join(root, "jobs", "onsnap.yaml"), `name: onsnap
triggers:
  on_snapshot: [bronze.t]
tasks:
  - {id: a, type: shell, params: {command: [true]}}
`)
	startServe(t, root, pwEnv)
	startServe(t, root, pwEnv)
	mustDuct(t, root, pwEnv, "query", "INSERT INTO bronze.t VALUES (1)")

	var runs int
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		runs, _ = strconv.Atoi(pg.sql(t, "jobs", "SELECT count(*) FROM duct.duct_jobs_runs WHERE job = 'tick' AND status = 'success'"))
		if runs >= 5 {
			break
		}
		time.Sleep(time.Second)
	}
	if runs < 5 {
		t.Fatalf("only %d successful runs", runs)
	}
	q := func(s string) string { return pg.sql(t, "jobs", s) }
	// The snapshot trigger works on the Postgres catalog and fired once, not once per server.
	for i := 0; i < 30 && q("SELECT count(*) FROM duct.duct_jobs_runs WHERE job = 'onsnap'") == "0"; i++ {
		time.Sleep(time.Second)
	}
	time.Sleep(5 * time.Second)
	if got := q("SELECT count(*) FROM duct.duct_jobs_runs WHERE job = 'onsnap' AND trigger_kind = 'snapshot'"); got != "1" {
		t.Errorf("snapshot trigger runs = %s, want 1", got)
	}
	if got := q("SELECT count(*) - count(DISTINCT scheduled_for) FROM duct.duct_jobs_runs WHERE job = 'tick'"); got != "0" {
		t.Errorf("a tick started more than one run (%s extra)", got)
	}
	if got := q("SELECT count(*) FROM duct.duct_jobs_runs r WHERE NOT EXISTS (SELECT 1 FROM duct.duct_jobs_fires f WHERE f.run_id = r.id)"); got != "0" {
		t.Errorf("%s runs without their fire key", got)
	}
	if got := q("SELECT count(*) FROM duct.duct_task_runs WHERE status = 'success' AND attempts <> 1"); got != "0" {
		t.Errorf("%s tasks ran more than once", got)
	}
	// Each task ran once per run, on whichever process claimed it.
	total := q("SELECT count(*) FROM duct.duct_jobs_runs WHERE job = 'tick' AND status = 'success'")
	lines := func(f string) string {
		b := mustDuctOut(t, root, f)
		return fmt.Sprint(strings.Count(b, "\n"))
	}
	t.Logf("runs=%s a.log=%s b.log=%s owners=%s", total, lines("a.log"), lines("b.log"),
		q("SELECT count(DISTINCT split_part(owner, '/', 1)) FROM duct.duct_task_runs"))
}

func mustDuctOut(t *testing.T, root, file string) string {
	t.Helper()
	out, err := exec_cat(filepath.Join(root, file))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func exec_cat(p string) (string, error) {
	b, err := os.ReadFile(p)
	return string(b), err
}
