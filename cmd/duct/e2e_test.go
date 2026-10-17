package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// duct runs the CLI in-process and returns stdout, stderr and the exit code.
func duct(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, &out, &errb)
	return out.String(), errb.String(), code
}

func mustDuct(t *testing.T, args ...string) string {
	t.Helper()
	out, errs, code := duct(t, args...)
	if code != 0 {
		t.Fatalf("duct %v: code=%d stdout=%q stderr=%q", args, code, out, errs)
	}
	return out
}

// TestE2ELocalLake: init -> create/insert -> query -> migrate -> gc.
func TestE2ELocalLake(t *testing.T) {
	root := filepath.Join(t.TempDir(), "demo")
	out := mustDuct(t, "init", root)
	if !strings.Contains(out, "Created Duct project") {
		t.Fatalf("init output: %q", out)
	}
	for _, p := range []string{"duct.toml", "lake", ".duct/catalog.sqlite", ".duct/duct.sqlite", ".gitignore"} {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Errorf("init did not create %s: %v", p, err)
		}
	}
	gi, _ := os.ReadFile(filepath.Join(root, ".gitignore"))
	for _, want := range []string{"lake/", ".duct/", ".env*"} {
		if !strings.Contains(string(gi), want) {
			t.Errorf(".gitignore lacks %q", want)
		}
	}
	// A second init on the same folder is refused.
	if _, errs, code := duct(t, "init", root); code == 0 || !strings.Contains(errs, "already") {
		t.Errorf("second init: code=%d err=%q", code, errs)
	}

	q := func(args ...string) string {
		return mustDuct(t, append([]string{"query", "--project", root}, args...)...)
	}
	q("CREATE TABLE events (id INTEGER, kind VARCHAR)")
	q("INSERT INTO events SELECT i, 'k' || (i % 3) FROM range(3000) r(i)")
	// Run from inside the project too (project discovery).
	t.Chdir(root)
	table := mustDuct(t, "query", "SELECT kind, count(*) AS n FROM events GROUP BY kind ORDER BY kind")
	for _, want := range []string{"kind", "k0 ", "1000", "(3 rows)"} {
		if !strings.Contains(table, want) {
			t.Errorf("table output lacks %q:\n%s", want, table)
		}
	}
	csvOut := mustDuct(t, "query", "--format", "csv", "SELECT id, kind FROM events WHERE id < 2 ORDER BY id")
	if csvOut != "id,kind\n0,k0\n1,k1\n" {
		t.Errorf("csv = %q", csvOut)
	}
	var rows []map[string]any
	jsonOut := mustDuct(t, "query", "--format", "json", "SELECT id, kind, NULL AS nada FROM events WHERE id < 2 ORDER BY id")
	if err := json.Unmarshal([]byte(jsonOut), &rows); err != nil || len(rows) != 2 || rows[1]["kind"] != "k1" || rows[0]["nada"] != nil {
		t.Errorf("json = %q (%v)", jsonOut, err)
	}

	// Duct-owned tables are in their own catalog, migrated by init.
	mig := mustDuct(t, "query", "SELECT version, name FROM duct.duct_migrations")
	if !strings.Contains(mig, "runs") {
		t.Errorf("duct_migrations = %q", mig)
	}
	if got := mustDuct(t, "migrate"); !strings.Contains(got, "up to date") {
		t.Errorf("migrate not idempotent: %q", got)
	}
	// DuckLake's own catalog tables are not touched by Duct (separate files).
	mustDuct(t, "query", "SELECT count(*) FROM duct.duct_runs")

	// Locked mode still reads the lake and denies the filesystem.
	if got := mustDuct(t, "query", "--locked", "SELECT count(*) AS n FROM events"); !strings.Contains(got, "3000") {
		t.Errorf("locked read: %q", got)
	}
	if _, errs, code := duct(t, "query", "--locked", "SELECT * FROM read_csv('/etc/passwd')"); code == 0 || !strings.Contains(errs, "Permission") {
		t.Errorf("locked read_csv: code=%d err=%q", code, errs)
	}

	// Make garbage for gc: several small commits, a delete, then maintenance.
	for i := range 3 {
		mustDuct(t, "query", "INSERT INTO events SELECT i, 'z' FROM range(1000) r(i)")
		_ = i
	}
	mustDuct(t, "query", "DELETE FROM events WHERE kind = 'z' AND id < 500")
	time.Sleep(1100 * time.Millisecond)
	gcOut := mustDuct(t, "gc", "--keep-snapshots", "1s")
	for _, want := range []string{"flush inlined data", "merge adjacent files", "expire snapshots", "cleanup old files", "wal checkpoint", "busy=0", "gc done"} {
		if !strings.Contains(gcOut, want) {
			t.Errorf("gc output lacks %q:\n%s", want, gcOut)
		}
	}
	// Data survives gc, and the catalog WAL was truncated.
	if got := mustDuct(t, "query", "SELECT count(*) AS n FROM events"); !strings.Contains(got, "4500") {
		t.Errorf("rows after gc: %q", got)
	}
	if fi, err := os.Stat(filepath.Join(root, ".duct/catalog.sqlite-wal")); err == nil && fi.Size() != 0 {
		t.Errorf("catalog WAL not truncated: %d bytes", fi.Size())
	}
}

func TestInitRefusesSyncedFolder(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "Dropbox", "demo")
	_, errs, code := duct(t, "init", dir)
	if code == 0 || !strings.Contains(errs, "--force") {
		t.Fatalf("expected refusal, code=%d err=%q", code, errs)
	}
	if _, err := os.Stat(dir); err == nil {
		t.Error("refused init must not create the folder")
	}
	mustDuct(t, "init", "--force", dir)
	if _, err := os.Stat(filepath.Join(dir, "duct.toml")); err != nil {
		t.Errorf("--force did not create the project: %v", err)
	}
}

func TestQueryOutsideProject(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, errs, code := duct(t, "query", "SELECT 1"); code != 1 || !strings.Contains(errs, "duct init") {
		t.Fatalf("code=%d err=%q", code, errs)
	}
	if _, _, code := duct(t, "query"); code != 2 {
		t.Fatalf("missing SQL should be a usage error, code=%d", code)
	}
	if _, _, code := duct(t, "query", "--format", "xml", "SELECT 1"); code != 2 {
		t.Fatalf("bad format should be a usage error, code=%d", code)
	}
}
