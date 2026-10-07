//go:build integration

package integration

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// parallel runs n copies of a duct command at once (released together) and
// returns their results.
func parallel(t *testing.T, n int, root string, args ...string) []ductRun {
	t.Helper()
	ductBinary(t) // build before the clock starts
	start := make(chan struct{})
	out := make([]ductRun, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out[i] = duct(t, root, pwEnv, args...)
		}()
	}
	close(start)
	wg.Wait()
	return out
}

// TestConcurrentRunSameTarget: N `duct run` of the same pipeline against one
// Postgres catalog, first when the target schema/table do not exist (races on
// CREATE SCHEMA and CREATE TABLE) and then when they do (races on REPLACE).
// Every process must exit 0 and the table must be valid (last writer wins).
func TestConcurrentRunSameTarget(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "crun")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("crun"))
	mustDuct(t, root, pwEnv, "query", "CREATE SCHEMA bronze")
	mustDuct(t, root, pwEnv, "query", "CREATE TABLE bronze.t AS SELECT i AS id, i % 7 AS k FROM range(5000) r(i)")
	write(t, filepath.Join(root, "pipelines", "gold.sql"),
		"CREATE MATERIALIZED VIEW gold.t (CONSTRAINT id_ok EXPECT (id >= 0) ON VIOLATION DROP ROW) AS SELECT * FROM bronze.t;\n")

	const n = 8
	for round, name := range []string{"fresh", "existing"} {
		for i, r := range parallel(t, n, root, "run") {
			if r.err != nil || !strings.Contains(r.stdout, "SUCCESS  gold.t") {
				t.Errorf("%s: process %d: %v\nstdout: %s\nstderr: %s", name, i, r.err, r.stdout, r.stderr)
			}
		}
		if got := csvQuery(t, root, pwEnv, "SELECT count(*) AS n, count(DISTINCT id) AS d FROM gold.t"); got != "n,d\n5000,5000" {
			t.Fatalf("%s: gold.t = %q, want 5000 distinct rows", name, got)
		}
		if got := pg.sql(t, "crun", "SELECT count(*) FROM duct.duct_pipeline_runs WHERE status='success'"); got != fmt.Sprint(n*(round+1)) {
			t.Errorf("%s: successful runs recorded = %s, want %d", name, got, n*(round+1))
		}
	}
}

// TestConcurrentIncrementalIngest: N processes ingest the same incremental
// source at once: rows are loaded exactly once and the watermark is the
// maximum, on the first load and on a later append.
func TestConcurrentIncrementalIngest(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "cing")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("cing"))
	csv := filepath.Join(root, "data", "ev.csv")
	rows := func(from, to int) string {
		var b strings.Builder
		for i := from; i <= to; i++ {
			fmt.Fprintf(&b, "%d,e%d,2024-01-%02d\n", i, i, i)
		}
		return b.String()
	}
	write(t, csv, "id,name,updated_at\n"+rows(1, 10))
	write(t, filepath.Join(root, "ingest", "ev.yaml"),
		"name: ev\nsource: files\nmode: incremental\nwatermark: updated_at\noptions:\n  path: data/ev.csv\n")

	const n = 8
	check := func(label string, want int, wm string) {
		t.Helper()
		if got := csvQuery(t, root, pwEnv, "SELECT count(*) AS n, count(DISTINCT id) AS d FROM bronze.ev"); got != fmt.Sprintf("n,d\n%d,%d", want, want) {
			t.Fatalf("%s: bronze.ev = %q, want %d distinct rows", label, got, want)
		}
		if got := pg.sql(t, "cing", "SELECT watermark FROM duct.duct_ingest_state WHERE name='ev'"); !strings.HasPrefix(got, wm) {
			t.Fatalf("%s: watermark = %q, want %s", label, got, wm)
		}
		if got := pg.sql(t, "cing", "SELECT count(*) FROM duct.duct_ingest_state WHERE name LIKE 'lock:%'"); got != "0" {
			t.Fatalf("%s: %s lock rows left", label, got)
		}
	}

	for i, r := range parallel(t, n, root, "ingest", "ev") {
		if r.err != nil {
			t.Errorf("first load: process %d: %v\nstdout: %s\nstderr: %s", i, r.err, r.stdout, r.stderr)
		}
	}
	check("first load", 10, "2024-01-10")

	write(t, csv, "id,name,updated_at\n"+rows(1, 10)+rows(11, 25))
	for i, r := range parallel(t, n, root, "ingest", "ev") {
		if r.err != nil {
			t.Errorf("append: process %d: %v\nstdout: %s\nstderr: %s", i, r.err, r.stdout, r.stderr)
		}
	}
	check("append", 25, "2024-01-25")

	// A different source runs alongside without waiting for ev's lock.
	write(t, filepath.Join(root, "data", "other.csv"), "id\n1\n2\n")
	write(t, filepath.Join(root, "ingest", "other.yaml"), "name: other\nsource: files\noptions:\n  path: data/other.csv\n")
	mustDuct(t, root, pwEnv, "ingest", "other")
}
