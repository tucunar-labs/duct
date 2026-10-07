package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func newProject(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "p")
	mustDuct(t, "init", root)
	return root
}

func queryCSV(t *testing.T, root, sql string) string {
	t.Helper()
	return strings.TrimSpace(mustDuct(t, "query", "--project", root, "--format", "csv", sql))
}

// TestE2EIngestCSV: CSV -> bronze -> duct query; full replace, incremental
// append, schema evolution and "state only saved on success".
func TestE2EIngestCSV(t *testing.T) {
	root := newProject(t)
	writeFile(t, filepath.Join(root, "data", "c.csv"), "id,name,updated_at\n1,ana,2024-01-01\n2,beto,2024-01-02\n")
	writeFile(t, filepath.Join(root, "ingest", "customers.yaml"), `name: customers
source: files
options:
  path: data/c.csv
`)
	writeFile(t, filepath.Join(root, "ingest", "events.yaml"), `name: events
source: files
mode: incremental
watermark: updated_at
options:
  path: data/c*.csv
`)
	out := mustDuct(t, "ingest", "--project", root)
	for _, want := range []string{"customers", "bronze.customers", "2 rows", "events"} {
		if !strings.Contains(out, want) {
			t.Fatalf("ingest output lacks %q:\n%s", want, out)
		}
	}
	if got := queryCSV(t, root, "SELECT id, name FROM bronze.customers ORDER BY id"); got != "id,name\n1,ana\n2,beto" {
		t.Fatalf("bronze.customers: %q", got)
	}

	// Full mode replaces; incremental appends only newer rows; a new column
	// evolves the table.
	writeFile(t, filepath.Join(root, "data", "c.csv"), "id,name,updated_at,city\n1,ana,2024-01-01,Santiago\n2,beto,2024-01-02,Lima\n3,cami,2024-01-03,Quito\n")
	out = mustDuct(t, "ingest", "--project", root, "customers", "events")
	if !strings.Contains(out, "3 rows") || !strings.Contains(out, "1 rows") {
		t.Fatalf("second run output:\n%s", out)
	}
	if got := queryCSV(t, root, "SELECT count(*) FROM bronze.customers"); got != "count_star()\n3" {
		t.Errorf("full mode must replace, got %q", got)
	}
	if got := queryCSV(t, root, "SELECT count(*) AS n FROM bronze.events"); got != "n\n3" {
		t.Errorf("incremental must append only new rows (2+1), got %q", got)
	}
	if got := queryCSV(t, root, "SELECT city FROM bronze.events WHERE id = 3"); got != "city\nQuito" {
		t.Errorf("schema evolution: %q", got)
	}
	state := queryCSV(t, root, "SELECT name, watermark FROM duct.duct_ingest_state ORDER BY name")
	if !strings.Contains(state, "events,2024-01-03") || !strings.Contains(state, "customers,") {
		t.Errorf("state: %q", state)
	}

	// Nothing new: zero rows, watermark kept.
	out = mustDuct(t, "ingest", "--project", root, "events")
	if !strings.Contains(out, "0 rows") {
		t.Errorf("idempotent rerun:\n%s", out)
	}

	// A failing run (bad file) leaves state and data untouched.
	writeFile(t, filepath.Join(root, "ingest", "broken.yaml"), "name: broken\nsource: files\nmode: incremental\nwatermark: nope\noptions:\n  path: data/c.csv\n")
	if _, errs, code := duct(t, "ingest", "--project", root, "broken"); code != 1 {
		t.Errorf("broken ingestion: code=%d err=%q", code, errs)
	}
	if got := queryCSV(t, root, "SELECT count(*) AS n FROM duct.duct_ingest_state WHERE name = 'broken'"); got != "n\n0" {
		t.Errorf("state saved for a failed run: %q", got)
	}
	if got := queryCSV(t, root, "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = 'broken'"); got != "n\n0" {
		t.Errorf("failed run left a table: %q", got)
	}

	// Usage errors.
	if _, errs, code := duct(t, "ingest", "--project", root, "nope"); code != 2 || !strings.Contains(errs, "no ingestion") {
		t.Errorf("unknown name: code=%d %q", code, errs)
	}
}

// TestE2EIngestBento: REST (a bare http_client input polls forever, so one
// generated message drives one request; see source-bento AGENTS.md)
// (httptest) through the Bento module -> bronze.
func TestE2EIngestBento(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `[{"id":1,"name":"a","meta":{"x":1}},{"id":2,"name":"b","extra":true}]`)
	}))
	defer srv.Close()
	root := newProject(t)
	writeFile(t, filepath.Join(root, "ingest", "api.yaml"), fmt.Sprintf(`name: api
source: bento
options:
  input:
    generate:
      count: 1
      interval: ""
      mapping: root = {}
  processors:
    - http:
        url: %s
        verb: GET
`, srv.URL))
	out := mustDuct(t, "ingest", "--project", root)
	if !strings.Contains(out, "bronze.api") || !strings.Contains(out, "2 rows") {
		t.Fatalf("output:\n%s", out)
	}
	if got := queryCSV(t, root, "SELECT id, name, meta, extra FROM bronze.api ORDER BY id"); got != "id,name,meta,extra\n1,a,\"{\"\"x\"\":1}\",\n2,b,,true" {
		t.Errorf("bronze.api: %q", got)
	}
}

func TestIngestNoDefinitions(t *testing.T) {
	root := newProject(t)
	if out := mustDuct(t, "ingest", "--project", root); !strings.Contains(out, "No ingestions") {
		t.Errorf("got %q", out)
	}
}
