package sourcefiles_test

import (
	"context"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
	_ "github.com/alonsoburon/duct/modules/source-files"
)

func plan(t *testing.T, opts map[string]any, wm string, st contract.IngestState) (contract.SQLPlan, error) {
	t.Helper()
	info, ok := registry.Lookup(contract.KindSource, "files")
	if !ok {
		t.Fatal("source/files not registered")
	}
	m := info.New().(contract.SQLSource)
	defer func() { _ = m.Close() }()
	cfg := contract.Config{"root": "/proj", "watermark": wm, "options": opts}
	if err := m.Init(context.Background(), cfg); err != nil {
		return contract.SQLPlan{}, err
	}
	return m.Plan(context.Background(), st)
}

func TestPlanFormats(t *testing.T) {
	cases := []struct {
		opts map[string]any
		want string
	}{
		{map[string]any{"path": "data/*.csv"}, "SELECT * FROM read_csv('/proj/data/*.csv', union_by_name = true)"},
		{map[string]any{"path": "/abs/a.parquet"}, "SELECT * FROM read_parquet('/abs/a.parquet', union_by_name = true)"},
		{map[string]any{"path": "e.ndjson"}, "SELECT * FROM read_json('/proj/e.ndjson', union_by_name = true)"},
		{map[string]any{"path": []any{"a.txt", "b.txt"}, "format": "csv", "reader": map[string]any{"header": true, "delim": ";"}},
			"SELECT * FROM read_csv(['/proj/a.txt', '/proj/b.txt'], delim = ';', header = true, union_by_name = true)"},
	}
	for _, c := range cases {
		p, err := plan(t, c.opts, "", contract.IngestState{})
		if err != nil || p.Select != c.want {
			t.Errorf("%v:\n got %q (%v)\nwant %q", c.opts, p.Select, err, c.want)
		}
	}
}

func TestPlanIncremental(t *testing.T) {
	p, err := plan(t, map[string]any{"path": "a.csv"}, "updated_at", contract.IngestState{Watermark: "2024-01-02 11:00:00"})
	if err != nil || !strings.HasSuffix(p.Select, `WHERE "updated_at" > '2024-01-02 11:00:00'`) {
		t.Fatalf("got %q (%v)", p.Select, err)
	}
	p, _ = plan(t, map[string]any{"path": "a.csv"}, "updated_at", contract.IngestState{})
	if strings.Contains(p.Select, "WHERE") {
		t.Errorf("first run must not filter: %q", p.Select)
	}
}

func TestInitErrors(t *testing.T) {
	for name, opts := range map[string]map[string]any{
		"no path":          {},
		"remote":           {"path": "s3://b/x.csv"},
		"unknown format":   {"path": "a.xlsx"},
		"bad reader param": {"path": "a.csv", "reader": map[string]any{"x; DROP": 1}},
	} {
		if _, err := plan(t, opts, "", contract.IngestState{}); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
