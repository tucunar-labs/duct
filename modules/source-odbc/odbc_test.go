package sourceodbc_test

import (
	"context"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
	_ "github.com/alonsoburon/duct/modules/source-odbc"
)

func plan(t *testing.T, opts map[string]any, wm string, st contract.IngestState) (contract.SQLPlan, error) {
	t.Helper()
	info, ok := registry.Lookup(contract.KindSource, "odbc")
	if !ok {
		t.Fatal("source/odbc not registered")
	}
	m := info.New().(contract.SQLSource)
	defer func() { _ = m.Close() }()
	if err := m.Init(context.Background(), contract.Config{"watermark": wm, "options": opts}); err != nil {
		return contract.SQLPlan{}, err
	}
	return m.Plan(context.Background(), st)
}

func TestPlanTableAndSetup(t *testing.T) {
	p, err := plan(t, map[string]any{"connection": "DSN=pg", "table": "public.t"}, "", contract.IngestState{})
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT * FROM odbc_query(getvariable('duct_odbc'), 'SELECT * FROM public.t')"
	if p.Select != want {
		t.Errorf("got %q want %q", p.Select, want)
	}
	if len(p.Setup) != 2 || p.Setup[0] != "INSTALL odbc_scanner" || p.Setup[1] != "LOAD odbc_scanner" {
		t.Errorf("setup: %v", p.Setup)
	}
	if len(p.Open) != 1 || p.Open[0] != "SET VARIABLE duct_odbc = odbc_connect('DSN=pg')" {
		t.Errorf("open: %v", p.Open)
	}
	if len(p.Close) != 1 || p.Close[0] != "SELECT odbc_close(getvariable('duct_odbc'))" {
		t.Errorf("close: %v", p.Close)
	}
}

func TestPlanIncrementalPushdown(t *testing.T) {
	opts := map[string]any{"connection": "DSN=pg", "query": "SELECT id, ts FROM t WHERE x = 'a';"}
	p, _ := plan(t, opts, "ts", contract.IngestState{Watermark: "2024-01-02 11:00:00"})
	want := `SELECT * FROM odbc_query(getvariable('duct_odbc'), 'SELECT * FROM (SELECT id, ts FROM t WHERE x = ''a'') duct_src WHERE ts > ''2024-01-02 11:00:00''')`
	if p.Select != want {
		t.Errorf("got %q\nwant %q", p.Select, want)
	}
	p, _ = plan(t, opts, "id", contract.IngestState{Watermark: "42"})
	if !strings.Contains(p.Select, "WHERE id > 42') ") && !strings.HasSuffix(p.Select, "WHERE id > 42')") {
		t.Errorf("numeric watermark must be bare: %q", p.Select)
	}
	p, _ = plan(t, opts, "ts", contract.IngestState{})
	if strings.Contains(p.Select, "duct_src") {
		t.Errorf("first run must not filter: %q", p.Select)
	}
}

func TestEnvExpansionAndErrors(t *testing.T) {
	t.Setenv("DUCT_TEST_PW", "s3cret")
	p, err := plan(t, map[string]any{"connection": "DSN=pg;Pwd=${DUCT_TEST_PW}", "table": "t"}, "", contract.IngestState{})
	if err != nil || len(p.Open) != 1 || !strings.Contains(p.Open[0], "Pwd=s3cret") {
		t.Fatalf("got %q (%v)", p.Open, err)
	}
	for name, opts := range map[string]map[string]any{
		"no connection": {"table": "t"},
		"no table":      {"connection": "DSN=x"},
		"both":          {"connection": "DSN=x", "table": "t", "query": "select 1"},
		"bad table":     {"connection": "DSN=x", "table": "t; drop table x"},
		"missing env":   {"connection": "Pwd=${DUCT_NOPE_UNSET}", "table": "t"},
	} {
		if _, err := plan(t, opts, "", contract.IngestState{}); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
