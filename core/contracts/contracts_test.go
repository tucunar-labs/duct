package contracts

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, name, got string) {
	t.Helper()
	p := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(p, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p) //nolint:gosec // testdata
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Errorf("%s differs (run go test -update):\n--- got\n%s\n--- want\n%s", name, got, want)
	}
}

func load(t *testing.T, file string) *Contract {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", file)) //nolint:gosec // testdata
	if err != nil {
		t.Fatal(err)
	}
	c, err := Parse("contracts/"+file, data)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCompileGolden(t *testing.T) {
	c := load(t, "revenue.odcs.yaml")
	if c.Table != "gold.revenue" {
		t.Fatalf("table = %q", c.Table)
	}
	p, err := c.Plan()
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	c.Describe(&b, p)
	golden(t, "revenue.compile.golden", b.String())
}

func TestPlanErrors(t *testing.T) {
	base, err := os.ReadFile("testdata/revenue.odcs.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct{ from, to, want string }{
		"bad argument":  {"pattern: \"^[a-z]+$\"", "bogus: 1", "does not take argument"},
		"column metric": {"- metric: rowCount", "- metric: nullValues", "must be on a property"},
		"bad extension": {"duct.row_filter", "duct.rowfilter", "unknown extension"},
		"bad mask col":  {"duct.mask.customer", "duct.mask.nope", "unknown column"},
		"bad on_viol":   {"value: drop_row", "value: explode", "must be drop_row"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			src := strings.Replace(string(base), tc.from, tc.to, 1)
			c, err := Parse("x.odcs.yaml", []byte(src))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Plan(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSchemaValidationRejects(t *testing.T) {
	base, _ := os.ReadFile("testdata/revenue.odcs.yaml")
	for _, tc := range []struct{ from, to string }{
		{"metric: rowCount", "metric: bogus"},
		{"        mustBeGreaterThan: 0\n", ""},
	} {
		if _, err := Parse("x", []byte(strings.Replace(string(base), tc.from, tc.to, 1))); err == nil {
			t.Errorf("accepted %q", tc.to)
		}
	}
}

func TestTypeCompatible(t *testing.T) {
	cases := []struct {
		logical, physical, duck string
		ok                      bool
	}{
		{"string", "", "VARCHAR", true},
		{"string", "", "BIGINT", false},
		{"integer", "", "HUGEINT", true},
		{"integer", "", "DOUBLE", false},
		{"number", "", "DECIMAL(18,3)", true},
		{"number", "", "INTEGER", true},
		{"number", "", "VARCHAR", false},
		{"date", "", "DATE", true},
		{"date", "", "TIMESTAMP", false},
		{"timestamp", "", "TIMESTAMP WITH TIME ZONE", true},
		{"boolean", "", "BOOLEAN", true},
		{"array", "", "INTEGER[]", true},
		{"object", "", "STRUCT(a INTEGER)", true},
		{"string", "varchar(20)", "VARCHAR", true},
		{"string", "int", "INTEGER", true}, // physical wins over logical
		{"number", "decimal(10,2)", "DECIMAL(10,2)", true},
		{"number", "decimal(10,2)", "DECIMAL(18,3)", false},
		{"number", "double precision", "DOUBLE", true},
	}
	for _, c := range cases {
		if got := TypeCompatible(c.logical, c.physical, c.duck); got != c.ok {
			t.Errorf("TypeCompatible(%q,%q,%q) = %v", c.logical, c.physical, c.duck, got)
		}
	}
}

func TestSchemaIssues(t *testing.T) {
	c := load(t, "revenue.odcs.yaml")
	got := c.SchemaIssues([]ActualColumn{{"customer", "BIGINT"}, {"extra", "VARCHAR"}})
	want := []string{
		`column "customer" has type BIGINT, the contract says string`,
		`column "total" is in the contract but not in the table`,
		`column "extra" (VARCHAR) is in the table but not in the contract`,
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %q", got)
	}
}

func TestThresholds(t *testing.T) {
	c := load(t, "revenue.odcs.yaml")
	p, _ := c.Plan()
	by := map[string]Check{}
	for _, ch := range p.Checks {
		by[ch.Name] = ch
	}
	if ch := by["nullValues(total)"]; !ch.Pass(5) || ch.Pass(5.1) {
		t.Error("percent threshold <= 5")
	}
	if ch := by["rowCount"]; ch.Pass(0) || !ch.Pass(1) {
		t.Error("rowCount > 0")
	}
}

func TestFreshness(t *testing.T) {
	c := load(t, "revenue.odcs.yaml")
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		last time.Time
		want string
	}{
		{now.Add(-23 * time.Hour), Fresh},
		{now.Add(-25 * time.Hour), Stale},
		{time.Time{}, Unknown},
	} {
		if s := Evaluate(c, tc.last, now); s.Status != tc.want || s.MaxSeconds != 86400 || s.Source != "latency" {
			t.Errorf("last=%v: %+v", tc.last, s)
		}
	}
	d, ok := duration(2, "h")
	if !ok || d != 2*time.Hour {
		t.Errorf("duration = %v %v", d, ok)
	}
}
