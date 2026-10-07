package dashboards

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

const sample1 = `
title: Ventas
filters:
  - {id: region, label: Región, type: select, dimension: region}
  - {id: periodo, type: daterange, dimension: fecha, charts: [serie]}
  - {id: cat, type: multiselect, dimension: categoria, measure: ingresos}
charts:
  - id: por_region
    title: Ingresos por región
    type: bar
    dims: [region]
    measures: [ingresos]
    sort: [{by: ingresos, desc: true}]
    limit: 10
    layout: {x: 0, y: 0, w: 6, h: 4}
    options: {stack: true, format: compact, prefix: "$", colors: ["#F26B1D"]}
  - id: serie
    type: line
    dims: [fecha]
    measures: [ingresos]
    where: [{field: canal, op: "=", value: web}]
  - id: total
    type: kpi
    measures: [ingresos]
    cross_filter: false
`

func mustParse(t *testing.T, y string) *Dashboard {
	t.Helper()
	d, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestParseValid(t *testing.T) {
	d := mustParse(t, sample1)
	if len(d.Charts) != 3 || len(d.Filters) != 3 {
		t.Fatalf("%+v", d)
	}
	if c := d.Charts[1]; c.Layout.W != 6 || c.Layout.H != 4 || c.Layout.X != nil {
		t.Errorf("layout defaults: %+v", c.Layout)
	}
	if !d.Charts[0].Emits() || d.Charts[2].Emits() || d.Charts[2].Receives() {
		t.Error("cross-filter flags")
	}
}

func TestParseErrors(t *testing.T) {
	for name, tc := range map[string]struct{ yaml, want string }{
		"empty":         {"", "empty file"},
		"unknown key":   {"title: x\ncharts: [{id: a, type: kpi, measures: [m], color: red}]", "field color not found"},
		"unsafe opt":    {"title: x\ncharts: [{id: a, type: kpi, measures: [m], options: {formatter: 'function(){}'}}]", "field formatter not found"},
		"no title":      {"charts: [{id: a, type: kpi, measures: [m]}]", "title is required"},
		"no charts":     {"title: x", "at least one chart"},
		"bad type":      {"title: x\ncharts: [{id: a, type: radar, measures: [m]}]", `type "radar"`},
		"dup id":        {"title: x\ncharts: [{id: a, type: kpi, measures: [m]}, {id: a, type: kpi, measures: [m]}]", "duplicate id"},
		"bad id":        {"title: x\ncharts: [{id: A-b, type: kpi, measures: [m]}]", "must match"},
		"kpi dims":      {"title: x\ncharts: [{id: a, type: kpi, dims: [d], measures: [m]}]", "kpi takes no dims"},
		"pie dims":      {"title: x\ncharts: [{id: a, type: pie, measures: [m]}]", "pie takes one dim"},
		"bar 2 dims 2m": {"title: x\ncharts: [{id: a, type: bar, dims: [a, b], measures: [m, n]}]", "exactly one measure"},
		"sort":          {"title: x\ncharts: [{id: a, type: bar, dims: [a], measures: [m], sort: [{by: z}]}]", `sort by "z"`},
		"layout":        {"title: x\ncharts: [{id: a, type: kpi, measures: [m], layout: {x: 8, w: 6}}]", "must fit"},
		"color":         {"title: x\ncharts: [{id: a, type: kpi, measures: [m], options: {colors: [red]}}]", "#rrggbb"},
		"filter type":   {"title: x\nfilters: [{id: f, type: text, dimension: d}]\ncharts: [{id: a, type: kpi, measures: [m]}]", "type"},
		"filter chart":  {"title: x\nfilters: [{id: f, type: select, dimension: d, charts: [nope]}]\ncharts: [{id: a, type: kpi, measures: [m]}]", "unknown chart"},
		"limit":         {"title: x\ncharts: [{id: a, type: kpi, measures: [m], limit: 99999}]", "limit must be"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestRequest(t *testing.T) {
	d := mustParse(t, sample1)
	st := State{
		Filters: map[string][]string{"region": {"CL"}, "periodo": {"2026-01-01", "2026-03-31"}, "cat": {"a", "b"}},
		Cross:   map[string][]string{"por_region": {"AR", "CL"}},
	}
	byID := func(id string) contract.SemanticRequest {
		c, _ := d.Chart(id)
		r, err := d.Request(c, st)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	// serie: static where + region + periodo (applies) + cat + cross from por_region.
	got := byID("serie").Filters
	want := []contract.SemanticFilter{
		{Field: "canal", Op: "=", Value: "web"},
		{Field: "region", Op: "=", Value: "CL"},
		{Field: "fecha", Op: "between", Value: []any{"2026-01-01", "2026-03-31"}},
		{Field: "categoria", Op: "in", Value: []any{"a", "b"}},
		{Field: "region", Op: "in", Value: []any{"AR", "CL"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("serie filters:\n got %+v\nwant %+v", got, want)
	}
	// The source chart ignores its own selection; periodo is limited to serie.
	for _, f := range byID("por_region").Filters {
		if f.Op == "in" && f.Field == "region" || f.Field == "fecha" {
			t.Errorf("por_region got %+v", f)
		}
	}
	// cross_filter: false does not receive.
	for _, f := range byID("total").Filters {
		if f.Field == "region" && f.Op == "in" {
			t.Errorf("total received the cross-filter: %+v", f)
		}
	}
	if r := byID("por_region"); r.Limit != 10 || len(r.Order) != 1 || !r.Order[0].Desc {
		t.Errorf("sort/limit: %+v", r)
	}
	// Open-ended range, empty values and bad input.
	f, _ := d.FilterByID("periodo")
	if c, err := f.Conditions([]string{"", "2026-02-01"}); err != nil || len(c) != 1 || c[0].Op != "<=" {
		t.Errorf("open range: %v %v", c, err)
	}
	if c, err := f.Conditions(nil); err != nil || c != nil {
		t.Errorf("no range: %v %v", c, err)
	}
	if _, err := f.Conditions([]string{"x'; DROP", ""}); err == nil {
		t.Error("bad date accepted")
	}
	c, _ := d.Chart("serie")
	if _, err := d.Request(c, State{Cross: map[string][]string{"ghost": {"a"}}}); err == nil {
		t.Error("cross-filter from unknown chart accepted")
	}
	if _, err := d.Request(c, State{Cross: map[string][]string{"total": {"a"}}}); err == nil {
		t.Error("cross-filter from a chart that does not emit accepted")
	}
	// Values list: filter measure or the first measure of an applicable chart.
	sel, _ := d.FilterByID("region")
	if r, err := d.ValuesRequest(sel); err != nil || r.Measures[0] != "ingresos" || r.Dims[0] != "region" {
		t.Errorf("values: %+v %v", r, err)
	}
}

func TestCheck(t *testing.T) {
	d := mustParse(t, sample1)
	var n int
	if err := d.Check(func(contract.SemanticRequest) error { n++; return nil }); err != nil || n == 0 {
		t.Fatalf("%v (%d requests)", err, n)
	}
	err := d.Check(func(r contract.SemanticRequest) error {
		for _, f := range r.Filters {
			if f.Field == "fecha" {
				return os.ErrInvalid
			}
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), `chart "serie" with filter "periodo"`) {
		t.Fatalf("got %v", err)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	w := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if ok, bad, err := Load(filepath.Join(dir, "nope")); err != nil || ok != nil || bad != nil {
		t.Fatalf("missing dir: %v %v %v", ok, bad, err)
	}
	w("b.yaml", sample1)
	w("a.yml", sample1)
	w("broken.yaml", "title: x\n")
	w("Bad Name.yaml", sample1)
	w("notes.txt", "x")
	ok, bad, err := Load(dir)
	if err != nil || len(ok) != 2 || ok[0].Name != "a" || ok[1].Name != "b" {
		t.Fatalf("%v %v", ok, err)
	}
	if bad["broken"] == nil || bad["Bad Name"] == nil || len(bad) != 2 {
		t.Errorf("bad: %v", bad)
	}
	if d, err := LoadOne(dir, "b"); err != nil || d.Title != "Ventas" {
		t.Errorf("LoadOne: %v %v", d, err)
	}
	if _, err := LoadOne(dir, "../etc/passwd"); err == nil {
		t.Error("path traversal")
	}
}

// specs/dashboard.schema.json accepts what Parse accepts and lists the same keys.
func TestSchema(t *testing.T) {
	raw, err := os.ReadFile("../../specs/dashboard.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("d.json", doc); err != nil {
		t.Fatal(err)
	}
	sc, err := c.Compile("d.json")
	if err != nil {
		t.Fatal(err)
	}
	validate := func(y string) error {
		var g any
		if err := yaml.Unmarshal([]byte(y), &g); err != nil {
			t.Fatal(err)
		}
		js, _ := json.Marshal(g)
		inst, _ := jsonschema.UnmarshalJSON(bytes.NewReader(js))
		return sc.Validate(inst)
	}
	if err := validate(sample1); err != nil {
		t.Errorf("sample rejected by the schema: %v", err)
	}
	for _, bad := range []string{
		"title: x\ncharts: [{id: a, type: kpi, measures: [m], options: {formatter: f}}]",
		"title: x\ncharts: [{id: a, type: radar, measures: [m]}]",
		"charts: []",
	} {
		if validate(bad) == nil {
			t.Errorf("schema accepted %q", bad)
		}
	}
	var s struct {
		Properties map[string]struct {
			Properties map[string]any `json:"properties"`
			Items      struct {
				Properties map[string]struct {
					Properties map[string]any `json:"properties"`
					Items      struct {
						Properties map[string]any `json:"properties"`
					} `json:"items"`
				} `json:"properties"`
			} `json:"items"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	keys := func(m map[string]any) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	tags := func(v any) []string {
		var out []string
		typ := reflect.TypeOf(v)
		for i := range typ.NumField() {
			if tag := typ.Field(i).Tag.Get("yaml"); tag != "" && tag != "-" {
				out = append(out, tag)
			}
		}
		sort.Strings(out)
		return out
	}
	top := map[string]any{}
	for k := range s.Properties {
		top[k] = nil
	}
	ch := s.Properties["charts"].Items.Properties
	for name, c := range map[string]struct {
		schema, goKeys []string
	}{
		"dashboard": {keys(top), tags(Dashboard{})},
		"filter":    {keys(toAny(s.Properties["filters"].Items.Properties)), tags(Filter{})},
		"chart":     {keys(toAny(ch)), tags(Chart{})},
		"layout":    {keys(ch["layout"].Properties), tags(Layout{})},
		"options":   {keys(ch["options"].Properties), tags(Options{})},
		"sort":      {keys(ch["sort"].Items.Properties), tags(Sort{})},
		"where":     {keys(ch["where"].Items.Properties), tags(Condition{})},
	} {
		if !reflect.DeepEqual(c.schema, c.goKeys) {
			t.Errorf("%s: schema keys %v != struct keys %v", name, c.schema, c.goKeys)
		}
	}
}

func toAny[V any](m map[string]V) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
