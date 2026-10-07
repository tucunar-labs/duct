package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// fakeSem is a QueryHandler that also answers semantic queries: dimensions
// get canned values (region: CL, AR), measures a number per row; an equality
// filter on region keeps only that region.
type fakeSem struct {
	fakeQ
	smu      sync.Mutex
	snapshot string
	deny     bool
	reqs     []contract.SemanticRequest
	who      []contract.Identity
	audits   []contract.AuditRecord
}

// Audit makes fakeSem a contract.Auditor, to see what the cache reports.
func (f *fakeSem) Audit(_ context.Context, rec contract.AuditRecord) {
	f.smu.Lock()
	f.audits = append(f.audits, rec)
	f.smu.Unlock()
}

func (f *fakeSem) LakeSnapshot(context.Context) (string, error) {
	f.smu.Lock()
	defer f.smu.Unlock()
	return f.snapshot, nil
}

func (f *fakeSem) setSnapshot(s string) {
	f.smu.Lock()
	f.snapshot = s
	f.smu.Unlock()
}

func (f *fakeSem) calls() []contract.SemanticRequest {
	f.smu.Lock()
	defer f.smu.Unlock()
	return append([]contract.SemanticRequest(nil), f.reqs...)
}

func (f *fakeSem) SemanticQuery(_ context.Context, id contract.Identity, req contract.SemanticRequest) (*contract.SemanticResult, error) {
	f.smu.Lock()
	f.reqs = append(f.reqs, req)
	f.who = append(f.who, id)
	deny := f.deny
	f.smu.Unlock()
	if deny {
		return nil, errors.New(`dataset not available to the role: "ventas" (gold.ventas) is not granted to role "` + id.Role + `"`)
	}
	values := map[string][]string{"region": {"CL", "AR"}, "fecha": {"2026-01-01", "2026-01-02"}}
	mem := memory.NewGoAllocator()
	n := 1
	if len(req.Dims) > 0 {
		n = 2
	}
	keep := map[string]bool{}
	for _, fl := range req.Filters {
		if fl.Field == "region" && fl.Op == "=" {
			keep[fl.Value.(string)] = true
		}
	}
	var fields []arrow.Field
	var cols []arrow.Array
	var out []contract.SemanticColumn
	for _, d := range req.Dims {
		b := array.NewStringBuilder(mem)
		vs := values[d]
		if vs == nil {
			vs = []string{"a", "b"}
		}
		for _, v := range vs[:n] {
			b.Append(v)
		}
		fields = append(fields, arrow.Field{Name: d, Type: arrow.BinaryTypes.String})
		cols = append(cols, b.NewArray())
		out = append(out, contract.SemanticColumn{Name: d, Kind: "dimension"})
	}
	for _, m := range req.Measures {
		b := array.NewFloat64Builder(mem)
		for i := 0; i < n; i++ {
			b.Append(float64(10 * (i + 1)))
		}
		fields = append(fields, arrow.Field{Name: m, Type: arrow.PrimitiveTypes.Float64})
		cols = append(cols, b.NewArray())
		out = append(out, contract.SemanticColumn{Name: m, Kind: "measure"})
	}
	sc := arrow.NewSchema(fields, nil)
	rec := array.NewRecordBatch(sc, cols, int64(n))
	if len(keep) > 0 && len(req.Dims) > 0 && req.Dims[0] == "region" {
		rec = array.NewRecordBatch(sc, cols, int64(n)) // same batch; filter below by slicing
		var idx []int
		for i, v := range values["region"][:n] {
			if keep[v] {
				idx = append(idx, i)
			}
		}
		if len(idx) == 1 {
			rec = rec.NewSlice(int64(idx[0]), int64(idx[0]+1))
		} else if len(idx) == 0 {
			rec = rec.NewSlice(0, 0)
		}
	}
	rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
	if err != nil {
		return nil, err
	}
	return &contract.SemanticResult{Stream: rdr, Columns: out, SQL: "-- fake"}, nil
}

const dashYAML = `
title: Ventas <b>x</b>
description: Resumen
filters:
  - {id: region, label: Región, type: select, dimension: region}
  - {id: periodo, type: daterange, dimension: fecha, charts: [serie]}
charts:
  - {id: por_region, title: Por región, type: bar, dims: [region], measures: [ingresos], sort: [{by: ingresos, desc: true}], layout: {x: 0, y: 0, w: 6, h: 4}}
  - {id: serie, type: line, dims: [fecha], measures: [ingresos]}
  - {id: total, type: kpi, measures: [ingresos], cross_filter: false}
`

type dashEnv struct {
	*env
	sem *fakeSem
	dir string
}

func newDashEnv(t *testing.T, files map[string]string, plain bool) *dashEnv {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sem := &fakeSem{snapshot: "7"}
	var q contract.QueryHandler = sem
	if plain {
		q = &sem.fakeQ
	}
	cfg := appConfig{rowLimit: 1000, queryTimeout: 5 * time.Second, sessionTTL: time.Hour, dashboardsDir: dir}
	srv := httptest.NewServer(newApp(q, &fakeAuth{}, cfg).routes())
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	e := &env{t: t, srv: srv, q: &sem.fakeQ, c: c}
	e.login()
	return &dashEnv{env: e, sem: sem, dir: dir}
}

// data POSTs the signals like Datastar does and returns the SSE body.
func (e *dashEnv) data(name string, signals map[string]any, token string) (*http.Response, string) {
	e.t.Helper()
	raw, _ := json.Marshal(signals)
	req, _ := http.NewRequest("POST", e.srv.URL+"/dashboards/"+name+"/data", strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Datastar-Request", "true")
	if token != "" {
		req.Header.Set("X-CSRF-Token", token)
	}
	return e.do(req)
}

var specRe = regexp.MustCompile(`(?s)<duct-chart id="chart-(\w+)"[^>]*?data-spec="([^"]*)"`)

// specs extracts the chart specs the SSE stream patched.
func specs(t *testing.T, body string) map[string]chartSpec {
	t.Helper()
	var lines []string
	for _, l := range strings.Split(body, "\n") {
		l = strings.TrimPrefix(l, "data: elements ")
		l = strings.TrimPrefix(l, "data: ")
		lines = append(lines, l)
	}
	out := map[string]chartSpec{}
	for _, m := range specRe.FindAllStringSubmatch(strings.Join(lines, "\n"), -1) {
		var s chartSpec
		if err := json.Unmarshal([]byte(html.UnescapeString(m[2])), &s); err != nil {
			t.Fatalf("spec of %s: %v (%s)", m[1], err, m[2])
		}
		out[m[1]] = s
	}
	return out
}

func hasFilter(r contract.SemanticRequest, field, op string) bool {
	for _, f := range r.Filters {
		if f.Field == field && f.Op == op {
			return true
		}
	}
	return false
}

func byDims(reqs []contract.SemanticRequest, dim string) []contract.SemanticRequest {
	var out []contract.SemanticRequest
	for _, r := range reqs {
		if len(r.Dims) == 1 && r.Dims[0] == dim {
			out = append(out, r)
		}
	}
	return out
}

func TestDashboardList(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML, "roto.yaml": "title: x\n"}, false)
	resp, body := e.get("/dashboards")
	if resp.StatusCode != 200 || !strings.Contains(body, `href="/dashboards/ventas"`) || !strings.Contains(body, "Ventas &lt;b&gt;x&lt;/b&gt;") {
		t.Fatalf("list: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "at least one chart") { // a broken file is shown, not served
		t.Errorf("broken dashboard not reported: %s", body)
	}
	if !strings.Contains(body, `href="/dashboards"`) {
		t.Error("no nav link")
	}
	if _, body := e.get("/catalog"); !strings.Contains(body, `href="/dashboards/ventas"`) {
		t.Errorf("catalog has no link to the dashboard: %s", body)
	}
	if resp, _ := e.get("/dashboards/roto"); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("broken dashboard served: %d", resp.StatusCode)
	}
	for _, p := range []string{"/dashboards/nope", "/dashboards/..%2Fx", "/dashboards/Ventas"} {
		if resp, _ := e.get(p); resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusUnprocessableEntity {
			t.Errorf("%s: %d", p, resp.StatusCode)
		}
	}
	// Needs a session.
	anon := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, _ := anon.Get(e.srv.URL + "/dashboards/ventas")
	if r.StatusCode != http.StatusSeeOther {
		t.Errorf("anonymous: %d", r.StatusCode)
	}
}

func TestDashboardRender(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML}, false)
	resp, body := e.get("/dashboards/ventas")
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	for _, want := range []string{
		"Ventas &lt;b&gt;x&lt;/b&gt;",                // escaped
		`<option value="CL">`, `<option value="AR">`, // picker values came from a semantic query
		`data-bind="f_region"`, `data-bind="f_periodo_from"`,
		`id="chart-por_region"`, `id="chart-serie"`, `id="chart-total"`,
		"/dashboards/ventas/data", "charts.js", "datastar.js", "--w:6;--h:4",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	if strings.Contains(body, "<b>x</b>") {
		t.Error("title not escaped")
	}
	if got := resp.Header.Get("Content-Security-Policy"); strings.Contains(got, "script-src 'self' 'unsafe-eval';") == false || strings.Contains(strings.Split(got, "style-src")[0], "unsafe-inline") {
		t.Errorf("CSP: %s", got)
	}
	// The picker query: the dimension, grouped by the first measure of an applicable chart, as ana's role.
	reqs := e.sem.calls()
	if len(reqs) != 1 || reqs[0].Dims[0] != "region" || reqs[0].Measures[0] != "ingresos" || e.sem.who[0].Role != "analyst" {
		t.Errorf("value queries: %+v %+v", reqs, e.sem.who)
	}
	// An inline <script> would break the CSP: only external scripts (module or
	// the classic shell.js, which must run before first paint).
	if n := strings.Count(body, "<script"); n != strings.Count(body, `<script src=`)+strings.Count(body, `<script type="module" src=`) {
		t.Errorf("inline script in the page (%d)", n)
	}
}

func TestDashboardDataAndFilters(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML}, false)
	tok := e.csrfToken()
	e.q.calls = nil // the catalog query of csrfToken is not under test
	base := map[string]any{"f_region": "", "f_periodo_from": "", "f_periodo_to": "", "cross": "{}", "ev": "", "_csrf": "x"}

	// No CSRF token: refused before any query.
	if resp, _ := e.data("ventas", base, ""); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("no csrf: %d", resp.StatusCode)
	}
	if len(e.sem.calls()) != 0 {
		t.Fatal("queried without CSRF token")
	}

	resp, body := e.data("ventas", base, tok)
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	sp := specs(t, body)
	if len(sp) != 3 {
		t.Fatalf("specs: %v", sp)
	}
	bar := sp["por_region"]
	if bar.Type != "bar" || !bar.Emit || bar.Error != "" || string(bar.Rows) != `[["CL",10],["AR",20]]` || len(bar.Columns) != 2 || bar.Columns[1].Kind != "measure" {
		t.Errorf("bar: %+v rows=%s", bar, bar.Rows)
	}
	if sp["total"].Emit || string(sp["total"].Rows) != `[[10]]` {
		t.Errorf("kpi: %+v", sp["total"])
	}
	// Every query ran as ana's role, through the semantic path (never raw SQL).
	for _, w := range e.sem.who {
		if w.User != "ana" || w.Role != "analyst" {
			t.Errorf("identity %+v", w)
		}
	}
	if len(e.q.calls) != 0 {
		t.Errorf("raw SQL issued: %v", e.q.calls)
	}

	// Filters become structured semantic filters; the date range only reaches its chart.
	before := len(e.sem.calls())
	f := map[string]any{"f_region": "CL", "f_periodo_from": "2026-01-01", "f_periodo_to": "2026-01-31", "cross": "{}", "ev": ""}
	_, body = e.data("ventas", f, tok)
	got := e.sem.calls()[before:]
	for _, r := range got {
		if !hasFilter(r, "region", "=") {
			t.Errorf("region filter missing: %+v", r)
		}
	}
	if serie := byDims(got, "fecha"); len(serie) != 1 || !hasFilter(serie[0], "fecha", "between") {
		t.Errorf("serie: %+v", serie)
	}
	if bar := byDims(got, "region"); len(bar) != 1 || hasFilter(bar[0], "fecha", "between") {
		t.Errorf("bar got the date range: %+v", bar)
	}
	if !strings.Contains(html.UnescapeString(body), `["CL",10]]`) {
		t.Errorf("filtered rows not returned: %s", body)
	}

	// Garbage never reaches SQL: a bad date is a chart error, not a query.
	before = len(e.sem.calls())
	f["f_periodo_from"] = "2026-01-01'; DROP TABLE x;--"
	_, body = e.data("ventas", f, tok)
	if s := specs(t, body)["serie"]; !strings.Contains(s.Error, "YYYY-MM-DD") {
		t.Errorf("bad date: %+v", s)
	}
	for _, r := range e.sem.calls()[before:] {
		if len(r.Dims) == 1 && r.Dims[0] == "fecha" {
			t.Errorf("serie ran with a bad date: %+v", r)
		}
	}
}

func TestDashboardCrossFilter(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML}, false)
	tok := e.csrfToken()
	n := 0
	fresh := func() { n++; e.sem.setSnapshot(fmt.Sprint("s", n)) } // a new snapshot: no cache hits hide the queries
	sig := func(cross, ev string) map[string]any {
		return map[string]any{"f_region": "", "f_periodo_from": "", "f_periodo_to": "", "cross": cross, "ev": ev}
	}

	// Click on a bar: the other charts get region = CL; the source keeps all bars.
	fresh()
	before := len(e.sem.calls())
	_, body := e.data("ventas", sig("{}", `{"chart":"por_region","values":["CL"]}`), tok)
	got := e.sem.calls()[before:]
	if serie := byDims(got, "fecha"); len(serie) != 1 || !hasFilter(serie[0], "region", "=") {
		t.Errorf("serie did not receive the cross-filter: %+v", serie)
	}
	if bar := byDims(got, "region"); len(bar) != 1 || len(bar[0].Filters) != 0 {
		t.Errorf("source chart filtered by itself: %+v", bar)
	}
	for _, r := range got {
		if len(r.Dims) == 0 && len(r.Filters) != 0 { // total has cross_filter: false
			t.Errorf("kpi opted out but got %+v", r.Filters)
		}
	}
	sp := specs(t, body)
	if got := sp["por_region"].Selected; len(got) != 1 || got[0] != "CL" {
		t.Errorf("selected: %v", got)
	}
	if !strings.Contains(body, `datastar-patch-signals`) || !strings.Contains(html.UnescapeString(body), `"cross":"{\"por_region\":[\"CL\"]}"`) {
		t.Errorf("cross state not sent back: %s", body)
	}
	if !strings.Contains(body, `id="dash-chips"`) || !strings.Contains(body, "CL") {
		t.Errorf("no chip: %s", body)
	}

	// Brush: several values become an IN list.
	fresh()
	before = len(e.sem.calls())
	e.data("ventas", sig("{}", `{"chart":"por_region","values":["CL","AR"]}`), tok)
	if serie := byDims(e.sem.calls()[before:], "fecha"); len(serie) != 1 || !hasFilter(serie[0], "region", "in") {
		t.Errorf("brush: %+v", serie)
	}

	// Clicking the same selection again clears it; so does an empty one.
	for _, ev := range []string{`{"chart":"por_region","values":["CL"]}`, `{"chart":"por_region","values":[]}`} {
		fresh()
		before = len(e.sem.calls())
		_, body = e.data("ventas", sig(`{"por_region":["CL"]}`, ev), tok)
		if serie := byDims(e.sem.calls()[before:], "fecha"); len(serie) != 1 || hasFilter(serie[0], "region", "=") {
			t.Errorf("selection not cleared by %s: %+v", ev, serie)
		}
		if !strings.Contains(body, `\"cross\":\"{}\"`) && !strings.Contains(html.UnescapeString(body), `"cross":"{}"`) {
			t.Errorf("cross state not reset: %s", body)
		}
	}

	// Forged events (unknown chart, a chart that does not emit) are ignored.
	for _, ev := range []string{`{"chart":"ghost","values":["x"]}`, `{"chart":"total","values":["x"]}`, `not json`} {
		fresh()
		before = len(e.sem.calls())
		e.data("ventas", sig("{}", ev), tok)
		if len(e.sem.calls()) == before {
			t.Errorf("event %s: nothing was queried", ev)
		}
		for _, r := range e.sem.calls()[before:] {
			if len(r.Filters) != 0 {
				t.Errorf("event %s produced filters: %+v", ev, r.Filters)
			}
		}
	}
}

func TestDashboardCacheBySnapshot(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML}, false)
	tok := e.csrfToken()
	sig := map[string]any{"f_region": "", "f_periodo_from": "", "f_periodo_to": "", "cross": "{}", "ev": ""}
	e.data("ventas", sig, tok)
	n1 := len(e.sem.calls())
	e.data("ventas", sig, tok)
	if n2 := len(e.sem.calls()); n2 != n1 {
		t.Fatalf("same snapshot, same request: %d new queries (want cache hits)", n2-n1)
	}
	// Every hit is reported to the audit log as a cached, successful statement.
	e.sem.smu.Lock()
	hits := append([]contract.AuditRecord(nil), e.sem.audits...)
	e.sem.smu.Unlock()
	if len(hits) != 3 {
		t.Fatalf("audit records for cache hits: %d, want 3 (one per chart)", len(hits))
	}
	for _, h := range hits {
		if !h.Cached || h.Err != "" || h.User == "" || h.Role == "" || !strings.HasPrefix(h.SQL, "-- semantic query (cache hit)") || h.Duration < 0 {
			t.Errorf("bad cache-hit audit record: %+v", h)
		}
	}
	// A different request is a different key.
	sig2 := map[string]any{"f_region": "CL", "f_periodo_from": "", "f_periodo_to": "", "cross": "{}", "ev": ""}
	e.data("ventas", sig2, tok)
	if n3 := len(e.sem.calls()); n3 == n1 {
		t.Fatal("filtered request served from the unfiltered entry")
	}
	// New DuckLake snapshot: everything is fetched again.
	e.sem.setSnapshot("8")
	n3 := len(e.sem.calls())
	e.data("ventas", sig, tok)
	if n4 := len(e.sem.calls()); n4-n3 != 3 {
		t.Fatalf("after a new snapshot: %d queries, want 3 (one per chart)", n4-n3)
	}
	// Snapshot unknown: no caching at all.
	e.sem.setSnapshot("")
	n4 := len(e.sem.calls())
	e.data("ventas", sig, tok)
	e.data("ventas", sig, tok)
	if n5 := len(e.sem.calls()); n5-n4 != 6 {
		t.Fatalf("without a snapshot: %d queries, want 6", n5-n4)
	}
}

func TestCacheKeyIsPerRoleAndSnapshot(t *testing.T) {
	r := contract.SemanticRequest{Dims: []string{"region"}, Measures: []string{"m"}}
	k := cacheKey("1", "analyst", r)
	if k == cacheKey("2", "analyst", r) || k == cacheKey("1", "boss", r) {
		t.Fatal("key ignores snapshot or role")
	}
	r2 := r
	r2.Filters = []contract.SemanticFilter{{Field: "region", Op: "=", Value: "CL"}}
	if k == cacheKey("1", "analyst", r2) || k != cacheKey("1", "analyst", r) {
		t.Fatal("key does not follow the request")
	}
}

func TestDashboardRoleWithoutAccess(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML}, false)
	tok := e.csrfToken()
	e.sem.deny = true
	sig := map[string]any{"f_region": "", "f_periodo_from": "", "f_periodo_to": "", "cross": "{}", "ev": ""}
	resp, body := e.data("ventas", sig, tok)
	if resp.StatusCode != 200 {
		t.Fatalf("%d", resp.StatusCode)
	}
	sp := specs(t, body)
	for id, s := range sp {
		if !strings.Contains(s.Error, "not granted to role") || s.Rows != nil {
			t.Errorf("%s: want an error panel, got %+v", id, s)
		}
	}
	// Errors are not cached: once access is restored the data appears.
	e.sem.deny = false
	_, body = e.data("ventas", sig, tok)
	if sp := specs(t, body); sp["por_region"].Error != "" || sp["por_region"].Rows == nil {
		t.Errorf("error stuck in the cache: %+v", sp["por_region"])
	}
	// The page itself still renders, with the picker error shown.
	e.sem.deny = true
	if resp, body := e.get("/dashboards/ventas"); resp.StatusCode != 200 || !strings.Contains(body, "not granted to role") {
		t.Errorf("page without access: %d", resp.StatusCode)
	}
}

func TestDashboardWithoutSemanticLayer(t *testing.T) {
	e := newDashEnv(t, map[string]string{"ventas.yaml": dashYAML}, true)
	tok := e.csrfToken()
	_, body := e.data("ventas", map[string]any{"cross": "{}", "ev": ""}, tok)
	sp := specs(t, body)
	if len(sp) != 3 || !strings.Contains(sp["por_region"].Error, "capa semántica") {
		t.Errorf("%+v", sp)
	}
	if resp, _ := e.get("/dashboards/ventas"); resp.StatusCode != 200 {
		t.Errorf("page: %d", resp.StatusCode)
	}
}

func TestDashboardsDisabled(t *testing.T) {
	e := newDashEnv(t, nil, false)
	if resp, body := e.get("/dashboards"); resp.StatusCode != 200 || !strings.Contains(body, "No hay dashboards") {
		t.Errorf("%d %s", resp.StatusCode, body)
	}
	if resp, _ := e.get("/dashboards/x"); resp.StatusCode != 404 {
		t.Errorf("%d", resp.StatusCode)
	}
}

func TestLRU(t *testing.T) {
	c := newLRU(2, 1000)
	mk := func(n int) *chartData { return &chartData{Rows: []byte(strings.Repeat("x", n))} }
	c.put("a", mk(10))
	c.put("b", mk(10))
	c.get("a") // a is now the most recent
	c.put("c", mk(10))
	if _, ok := c.get("b"); ok {
		t.Error("b should have been evicted")
	}
	if _, ok := c.get("a"); !ok {
		t.Error("a evicted")
	}
	c.put("huge", mk(5000)) // larger than the whole cache: ignored
	if _, ok := c.get("huge"); ok || c.len() != 2 {
		t.Errorf("huge entry stored (len %d)", c.len())
	}
	small := newLRU(100, 400)
	for i := range 20 {
		small.put(fmt.Sprint(i), mk(100))
	}
	if small.len() > 2 {
		t.Errorf("byte bound not enforced: %d entries", small.len())
	}
}
