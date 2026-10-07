package main

import (
	"context"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/project"
)

const shopDashboard = `title: Tienda
description: Ventas por región y estado
filters:
  - {id: region, label: Región, type: select, dimension: region}
charts:
  - {id: por_region, title: Ingresos por región, type: bar, dims: [region], measures: [revenue], sort: [{by: revenue, desc: true}], layout: {x: 0, y: 0, w: 7, h: 4}}
  - {id: por_estado, title: Ingresos por estado, type: pie, dims: [status], measures: [revenue], layout: {x: 7, y: 0, w: 5, h: 4}}
  - {id: total, title: Ingresos, type: kpi, measures: [revenue], layout: {w: 3, h: 2}, options: {format: integer, prefix: "$"}}
  - {id: detalle, title: Detalle, type: table, dims: [region, status], measures: [revenue, units], layout: {w: 9, h: 2}}
`

func TestDashboardLint(t *testing.T) {
	root := semanticProject(t)
	writeFile(t, filepath.Join(root, "dashboards", "tienda.yaml"), shopDashboard)
	out := mustDuct(t, "dashboard", "lint", "--project", root)
	if !strings.Contains(out, "1 dashboards checked, 0 with errors") {
		t.Errorf("lint: %q", out)
	}
	if out := mustDuct(t, "dashboard", "list", "--project", root); !strings.Contains(out, "tienda\tTienda\t4 charts") {
		t.Errorf("list: %q", out)
	}
	// Names that the model does not know, a filter that cannot reach a fact, bad format.
	writeFile(t, filepath.Join(root, "dashboards", "malo.yaml"), `title: Malo
filters:
  - {id: qty, type: select, dimension: order_items.qty, measure: revenue}
charts:
  - {id: a, type: bar, dims: [region], measures: [nope]}
  - {id: b, type: kpi, measures: [revenue]}
`)
	writeFile(t, filepath.Join(root, "dashboards", "feo.yaml"), "title: Feo\ncharts: [{id: a, type: radar, measures: [revenue]}]\n")
	out, errs, code := duct(t, "dashboard", "lint", "--project", root)
	if code != 1 {
		t.Fatalf("code %d\n%s%s", code, out, errs)
	}
	for _, want := range []string{`unknown measure "nope"`, `chart "b" with filter "qty"`, `type "radar"`, "3 dashboards checked, 2 with errors"} {
		if !strings.Contains(out, want) {
			t.Errorf("lint output lacks %q:\n%s", want, out)
		}
	}
}

// dashClient is a browser without JavaScript: cookies, the page, the SSE endpoint.
type dashClient struct {
	t    *testing.T
	base string
	c    *http.Client
	csrf string
}

func loginWeb(t *testing.T, base, user, pass string) *dashClient {
	t.Helper()
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	d := &dashClient{t: t, base: base, c: c}
	_, _ = d.do("GET", "/login", "", nil)
	u, _ := url.Parse(base)
	var csrf string
	for _, ck := range jar.Cookies(u) {
		if ck.Name == "duct_csrf" {
			csrf = ck.Value
		}
	}
	form := url.Values{"csrf": {csrf}, "user": {user}, "password": {pass}, "next": {"/dashboards"}}
	resp, _ := d.do("POST", "/login", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login %s: %d", user, resp.StatusCode)
	}
	return d
}

func (d *dashClient) do(method, path, body string, h map[string]string) (*http.Response, string) {
	d.t.Helper()
	req, _ := http.NewRequest(method, d.base+path, strings.NewReader(body))
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := d.c.Do(req)
	if err != nil {
		d.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

var csrfRe = regexp.MustCompile(`_csrf&#34;:&#34;([^&]+)&#34;`)

func (d *dashClient) page(name string) string {
	d.t.Helper()
	resp, body := d.do("GET", "/dashboards/"+name, "", nil)
	if resp.StatusCode != 200 {
		d.t.Fatalf("GET %s: %d %s", name, resp.StatusCode, body)
	}
	if m := csrfRe.FindStringSubmatch(body); m != nil {
		d.csrf = m[1]
	}
	return body
}

type e2eSpec struct {
	Type    string          `json:"type"`
	Rows    json.RawMessage `json:"rows"`
	Error   string          `json:"error"`
	Columns []struct{ Name, Kind string }
}

var chartRe = regexp.MustCompile(`(?s)<duct-chart id="chart-(\w+)"[^>]*?data-spec="([^"]*)"`)

// data is what the browser does on load, on a filter change and on a click.
func (d *dashClient) data(name string, signals map[string]any) map[string]e2eSpec {
	d.t.Helper()
	raw, _ := json.Marshal(signals)
	resp, body := d.do("POST", "/dashboards/"+name+"/data", string(raw), map[string]string{
		"Content-Type": "application/json", "Datastar-Request": "true", "X-CSRF-Token": d.csrf})
	if resp.StatusCode != 200 {
		d.t.Fatalf("data: %d %s", resp.StatusCode, body)
	}
	var lines []string
	for _, l := range strings.Split(body, "\n") {
		l = strings.TrimPrefix(strings.TrimPrefix(l, "data: elements "), "data: ")
		lines = append(lines, l)
	}
	out := map[string]e2eSpec{}
	for _, m := range chartRe.FindAllStringSubmatch(strings.Join(lines, "\n"), -1) {
		var s e2eSpec
		if err := json.Unmarshal([]byte(html.UnescapeString(m[2])), &s); err != nil {
			d.t.Fatalf("spec %s: %v", m[1], err)
		}
		out[m[1]] = s
	}
	return out
}

func startWeb(t *testing.T, root string) string {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{Protocols: []string{"web"}, Listen: map[string]string{"web": "127.0.0.1:0"},
			Ready: func(a map[string]string) { ready <- a }})
	}()
	var addr string
	select {
	case a := <-ready:
		addr = a["web"]
	case err := <-done:
		cancel()
		t.Fatalf("serve exited: %v", err)
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("serve did not start")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve returned %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("serve did not shut down")
		}
	})
	return "http://" + addr
}

// TestE2EDashboards: `duct serve` with a dashboard in a temp project; each
// role sees its own numbers (row filters), the cross-filter narrows the other
// charts, and a role without the views gets error panels.
func TestE2EDashboards(t *testing.T) {
	root := semanticProject(t)
	writeFile(t, filepath.Join(root, "dashboards", "tienda.yaml"), shopDashboard)
	base := startWeb(t, root)

	sig := func(region, cross, ev string) map[string]any {
		return map[string]any{"f_region": region, "cross": cross, "ev": ev}
	}
	rows := func(s e2eSpec) string { return string(s.Rows) }

	// jefa sees everything: revenue CL 450 (150 paid + 300 void), AR 200.
	jefa := loginWeb(t, base, "jefa", "pw-jefa-1234")
	if body := jefa.page("tienda"); !strings.Contains(body, `<option value="CL">`) || !strings.Contains(body, `<option value="AR">`) {
		t.Errorf("picker values missing: %s", body)
	}
	got := jefa.data("tienda", sig("", "{}", ""))
	if rows(got["por_region"]) != `[["CL",450],["AR",200]]` {
		t.Errorf("jefa por_region: %s (%s)", rows(got["por_region"]), got["por_region"].Error)
	}
	if rows(got["total"]) != `[[650]]` {
		t.Errorf("jefa total: %s", rows(got["total"]))
	}
	if r := rows(got["detalle"]); !strings.Contains(r, `["CL","paid",150,8]`) || !strings.Contains(r, `["CL","void",300,7]`) {
		t.Errorf("jefa detalle: %s", r)
	}
	// Picker: region = CL narrows every chart.
	got = jefa.data("tienda", sig("CL", "{}", ""))
	if rows(got["por_region"]) != `[["CL",450]]` || rows(got["total"]) != `[[450]]` {
		t.Errorf("filtered: %s %s", rows(got["por_region"]), rows(got["total"]))
	}
	// Cross-filter: click on the AR bar; the pie and kpi follow, the bars stay.
	got = jefa.data("tienda", sig("", "{}", `{"chart":"por_region","values":["AR"]}`))
	if rows(got["por_region"]) != `[["CL",450],["AR",200]]` {
		t.Errorf("source chart changed: %s", rows(got["por_region"]))
	}
	if rows(got["por_estado"]) != `[["paid",200]]` || rows(got["total"]) != `[[200]]` {
		t.Errorf("cross-filter: pie %s kpi %s", rows(got["por_estado"]), rows(got["total"]))
	}

	// ana: row filter status = 'paid' applies under the same dashboard.
	ana := loginWeb(t, base, "ana", "pw-ana-12345")
	ana.page("tienda")
	got = ana.data("tienda", sig("", "{}", ""))
	if rows(got["por_region"]) != `[["AR",200],["CL",150]]` || rows(got["total"]) != `[[350]]` {
		t.Errorf("ana: %s %s (%s)", rows(got["por_region"]), rows(got["total"]), got["total"].Error)
	}
	if r := rows(got["por_estado"]); strings.Contains(r, "void") {
		t.Errorf("ana sees voided orders: %s", r)
	}

	// lim has only gold.orders: no customers, so charts by region are an error panel.
	lim := loginWeb(t, base, "lim", "pw-lim-12345")
	lim.page("tienda")
	got = lim.data("tienda", sig("", "{}", ""))
	if !strings.Contains(got["por_region"].Error, "not granted") || got["por_region"].Rows != nil {
		t.Errorf("lim por_region: %+v", got["por_region"])
	}
	if got["total"].Error != "" || rows(got["total"]) != `[[650]]` {
		t.Errorf("lim total: %+v", got["total"])
	}
}
