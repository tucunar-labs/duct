//go:build e2e

// Examples e2e: penguins and tpch run end to end offline (init, ingest, run, contracts,
// semantic layer, dashboards, job, serve); nyc-taxi needs a 50 MB download, so only its
// files are linted.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	builtBin  string
	buildErr  error
)

// ductBinary builds cmd/duct once per test process.
func ductBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "duct-e2e-bin-")
		if err != nil {
			buildErr = err
			return
		}
		builtBin = filepath.Join(dir, "duct")
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "build", "-tags", "duckdb_arrow", "-o", builtBin, "./cmd/duct") //nolint:gosec // test
		cmd.Dir = repoRoot(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = &buildError{err, string(out)}
		}
	})
	if buildErr != nil {
		t.Fatalf("go build: %v", buildErr)
	}
	return builtBin
}

type buildError struct {
	err error
	out string
}

func (e *buildError) Error() string { return e.err.Error() + "\n" + e.out }

func exampleCLI(t *testing.T, name string) (*cli, string) {
	t.Helper()
	bin := ductBinary(t)
	dir := filepath.Join(t.TempDir(), name)
	copyDir(t, filepath.Join(repoRoot(t), "examples", name), dir)
	return &cli{t: t, bin: bin, root: dir}, dir
}

func (c *cli) script(name string, env ...string) (string, error) {
	cmd := exec.Command("bash", name) //nolint:gosec // test
	cmd.Dir = c.root
	cmd.Env = append(append(os.Environ(), "DUCT="+c.bin), env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (c *cli) mustHave(out string, wants ...string) {
	c.t.Helper()
	for _, w := range wants {
		if !strings.Contains(out, w) {
			c.t.Errorf("output lacks %q:\n%s", w, out)
		}
	}
}

// governanceChecks is what every example must pass once its data is loaded.
func (c *cli) governanceChecks(wantContracts, wantDashboards int, job string) {
	t := c.t
	out := c.must("contract", "lint")
	c.mustHave(out, strings.Join([]string{itoa(wantContracts), "contracts checked"}, " "))
	out = c.must("contract", "test")
	if strings.Contains(out, "FAIL") || !strings.Contains(out, "PASS") {
		t.Errorf("contract test:\n%s", out)
	}
	if out := c.must("contract", "freshness"); !strings.HasPrefix(out, "FRESH ") {
		t.Errorf("contract freshness: %s", out)
	}
	c.mustHave(c.must("semantic", "validate"), "ok: 1 documents")
	c.mustHave(c.must("dashboard", "lint"), itoa(wantDashboards)+" dashboards checked, 0 with errors")
	c.mustHave(c.must("jobs", "list"), job)
}

func itoa(n int) string { return strconv.Itoa(n) }

// serveWeb starts `duct serve` (web + http) and returns the addresses.
func (c *cli) serveWeb() map[string]string {
	t := c.t
	ctx, cancel := context.WithCancel(context.Background())
	srv := exec.CommandContext(ctx, c.bin, "serve", "--project", c.root, //nolint:gosec // test
		"--listen", "http=127.0.0.1:0", "--listen", "web=127.0.0.1:0", "--protocols", "http,web")
	srv.Dir = c.root
	pipe, err := srv.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var srvErr bytes.Buffer
	srv.Stderr = &srvErr
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { _ = srv.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			_ = srv.Process.Kill()
		}
	})
	addrs := map[string]string{}
	var mu sync.Mutex
	ready := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(pipe)
		for sc.Scan() {
			f := strings.Fields(sc.Text())
			if len(f) == 4 && f[0] == "serving" {
				mu.Lock()
				addrs[f[1]] = f[3]
				if len(addrs) == 2 {
					close(ready)
				}
				mu.Unlock()
			}
		}
		_, _ = io.Copy(io.Discard, pipe)
	}()
	select {
	case <-ready:
	case <-time.After(90 * time.Second):
		t.Fatalf("serve did not start\nstderr: %s", srvErr.String())
	}
	mu.Lock()
	defer mu.Unlock()
	return map[string]string{"http": addrs["http"], "web": addrs["web"]}
}

// browser is a client without JavaScript: cookies, the dashboard page, its data endpoint.
type browser struct {
	t    *testing.T
	base string
	c    *http.Client
	csrf string
}

func login(t *testing.T, base, user, pass string) *browser {
	t.Helper()
	jar, _ := cookiejar.New(nil)
	b := &browser{t: t, base: base, c: &http.Client{Jar: jar, Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	resp, page := b.do("GET", "/login", "", nil)
	m := regexp.MustCompile(`name="csrf"[^>]*value="([^"]+)"`).FindStringSubmatch(page)
	if resp.StatusCode != 200 || m == nil {
		t.Fatalf("GET /login: %d, csrf found=%v", resp.StatusCode, m != nil)
	}
	form := url.Values{"csrf": {m[1]}, "user": {user}, "password": {pass}, "next": {"/dashboards"}}
	resp, _ = b.do("POST", "/login", form.Encode(), map[string]string{
		"Content-Type": "application/x-www-form-urlencoded", "Origin": base})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login %s: %d", user, resp.StatusCode)
	}
	return b
}

func (b *browser) do(method, path, body string, h map[string]string) (*http.Response, string) {
	b.t.Helper()
	req, _ := http.NewRequest(method, b.base+path, strings.NewReader(body))
	for k, v := range h {
		req.Header.Set(k, v)
	}
	resp, err := b.c.Do(req)
	if err != nil {
		b.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp, string(raw)
}

type chartData struct {
	Rows  [][]any `json:"rows"`
	Error string  `json:"error"`
}

var (
	pageCSRFRe = regexp.MustCompile(`_csrf&#34;:&#34;([^&]+)&#34;`)
	chartSpec  = regexp.MustCompile(`(?s)<duct-chart id="chart-(\w+)"[^>]*?data-spec="([^"]*)"`)
)

// dashboard loads the page and runs every chart the way the browser does.
func (b *browser) dashboard(name string) map[string]chartData {
	b.t.Helper()
	resp, page := b.do("GET", "/dashboards/"+name, "", nil)
	if resp.StatusCode != 200 {
		b.t.Fatalf("GET dashboards/%s: %d %s", name, resp.StatusCode, page)
	}
	if m := pageCSRFRe.FindStringSubmatch(page); m != nil {
		b.csrf = m[1]
	}
	resp, body := b.do("POST", "/dashboards/"+name+"/data", `{"cross":"{}","ev":""}`, map[string]string{
		"Content-Type": "application/json", "Datastar-Request": "true", "X-CSRF-Token": b.csrf})
	if resp.StatusCode != 200 {
		b.t.Fatalf("data %s: %d %s", name, resp.StatusCode, body)
	}
	var lines []string
	for _, l := range strings.Split(body, "\n") {
		lines = append(lines, strings.TrimPrefix(strings.TrimPrefix(l, "data: elements "), "data: "))
	}
	out := map[string]chartData{}
	for _, m := range chartSpec.FindAllStringSubmatch(strings.Join(lines, "\n"), -1) {
		var d chartData
		if err := json.Unmarshal([]byte(html.UnescapeString(m[2])), &d); err != nil {
			b.t.Fatalf("spec %s: %v", m[1], err)
		}
		out[m[1]] = d
	}
	return out
}

// requireCharts fails when a chart is missing, has an error panel or has no rows.
func requireCharts(t *testing.T, got map[string]chartData, ids ...string) {
	t.Helper()
	for _, id := range ids {
		d, ok := got[id]
		if !ok {
			t.Errorf("chart %q missing (got %d charts)", id, len(got))
			continue
		}
		if d.Error != "" || len(d.Rows) == 0 {
			t.Errorf("chart %q: error=%q rows=%d", id, d.Error, len(d.Rows))
		}
	}
}

// semanticOverHTTP checks the semantic layer end to end through duct serve:
// the dashboards (web) as ana, plus the raw SQL endpoint, which must deny base tables.
func (c *cli) serveAndCheck(dashboards map[string][]string, first map[string]string) {
	t := c.t
	addrs := c.serveWeb()
	b := login(t, "http://"+addrs["web"], "ana", demoPassword)
	for name, ids := range dashboards {
		got := b.dashboard(name)
		requireCharts(t, got, ids...)
		if want, ok := first[name]; ok {
			id, val, _ := strings.Cut(want, "=")
			rows, _ := json.Marshal(got[id].Rows)
			if !strings.Contains(string(rows), val) {
				t.Errorf("%s/%s rows %s lack %s", name, id, rows, val)
			}
		}
	}
	token := strings.TrimSpace(c.must("token", "create", "--user", "ana", "--ttl", "1h"))
	req, _ := http.NewRequest("POST", "http://"+addrs["http"]+"/query", strings.NewReader("SELECT * FROM silver.penguins"))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Errorf("silver table must be denied to analistas over http, got 200")
	}
}

func TestExamplePenguins(t *testing.T) {
	c, dir := exampleCLI(t, "penguins")
	c.must("init", ".")
	out := c.must("ingest")
	c.mustHave(out, "bronze.penguins", "344 rows")
	out = c.must("run")
	c.mustHave(out, "SUCCESS  silver.penguins", "342 rows (2 dropped)", "tiene_medidas [drop_row]: 2 violations",
		"SUCCESS  gold.penguin_stats", "SUCCESS  gold.penguins")
	if n := c.n("gold.penguin_stats"); n != 13 {
		t.Errorf("gold.penguin_stats = %v", n)
	}
	c.governanceChecks(1, 1, "refresh")
	c.mustHave(c.must("jobs", "run", "refresh"), "SUCCESS (manual)")

	if o, err := c.script("setup.sh"); err != nil {
		t.Fatalf("setup.sh: %v\n%s", err, o)
	}
	// Through the semantic layer, as the role: 151 Adelie, 68 Chinstrap, 123 Gentoo.
	out = c.must("semantic", "query", "--role", "analistas", "--dims", "species", "--measures", "penguin_count", "--format", "csv")
	c.mustHave(out, "Adelie,151", "Chinstrap,68", "Gentoo,123")
	c.serveAndCheck(map[string][]string{"especies": {"total", "masa", "por_isla", "adelie", "chinstrap", "gentoo", "masa_isla"}},
		map[string]string{"especies": "total=342"})
	_ = dir
}

func TestExampleTPCH(t *testing.T) {
	c, _ := exampleCLI(t, "tpch")
	c.must("init", ".")
	if o, err := c.script("generate.sh"); err != nil {
		if regexp.MustCompile(`(?i)download|connect|http|resolve|network|extension`).MatchString(o) {
			t.Skipf("SKIPPED: the DuckDB tpch extension could not be installed (offline?): %s", o)
		}
		t.Fatalf("generate.sh: %v\n%s", err, o)
	}
	out := c.must("ingest")
	c.mustHave(out, "bronze.lineitem", "600572 rows", "bronze.orders", "150000 rows")
	out = c.must("run")
	c.mustHave(out, "SUCCESS  silver.lineitem", "SUCCESS  gold.revenue_by_nation", "SUCCESS  gold.top_customers")
	if n := c.n("gold.top_customers"); n != 100 {
		t.Errorf("gold.top_customers = %v", n)
	}
	c.governanceChecks(1, 2, "refresh")
	c.mustHave(c.must("jobs", "run", "refresh"), "SUCCESS (manual)")

	if o, err := c.script("setup.sh"); err != nil {
		t.Fatalf("setup.sh: %v\n%s", err, o)
	}
	out = c.must("semantic", "query", "--role", "analistas", "--dims", "region", "--measures", "revenue,orders", "--format", "csv")
	for _, r := range []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"} {
		c.mustHave(out, r+",")
	}
	// The semantic layer and the table agree.
	want := c.rows("SELECT round(sum(revenue), 2) AS r FROM gold.revenue_by_nation")[0]["r"]
	got := c.rows("SELECT round(sum(revenue), 2) AS r FROM gold.revenue_by_nation WHERE region = 'ASIA'")[0]["r"]
	if want == got {
		t.Errorf("region filter changed nothing: %v", got)
	}
	c.serveAndCheck(map[string][]string{
		"ingresos":      {"total", "pedidos", "ticket", "por_anio", "por_mes", "por_nacion"},
		"participacion": {"region", "region_anio", "segmento", "nacion", "clientes"},
	}, nil)
}

// nyc-taxi: no download in CI; the project files must still be coherent.
func TestExampleNYCTaxiLint(t *testing.T) {
	c, dir := exampleCLI(t, "nyc-taxi")
	c.must("init", ".")
	c.mustHave(c.must("contract", "lint"), "1 contracts checked")
	c.mustHave(c.must("semantic", "validate"), "ok: 1 documents")
	c.mustHave(c.must("dashboard", "lint"), "1 dashboards checked, 0 with errors")
	c.mustHave(c.must("jobs", "list"), "monthly")
	for _, f := range []string{"download.sh", "ingest/trips.yaml", "ingest/zones.yaml", "pipelines/silver.sql", "pipelines/gold.sql", "README.md"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("missing %s", f)
		}
	}
	if o, err := exec.Command("bash", "-n", filepath.Join(dir, "download.sh")).CombinedOutput(); err != nil { //nolint:gosec // test
		t.Errorf("download.sh syntax: %v\n%s", err, o)
	}
	// No data may be committed with the example.
	matches, _ := filepath.Glob(filepath.Join(repoRoot(t), "examples", "nyc-taxi", "data", "*"))
	if len(matches) > 0 {
		t.Errorf("examples/nyc-taxi/data must stay empty in git: %v", matches)
	}
}
