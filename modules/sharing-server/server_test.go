package sharingserver_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
	"github.com/alonsoburon/duct/core/sharing"
	_ "github.com/alonsoburon/duct/modules/engine-duckdb"
	_ "github.com/alonsoburon/duct/modules/sharing-server"
)

type fixture struct {
	t     *testing.T
	eng   contract.Engine
	st    *sharing.Store
	base  string // REST endpoint
	root  string // project root
	token string
}

func (f *fixture) sql(q string) {
	f.t.Helper()
	if err := f.eng.Exec(context.Background(), q); err != nil {
		f.t.Fatalf("%s: %v", q, err)
	}
}

func (f *fixture) regen() {
	f.t.Helper()
	if _, err := f.st.Governance().Regenerate(context.Background()); err != nil {
		f.t.Fatal(err)
	}
}

func newFixture(t *testing.T, extra contract.Config) *fixture {
	t.Helper()
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "p")
	p, err := project.Scaffold(root)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := registry.Lookup(contract.KindEngine, "duckdb")
	eng := info.New().(contract.Engine)
	if err := eng.Init(ctx, p.EngineConfig(false)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	if _, err := project.Migrate(ctx, eng); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, eng: eng, st: sharing.NewStore(eng), root: root}
	f.sql(`CREATE SCHEMA gold`)
	f.sql(`CREATE TABLE gold.sales AS SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'cl' ELSE 'pe' END AS country, 'u' || i || '@x.com' AS email, CAST(i * 1.5 AS DOUBLE) AS amount FROM range(1, 11) t(i)`)
	f.sql(`CREATE TABLE gold.other AS SELECT 1 AS x`)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(f.st.CreateShare(ctx, "ventas"))
	must(f.st.AddTable(ctx, "ventas", "gold.sales", "", ""))
	is, err := f.st.CreateRecipient(ctx, "acme", time.Hour)
	must(err)
	f.token = is.Token
	must(f.st.Grant(ctx, "ventas", "acme", sharing.GrantOptions{RowFilter: "country = 'cl'", Masks: map[string]string{"email": "'***'"}}))
	f.regen()

	mi, _ := registry.Lookup(contract.KindProtocol, "sharing")
	mod := mi.New().(contract.Protocol)
	cfg := contract.Config{
		"listen": "127.0.0.1:0", "storage": filepath.Join(root, "shares"), "data_path": p.DataPath(),
		"signing_key_file": filepath.Join(root, ".duct", "sharing.key"),
	}
	for k, v := range extra {
		cfg[k] = v
	}
	if err := mod.Init(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	mod.(sharing.EngineUser).UseEngine(eng)
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mod.Serve(sctx, nil, governance.NewStore(eng)) }()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = mod.Close()
	})
	f.base = "http://" + mod.(contract.Addresser).Addr() + "/delta-sharing"
	return f
}

func (f *fixture) do(method, path, token, body string) (*http.Response, string) {
	f.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, f.base+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(b)
}

func ndjson(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(strings.NewReader(body))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad NDJSON line %q: %v", sc.Text(), err)
		}
		out = append(out, m)
	}
	return out
}

// fileURLs runs a query and returns the version header and the file objects.
func (f *fixture) query(token, body string) (int64, []map[string]any) {
	f.t.Helper()
	resp, b := f.do("POST", "/shares/ventas/schemas/gold/tables/sales/query", token, body)
	if resp.StatusCode != 200 {
		f.t.Fatalf("query: %d %s", resp.StatusCode, b)
	}
	v, _ := strconv.ParseInt(resp.Header.Get("Delta-Table-Version"), 10, 64)
	lines := ndjson(f.t, b)
	if len(lines) < 2 || lines[0]["protocol"] == nil || lines[1]["metaData"] == nil {
		f.t.Fatalf("query must start with protocol and metaData: %s", b)
	}
	var files []map[string]any
	for _, l := range lines[2:] {
		files = append(files, l["file"].(map[string]any))
	}
	return v, files
}

// rowsOf downloads a Parquet file and reads it with DuckDB.
func (f *fixture) rowsOf(fileURL string) [][]string {
	f.t.Helper()
	resp, err := http.Get(fileURL)
	if err != nil {
		f.t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		f.t.Fatalf("download: %d %s", resp.StatusCode, b)
	}
	tmp := filepath.Join(f.t.TempDir(), "x.parquet")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		f.t.Fatal(err)
	}
	st, err := f.eng.Query(context.Background(), "SELECT id, country, email, amount FROM read_parquet('"+tmp+"') ORDER BY id")
	if err != nil {
		f.t.Fatal(err)
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				row[c] = strings.Clone(rec.Column(c).ValueStr(i))
			}
			out = append(out, row)
		}
	}
	return out
}

func TestProtocol(t *testing.T) {
	f := newFixture(t, contract.Config{"url_ttl": "2s"})

	// Authentication.
	for _, tok := range []string{"", "dct_nope", "garbage"} {
		if resp, _ := f.do("GET", "/shares", tok, ""); resp.StatusCode != 401 {
			t.Errorf("token %q: status %d, want 401", tok, resp.StatusCode)
		}
	}
	// A governance user that is not a recipient is refused.
	if err := f.st.Governance().CreateRole(context.Background(), "analistas"); err != nil {
		t.Fatal(err)
	}
	if err := f.st.Governance().CreateUser(context.Background(), "ana", "analistas", "pw-ana-123456"); err != nil {
		t.Fatal(err)
	}
	other, _, err := f.st.Governance().CreateToken(context.Background(), "ana", "t", 0)
	if err != nil {
		t.Fatal(err)
	}
	if resp, _ := f.do("GET", "/shares", other, ""); resp.StatusCode != 403 {
		t.Errorf("non-recipient token: %d, want 403", resp.StatusCode)
	}

	// Listings.
	resp, b := f.do("GET", "/shares", f.token, "")
	if resp.StatusCode != 200 || !strings.Contains(b, `"ventas"`) {
		t.Fatalf("shares: %d %s", resp.StatusCode, b)
	}
	if _, b = f.do("GET", "/shares/ventas/schemas", f.token, ""); !strings.Contains(b, `"gold"`) {
		t.Errorf("schemas: %s", b)
	}
	if _, b = f.do("GET", "/shares/ventas/schemas/gold/tables", f.token, ""); !strings.Contains(b, `"sales"`) || strings.Contains(b, "other") {
		t.Errorf("tables: %s", b)
	}
	if _, b = f.do("GET", "/shares/ventas/all-tables", f.token, ""); !strings.Contains(b, `"sales"`) {
		t.Errorf("all-tables: %s", b)
	}
	if resp, _ = f.do("GET", "/shares/ventas/schemas/nope/tables", f.token, ""); resp.StatusCode != 404 {
		t.Errorf("unknown schema: %d", resp.StatusCode)
	}
	if resp, _ = f.do("GET", "/shares/secreto", f.token, ""); resp.StatusCode != 404 {
		t.Errorf("unknown share: %d", resp.StatusCode)
	}
	if resp, _ = f.do("GET", "/shares/ventas/schemas/gold/tables/other/version", f.token, ""); resp.StatusCode != 404 {
		t.Errorf("table not in the share must be 404, got %d", resp.StatusCode)
	}
	// Pagination.
	if _, b = f.do("GET", "/shares?maxResults=1", f.token, ""); strings.Contains(b, "nextPageToken") {
		t.Errorf("one share, one page: %s", b)
	}

	// Version, metadata.
	resp, _ = f.do("HEAD", "/shares/ventas/schemas/gold/tables/sales/version", f.token, "")
	v1, _ := strconv.ParseInt(resp.Header.Get("Delta-Table-Version"), 10, 64)
	if resp.StatusCode != 200 || v1 < 1 {
		t.Fatalf("version: %d %v", resp.StatusCode, resp.Header)
	}
	resp, b = f.do("GET", "/shares/ventas/schemas/gold/tables/sales/metadata", f.token, "")
	lines := ndjson(t, b)
	if resp.Header.Get("Content-Type") != "application/x-ndjson; charset=utf-8" || len(lines) != 2 {
		t.Fatalf("metadata: %v %s", resp.Header, b)
	}
	meta := lines[1]["metaData"].(map[string]any)
	if meta["format"].(map[string]any)["provider"] != "parquet" || !strings.Contains(meta["schemaString"].(string), `"name":"amount","type":"double"`) {
		t.Errorf("metaData = %v", meta)
	}

	// Query: filtered and masked copy through a short URL.
	v, files := f.query(f.token, `{"limitHint": 100}`)
	if v != v1 || len(files) != 1 {
		t.Fatalf("version %d (want %d), files %d", v, v1, len(files))
	}
	fileURL := files[0]["url"].(string)
	if strings.Contains(fileURL, "lake/") || strings.Contains(fileURL, "ducklake-") {
		t.Errorf("a URL must never point at base data: %s", fileURL)
	}
	rows := f.rowsOf(fileURL)
	if len(rows) != 5 {
		t.Fatalf("rows = %v, want the 5 'cl' rows", rows)
	}
	for _, r := range rows {
		if r[1] != "cl" || r[2] != "***" {
			t.Errorf("row not filtered or masked: %v", r)
		}
	}
	if files[0]["size"].(float64) <= 0 || !strings.Contains(files[0]["stats"].(string), `"numRecords":5`) {
		t.Errorf("file = %v", files[0])
	}

	// Tampered, malformed and expired URLs are refused.
	u, _ := url.Parse(fileURL)
	bad := *u
	q := bad.Query()
	q.Set("sig", strings.Repeat("0", 64))
	bad.RawQuery = q.Encode()
	if r, _ := http.Get(bad.String()); r.StatusCode != 403 {
		t.Errorf("bad signature: %d", r.StatusCode)
	}
	trav := *u
	trav.Path = strings.Replace(u.Path, "/ventas/", "/ventas/../../../duct.toml/", 1)
	if r, _ := http.Get(trav.String()); r.StatusCode != 403 && r.StatusCode != 404 {
		t.Errorf("traversal: %d", r.StatusCode)
	}
	noSig := *u
	noSig.RawQuery = ""
	if r, _ := http.Get(noSig.String()); r.StatusCode != 403 {
		t.Errorf("unsigned: %d", r.StatusCode)
	}
	time.Sleep(3 * time.Second) // url_ttl is 2 s
	if r, _ := http.Get(fileURL); r.StatusCode != 403 {
		t.Errorf("expired URL: %d, want 403", r.StatusCode)
	}

	// Changing an unrelated table moves the lake snapshot but not the version.
	f.sql(`INSERT INTO gold.other VALUES (2)`)
	if v2, _ := f.query(f.token, ""); v2 != v1 {
		t.Errorf("unrelated change bumped the version: %d -> %d", v1, v2)
	}
	// New rows: new version, new copy; the old URLs' files stay for the grace period.
	f.sql(`INSERT INTO gold.sales VALUES (20, 'cl', 'z@x.com', 9.5), (21, 'pe', 'w@x.com', 1.0)`)
	v3, files3 := f.query(f.token, "")
	if v3 <= v1 {
		t.Fatalf("version did not advance: %d -> %d", v1, v3)
	}
	if got := f.rowsOf(files3[0]["url"].(string)); len(got) != 6 {
		t.Errorf("after insert: %d rows, want 6", len(got))
	}
	// Time travel and unsupported endpoints say so.
	if resp, _ = f.do("POST", "/shares/ventas/schemas/gold/tables/sales/query", f.token, `{"version": 1}`); resp.StatusCode != 400 {
		t.Errorf("old version: %d", resp.StatusCode)
	}
	if resp, b = f.do("GET", "/shares/ventas/schemas/gold/tables/sales/changes?startingVersion=1", f.token, ""); resp.StatusCode != 400 || !strings.Contains(b, "without history") {
		t.Errorf("changes without history: %d %s", resp.StatusCode, b)
	}

	// Every call is audited under the sharing protocol, with the recipient as user.
	time.Sleep(200 * time.Millisecond)
	waitAudit(t, f, "share_acme", "POST /delta-sharing/shares/ventas/schemas/gold/tables/sales/query")

	// Revoking the recipient: its token stops working at once.
	if err := f.st.RevokeRecipient(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	if resp, _ = f.do("GET", "/shares", f.token, ""); resp.StatusCode != 401 {
		t.Errorf("revoked recipient: %d, want 401", resp.StatusCode)
	}
	// A new token for the same recipient works again.
	is, err := f.st.NewToken(context.Background(), "acme", time.Hour, true)
	if err != nil {
		t.Fatal(err)
	}
	if resp, _ = f.do("GET", "/shares", f.token, ""); resp.StatusCode != 401 {
		t.Errorf("rotated-out token: %d, want 401", resp.StatusCode)
	}
	if resp, _ = f.do("GET", "/shares", is.Token, ""); resp.StatusCode != 200 {
		t.Errorf("new token: %d", resp.StatusCode)
	}
	// Revoking the grant: the share disappears and its copy is deleted.
	prefixes, err := f.st.Revoke(context.Background(), "ventas", "acme")
	if err != nil || len(prefixes) != 1 {
		t.Fatalf("revoke: %v %v", prefixes, err)
	}
	if resp, _ = f.do("GET", "/shares/ventas/schemas/gold/tables/sales/metadata", is.Token, ""); resp.StatusCode != 404 {
		t.Errorf("revoked grant: %d, want 404", resp.StatusCode)
	}
}

func waitAudit(t *testing.T, f *fixture, user, sql string) {
	t.Helper()
	for range 40 {
		st, err := f.eng.Query(context.Background(), `SELECT count(*) FROM duct.duct_audit WHERE protocol = 'sharing' AND user_name = '`+user+`' AND sql_text = '`+sql+`' AND status = 'ok'`)
		if err != nil {
			t.Fatal(err)
		}
		n := ""
		for st.Next() {
			n = st.RecordBatch().Column(0).ValueStr(0)
		}
		st.Release()
		if n != "0" && n != "" {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Errorf("no audit record for %s %q", user, sql)
}

func TestInitRefusesUnsafeConfig(t *testing.T) {
	mi, _ := registry.Lookup(contract.KindProtocol, "sharing")
	dir := t.TempDir()
	for name, cfg := range map[string]contract.Config{
		"inside the lake":       {"listen": "127.0.0.1:0", "storage": filepath.Join(dir, "lake", "shares"), "data_path": filepath.Join(dir, "lake")},
		"no storage":            {"listen": "127.0.0.1:0"},
		"clear text, not local": {"listen": "0.0.0.0:0", "storage": filepath.Join(dir, "s")},
	} {
		if err := mi.New().Init(context.Background(), cfg); err == nil {
			t.Errorf("%s: Init accepted the config", name)
		}
	}
}
