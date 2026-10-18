package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/project"
)

// deltaSharingPython returns a Python with the official `delta-sharing` client:
// $DUCT_TEST_DELTA_SHARING_PYTHON, or a venv built once with uv and cached in the
// user cache dir. The test is skipped, with the reason, when neither exists.
func deltaSharingPython(t *testing.T) string {
	t.Helper()
	if py := os.Getenv("DUCT_TEST_DELTA_SHARING_PYTHON"); py != "" {
		return py
	}
	uv, err := exec.LookPath("uv")
	if err != nil {
		if h, herr := os.UserHomeDir(); herr == nil {
			if _, serr := os.Stat(filepath.Join(h, ".local", "bin", "uv")); serr == nil {
				uv = filepath.Join(h, ".local", "bin", "uv")
			}
		}
	}
	if uv == "" {
		t.Skip("delta-sharing client not available: install uv or set DUCT_TEST_DELTA_SHARING_PYTHON to a Python with `pip install delta-sharing`")
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		cache = t.TempDir()
	}
	venv := filepath.Join(cache, "duct-test", "delta-sharing-venv")
	py := filepath.Join(venv, "bin", "python")
	if out, err := exec.Command(py, "-c", "import delta_sharing").CombinedOutput(); err == nil {
		_ = out
		return py
	}
	if err := os.MkdirAll(filepath.Dir(venv), 0o750); err != nil {
		t.Skipf("cannot create %s: %v", venv, err)
	}
	if out, err := exec.Command(uv, "venv", "-q", venv).CombinedOutput(); err != nil {
		t.Skipf("uv venv failed (no network or Python?): %v\n%s", err, out)
	}
	if out, err := exec.Command(uv, "pip", "install", "-q", "--python", py, "delta-sharing").CombinedOutput(); err != nil {
		t.Skipf("pip install delta-sharing failed (no network?): %v\n%s", err, out)
	}
	return py
}

const readShareScript = `
import json, sys
import delta_sharing
profile, table = sys.argv[1], sys.argv[2]
try:
    df = delta_sharing.load_as_pandas(profile + "#" + table)
except Exception as e:
    print(type(e).__name__ + ": " + str(e), file=sys.stderr)
    sys.exit(3)
df = df.sort_values("id")
print(json.dumps({"rows": json.loads(df.to_json(orient="records", date_format="iso")),
                  "version": delta_sharing.get_table_version(profile + "#" + table),
                  "tables": [t.name for t in delta_sharing.SharingClient(profile).list_all_tables()]}))
`

func readShare(t *testing.T, py, profile string) (rows []map[string]any, version int64, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(py, "-c", readShareScript, profile, "ventas.gold.sales")
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	err := cmd.Run()
	if err != nil {
		code = 1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return nil, 0, errb.String(), code
	}
	var res struct {
		Rows    []map[string]any `json:"rows"`
		Version int64            `json:"version"`
		Tables  []string         `json:"tables"`
	}
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		t.Fatalf("python output %q: %v", out.String(), err)
	}
	if len(res.Tables) != 1 || res.Tables[0] != "sales" {
		t.Errorf("list_all_tables = %v, want [sales] (other tables must stay hidden)", res.Tables)
	}
	return res.Rows, res.Version, errb.String(), 0
}

// startShareServe runs `duct serve` with the http and sharing protocols.
func startShareServe(t *testing.T, root string, sharingCfg map[string]any) (httpAddr, sharingAddr string) {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	p.Config.Serve.Modules = map[string]map[string]any{"sharing": sharingCfg}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan map[string]string, 1)
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, p, serveOptions{
			Protocols: []string{"http", "sharing"},
			Listen:    map[string]string{"http": "127.0.0.1:0", "sharing": "127.0.0.1:0"},
			NoJobs:    true,
			Ready:     func(a map[string]string) { ready <- a },
		})
	}()
	select {
	case a := <-ready:
		httpAddr, sharingAddr = a["http"], a["sharing"]
	case err := <-done:
		cancel()
		t.Fatalf("serve exited early: %v", err)
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
	return httpAddr, sharingAddr
}

func httpQuery(t *testing.T, addr, token, sql string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://"+addr+"/query?default_format=JSONEachRow", strings.NewReader(sql))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, string(b)
}

func profileOf(t *testing.T, path string) (endpoint, token string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(path)
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("profile mode = %v, want 600", fi.Mode().Perm())
	}
	var pr struct {
		Version  int    `json:"shareCredentialsVersion"`
		Endpoint string `json:"endpoint"`
		Token    string `json:"bearerToken"`
		Expires  string `json:"expirationTime"`
	}
	if err := json.Unmarshal(b, &pr); err != nil || pr.Version != 1 || pr.Endpoint == "" || pr.Token == "" || pr.Expires == "" {
		t.Fatalf("profile = %s (%v)", b, err)
	}
	return pr.Endpoint, pr.Token
}

func rewriteEndpoint(t *testing.T, path, endpoint string) {
	t.Helper()
	b, _ := os.ReadFile(path)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	m["endpoint"] = endpoint
	nb, _ := json.Marshal(m)
	if err := os.WriteFile(path, nb, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestE2EShare: the whole of ADR 0014 with the real CLI and the official
// Python client, on a local lake.
func TestE2EShare(t *testing.T) {
	t.Parallel()
	py := deltaSharingPython(t)
	root := newProject(t)
	g := func(args ...string) string { return mustDuct(t, append(args, "--project", root)...) }
	g("query", "CREATE SCHEMA gold")
	g("query", "CREATE TABLE gold.sales AS SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'cl' ELSE 'pe' END AS country, 'u' || i || '@x.com' AS email, CAST(i * 1.5 AS DOUBLE) AS amount FROM range(1, 11) t(i)")
	g("query", "CREATE TABLE gold.secret AS SELECT 'top-secret' AS v")

	// Model.
	g("share", "create", "ventas")
	g("share", "add", "ventas", "gold.sales")
	out := g("recipient", "create", "acme", "--profile", filepath.Join(root, "acme.share"))
	if !strings.Contains(out, "acme.share") || strings.Contains(out, "dct_") {
		t.Fatalf("recipient create must write the token to the profile, not print it: %q", out)
	}
	if _, errs, code := duct(t, "recipient", "create", "acme", "--project", root); code == 0 {
		t.Errorf("duplicate recipient accepted (%q)", errs)
	}
	g("share", "grant", "ventas", "to", "acme", "--row-filter", "country = 'cl'", "--mask", "email='***'")
	if _, errs, code := duct(t, "share", "grant", "ventas", "to", "acme", "--row-filter", "country = (SELECT 'x')", "--project", root); code == 0 {
		t.Errorf("hostile row filter accepted (%q)", errs)
	}
	list := g("share", "list")
	for _, want := range []string{"ventas", "gold.sales", "recipient acme", "row filter country = 'cl'", "mask email = '***'"} {
		if !strings.Contains(list, want) {
			t.Errorf("share list lacks %q:\n%s", want, list)
		}
	}
	profile := filepath.Join(root, "acme.share")
	_, token := profileOf(t, profile)

	httpAddr, shareAddr := startShareServe(t, root, map[string]any{"listen": "127.0.0.1:0", "url_ttl": "3s"})
	rewriteEndpoint(t, profile, "http://"+shareAddr+"/delta-sharing")

	// Phase 1: the same token queries its views over HTTP.
	code, body := httpQuery(t, httpAddr, token, "SELECT id, country, email FROM sales ORDER BY id")
	if code != 200 || strings.Count(body, `"country":"cl"`) != 5 || strings.Contains(body, "pe") || !strings.Contains(body, `"email":"***"`) {
		t.Errorf("phase 1: %d %s", code, body)
	}
	if code, _ = httpQuery(t, httpAddr, token, "SELECT * FROM gold.sales"); code != 403 {
		t.Errorf("recipient reading the base table: %d, want 403", code)
	}
	if code, _ = httpQuery(t, httpAddr, token, "SELECT * FROM secret"); code == 200 {
		t.Errorf("recipient reading an ungranted table: %d", code)
	}

	// Phase 2: the official client.
	rows, v1, errs, rc := readShare(t, py, profile)
	if rc != 0 {
		t.Fatalf("delta-sharing client failed: %s", errs)
	}
	if len(rows) != 5 {
		t.Fatalf("rows = %v, want the 5 'cl' rows", rows)
	}
	for _, r := range rows {
		if r["country"] != "cl" || r["email"] != "***" {
			t.Errorf("row not filtered or masked: %v", r)
		}
	}
	if _, ok := rows[0]["id"]; !ok || v1 < 1 {
		t.Errorf("rows[0] = %v, version %d", rows[0], v1)
	}

	// Copies live outside the lake, as Parquet, and survive `duct gc`.
	copies, _ := filepath.Glob(filepath.Join(root, "shares", "ventas", "acme", "sales", "v*", "*.parquet"))
	if len(copies) != 1 {
		t.Fatalf("copies on disk = %v", copies)
	}
	g("gc")
	if _, err := os.Stat(copies[0]); err != nil {
		t.Errorf("`duct gc` removed the copy: %v", err)
	}

	// New data: refresh by snapshot (here eagerly with the CLI) gives a new version.
	g("query", "INSERT INTO gold.sales VALUES (12, 'cl', 'n@x.com', 1.0), (13, 'pe', 'm@x.com', 2.0)")
	if out := g("share", "refresh"); !strings.Contains(out, "rewrote 1") {
		t.Errorf("refresh: %q", out)
	}
	if out := g("share", "refresh"); !strings.Contains(out, "rewrote 0") {
		t.Errorf("second refresh must find nothing to do: %q", out)
	}
	var v2 int64
	rows, v2, errs, rc = readShare(t, py, profile)
	if rc != 0 || len(rows) != 6 || v2 <= v1 {
		t.Fatalf("after insert: rc=%d rows=%d version %d -> %d: %s", rc, len(rows), v1, v2, errs)
	}

	// A short-lived URL stops working.
	req, _ := http.NewRequest("POST", "http://"+shareAddr+"/delta-sharing/shares/ventas/schemas/gold/tables/sales/query", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	qb, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var fileURL string
	for _, line := range strings.Split(strings.TrimSpace(string(qb)), "\n") {
		var m struct {
			File struct {
				URL string `json:"url"`
			} `json:"file"`
		}
		if json.Unmarshal([]byte(line), &m) == nil && m.File.URL != "" {
			fileURL = m.File.URL
		}
	}
	if fileURL == "" {
		t.Fatalf("no file in %s", qb)
	}
	if r, err := http.Get(fileURL); err != nil || r.StatusCode != 200 {
		t.Fatalf("fresh URL: %v %v", r, err)
	}
	time.Sleep(4 * time.Second)
	if r, _ := http.Get(fileURL); r.StatusCode != 403 {
		t.Errorf("expired URL: %d, want 403", r.StatusCode)
	}

	// Revoking the recipient: 401 for the client; a new token brings it back.
	g("recipient", "revoke", "acme")
	_, _, errs, rc = readShare(t, py, profile)
	if rc == 0 || !strings.Contains(errs, "401") {
		t.Errorf("revoked token: rc=%d stderr=%s", rc, errs)
	}
	g("recipient", "token", "acme", "--rotate", "--profile", filepath.Join(root, "acme2.share"))
	rewriteEndpoint(t, filepath.Join(root, "acme2.share"), "http://"+shareAddr+"/delta-sharing")
	if _, _, errs, rc = readShare(t, py, filepath.Join(root, "acme2.share")); rc != 0 {
		t.Errorf("new token: %s", errs)
	}

	// Revoking the grant deletes the copy and hides the share.
	g("share", "revoke", "ventas", "from", "acme")
	if left, _ := filepath.Glob(filepath.Join(root, "shares", "ventas", "acme", "*", "*", "*")); len(left) != 0 {
		t.Errorf("copies left after revoke: %v", left)
	}
	_, _, errs, rc = readShare(t, py, filepath.Join(root, "acme2.share"))
	if rc == 0 || !strings.Contains(errs, "404") {
		t.Errorf("revoked grant: rc=%d stderr=%s", rc, errs)
	}
	if code, _ = httpQuery(t, httpAddr, token, "SELECT * FROM sales"); code == 200 {
		t.Errorf("phase 1 view must go with the grant")
	}
}

const historyScript = `
import json, sys, datetime
import delta_sharing
profile = sys.argv[1]
url = profile + "#ventas.gold.sales"
mode = sys.argv[2]

def rows(df, cols):
    df = df.sort_values(cols)
    return json.loads(df.to_json(orient="records"))

def iso(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

try:
    if mode == "changes":
        start = int(sys.argv[3])
        df = delta_sharing.load_table_changes_as_pandas(url, starting_version=start)
        print(json.dumps(rows(df, ["_commit_version", "id", "_change_type"])))
    elif mode == "changes_ts":
        df = delta_sharing.load_table_changes_as_pandas(url, starting_timestamp=sys.argv[3])
        print(json.dumps(rows(df, ["_commit_version", "id", "_change_type"])))
    elif mode == "version":
        df = delta_sharing.load_as_pandas(url, version=int(sys.argv[3]))
        print(json.dumps(rows(df, ["id"])))
    elif mode == "timestamp":
        df = delta_sharing.load_as_pandas(url, timestamp=sys.argv[3])
        print(json.dumps(rows(df, ["id"])))
    elif mode == "tsof":
        df = delta_sharing.load_table_changes_as_pandas(url, starting_version=int(sys.argv[3]), ending_version=int(sys.argv[3]))
        print(json.dumps(iso(int(df["_commit_timestamp"].iloc[0]))))
except Exception as e:
    print(type(e).__name__ + ": " + str(e), file=sys.stderr)
    sys.exit(3)
`

func runHistory(t *testing.T, py, profile string, args ...string) ([]map[string]any, string, int) {
	t.Helper()
	cmd := exec.Command(py, append([]string{"-c", historyScript, profile}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		code := 1
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return nil, errb.String(), code
	}
	var rows []map[string]any
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		var s string
		if json.Unmarshal(out.Bytes(), &s) == nil {
			return []map[string]any{{"ts": s}}, "", 0
		}
		t.Fatalf("python output %q: %v", out.String(), err)
	}
	return rows, "", 0
}

// TestE2EShareHistory: time travel and the change feed with the official
// Python client, with the recipient's filter and masks.
func TestE2EShareHistory(t *testing.T) {
	t.Parallel()
	py := deltaSharingPython(t)
	root := newProject(t)
	g := func(args ...string) string { return mustDuct(t, append(args, "--project", root)...) }
	g("query", "CREATE SCHEMA gold")
	g("query", "CREATE TABLE gold.sales AS SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'cl' ELSE 'pe' END AS country, 'u' || i || '@x.com' AS email, CAST(i * 1.5 AS DOUBLE) AS amount FROM range(1, 11) t(i)")
	g("share", "create", "ventas")
	g("share", "add", "ventas", "gold.sales", "--history")
	if list := g("share", "list"); !strings.Contains(list, "[history]") {
		t.Errorf("share list lacks the history mark:\n%s", list)
	}
	profile := filepath.Join(root, "acme.share")
	g("recipient", "create", "acme", "--profile", profile)
	g("share", "grant", "ventas", "to", "acme", "--row-filter", "country = 'cl'", "--mask", "email='***'")
	_, shareAddr := startShareServe(t, root, map[string]any{"listen": "127.0.0.1:0", "keep_versions": 5})
	rewriteEndpoint(t, profile, "http://"+shareAddr+"/delta-sharing")

	// Version 1.
	if rows, errs, rc := runHistory(t, py, profile, "version", "1"); rc != 0 || len(rows) != 5 {
		t.Fatalf("version 1: rc=%d rows=%v %s", rc, rows, errs)
	}
	time.Sleep(1100 * time.Millisecond)
	g("query", "INSERT INTO gold.sales VALUES (12, 'cl', 'n@x.com', 1.0), (13, 'pe', 'm@x.com', 2.0)")
	g("share", "refresh")
	time.Sleep(1100 * time.Millisecond)
	g("query", "UPDATE gold.sales SET country = 'pe' WHERE id = 2")
	g("query", "UPDATE gold.sales SET amount = 99.0 WHERE id = 4")
	g("query", "UPDATE gold.sales SET country = 'cl' WHERE id = 3")
	g("query", "DELETE FROM gold.sales WHERE id = 8")
	g("share", "refresh")

	feed, errs, rc := runHistory(t, py, profile, "changes", "2")
	if rc != 0 {
		t.Fatalf("load_table_changes_as_pandas: %s", errs)
	}
	got := map[string]bool{}
	for _, r := range feed {
		got[fmt.Sprintf("%v/%v/%v", r["_commit_version"], r["id"], r["_change_type"])] = true
		if r["email"] != "***" || r["country"] != "cl" {
			t.Errorf("feed row not filtered or masked: %v", r)
		}
		if ts, _ := r["_commit_timestamp"].(float64); ts < 1e12 {
			t.Errorf("_commit_timestamp = %v", r["_commit_timestamp"])
		}
	}
	for _, w := range []string{"2/12/insert", "3/2/delete", "3/3/insert", "3/4/update_preimage", "3/4/update_postimage", "3/8/delete"} {
		if !got[w] {
			t.Errorf("feed lacks %s: %v", w, feed)
		}
	}
	if len(feed) != 6 {
		t.Errorf("feed has %d rows, want 6: %v", len(feed), feed)
	}
	// From version 1: its rows are inserts.
	if feed, errs, rc = runHistory(t, py, profile, "changes", "1"); rc != 0 || len(feed) != 11 {
		t.Errorf("feed from version 1: rc=%d rows=%d %s", rc, len(feed), errs)
	}

	// Time travel by version and timestamp.
	if rows, errs, rc := runHistory(t, py, profile, "version", "1"); rc != 0 || len(rows) != 5 {
		t.Errorf("version 1 again: rc=%d rows=%d %s", rc, len(rows), errs)
	}
	if rows, errs, rc := runHistory(t, py, profile, "version", "2"); rc != 0 || len(rows) != 6 {
		t.Errorf("version 2: rc=%d rows=%d %s", rc, len(rows), errs)
	}
	tsv, errs, rc := runHistory(t, py, profile, "tsof", "2")
	if rc != 0 {
		t.Fatalf("timestamp of version 2: %s", errs)
	}
	ts2, _ := tsv[0]["ts"].(string)
	if rows, errs, rc := runHistory(t, py, profile, "timestamp", ts2); rc != 0 || len(rows) != 6 {
		t.Errorf("timestamp %s: rc=%d rows=%d %s", ts2, rc, len(rows), errs)
	}
	if rows, errs, rc := runHistory(t, py, profile, "changes_ts", ts2); rc != 0 || len(rows) != 6 {
		t.Errorf("changes since %s: rc=%d rows=%d %s", ts2, rc, len(rows), errs)
	}
	// A version that does not exist is an error the client shows.
	if _, errs, rc := runHistory(t, py, profile, "version", "9"); rc == 0 || !strings.Contains(errs, "400") {
		t.Errorf("version 9: rc=%d %s", rc, errs)
	}
}
