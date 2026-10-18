//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// deltaSharingPython returns a Python with the official delta-sharing client
// (see cmd/duct/share_test.go); the test is skipped when it is unavailable.
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
		t.Skip("delta-sharing client not available: install uv or set DUCT_TEST_DELTA_SHARING_PYTHON")
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		cache = t.TempDir()
	}
	venv := filepath.Join(cache, "duct-test", "delta-sharing-venv")
	py := filepath.Join(venv, "bin", "python")
	if exec.Command(py, "-c", "import delta_sharing").Run() == nil {
		return py
	}
	if err := os.MkdirAll(filepath.Dir(venv), 0o750); err != nil {
		t.Skipf("cannot create %s: %v", venv, err)
	}
	if out, err := exec.Command(uv, "venv", "-q", venv).CombinedOutput(); err != nil {
		t.Skipf("uv venv failed: %v\n%s", err, out)
	}
	if out, err := exec.Command(uv, "pip", "install", "-q", "--python", py, "delta-sharing").CombinedOutput(); err != nil {
		t.Skipf("pip install delta-sharing failed (no network?): %v\n%s", err, out)
	}
	return py
}

const readShareScript = `
import json, sys
import delta_sharing
try:
    df = delta_sharing.load_as_pandas(sys.argv[1] + "#ventas.gold.sales")
except Exception as e:
    print(type(e).__name__ + ": " + str(e), file=sys.stderr)
    sys.exit(3)
df = df.sort_values("id")
print(json.dumps(json.loads(df.to_json(orient="records"))))
`

func readShare(t *testing.T, py, profile string) ([]map[string]any, string, bool) {
	t.Helper()
	cmd := exec.Command(py, "-c", readShareScript, profile)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, errb.String(), false
	}
	var rows []map[string]any
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatalf("python output %q: %v", out.String(), err)
	}
	return rows, "", true
}

// startSharing runs `duct serve` with the sharing protocol and returns its address.
func startSharing(t *testing.T, root string, env []string) string {
	t.Helper()
	cmd := exec.Command(ductBinary(t), "serve", "--protocols", "sharing", "--no-jobs")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGINT)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
		}
	})
	addr := make(chan string, 1)
	re := regexp.MustCompile(`serving sharing\s+on (\S+)`)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if m := re.FindStringSubmatch(sc.Text()); m != nil {
				addr <- m[1]
			}
		}
	}()
	select {
	case a := <-addr:
		return a
	case <-time.After(120 * time.Second):
		t.Fatalf("duct serve did not start: %s", errb.String())
	}
	return ""
}

// TestShareS3RustFS: sharing over S3 storage. The copies go to
// s3://bucket/proj.shares (outside the lake prefix, so `duct gc` leaves them)
// and the URLs are presigned S3 URLs of that prefix only.
func TestShareS3RustFS(t *testing.T) {
	py := deltaSharingPython(t)
	s3 := startRustFS(t)
	const bucket = "lake-share"
	s3.createBucket(t, bucket)
	env := s3.env()
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), env, append([]string{"init", root}, s3.initFlags("s3://"+bucket+"/proj")...)...)
	f, err := os.OpenFile(filepath.Join(root, "duct.toml"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\n[serve.sharing]\nlisten = \"127.0.0.1:0\"\nurl_ttl = \"3s\"\n")
	_ = f.Close()

	d := func(args ...string) string { return mustDuct(t, root, env, args...) }
	d("query", "CREATE SCHEMA gold")
	d("query", "CREATE TABLE gold.sales AS SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'cl' ELSE 'pe' END AS country, 'u' || i || '@x.com' AS email FROM range(1, 11) t(i)")
	d("query", "CREATE TABLE gold.secret AS SELECT 'top-secret' AS v")
	d("share", "create", "ventas")
	d("share", "add", "ventas", "gold.sales")
	d("recipient", "create", "acme", "--profile", filepath.Join(root, "acme.share"))
	d("share", "grant", "ventas", "to", "acme", "--row-filter", "country = 'cl'", "--mask", "email='***'")

	addr := startSharing(t, root, env)
	endpoint := "http://" + addr + "/delta-sharing"
	profile := filepath.Join(root, "acme2.share")
	d("recipient", "token", "acme", "--rotate", "--endpoint", endpoint, "--profile", profile)
	pb, _ := os.ReadFile(profile)
	var pr struct {
		Token string `json:"bearerToken"`
	}
	if err := json.Unmarshal(pb, &pr); err != nil {
		t.Fatal(err)
	}

	rows, errs, ok := readShare(t, py, profile)
	if !ok {
		t.Fatalf("delta-sharing client: %s", errs)
	}
	if len(rows) != 5 {
		t.Fatalf("rows = %v, want 5", rows)
	}
	for _, r := range rows {
		if r["country"] != "cl" || r["email"] != "***" {
			t.Errorf("row not filtered or masked: %v", r)
		}
	}

	// The copy sits under proj.shares/, in Parquet; the lake prefix has none of it.
	keys := s3.keys(t, bucket, "proj.shares/ventas/acme/sales/")
	if len(keys) != 1 || !strings.HasSuffix(keys[0], ".parquet") {
		t.Fatalf("copy objects = %v (all: %v)", keys, s3.keys(t, bucket, ""))
	}
	for _, k := range s3.keys(t, bucket, "proj/") {
		if strings.Contains(k, "ventas") {
			t.Errorf("share data inside the lake prefix: %s", k)
		}
	}

	// URLs: presigned, S3, only for the copy, and they expire.
	req, _ := http.NewRequest("POST", endpoint+"/shares/ventas/schemas/gold/tables/sales/query", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer "+pr.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	var fileURL string
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var m struct {
			File struct {
				URL string `json:"url"`
			} `json:"file"`
		}
		if json.Unmarshal([]byte(line), &m) == nil && m.File.URL != "" {
			fileURL = m.File.URL
		}
	}
	if !strings.HasPrefix(fileURL, "http://"+s3.endpoint+"/"+bucket+"/proj.shares/ventas/acme/sales/") || !strings.Contains(fileURL, "X-Amz-Signature=") || strings.Contains(fileURL, "/proj/") {
		t.Fatalf("file URL = %s", fileURL)
	}
	if r, err := http.Get(fileURL); err != nil || r.StatusCode != 200 {
		t.Fatalf("presigned GET: %v %v", r, err)
	}
	// Without the signature the object is private.
	if r, _ := http.Get(strings.SplitN(fileURL, "?", 2)[0]); r.StatusCode == 200 {
		t.Errorf("unsigned GET of the copy: %d", r.StatusCode)
	}
	time.Sleep(5 * time.Second)
	if r, _ := http.Get(fileURL); r.StatusCode != 403 {
		t.Errorf("expired presigned URL: %d, want 403", r.StatusCode)
	}

	// `duct gc` (DuckLake orphan cleanup) leaves the copy alone.
	time.Sleep(2 * time.Second)
	d("gc", "--keep-snapshots", "1s")
	if got := s3.keys(t, bucket, "proj.shares/ventas/acme/sales/"); len(got) != 1 {
		t.Errorf("copy after duct gc = %v", got)
	}

	// New data: a refresh rewrites the copy; the superseded one waits for gc.
	d("query", "INSERT INTO gold.sales VALUES (12, 'cl', 'n@x.com')")
	if out := d("share", "refresh"); !strings.Contains(out, "rewrote 1") {
		t.Errorf("refresh: %s", out)
	}
	if rows, errs, ok = readShare(t, py, profile); !ok || len(rows) != 6 {
		t.Fatalf("after refresh: ok=%v rows=%d %s", ok, len(rows), errs)
	}

	// Revoking the grant deletes the copies from the bucket.
	d("share", "revoke", "ventas", "from", "acme")
	if got := s3.keys(t, bucket, "proj.shares/"); len(got) != 0 {
		t.Errorf("objects left after revoke: %v", got)
	}
	if _, errs, ok = readShare(t, py, profile); ok || !strings.Contains(errs, "404") {
		t.Errorf("revoked grant: ok=%v %s", ok, errs)
	}
}

const historyScript = `
import json, sys
import delta_sharing
url = sys.argv[1] + "#ventas.gold.sales"
try:
    if sys.argv[2] == "changes":
        df = delta_sharing.load_table_changes_as_pandas(url, starting_version=int(sys.argv[3]))
        df = df.sort_values(["_commit_version", "id", "_change_type"])
    else:
        df = delta_sharing.load_as_pandas(url, version=int(sys.argv[3])).sort_values("id")
    print(json.dumps(json.loads(df.to_json(orient="records"))))
except Exception as e:
    print(type(e).__name__ + ": " + str(e), file=sys.stderr)
    sys.exit(3)
`

func runHistory(t *testing.T, py, profile string, args ...string) ([]map[string]any, string, bool) {
	t.Helper()
	cmd := exec.Command(py, append([]string{"-c", historyScript, profile}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, errb.String(), false
	}
	var rows []map[string]any
	if err := json.Unmarshal(out.Bytes(), &rows); err != nil {
		t.Fatalf("python output %q: %v", out.String(), err)
	}
	return rows, "", true
}

// TestShareHistoryS3RustFS: versions and the change feed over S3 storage, read
// with the official client through presigned URLs; retention deletes the files
// of the versions it forgets once the grace period has passed.
func TestShareHistoryS3RustFS(t *testing.T) {
	py := deltaSharingPython(t)
	s3 := startRustFS(t)
	const bucket = "lake-share-hist"
	s3.createBucket(t, bucket)
	env := s3.env()
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), env, append([]string{"init", root}, s3.initFlags("s3://"+bucket+"/proj")...)...)
	f, err := os.OpenFile(filepath.Join(root, "duct.toml"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("\n[serve.sharing]\nlisten = \"127.0.0.1:0\"\nkeep_versions = 2\ngrace = \"1s\"\n")
	_ = f.Close()

	d := func(args ...string) string { return mustDuct(t, root, env, args...) }
	d("query", "CREATE SCHEMA gold")
	d("query", "CREATE TABLE gold.sales AS SELECT i AS id, CASE WHEN i % 2 = 0 THEN 'cl' ELSE 'pe' END AS country, 'u' || i || '@x.com' AS email FROM range(1, 11) t(i)")
	d("share", "create", "ventas")
	d("share", "add", "ventas", "gold.sales", "--history")
	d("recipient", "create", "acme", "--profile", filepath.Join(root, "acme.share"))
	d("share", "grant", "ventas", "to", "acme", "--row-filter", "country = 'cl'", "--mask", "email='***'")

	addr := startSharing(t, root, env)
	profile := filepath.Join(root, "acme2.share")
	d("recipient", "token", "acme", "--rotate", "--endpoint", "http://"+addr+"/delta-sharing", "--profile", profile)

	if rows, errs, ok := runHistory(t, py, profile, "version", "1"); !ok || len(rows) != 5 {
		t.Fatalf("version 1: ok=%v rows=%d %s", ok, len(rows), errs)
	}
	d("query", "INSERT INTO gold.sales VALUES (12, 'cl', 'n@x.com'), (13, 'pe', 'm@x.com')")
	d("share", "refresh")
	d("query", "UPDATE gold.sales SET country = 'pe' WHERE id = 2")
	d("query", "DELETE FROM gold.sales WHERE id = 4")
	d("share", "refresh")

	feed, errs, ok := runHistory(t, py, profile, "changes", "2")
	if !ok {
		t.Fatalf("change feed over S3: %s", errs)
	}
	got := map[string]bool{}
	for _, r := range feed {
		got[fmt.Sprintf("%v/%v/%v", r["_commit_version"], r["id"], r["_change_type"])] = true
		if r["email"] != "***" || r["country"] != "cl" {
			t.Errorf("feed row not filtered or masked: %v", r)
		}
	}
	for _, w := range []string{"2/12/insert", "3/2/delete", "3/4/delete"} {
		if !got[w] {
			t.Errorf("feed lacks %s: %v", w, feed)
		}
	}
	if len(feed) != 3 {
		t.Errorf("feed = %v, want 3 rows", feed)
	}
	// Time travel to a retained version (keep_versions = 2: versions 2 and 3).
	if rows, errs, ok := runHistory(t, py, profile, "version", "2"); !ok || len(rows) != 6 {
		t.Errorf("version 2: ok=%v rows=%d %s", ok, len(rows), errs)
	}
	if _, errs, ok := runHistory(t, py, profile, "version", "1"); ok || !strings.Contains(errs, "400") {
		t.Errorf("version 1 was pruned, yet: ok=%v %s", ok, errs)
	}
	// Versions 2 and 3 hold copies and a feed; version 1's files go after the grace period.
	time.Sleep(2 * time.Second)
	d("share", "gc")
	keys := s3.keys(t, bucket, "proj.shares/ventas/acme/sales/")
	var parts, cdfs int
	for _, k := range keys {
		switch {
		case strings.Contains(k, "/cdf-"):
			cdfs++
		case strings.Contains(k, "/part-"):
			parts++
		}
	}
	if parts != 2 || cdfs != 2 {
		t.Errorf("objects after gc = %v, want 2 copies and 2 feed files", keys)
	}
	// `duct gc` of the lake does not touch them.
	d("gc", "--keep-snapshots", "1s")
	if got := s3.keys(t, bucket, "proj.shares/ventas/acme/sales/"); len(got) != len(keys) {
		t.Errorf("after duct gc: %v", got)
	}
}
