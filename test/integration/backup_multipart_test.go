//go:build integration

package integration

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"
)

// listETags returns key -> ETag for the objects under prefix (one page).
func (s *s3Server) listETags(t *testing.T, bucket, prefix string) map[string]string {
	t.Helper()
	out := s.must(t, http.MethodGet, "/"+bucket+"?list-type=2&prefix="+prefix, nil)
	m := map[string]string{}
	for _, part := range strings.Split(out, "<Contents>")[1:] {
		key := between(part, "<Key>", "</Key>")
		m[key] = strings.Trim(between(part, "<ETag>", "</ETag>"), `"&quot;`)
	}
	return m
}

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	s = s[i+len(a):]
	if j := strings.Index(s, b); j >= 0 {
		return s[:j]
	}
	return s
}

// TestBackupMultipartS3 forces multipart transfers with small thresholds
// (DUCT_S3_MULTIPART_THRESHOLD / DUCT_S3_PART_SIZE: the same code path that
// objects above 5 GiB take) against RustFS: UploadPartCopy for the server-side
// copy of data files, a multipart PUT for an encrypted backup, and restores that
// give back the same rows, whole and by table.
func TestBackupMultipartS3(t *testing.T) {
	s3 := startRustFS(t)
	s3.createBucket(t, "mpbk")
	env := append(s3.env(), "DUCT_S3_MULTIPART_THRESHOLD=6291456", "DUCT_S3_PART_SIZE=5242880")
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), env, append([]string{"init", root}, s3.initFlags("s3://mpbk/lake")...)...)
	q := func(sql string) { mustDuct(t, root, env, "query", sql) }
	// ~25 MB of incompressible-ish text in one data file, plus a small table.
	q("CREATE TABLE main.rnd AS SELECT i AS id, sha256(i::VARCHAR) || sha256((i * 7)::VARCHAR) AS h FROM range(400000) r(i)")
	q("CREATE TABLE main.tiny AS SELECT 1 AS x")
	want := csvQuery(t, root, env, "SELECT count(*) AS n, sum(id) AS s, min(h) AS m FROM main.rnd")

	// 1. Plain backup into the same bucket: UploadPartCopy.
	mustDuct(t, root, env, "backup", "--to", "s3://mpbk/backups")
	multipart := 0
	for k, etag := range s3.listETags(t, "mpbk", "backups/") {
		if strings.Contains(k, "/data/") && strings.Contains(etag, "-") {
			multipart++
		}
	}
	if multipart == 0 {
		t.Fatal("no data object of the backup was copied in parts (ETag without -N)")
	}

	check := func(dir string) {
		t.Helper()
		if got := csvQuery(t, dir, env, "SELECT count(*) AS n, sum(id) AS s, min(h) AS m FROM main.rnd"); got != want {
			t.Errorf("rnd after restore = %q, want %q", got, want)
		}
	}
	r1 := filepath.Join(t.TempDir(), "r1")
	if out := mustDuct(t, t.TempDir(), env, "restore", "--from", "s3://mpbk/backups", "--storage", "s3://mpbk/restored1", r1); !strings.Contains(out, "verified:") {
		t.Fatalf("restore: %s", out)
	}
	check(r1)
	restoredParts := 0
	for k, etag := range s3.listETags(t, "mpbk", "restored1/") {
		if strings.HasSuffix(k, ".parquet") && strings.Contains(etag, "-") {
			restoredParts++
		}
	}
	if restoredParts == 0 {
		t.Error("the restore did not copy in parts")
	}

	// 2. Encrypted backup to S3: the ciphertext is spooled and PUT in parts;
	// restore (whole and by table) decrypts it.
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	idFile := filepath.Join(t.TempDir(), "k")
	write(t, idFile, id.String()+"\n")
	mustDuct(t, root, env, "backup", "--to", "s3://mpbk/enc", "--recipient", id.Recipient().String())
	encParts := 0
	for k, etag := range s3.listETags(t, "mpbk", "enc/") {
		if strings.HasSuffix(k, ".parquet.age") && strings.Contains(etag, "-") {
			encParts++
		}
	}
	if encParts == 0 {
		t.Error("the encrypted data object was not uploaded in parts")
	}
	r2 := filepath.Join(t.TempDir(), "r2")
	if out := mustDuct(t, t.TempDir(), env, "restore", "--from", "s3://mpbk/enc", "--identity", idFile, "--storage", "s3://mpbk/restored2", r2); !strings.Contains(out, "verified:") {
		t.Fatalf("encrypted restore: %s", out)
	}
	check(r2)

	// 3. Partial restore from the S3 backups into an existing project (staging
	// from S3, plain and encrypted).
	for _, from := range []string{"s3://mpbk/backups", "s3://mpbk/enc"} {
		dst := filepath.Join(t.TempDir(), "p")
		mustDuct(t, t.TempDir(), env, append([]string{"init", dst}, s3.initFlags("s3://mpbk/partial-"+strings.TrimPrefix(from, "s3://mpbk/"))...)...)
		args := []string{"restore", "--from", from, "--table", "main.rnd", dst}
		if strings.HasSuffix(from, "enc") {
			args = append(args[:len(args)-1], "--identity", idFile, dst)
		}
		if out := mustDuct(t, t.TempDir(), env, args...); !strings.Contains(out, "table main.rnd: 400000 rows") {
			t.Fatalf("partial restore from %s: %s", from, out)
		}
		check(dst)
		if got := csvQuery(t, dst, env, "SELECT count(*) AS n FROM duckdb_tables() WHERE database_name = 'lake' AND NOT internal AND table_name = 'tiny'"); got != "n\n0" {
			t.Errorf("a table that was not asked for was restored: %q", got)
		}
	}
}
