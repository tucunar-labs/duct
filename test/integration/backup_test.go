//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alonsoburon/duct/core/backup"
)

// TestBackupRestoreS3 drives `duct backup` and `duct restore` over a lake on
// RustFS, for a SQLite and a Postgres catalog (ADR 0013): backup into the same
// bucket (server-side copy), into a local folder (streamed), catalog-only,
// restore into a new catalog and a new prefix, and the checks that the restore
// refuses a catalog-only backup whose data is not there.
func TestBackupRestoreS3(t *testing.T) {
	s3 := startRustFS(t)
	for _, tc := range []struct {
		name     string
		postgres bool
	}{{"sqlite", false}, {"postgres", true}} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := "bk-" + tc.name
			s3.createBucket(t, bucket)
			storage := "s3://" + bucket + "/lake"
			env := s3.env()
			var pg *pgServer
			root := filepath.Join(t.TempDir(), "proj")
			args := append([]string{"init", root}, s3.initFlags(storage)...)
			if tc.postgres {
				pg = startPG(t)
				pg.createDB(t, "bksrc")
				args = append(args, "--catalog", pg.url("bksrc"))
				env = append(env, pwEnv...)
			}
			mustDuct(t, t.TempDir(), env, args...)
			q := func(sql string) { mustDuct(t, root, env, "query", sql) }
			q("CREATE SCHEMA bronze")
			q("CREATE TABLE bronze.big AS SELECT i AS id, 'v' || i AS v, now() AS ts FROM range(3000) r(i)")
			q("DELETE FROM bronze.big WHERE id < 50")
			q("CREATE TABLE bronze.small AS SELECT 1 AS id") // inlined in the catalog
			mustDuct(t, root, env, "role", "create", "analistas")
			mustDuct(t, root, env, "user", "create", "ana", "--role", "analistas", "--password", "pw-ana-123")
			write(t, filepath.Join(root, "pipelines", "p.sql"), "CREATE MATERIALIZED VIEW gold.big AS SELECT * FROM bronze.big;\n")
			mustDuct(t, root, env, "run")
			wantBig := csvQuery(t, root, env, "SELECT count(*) AS n, sum(id) AS s FROM gold.big")
			wantSnap := csvQuery(t, root, env, "SELECT max(snapshot_id) AS s FROM ducklake_snapshots('lake')")

			noSecrets := func(what string, r ductRun) {
				t.Helper()
				for _, bad := range []string{s3.secret, pgPassword} {
					if strings.Contains(r.stdout+r.stderr, bad) {
						t.Fatalf("%s printed a secret", what)
					}
				}
			}

			// 1. Backup into the same bucket: data files are copied inside the server.
			r := duct(t, root, env, "backup", "--to", "s3://"+bucket+"/backups")
			if r.err != nil {
				t.Fatalf("backup to s3: %v\n%s\n%s", r.err, r.stdout, r.stderr)
			}
			noSecrets("backup", r)
			ids := s3.keys(t, bucket, "backups/")
			var id string
			for _, k := range ids {
				if strings.HasSuffix(k, "/manifest.json") {
					id = strings.Split(strings.TrimPrefix(k, "backups/"), "/")[0]
				}
			}
			if id == "" {
				t.Fatalf("no manifest under backups/: %v", ids)
			}
			code, body := s3.do(http.MethodGet, "/"+bucket+"/backups/"+id+"/manifest.json", nil)
			if code != 200 {
				t.Fatalf("manifest: %d", code)
			}
			var m backup.Manifest
			if err := json.Unmarshal([]byte(body), &m); err != nil {
				t.Fatal(err)
			}
			wantCat := "sqlite"
			if tc.postgres {
				wantCat = "postgres"
			}
			if m.CatalogType != wantCat || m.StorageKind != "s3" || m.CatalogOnly || len(m.LakeFiles) == 0 {
				t.Fatalf("manifest: %+v", m)
			}
			serverSide := 0
			for _, f := range m.Files {
				if f.Kind == "data" {
					if f.SHA256 == "" {
						serverSide++
					}
					if f.Size <= 0 {
						t.Errorf("data file without size: %+v", f)
					}
				}
			}
			if serverSide != len(m.LakeFiles) {
				t.Errorf("%d of %d data files were not copied server-side", len(m.LakeFiles)-serverSide, len(m.LakeFiles))
			}
			for _, lf := range m.LakeFiles {
				found := false
				for _, k := range ids {
					found = found || k == "backups/"+id+"/data/"+lf.Path
				}
				if !found {
					t.Errorf("lake file %s not in the backup", lf.Path)
				}
			}

			// 2. Backup to a local folder (streamed) and catalog-only into S3.
			local := filepath.Join(t.TempDir(), "local-backups")
			noSecrets("backup to folder", duct(t, root, env, "backup", "--to", local))
			if out := mustDuct(t, root, env, "backup", "--to", "s3://"+bucket+"/backups-co", "--catalog-only"); !strings.Contains(out, "catalog only") {
				t.Fatalf("catalog-only: %s", out)
			}
			for _, k := range s3.keys(t, bucket, "backups-co/") {
				if strings.Contains(k, "/data/") {
					t.Fatalf("catalog-only backup holds data: %s", k)
				}
			}

			// 3. Restore from S3 into a new catalog and a new prefix.
			restoreArgs := func(from, dir, prefix, db string) ([]string, []string) {
				a := []string{"restore", "--from", from, "--storage", "s3://" + bucket + "/" + prefix, dir}
				if tc.postgres {
					pg.createDB(t, db)
					a = append(a[:len(a)-1], "--catalog", pg.url(db), dir)
				}
				return a, env
			}
			check := func(dir string) {
				t.Helper()
				if got := csvQuery(t, dir, env, "SELECT count(*) AS n, sum(id) AS s FROM gold.big"); got != wantBig {
					t.Errorf("gold.big after restore = %q, want %q", got, wantBig)
				}
				if got := csvQuery(t, dir, env, "SELECT id FROM bronze.small"); got != "id\n1" {
					t.Errorf("inlined table after restore = %q", got)
				}
				if got := csvQuery(t, dir, env, "SELECT max(snapshot_id) AS s FROM ducklake_snapshots('lake')"); got != wantSnap {
					t.Errorf("snapshot after restore = %q, want %q", got, wantSnap)
				}
				if out := mustDuct(t, dir, env, "user", "list"); !strings.Contains(out, "ana") {
					t.Errorf("users after restore: %q", out)
				}
			}
			a, e := restoreArgs("s3://"+bucket+"/backups", filepath.Join(t.TempDir(), "r1"), "restored1", "bkr1")
			r = duct(t, t.TempDir(), e, a...)
			if r.err != nil || !strings.Contains(r.stdout, "verified:") {
				t.Fatalf("restore from s3: %v\n%s\n%s", r.err, r.stdout, r.stderr)
			}
			noSecrets("restore", r)
			check(a[len(a)-1])
			if len(s3.keys(t, bucket, "restored1/")) == 0 {
				t.Error("the restore wrote no data files to the new prefix")
			}

			// 4. Restore from the local folder (streamed up) into another prefix.
			a, e = restoreArgs(local, filepath.Join(t.TempDir(), "r2"), "restored2", "bkr2")
			if r := duct(t, t.TempDir(), e, a...); r.err != nil || !strings.Contains(r.stdout, "verified:") {
				t.Fatalf("restore from folder: %v\n%s\n%s", r.err, r.stdout, r.stderr)
			}
			check(a[len(a)-1])

			// 5. Catalog-only: restores where the data is, refuses where it is not.
			a, e = restoreArgs("s3://"+bucket+"/backups-co", filepath.Join(t.TempDir(), "r3"), "empty-prefix", "bkr3")
			r = duct(t, t.TempDir(), e, a...)
			if r.err == nil || !strings.Contains(r.stderr, "catalog-only backup is not at") {
				t.Fatalf("catalog-only restore without data: err=%v\n%s", r.err, r.stderr)
			}
			a, e = restoreArgs("s3://"+bucket+"/backups-co", filepath.Join(t.TempDir(), "r4"), "lake", "bkr4")
			if r := duct(t, t.TempDir(), e, a...); r.err != nil || !strings.Contains(r.stdout, "verified:") {
				t.Fatalf("catalog-only restore in place: %v\n%s\n%s", r.err, r.stdout, r.stderr)
			}
			check(a[len(a)-1])

			// 6. The source project was never touched by any of it.
			if got := csvQuery(t, root, env, "SELECT count(*) AS n, sum(id) AS s FROM gold.big"); got != wantBig {
				t.Errorf("source changed: %q", got)
			}
			if _, err := os.Stat(filepath.Join(root, "lake")); err == nil {
				t.Error("a local lake/ appeared in the source project")
			}
		})
	}
}
