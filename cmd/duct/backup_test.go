package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/alonsoburon/duct/core/backup"
)

func readManifest(t *testing.T, dir string) backup.Manifest {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, backup.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	var m backup.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func backupIDs(t *testing.T, root string) []string {
	t.Helper()
	ents, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, e := range ents {
		ids = append(ids, e.Name())
	}
	return ids
}

// TestE2EBackupRestoreSQLite: SQLite catalog + local lake, with history (time
// travel), deletes, inlined rows, governance state and project files; backup,
// restore into an empty folder and compare everything.
func TestE2EBackupRestoreSQLite(t *testing.T) {
	root := governedProject(t)
	// More shapes: types, a table with deletes, inlined rows, history.
	q := func(sql string) { mustDuct(t, "query", "--project", root, sql) }
	q("CREATE TABLE main.kinds (a INTEGER, b VARCHAR, c TIMESTAMP, d DOUBLE, e BOOLEAN, f DATE, g DECIMAL(10,2))")
	q("INSERT INTO main.kinds VALUES (1, 'x', TIMESTAMP '2026-01-02 03:04:05', 1.5, true, DATE '2026-01-02', 3.25)")
	q("CREATE TABLE main.big AS SELECT i AS id, 'row ' || i AS v FROM range(5000) r(i)")
	q("DELETE FROM main.big WHERE id < 100")
	mid := strings.TrimPrefix(queryCSV(t, root, "SELECT max(snapshot_id) AS s FROM ducklake_snapshots('lake')"), "s\n")
	q("INSERT INTO main.big SELECT i + 10000, 'late' FROM range(3) r(i)")
	writeFile(t, filepath.Join(root, "notes", "readme.txt"), "keep me")
	writeFile(t, filepath.Join(root, ".env"), "TOP_SECRET=hunter2\n")
	snapBefore := queryCSV(t, root, "SELECT max(snapshot_id) AS s FROM ducklake_snapshots('lake')")
	bigBefore := queryCSV(t, root, "SELECT count(*) AS n, sum(id) AS s FROM main.big")
	oldBefore := queryCSV(t, root, "SELECT count(*) AS n FROM main.big AT (VERSION => "+mid+")")

	dest := filepath.Join(t.TempDir(), "backups")
	out := mustDuct(t, "backup", "--project", root, "--to", dest)
	if !strings.Contains(out, "done") || !strings.Contains(out, "snapshot") {
		t.Fatalf("backup output: %s", out)
	}
	ids := backupIDs(t, dest)
	if len(ids) != 1 {
		t.Fatalf("backups: %v", ids)
	}
	bdir := filepath.Join(dest, ids[0])
	m := readManifest(t, bdir)
	if m.Format != 1 || m.CatalogType != "sqlite" || m.StorageKind != "local" || m.CatalogOnly {
		t.Errorf("manifest: %+v", m)
	}
	if got := queryCSV(t, root, "SELECT max(snapshot_id) AS s FROM ducklake_snapshots('lake')"); got != snapBefore {
		t.Errorf("backup changed the lake: snapshot %s -> %s", snapBefore, got)
	}
	if snapBefore != "s\n"+itoa(m.SnapshotID) {
		t.Errorf("manifest snapshot %d, lake says %q", m.SnapshotID, snapBefore)
	}
	if len(m.Tables) < 4 || len(m.LakeFiles) == 0 || len(m.Migrations) == 0 {
		t.Errorf("manifest too thin: %d tables, %d lake files, migrations %v", len(m.Tables), len(m.LakeFiles), m.Migrations)
	}
	for _, f := range m.Files {
		if f.SHA256 == "" || f.Size < 0 {
			t.Errorf("file without checksum: %+v", f)
		}
		if strings.Contains(f.Path, ".env") || strings.Contains(f.Path, "web_session") || strings.HasPrefix(f.Path, "project/.duct/") {
			t.Errorf("a secret or state file went into the backup: %s", f.Path)
		}
	}
	if _, err := os.Stat(filepath.Join(bdir, "project", "notes", "readme.txt")); err != nil {
		t.Errorf("project file missing: %v", err)
	}
	if got := mustDuct(t, "backup", "list", "--project", root, "--to", dest); !strings.Contains(got, ids[0]+"  complete") {
		t.Errorf("backup list: %q", got)
	}

	// Restore into an empty folder and compare.
	target := filepath.Join(t.TempDir(), "restored")
	rout := mustDuct(t, "restore", "--from", dest, "--force", target)
	if !strings.Contains(rout, "verified: snapshot "+itoa(m.SnapshotID)) || !strings.Contains(rout, "restore done") {
		t.Fatalf("restore output: %s", rout)
	}
	if got := queryCSV(t, target, "SELECT count(*) AS n, sum(id) AS s FROM main.big"); got != bigBefore {
		t.Errorf("big after restore = %q, want %q", got, bigBefore)
	}
	if got := queryCSV(t, target, "SELECT count(*) AS n FROM main.big AT (VERSION => "+mid+")"); got != oldBefore {
		t.Errorf("time travel after restore = %q, want %q", got, oldBefore)
	}
	if got := queryCSV(t, target, "SELECT a, b, CAST(c AS VARCHAR) AS c, d, e, CAST(f AS VARCHAR) AS f, g FROM main.kinds"); got != "a,b,c,d,e,f,g\n1,x,2026-01-02 03:04:05,1.5,true,2026-01-02,3.25" {
		t.Errorf("types after restore: %q", got)
	}
	if got := queryCSV(t, target, "SELECT max(snapshot_id) AS s FROM ducklake_snapshots('lake')"); got != snapBefore {
		t.Errorf("snapshot after restore = %q, want %q", got, snapBefore)
	}
	if b, err := os.ReadFile(filepath.Join(target, "notes", "readme.txt")); err != nil || string(b) != "keep me" {
		t.Errorf("project file after restore: %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(target, ".env")); err == nil {
		t.Error(".env must not be restored")
	}
	if got := mustDuct(t, "user", "list", "--project", target); !strings.Contains(got, "ana") || !strings.Contains(got, "baja") {
		t.Errorf("users after restore: %q", got)
	}
	// The restored project keeps working (writes) and the governed views still filter.
	mustDuct(t, "query", "--project", target, "INSERT INTO main.big VALUES (-1, 'after restore')")
	if got := queryCSV(t, target, "SELECT count(*) AS n FROM main.big"); got == bigBefore {
		t.Errorf("restored lake does not accept writes")
	}
	srv := startServe(t, target, nil)
	c := api{t: t, base: srv.http}
	if code, body := c.query("", "ana", "pw-ana-123", "SELECT name, rut FROM customers ORDER BY name"); code != 200 || !strings.Contains(body, "Ana") || strings.Contains(body, "Beto") || !strings.Contains(body, "***") {
		t.Errorf("governed query on the restored project: %d %s", code, body)
	}
	srv.stop()

	// Refusals.
	if _, errs, code := duct(t, "restore", "--from", dest, "--force", target); code == 0 || !strings.Contains(errs, "not empty") {
		t.Errorf("restore into a used folder: code=%d %q", code, errs)
	}
	if _, errs, code := duct(t, "backup", "--project", root, "--to", dest, "--catalog-only"); code == 0 || !strings.Contains(errs, "s3://") {
		t.Errorf("catalog-only on a local lake: code=%d %q", code, errs)
	}
	if _, errs, code := duct(t, "backup", "--project", root, "--to", filepath.Join(root, "inside")); code == 0 || !strings.Contains(errs, "inside the project") {
		t.Errorf("backup inside the project: code=%d %q", code, errs)
	}

	// A damaged data file is caught, not restored.
	var data string
	for _, f := range m.Files {
		if f.Kind == "data" {
			data = filepath.Join(bdir, filepath.FromSlash(f.Path))
			break
		}
	}
	orig, err := os.ReadFile(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(data, append([]byte("x"), orig[1:]...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, errs, code := duct(t, "restore", "--from", bdir, "--force", filepath.Join(t.TempDir(), "bad")); code == 0 || !strings.Contains(errs, "does not match") {
		t.Errorf("restore of a damaged backup: code=%d %q", code, errs)
	}
	if err := os.WriteFile(data, orig, 0o600); err != nil {
		t.Fatal(err)
	}

	// Retention: a second and third backup with --keep 2 leave the newest two.
	mustDuct(t, "backup", "--project", root, "--to", dest)
	third := mustDuct(t, "backup", "--project", root, "--to", dest, "--keep", "2")
	if !strings.Contains(third, "retention: removed backup "+ids[0]) {
		t.Errorf("retention output: %s", third)
	}
	if left := backupIDs(t, dest); len(left) != 2 || left[0] == ids[0] {
		t.Errorf("after --keep 2: %v (first was %s)", left, ids[0])
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
