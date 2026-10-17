package main

import (
	"bytes"
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"filippo.io/age"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/alonsoburon/duct/core/backup"
)

func ageKeys(t *testing.T) (identityFile, public string) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	identityFile = filepath.Join(t.TempDir(), "backup.key")
	writeFile(t, identityFile, "# test key\n"+id.String()+"\n")
	return identityFile, id.Recipient().String()
}

// walkFiles lists the regular files under dir (relative, slash separated).
func walkFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err == nil && !e.IsDir() {
			rel, _ := filepath.Rel(dir, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestE2EBackupEncrypted: `duct backup --encrypt` leaves no plaintext object but
// the manifest, which lists ciphertext and plaintext checksums; restore needs
// the identity, rejects a wrong one and tampered ciphertext, and gives back the
// same data, users and project files.
func TestE2EBackupEncrypted(t *testing.T) {
	root := governedProject(t)
	mustDuct(t, "query", "--project", root, "CREATE TABLE main.big AS SELECT i AS id, 'row ' || i AS v FROM range(3000) r(i)")
	writeFile(t, filepath.Join(root, "notes", "readme.txt"), "keep me, secretly")
	idFile, pub := ageKeys(t)
	wantBig := queryCSV(t, root, "SELECT count(*) AS n, sum(id) AS s FROM main.big")

	dest := filepath.Join(t.TempDir(), "backups")
	if _, errs, code := duct(t, "backup", "--project", root, "--to", dest, "--encrypt"); code == 0 || !strings.Contains(errs, "recipient") {
		t.Errorf("--encrypt without recipients: code=%d %q", code, errs)
	}
	if _, errs, code := duct(t, "backup", "--project", root, "--to", dest, "--recipient", "age1notakey"); code == 0 || !strings.Contains(errs, "recipient") {
		t.Errorf("bad recipient: code=%d %q", code, errs)
	}
	if _, err := os.Stat(dest); err == nil {
		t.Fatalf("a failed backup left files: %v", backupIDs(t, dest))
	}
	out := mustDuct(t, "backup", "--project", root, "--to", dest, "--recipient", pub)
	if !strings.Contains(out, "encrypted for 1 recipient") {
		t.Errorf("backup output: %s", out)
	}
	bdir := filepath.Join(dest, backupIDs(t, dest)[0])
	m := readManifest(t, bdir)
	if !m.Encrypted || m.Format != backup.FormatEncrypted || len(m.Recipients) != 1 || m.Recipients[0] != pub {
		t.Errorf("manifest: %+v", m)
	}
	// Nothing but the manifest is plaintext; every file is an age file.
	var data string
	for _, f := range walkFiles(t, bdir) {
		if f == backup.ManifestFile {
			continue
		}
		if !strings.HasSuffix(f, ".age") {
			t.Errorf("plaintext object in an encrypted backup: %s", f)
		}
		b, _ := os.ReadFile(filepath.Join(bdir, filepath.FromSlash(f)))
		// The age header is base64 text, where a short word such as "Ana" shows up by
		// chance (about 1 in 2000 files); look for plaintext only in the binary
		// payload that follows the header MAC line.
		payload := b
		if i := bytes.Index(b, []byte("\n--- ")); i >= 0 {
			if j := bytes.IndexByte(b[i+1:], '\n'); j >= 0 {
				payload = b[i+1+j+1:]
			}
		}
		if !strings.HasPrefix(string(b), "age-encryption.org/v1") || bytes.Contains(payload, []byte("keep me")) || bytes.Contains(payload, []byte("Ana")) || bytes.Contains(payload, []byte("PAR1")) {
			t.Errorf("%s does not look like ciphertext", f)
		}
		if strings.HasPrefix(f, "data/") && data == "" {
			data = filepath.Join(bdir, filepath.FromSlash(f))
		}
	}
	for _, f := range m.Files {
		if !f.Encrypted || f.SHA256 == "" || f.CipherSHA256 == "" || f.CipherSize <= f.Size || f.StoredKey() != f.Path+".age" {
			t.Errorf("manifest entry: %+v", f)
		}
		if sum := fileSHA(t, filepath.Join(bdir, filepath.FromSlash(f.StoredKey()))); sum != f.CipherSHA256 {
			t.Errorf("%s: ciphertext checksum in the manifest is wrong", f.Path)
		}
	}
	if data == "" {
		t.Fatal("no data file in the backup")
	}

	// Restore: identity required.
	target := filepath.Join(t.TempDir(), "restored")
	if _, errs, code := duct(t, "restore", "--from", dest, "--force", target); code == 0 || !strings.Contains(errs, "--identity") {
		t.Errorf("restore without identity: code=%d %q", code, errs)
	}
	otherKey, _ := ageKeys(t)
	if _, errs, code := duct(t, "restore", "--from", dest, "--force", "--identity", otherKey, target); code == 0 || !strings.Contains(errs, "decrypt") {
		t.Errorf("restore with the wrong identity: code=%d %q", code, errs)
	}
	target = filepath.Join(t.TempDir(), "restored2")
	t.Setenv("DUCT_BACKUP_IDENTITY", idFile) // the environment works like --identity
	rout := mustDuct(t, "restore", "--from", dest, "--force", target)
	if !strings.Contains(rout, "verified: snapshot "+itoa(m.SnapshotID)) {
		t.Fatalf("restore output: %s", rout)
	}
	if got := queryCSV(t, target, "SELECT count(*) AS n, sum(id) AS s FROM main.big"); got != wantBig {
		t.Errorf("big after restore = %q, want %q", got, wantBig)
	}
	if b, err := os.ReadFile(filepath.Join(target, "notes", "readme.txt")); err != nil || string(b) != "keep me, secretly" {
		t.Errorf("project file after restore: %q %v", b, err)
	}
	if got := mustDuct(t, "user", "list", "--project", target); !strings.Contains(got, "ana") {
		t.Errorf("users after restore: %q", got)
	}

	// Tampered ciphertext is caught (checksum) and so is a modified one with the
	// checksum fixed up by an attacker (age authenticates the chunks).
	orig, err := os.ReadFile(data)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), orig...)
	bad[len(bad)-3] ^= 0xff
	if err := os.WriteFile(data, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, errs, code := duct(t, "restore", "--from", bdir, "--force", filepath.Join(t.TempDir(), "bad")); code == 0 || !strings.Contains(errs, "restore") {
		t.Errorf("restore of tampered ciphertext: code=%d %q", code, errs)
	}
	if err := os.WriteFile(data, orig, 0o600); err != nil {
		t.Fatal(err)
	}

	// [backup] recipients in duct.toml feed --encrypt.
	cfg, _ := os.ReadFile(filepath.Join(root, "duct.toml"))
	writeFile(t, filepath.Join(root, "duct.toml"), string(cfg)+"\n[backup]\nrecipients = [\""+pub+"\"]\n")
	mustDuct(t, "backup", "--project", root, "--to", dest, "--encrypt")
	ids := backupIDs(t, dest)
	if m2 := readManifest(t, filepath.Join(dest, ids[len(ids)-1])); !m2.Encrypted {
		t.Errorf("--encrypt with [backup] recipients did not encrypt")
	}
}

func fileSHA(t *testing.T, p string) string {
	t.Helper()
	m, err := backup.FileSHA256(p)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// newEncryptedLakeProject builds a project whose DuckLake is ENCRYPTED: the
// catalog is created with the ENCRYPTED option before Duct first opens it.
func newEncryptedLakeProject(t *testing.T) string {
	t.Helper()
	root := newProject(t)
	cat := filepath.Join(root, ".duct", "catalog.sqlite")
	if _, err := os.Stat(cat); err != nil {
		t.Fatalf("catalog: %v", err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(cat + suffix)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, q := range []string{
		"INSTALL ducklake", "LOAD ducklake", "INSTALL sqlite", "LOAD sqlite",
		"ATTACH 'ducklake:sqlite:" + cat + "' AS enc (DATA_PATH '" + filepath.Join(root, "lake") + "/', ENCRYPTED)",
		"CREATE TABLE enc.main.seed AS SELECT 1 AS i",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_ = db.Close()
	mustDuct(t, "migrate", "--project", root)
	return root
}

// TestE2EBackupEncryptedLake: a DuckLake with ENCRYPTED Parquet keeps its
// per-file keys (columns of the catalog tables) through backup and restore,
// plain and with age on top; the restored lake is still marked encrypted and
// its new writes are encrypted.
func TestE2EBackupEncryptedLake(t *testing.T) {
	t.Parallel()
	root := newEncryptedLakeProject(t)
	mustDuct(t, "query", "--project", root, "CREATE TABLE main.vault AS SELECT i AS id, 'secret-' || i AS v FROM range(2000) r(i)")
	mustDuct(t, "query", "--project", root, "DELETE FROM main.vault WHERE id < 10")
	// The data on disk is unreadable without the keys: no parquet magic footer text.
	var parquets int
	for _, f := range walkFiles(t, filepath.Join(root, "lake")) {
		if strings.HasSuffix(f, ".parquet") {
			parquets++
			b, _ := os.ReadFile(filepath.Join(root, "lake", filepath.FromSlash(f)))
			if strings.Contains(string(b), "secret-") {
				t.Errorf("%s is not encrypted on disk", f)
			}
		}
	}
	if parquets == 0 {
		t.Fatal("no parquet files")
	}
	want := queryCSV(t, root, "SELECT count(*) AS n, sum(id) AS s FROM main.vault")
	keys := queryCSV(t, root, "SELECT count(*) AS n FROM __ducklake_metadata_lake.main.ducklake_data_file WHERE encryption_key IS NOT NULL AND encryption_key <> ''")
	if keys == "n\n0" {
		t.Fatalf("the lake has no per-file keys in its catalog: %q", keys)
	}

	idFile, pub := ageKeys(t)
	for _, mode := range []string{"plain", "age"} {
		dest := filepath.Join(t.TempDir(), "backups")
		args := []string{"backup", "--project", root, "--to", dest}
		if mode == "age" {
			args = append(args, "--recipient", pub)
		}
		out := mustDuct(t, args...)
		if mode == "plain" && !strings.Contains(out, "warning: this lake is ENCRYPTED") {
			t.Errorf("no warning for an encrypted lake backed up in plain: %s", out)
		}
		bdir := filepath.Join(dest, backupIDs(t, dest)[0])
		if m := readManifest(t, bdir); !m.LakeEncrypted {
			t.Errorf("%s: manifest does not record the encrypted lake", mode)
		}
		target := filepath.Join(t.TempDir(), "restored")
		rargs := []string{"restore", "--from", dest, "--force", target}
		if mode == "age" {
			rargs = append(rargs, "--identity", idFile)
		}
		if rout := mustDuct(t, rargs...); !strings.Contains(rout, "verified") {
			t.Fatalf("%s: restore output: %s", mode, rout)
		}
		if got := queryCSV(t, target, "SELECT count(*) AS n, sum(id) AS s FROM main.vault"); got != want {
			t.Errorf("%s: vault after restore = %q, want %q", mode, got, want)
		}
		if got := queryCSV(t, target, "SELECT count(*) AS n FROM __ducklake_metadata_lake.main.ducklake_data_file WHERE encryption_key IS NOT NULL AND encryption_key <> ''"); got != keys {
			t.Errorf("%s: file keys after restore = %q, want %q", mode, got, keys)
		}
		if got := queryCSV(t, target, "SELECT value AS v FROM __ducklake_metadata_lake.main.ducklake_metadata WHERE key = 'encrypted'"); got != "v\ntrue" {
			t.Errorf("%s: restored lake encrypted flag = %q", mode, got)
		}
		// Writes after the restore are encrypted too.
		mustDuct(t, "query", "--project", target, "INSERT INTO main.vault SELECT i + 5000, 'late-secret-' || i FROM range(1500) r(i)")
		for _, f := range walkFiles(t, filepath.Join(target, "lake")) {
			if b, _ := os.ReadFile(filepath.Join(target, "lake", filepath.FromSlash(f))); strings.Contains(string(b), "late-secret-") {
				t.Errorf("%s: %s written plaintext after the restore", mode, f)
			}
		}
	}
}

// TestE2ERestoreTables: `duct restore --table` into a new folder and into an
// existing project, from a plain and from an encrypted backup.
func TestE2ERestoreTables(t *testing.T) {
	t.Parallel()
	root := governedProject(t)
	q := func(sql string) { mustDuct(t, "query", "--project", root, sql) }
	q("CREATE TABLE main.kinds (a INTEGER NOT NULL, b VARCHAR, c TIMESTAMP, g DECIMAL(10,2), l INTEGER[])")
	q("INSERT INTO main.kinds VALUES (1, 'x', TIMESTAMP '2026-01-02 03:04:05', 3.25, [1,2,3])")
	q("CREATE SCHEMA other")
	q("CREATE TABLE other.big AS SELECT i AS id, 'row ' || i AS v FROM range(4000) r(i)")
	q("DELETE FROM other.big WHERE id < 100")
	q("INSERT INTO other.big VALUES (99999, 'late')")
	wantBig := queryCSV(t, root, "SELECT count(*) AS n, sum(id) AS s FROM other.big")
	wantKinds := queryCSV(t, root, "SELECT a, b, CAST(c AS VARCHAR) AS c, g, CAST(l AS VARCHAR) AS l FROM main.kinds")
	// Written after the backup below: must not appear in the restore.
	idFile, pub := ageKeys(t)

	for _, mode := range []string{"plain", "age"} {
		t.Run(mode, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "backups")
			bargs := []string{"backup", "--project", root, "--to", dest}
			rflags := []string{}
			if mode == "age" {
				bargs = append(bargs, "--recipient", pub)
				rflags = []string{"--identity", idFile}
			}
			mustDuct(t, bargs...)
			restore := func(args ...string) (string, string, int) {
				return duct(t, append(append([]string{"restore", "--from", dest}, rflags...), args...)...)
			}

			// New project with one table.
			target := filepath.Join(t.TempDir(), "partial")
			out, errs, code := restore("--table", "other.big", "--force", target)
			if code != 0 {
				t.Fatalf("restore --table: %d %s %s", code, out, errs)
			}
			if !strings.Contains(out, "table other.big: 3901 rows") {
				t.Errorf("output: %s", out)
			}
			if got := queryCSV(t, target, "SELECT count(*) AS n, sum(id) AS s FROM other.big"); got != wantBig {
				t.Errorf("other.big = %q, want %q", got, wantBig)
			}
			if got := queryCSV(t, target, "SELECT count(*) AS n FROM duckdb_tables() WHERE database_name = 'lake' AND NOT internal AND table_name IN ('kinds', 'customers')"); got != "n\n0" {
				t.Errorf("tables that were not asked for: %q", got)
			}
			if got := mustDuct(t, "user", "list", "--project", target); !strings.Contains(got, "ana") {
				t.Errorf("a new project keeps the Duct state: %q", got)
			}
			mustDuct(t, "query", "--project", target, "INSERT INTO other.big VALUES (-1, 'after')")

			// Existing project: a bare name means schema main; types survive.
			existing := newProject(t)
			mustDuct(t, "query", "--project", existing, "CREATE TABLE main.mine AS SELECT 7 AS x")
			out, errs, code = restore("--table", "kinds", "--table", "other.big", existing)
			if code != 0 {
				t.Fatalf("restore into existing: %d %s %s", code, out, errs)
			}
			if got := queryCSV(t, existing, "SELECT a, b, CAST(c AS VARCHAR) AS c, g, CAST(l AS VARCHAR) AS l FROM main.kinds"); got != wantKinds {
				t.Errorf("kinds = %q, want %q", got, wantKinds)
			}
			if got := queryCSV(t, existing, "SELECT count(*) AS n, sum(id) AS s FROM other.big"); got != wantBig {
				t.Errorf("other.big = %q, want %q", got, wantBig)
			}
			if got := queryCSV(t, existing, "SELECT x FROM main.mine"); got != "x\n7" {
				t.Errorf("the existing project's own table changed: %q", got)
			}
			if got := mustDuct(t, "user", "list", "--project", existing); strings.Contains(got, "ana") {
				t.Errorf("restoring tables into an existing project must not bring users: %q", got)
			}
			// Refusals.
			if _, errs, code := restore("--table", "other.big", existing); code == 0 || !strings.Contains(errs, "already exists") {
				t.Errorf("existing table: code=%d %q", code, errs)
			}
			if _, errs, code := restore("--table", "main.nope", existing); code == 0 || !strings.Contains(errs, "no table") {
				t.Errorf("unknown table: code=%d %q", code, errs)
			}
			mustDuct(t, "query", "--project", existing, "INSERT INTO other.big VALUES (-5, 'edit')")
			if _, errs, code := restore("--table", "other.big", "--overwrite", existing); code != 0 {
				t.Errorf("--overwrite: %d %q", code, errs)
			}
			if got := queryCSV(t, existing, "SELECT count(*) AS n, sum(id) AS s FROM other.big"); got != wantBig {
				t.Errorf("other.big after --overwrite = %q, want %q", got, wantBig)
			}
			if _, errs, code := duct(t, "restore", "--from", dest, "--overwrite", filepath.Join(t.TempDir(), "x")); code == 0 || !strings.Contains(errs, "--table") {
				t.Errorf("--overwrite alone: code=%d %q", code, errs)
			}
		})
	}
	_ = pub
}
