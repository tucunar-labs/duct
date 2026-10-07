package project

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScaffoldAndLoad(t *testing.T) {
	root := filepath.Join(t.TempDir(), "p")
	if _, err := Scaffold(root); err != nil {
		t.Fatal(err)
	}
	p, err := Find(filepath.Join(root, "lake"))
	if err != nil {
		t.Fatal(err)
	}
	if p.Root != root || p.DataPath() != filepath.Join(root, "lake") || p.CatalogPath() != filepath.Join(root, ".duct", "catalog.sqlite") {
		t.Errorf("paths: %+v %s %s", p, p.DataPath(), p.CatalogPath())
	}
	if _, err := Scaffold(root); err == nil {
		t.Error("second Scaffold must fail")
	}
	if _, err := Find(t.TempDir()); err == nil {
		t.Error("Find outside a project must fail")
	}
}

func TestGitignoreMerges(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("node_modules\nlake/"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Scaffold(root); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(root, ".gitignore"))
	s := string(b)
	if !strings.Contains(s, "node_modules\n") || strings.Count(s, "lake/") != 1 || !strings.Contains(s, ".duct/") || !strings.Contains(s, ".env*") {
		t.Errorf(".gitignore = %q", s)
	}
}

func TestRemoteStorage(t *testing.T) {
	p := &Project{Root: "/r", Config: Config{Storage: "s3://b/p", Catalog: "c.sqlite"}}
	if p.DataPath() != "s3://b/p" || p.CatalogPath() != "/r/c.sqlite" {
		t.Errorf("%s %s", p.DataPath(), p.CatalogPath())
	}
}

const mountinfo = `22 1 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw
40 22 0:35 / /mnt/nas rw,relatime shared:5 - nfs4 srv:/export rw
41 22 0:36 / /mnt/my\040share rw,relatime - cifs //srv/s rw
42 22 0:37 / /home/u/remote rw - fuse.sshfs u@h:/ rw
43 40 8:3 / /mnt/nas/local rw - ext4 /dev/sdb1 rw
`

func TestCheckMounts(t *testing.T) {
	for path, bad := range map[string]bool{
		"/home/u/proj":         false,
		"/mnt/nas/proj":        true,
		"/mnt/nas":             true,
		"/mnt/nas/local/proj":  false, // nested local mount wins
		"/mnt/my share/x":      true,
		"/home/u/remote/proj":  true,
		"/mnt/nasty/proj":      false, // prefix is not a path boundary
		"/home/u/remote2/proj": false,
	} {
		err := checkMounts(path, strings.NewReader(mountinfo))
		if (err != nil) != bad {
			t.Errorf("%s: err=%v, want bad=%v", path, err, bad)
		}
	}
}

func TestCheckSyncedPath(t *testing.T) {
	for path, bad := range map[string]bool{
		"/home/u/Dropbox/proj":                     true,
		"/home/u/OneDrive - Acme/proj":             true,
		"/Users/u/Library/CloudStorage/x/proj":     true,
		"/Users/u/Library/Mobile Documents/c/proj": true,
		"/home/u/Google Drive/proj":                true,
		"/home/u/code/proj":                        false,
		"/home/u/dropboxer/proj":                   false,
	} {
		if err := checkSyncedPath(path); (err != nil) != bad {
			t.Errorf("%s: err=%v, want bad=%v", path, err, bad)
		}
	}
}

func TestCheckLocalFolderTemp(t *testing.T) {
	if err := CheckLocalFolder(filepath.Join(t.TempDir(), "new", "deep")); err != nil {
		t.Errorf("temp dir rejected: %v", err)
	}
}

func TestMigrationsAreOrderedAndPortable(t *testing.T) {
	for i, m := range Migrations {
		if m.Version != i+1 {
			t.Errorf("migration %d has version %d: versions must be 1..n without gaps", i, m.Version)
		}
		for _, s := range m.SQL {
			low := strings.ToLower(s)
			if !strings.Contains(low, "duct.duct_") {
				t.Errorf("migration %d: tables must be duct.duct_*: %s", m.Version, s)
			}
			for _, banned := range []string{"autoincrement", "serial", "pragma", "ducklake"} {
				if strings.Contains(low, banned) {
					t.Errorf("migration %d is not portable (%s): %s", m.Version, banned, s)
				}
			}
		}
	}
}

func TestPostgresCatalogConfig(t *testing.T) {
	root := filepath.Join(t.TempDir(), "p")
	p, stripped, err := ScaffoldWithCatalog(root, "postgres://duct:s3cret@db.example:5432/lake")
	if err != nil {
		t.Fatal(err)
	}
	if !stripped || !p.CatalogIsPostgres() || p.CatalogPath() != "" {
		t.Fatalf("stripped=%v pg=%v path=%q", stripped, p.CatalogIsPostgres(), p.CatalogPath())
	}
	toml, _ := os.ReadFile(filepath.Join(root, ConfigFile))
	if strings.Contains(string(toml), "s3cret") || !strings.Contains(string(toml), "postgres://duct@db.example:5432/lake") {
		t.Fatalf("duct.toml: %s", toml)
	}
	if strings.Contains(p.CatalogRef(), "s3cret") || !strings.Contains(p.CatalogRef(), "duct@db.example:5432/lake") {
		t.Errorf("CatalogRef leaks or is wrong: %q", p.CatalogRef())
	}
	// In this process the full string is used; a fresh Load needs the env.
	cfg := p.EngineConfig(false)
	if !strings.Contains(cfg["catalog"].(string), "s3cret") || cfg["state"] != cfg["catalog"] {
		t.Errorf("engine config: %v", cfg)
	}
	p2, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvCatalogPassword, "from-env")
	if dsn, err := p2.CatalogDSN(); err != nil || !strings.Contains(dsn, "duct:from-env@") {
		t.Errorf("DUCT_CATALOG_PASSWORD not applied: %q %v", dsn, err)
	}
	t.Setenv(EnvCatalogURL, "postgres://other:pw@elsewhere/db2")
	if dsn, _ := p2.CatalogDSN(); !strings.Contains(dsn, "other:pw@elsewhere") {
		t.Errorf("DUCT_CATALOG_URL must win: %q", dsn)
	}
}

func TestLoadRejectsBadPostgresCatalog(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ConfigFile), []byte("storage = \"./lake\"\ncatalog = \"postgres://host-without-db\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(root); err == nil {
		t.Fatal("a Postgres catalog without database must be rejected")
	}
}
