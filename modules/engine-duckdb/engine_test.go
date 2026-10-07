package engineduckdb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/s3conn"
)

func open(t *testing.T, locked bool) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	e := &Engine{}
	err := e.Init(context.Background(), contract.Config{
		"catalog":   filepath.Join(dir, "catalog.sqlite"),
		"data_path": filepath.Join(dir, "lake"),
		"state":     filepath.Join(dir, "duct.sqlite"),
		"locked":    locked,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, dir
}

func scalar(t *testing.T, e *Engine, q string) string {
	t.Helper()
	st, err := e.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer st.Release()
	if !st.Next() {
		t.Fatalf("query %q: no rows (%v)", q, st.Err())
	}
	return strings.Clone(st.RecordBatch().Column(0).ValueStr(0))
}

func TestLakeRoundTrip(t *testing.T) {
	e, dir := open(t, false)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE TABLE t (id INTEGER, name VARCHAR)",
		"INSERT INTO t VALUES (1, 'a'), (2, 'b')",
		// DuckLake inlines tiny inserts in the catalog; this one writes Parquet.
		"INSERT INTO t SELECT i, 'x' FROM range(3, 2003) r(i)",
	} {
		if err := e.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if got := scalar(t, e, "SELECT count(*) FROM t"); got != "2002" {
		t.Fatalf("count = %s", got)
	}
	// Parquet landed under the lake dir.
	matches, _ := filepath.Glob(filepath.Join(dir, "lake", "main", "t", "*.parquet"))
	if len(matches) == 0 {
		t.Fatal("no parquet file in lake/main/t")
	}
	// Catalog is in WAL mode (header bytes 18 and 19 are 2).
	hdr, err := os.ReadFile(filepath.Join(dir, "catalog.sqlite"))
	if err != nil || hdr[18] != 2 || hdr[19] != 2 {
		t.Fatalf("catalog not in WAL mode: %v", err)
	}
	// Duct-owned tables live in the separate catalog.
	if err := e.Exec(ctx, "CREATE TABLE duct.duct_x (a INTEGER)"); err != nil {
		t.Fatal(err)
	}
}

func TestExecTxRollsBack(t *testing.T) {
	e, _ := open(t, false)
	ctx := context.Background()
	if err := e.Exec(ctx, "CREATE TABLE duct.k (a INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	err := e.ExecTx(ctx, "INSERT INTO duct.k VALUES (1)", "INSERT INTO duct.k VALUES (1)")
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
	if got := scalar(t, e, "SELECT count(*) FROM duct.k"); got != "0" {
		t.Fatalf("rows after rollback = %s", got)
	}
}

func TestLockedMode(t *testing.T) {
	// Prepare a lake unlocked, then reopen locked on the same files.
	e, dir := open(t, false)
	ctx := context.Background()
	if err := e.Exec(ctx, "CREATE TABLE t (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := e.Exec(ctx, "INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	_ = e.Close()

	l := &Engine{}
	err := l.Init(ctx, contract.Config{
		"catalog":   filepath.Join(dir, "catalog.sqlite"),
		"data_path": filepath.Join(dir, "lake"),
		"state":     filepath.Join(dir, "duct.sqlite"),
		"locked":    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	// Reading and writing lake tables still works.
	if got := scalar(t, l, "SELECT count(*) FROM t"); got != "1" {
		t.Fatalf("count = %s", got)
	}
	if err := l.Exec(ctx, "INSERT INTO t SELECT * FROM range(2, 2002)"); err != nil {
		t.Fatalf("write in locked mode: %v", err)
	}
	if got := scalar(t, l, "SELECT count(*) FROM t"); got != "2001" {
		t.Fatalf("count = %s", got)
	}
	// Everything else is denied.
	for _, q := range []string{
		"SELECT * FROM read_csv('/etc/passwd')",
		"COPY (SELECT 1) TO '/tmp/duct-locked-escape.csv'",
		"SET enable_external_access = true",
		"SET allowed_directories = ['/']",
		"INSTALL httpfs",
		"ATTACH '/tmp/duct-locked-other.db' AS x",
	} {
		if err := l.Exec(ctx, q); err == nil {
			t.Errorf("locked mode allowed: %s", q)
		}
	}
	if _, err := os.Stat("/tmp/duct-locked-escape.csv"); err == nil {
		t.Error("locked mode wrote outside the lake")
		_ = os.Remove("/tmp/duct-locked-escape.csv")
	}
	err = l.Exec(ctx, "SELECT * FROM read_csv('/etc/passwd')")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "permission") {
		t.Errorf("read_csv error should be a permission error, got: %v", err)
	}
}

func TestInitConfigErrors(t *testing.T) {
	for name, cfg := range map[string]contract.Config{
		"missing":       {},
		"relative":      {"catalog": "c.sqlite", "data_path": "/tmp/x"},
		"locked remote": {"catalog": "/tmp/c.sqlite", "data_path": "s3://b/p", "locked": true},
	} {
		if err := (&Engine{}).Init(context.Background(), cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	e := &Engine{}
	if err := e.Close(); err != nil || e.Close() != nil {
		t.Fatal("Close must be idempotent and safe without Init")
	}
	if err := e.Exec(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("Exec after Close must fail")
	}
}

func TestParseConfigPostgres(t *testing.T) {
	c, err := parseConfig(contract.Config{
		"catalog":   "postgres://u:p@h:5433/db",
		"data_path": "/tmp/lake",
		"state":     "postgres://u:p@h:5433/db",
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.pgCatalog["host"] != "h" || c.pgState == nil || c.stateSchema != "duct" || c.catalog != "" {
		t.Fatalf("config: %+v", c)
	}
	exts := strings.Join(loadStatements(c), ";")
	if !strings.Contains(exts, "LOAD postgres") {
		t.Errorf("postgres extension not loaded: %s", exts)
	}
	if _, err := parseConfig(contract.Config{"catalog": "postgres://nodb", "data_path": "/tmp/lake"}); err == nil {
		t.Error("a catalog without database must fail")
	}
	// The password never survives in an error message.
	if got := c.scrub(os.ErrInvalid); got != os.ErrInvalid {
		t.Errorf("scrub changed a clean error: %v", got)
	}
	if got := c.scrub(&scrubErr{"failed for postgres://u:p@h:5433/db"}); strings.Contains(got.Error(), ":p@") {
		t.Errorf("scrub kept the password: %v", got)
	}
}

type scrubErr struct{ s string }

func (e *scrubErr) Error() string { return e.s }

func TestParseConfigS3(t *testing.T) {
	base := func() contract.Config {
		return contract.Config{"catalog": "/tmp/c.sqlite", "data_path": "s3://lake/prod"}
	}
	if _, err := parseConfig(base()); err == nil || !strings.Contains(err.Error(), "s3 key") {
		t.Errorf("s3 data_path without s3 key: %v", err)
	}
	cfg := base()
	cfg["s3"] = s3conn.Config{Endpoint: "h:9000", KeyID: "a", Secret: "b"}
	c, err := parseConfig(cfg)
	if err != nil || c.localData || c.s3Scope != "s3://lake/prod/" {
		t.Fatalf("parse: %+v %v", c, err)
	}
	if got := lockStatements(c)[0]; got != "SET allowed_directories = ['s3://lake/prod/']" {
		t.Errorf("lock: %s", got)
	}
	if !strings.Contains(strings.Join(loadStatements(c), ";"), "LOAD httpfs") {
		t.Error("httpfs not loaded for s3 storage")
	}
	cfg["s3"] = s3conn.Config{}
	if _, err := parseConfig(cfg); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Errorf("missing credentials: %v", err)
	}
	bad := base()
	bad["data_path"] = "gs://b/p"
	bad["s3"] = s3conn.Config{KeyID: "a", Secret: "b"}
	if _, err := parseConfig(bad); err == nil {
		t.Error("gs:// accepted")
	}
}
