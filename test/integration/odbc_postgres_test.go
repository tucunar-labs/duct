//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
	_ "github.com/alonsoburon/duct/modules/engine-duckdb"
	_ "github.com/alonsoburon/duct/modules/source-odbc"
)

// pgDriver is the psqlODBC driver: Arch puts it in /usr/lib, Debian and Ubuntu
// (apt install odbc-postgresql) under the multiarch folder.
var pgDriver = func() string {
	for _, p := range []string{"/usr/lib/psqlodbcw.so", "/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so", "/usr/lib/aarch64-linux-gnu/odbc/psqlodbcw.so"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}()

func podman(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("podman", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("podman %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func count(t *testing.T, eng contract.Engine, sql string) string {
	t.Helper()
	st, err := eng.Query(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Release()
	if !st.Next() {
		t.Fatalf("no rows: %s", sql)
	}
	return st.RecordBatch().Column(0).ValueStr(0)
}

// TestODBCPostgresIncremental: Postgres in Podman read through psqlODBC and
// DuckDB's odbc_scanner; incremental by watermark, state saved on success.
func TestODBCPostgresIncremental(t *testing.T) {
	if pgDriver == "" {
		t.Skip("psqlODBC driver not installed (apt install odbc-postgresql)")
	}
	if _, err := os.Stat("/usr/lib/libodbc.so.2"); err != nil {
		if _, err := os.Stat("/usr/lib/x86_64-linux-gnu/libodbc.so.2"); err != nil {
			t.Skip("unixODBC (libodbc.so.2) not installed")
		}
	}
	pg := startPG(t) // Podman, or DUCT_TEST_PG_DSN (CI service container)
	pg.createDB(t, "odbcsrc")
	pg.sql(t, "odbcsrc", "CREATE TABLE ventas (id int, monto numeric(10,2), ts timestamp); INSERT INTO ventas VALUES (1, 10.5, '2024-01-01 10:00'), (2, 20, '2024-01-02 10:00')")

	t.Setenv("PGPW", pg.password)
	root := t.TempDir()
	p, err := project.Scaffold(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	info, _ := registry.Lookup(contract.KindEngine, "duckdb")
	eng := info.New().(contract.Engine)
	if err := eng.Init(ctx, p.EngineConfig(false)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		t.Fatal(err)
	}
	cfg := ingest.Config{
		Name: "ventas", Source: "odbc", Mode: ingest.ModeIncremental, Watermark: "ts",
		Options: map[string]any{
			"connection": fmt.Sprintf("Driver=%s;Server=%s;Port=%d;Database=odbcsrc;Uid=%s;Pwd=${PGPW}", pgDriver, pg.host, pg.port, pg.user),
			"table":      "public.ventas",
		},
	}
	run := func(want int64) ingest.Result {
		t.Helper()
		r, err := ingest.Run(ctx, eng, root, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if r.Rows != want {
			t.Fatalf("rows = %d, want %d (%+v)", r.Rows, want, r)
		}
		return r
	}
	run(2)
	pg.sql(t, "odbcsrc", "INSERT INTO ventas VALUES (3, 30, '2024-01-03 10:00')")
	r := run(1)
	t.Logf("watermark after second run: %q", r.Watermark)
	run(0)
	if got := count(t, eng, "SELECT count(*) FROM bronze.ventas"); got != "3" {
		t.Fatalf("bronze.ventas count = %s", got)
	}
}
