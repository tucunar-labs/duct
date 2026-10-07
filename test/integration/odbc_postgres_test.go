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
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
	_ "github.com/alonsoburon/duct/modules/engine-duckdb"
	_ "github.com/alonsoburon/duct/modules/source-odbc"
)

const pgDriver = "/usr/lib/psqlodbcw.so"

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

func psql(t *testing.T, id, sql string) {
	t.Helper()
	podman(t, "exec", id, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", sql)
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
	if _, err := os.Stat(pgDriver); err != nil {
		t.Skip("psqlODBC driver not installed: " + pgDriver)
	}
	if _, err := os.Stat("/usr/lib/libodbc.so.2"); err != nil {
		if _, err := os.Stat("/usr/lib/x86_64-linux-gnu/libodbc.so.2"); err != nil {
			t.Skip("unixODBC (libodbc.so.2) not installed")
		}
	}
	if _, err := exec.LookPath("podman"); err != nil {
		t.Skip("podman not available")
	}
	port := freePort(t)
	id := podman(t, "run", "-d", "--rm", "-e", "POSTGRES_PASSWORD=pw", "-p", fmt.Sprintf("127.0.0.1:%d:5432", port), "docker.io/library/postgres:17-bookworm")
	t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", id).Run() })
	for i := 0; ; i++ {
		if exec.Command("podman", "exec", id, "psql", "-U", "postgres", "-c", "select 1").Run() == nil {
			break
		}
		if i > 150 {
			t.Fatal("postgres did not start")
		}
		time.Sleep(time.Second)
	}
	psql(t, id, "CREATE TABLE ventas (id int, monto numeric(10,2), ts timestamp); INSERT INTO ventas VALUES (1, 10.5, '2024-01-01 10:00'), (2, 20, '2024-01-02 10:00')")

	t.Setenv("PGPW", "pw")
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
			"connection": fmt.Sprintf("Driver=%s;Server=127.0.0.1;Port=%d;Database=postgres;Uid=postgres;Pwd=${PGPW}", pgDriver, port),
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
	psql(t, id, "INSERT INTO ventas VALUES (3, 30, '2024-01-03 10:00')")
	r := run(1)
	t.Logf("watermark after second run: %q", r.Watermark)
	run(0)
	if got := count(t, eng, "SELECT count(*) FROM bronze.ventas"); got != "3" {
		t.Fatalf("bronze.ventas count = %s", got)
	}
}
