package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/metrics"
	"github.com/alonsoburon/duct/core/project"
)

func quotaCounter(kind string) float64 {
	var b bytes.Buffer
	metrics.Default().WriteText(&b)
	prefix := fmt.Sprintf(`duct_quota_rejections_total{kind=%q} `, kind)
	for _, l := range strings.Split(b.String(), "\n") {
		if strings.HasPrefix(l, prefix) {
			var v float64
			_, _ = fmt.Sscanf(strings.TrimPrefix(l, prefix), "%g", &v)
			return v
		}
	}
	return 0
}

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// setQuota writes the quota in the state database (the server re-reads it within 5 s;
// we wait for it by retrying in the callers).
func setQuota(t *testing.T, root, role string, conc *int, rows *int64) {
	t.Helper()
	ctx := context.Background()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	if err := governance.NewStore(eng).SetQuota(ctx, role, conc, rows); err != nil {
		t.Fatal(err)
	}
}

// TestE2EServePGQuotas: max_rows and max_concurrent also bind the Postgres
// protocol, with SQLSTATE 53400 and the same counters as HTTP/MCP/web.
func TestE2EServePGQuotas(t *testing.T) {
	t.Parallel()
	root := governedProject(t)
	srv := startServe(t, root, nil)
	one, rows1 := 1, int64(1)

	t.Run("max_rows", func(t *testing.T) {
		setQuota(t, root, "analistas", nil, &rows1)
		before := quotaCounter("rows")
		c, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close(context.Background()) }()
		var got error
		for i := 0; i < 20; i++ { // the server re-reads quotas within 5 s
			_, got = pgRows(t, c, "SELECT name FROM customers ORDER BY name")
			if got != nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if pgCode(got) != "53400" || !strings.Contains(got.Error(), "quota exceeded") {
			t.Fatalf("2 rows with max_rows=1: %v (code %q), want 53400 quota error", got, pgCode(got))
		}
		if _, err := pgRows(t, c, "SELECT name FROM customers WHERE name = 'Ana'"); err != nil {
			t.Errorf("1 row within the limit: %v", err)
		}
		// extended protocol, same limit
		if _, err := c.Exec(context.Background(), "SELECT name FROM customers ORDER BY name"); pgCode(err) != "53400" {
			t.Errorf("extended protocol: %v, want 53400", err)
		}
		if quotaCounter("rows") <= before {
			t.Errorf("duct_quota_rejections_total{kind=rows} did not increase (%v)", before)
		}
		setQuota(t, root, "analistas", nil, nil)
	})

	t.Run("max_concurrent", func(t *testing.T) {
		setQuota(t, root, "analistas", &one, nil)
		before := quotaCounter("concurrent")
		a, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = a.Close(context.Background()) }()
		b, err := pgConnect(t, srv.pg, "ana", "pw-ana-123", "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = b.Close(context.Background()) }()
		other, err := pgConnect(t, srv.pg, "nadie", "pw-nadie-123", "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = other.Close(context.Background()) }()

		// wait until the server dropped the previous subtest's max_rows (5 s cache)
		for i := 0; ; i++ {
			rs, err := pgRows(t, b, "SELECT name FROM customers")
			if err == nil && len(rs) == 2 {
				break
			}
			if i > 40 {
				t.Fatalf("max_rows quota never cleared: %v", err)
			}
			time.Sleep(250 * time.Millisecond)
		}

		// A holds its slot with a long statement (cross join of the 2 visible rows 36 times).
		var sb strings.Builder
		sb.WriteString("SELECT count(*) FROM customers c0")
		for i := 1; i < 36; i++ {
			fmt.Fprintf(&sb, ", customers c%d", i)
		}
		slow := sb.String()
		// Start A and make sure it actually holds the role's only slot before B
		// probes: B's probes share the role, so if a probe wins the slot first A
		// is the one rejected with 53400. Retry A until it is in flight.
		var actx context.Context
		var cancelA context.CancelFunc
		var done chan error
		for attempt := 0; ; attempt++ {
			actx, cancelA = context.WithCancel(context.Background())
			done = make(chan error, 1)
			go func(ctx context.Context, ch chan<- error) {
				_, err := a.Exec(ctx, slow, pgx.QueryExecModeSimpleProtocol)
				ch <- err
			}(actx, done)
			select {
			case err := <-done:
				cancelA()
				if attempt >= 10 {
					t.Fatalf("slow statement A never got the slot: %v", err)
				}
				time.Sleep(300 * time.Millisecond)
				continue
			case <-time.After(1500 * time.Millisecond):
			}
			break
		}
		var got error
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			_, got = pgRows(t, b, "SELECT name FROM customers")
			if pgCode(got) == "53400" {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if pgCode(got) != "53400" {
			cancelA()
			var aErr error
			select {
			case aErr = <-done:
			case <-time.After(5 * time.Second):
				aErr = errors.New("still running")
			}
			t.Fatalf("second concurrent statement: %v, want 53400 (slow statement A ended with: %v)", got, aErr)
		}
		if quotaCounter("concurrent") <= before {
			t.Errorf("duct_quota_rejections_total{kind=concurrent} did not increase")
		}
		// another role is not affected while A runs
		if _, err := pgRows(t, other, "SELECT 1"); pgCode(err) == "53400" {
			t.Errorf("quota leaked to another role: %v", err)
		}
		cancelA()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("the long statement did not stop after cancel")
		}
		// the slot is back
		var last error
		for i := 0; i < 40; i++ {
			if _, last = pgRows(t, b, "SELECT name FROM customers"); last == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if last != nil {
			t.Errorf("after the slot was released: %v", last)
		}
		setQuota(t, root, "analistas", nil, nil)
	})
}
