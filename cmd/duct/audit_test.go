package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
)

func auditProject(t *testing.T) string {
	t.Helper()
	t.Setenv("USER", "tester")
	root := filepath.Join(t.TempDir(), "demo")
	mustDuct(t, "init", root)
	return root
}

func appendConfig(t *testing.T, root, text string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(root, "duct.toml"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString("\n" + text + "\n"); err != nil {
		t.Fatal(err)
	}
}

func insertAudit(t *testing.T, root string, at time.Time, user, sql string) {
	t.Helper()
	mustDuct(t, "query", "--project", root, fmt.Sprintf(
		"INSERT INTO duct.duct_audit (occurred_at, user_name, role, protocol, sql_text, duration_ms, rows_returned, status, audit_id) VALUES ('%s','%s','r','pg','%s',1,1,'ok','id-%s-%d')",
		at.UTC().Format(project.AuditTimeLayout), user, sql, user, at.UnixNano()))
}

func openAuditEngine(t *testing.T, root string) (contract.Engine, func()) {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := openEngine(context.Background(), p, false)
	if err != nil {
		t.Fatal(err)
	}
	return eng, func() { _ = eng.Close() }
}

func TestAuditCLIRows(t *testing.T) {
	root := auditProject(t)
	mustDuct(t, "query", "--project", root, "CREATE TABLE t (id INTEGER)")
	mustDuct(t, "query", "--project", root, "INSERT INTO t VALUES (1),(2),(3)")
	mustDuct(t, "query", "--project", root, "SELECT * FROM t")
	if _, _, code := duct(t, "query", "--project", root, "SELECT * FROM nope"); code == 0 {
		t.Fatal("expected failure")
	}
	rows := queryRows(t, root, "SELECT user_name, role, protocol, status, rows_returned, sql_text FROM duct.duct_audit WHERE protocol = 'cli' AND sql_text LIKE '%FROM t' OR sql_text LIKE '%nope'")
	if len(rows) != 2 {
		t.Fatalf("cli rows: %v", rows)
	}
	byStatus := map[string]map[string]any{}
	for _, r := range rows {
		byStatus[r["status"].(string)] = r
		if r["user_name"] != "tester" || r["role"] != "local" || r["protocol"] != "cli" {
			t.Errorf("identity: %v", r)
		}
	}
	if byStatus["ok"] == nil || byStatus["ok"]["rows_returned"].(float64) != 3 || byStatus["error"] == nil {
		t.Errorf("ok/error rows: %v", byStatus)
	}
	// duct sql (non governance and governance) and `duct audit query`.
	mustDuct(t, "sql", "--project", root, "CREATE ROLE analistas")
	out := mustDuct(t, "audit", "query", "--project", root, "--user", "tester", "--since", "1h", "--limit", "50")
	for _, want := range []string{"cli", "CREATE ROLE analistas", "SELECT * FROM t"} {
		if !strings.Contains(out, want) {
			t.Errorf("audit query lacks %q:\n%s", want, out)
		}
	}
	if out := mustDuct(t, "audit", "query", "--project", root, "--status", "error", "--format", "json"); !strings.Contains(out, "nope") || strings.Contains(out, "CREATE ROLE") {
		t.Errorf("status filter: %s", out)
	}

	// [audit] cli = false: nothing more is recorded.
	appendConfig(t, root, "[audit]\ncli = false")
	before := count(t, root, "duct.duct_audit")
	mustDuct(t, "query", "--project", root, "SELECT 42")
	mustDuct(t, "sql", "--project", root, "SELECT 43")
	if after := count(t, root, "duct.duct_audit"); after != before {
		t.Errorf("cli=false still audits: %v -> %v", before, after)
	}
}

func TestAuditRedaction(t *testing.T) {
	root := auditProject(t)
	mustDuct(t, "query", "--project", root, "CREATE SECRET s1 (TYPE s3, KEY_ID 'AKIAXXXX', SECRET 'topsecretvalue')")
	mustDuct(t, "query", "--project", root, "SELECT 'x' AS a /* password=hunter2 */")
	rows := queryRows(t, root, "SELECT sql_text FROM duct.duct_audit WHERE protocol = 'cli'")
	for _, r := range rows {
		s := r["sql_text"].(string)
		if strings.Contains(s, "topsecretvalue") || strings.Contains(s, "hunter2") || strings.Contains(s, "AKIAXXXX") {
			t.Errorf("secret stored: %s", s)
		}
	}
}

func TestRedactSQL(t *testing.T) {
	for in, leak := range map[string]string{
		"CREATE SECRET s (TYPE s3, KEY_ID 'a', SECRET 'b-leak')":         "b-leak",
		"create or replace persistent secret x (type s3, secret 'LEAK')": "LEAK",
		"ALTER USER bob PASSWORD 'LEAK'":                                 "LEAK",
		"SELECT * FROM t WHERE password = 'LEAK'":                        "LEAK",
		"ATTACH 'postgres://u:LEAK@h/db' AS x":                           "LEAK",
		"SELECT 1 -- api_key=LEAK123":                                    "LEAK123",
		"SELECT 'Authorization: Bearer abcdefghijklmnop-LEAK'":           "LEAK",
		"CALL f(token := 'x', secret_key: LEAK)":                         "LEAK",
		"SELECT 'dct_" + strings.Repeat("A", 30) + "'":                   "AAAAAAAAAA",
		"ATTACH 'host=h password=LEAK dbname=d' AS p (TYPE postgres)":    "LEAK",
	} {
		if got := governance.RedactSQL(in); strings.Contains(got, leak) {
			t.Errorf("RedactSQL(%q) = %q leaks %q", in, got, leak)
		}
	}
	for _, keep := range []string{"SELECT id, name FROM customers WHERE amount > 10", "SELECT 'tokenize' AS a", "SELECT 1 AS passwords_count"} {
		if got := governance.RedactSQL(keep); got != keep {
			t.Errorf("RedactSQL altered %q -> %q", keep, got)
		}
	}
}

func TestAuditPruneBatches(t *testing.T) {
	root := auditProject(t)
	eng, closeEng := openAuditEngine(t, root)
	defer closeEng()
	now := time.Now()
	for i := range 25 {
		insertAudit(t, root, now.Add(-time.Duration(40+i)*24*time.Hour), "old", "SELECT 1")
	}
	for i := range 5 {
		insertAudit(t, root, now.Add(-time.Duration(i)*time.Hour), "new", "SELECT 2")
	}
	n, err := project.PruneAudit(context.Background(), eng, now.Add(-30*24*time.Hour), 7) // 25 rows in batches of 7
	if err != nil || n != 25 {
		t.Fatalf("prune: %d %v", n, err)
	}
	if n, err := project.PruneAudit(context.Background(), eng, now.Add(-30*24*time.Hour), 7); err != nil || n != 0 {
		t.Fatalf("second prune: %d %v", n, err)
	}
	if got := queryRows(t, root, "SELECT count(*) AS n FROM duct.duct_audit WHERE user_name IN ('old','new') GROUP BY user_name ORDER BY user_name"); len(got) != 1 || got[0]["n"].(float64) != 5 {
		t.Errorf("remaining: %v", got)
	}
	// CLI: prune --older-than, and gc applies [audit] retention (default 90d, 0 = forever).
	insertAudit(t, root, now.Add(-100*24*time.Hour), "ancient", "SELECT 3")
	insertAudit(t, root, now.Add(-45*24*time.Hour), "mid", "SELECT 4")
	out := mustDuct(t, "gc", "--project", root)
	if !strings.Contains(out, "prune audit") || count(t, root, "duct.duct_audit WHERE user_name = 'ancient'") != 0 || count(t, root, "duct.duct_audit WHERE user_name = 'mid'") != 1 {
		t.Errorf("gc default retention:\n%s", out)
	}
	if out := mustDuct(t, "audit", "prune", "--project", root, "--older-than", "30d"); !strings.Contains(out, "pruned 1 ") || count(t, root, "duct.duct_audit WHERE user_name = 'mid'") != 0 {
		t.Errorf("audit prune: %s", out)
	}
	appendConfig(t, root, "[audit]\nretention = 0")
	insertAudit(t, root, now.Add(-1000*24*time.Hour), "forever", "SELECT 5")
	mustDuct(t, "gc", "--project", root)
	if count(t, root, "duct.duct_audit WHERE user_name = 'forever'") != 1 {
		t.Error("retention = 0 must keep everything")
	}
}

func TestAuditExportIdempotent(t *testing.T) {
	root := auditProject(t)
	now := time.Now()
	for i := range 4 {
		insertAudit(t, root, now.Add(-time.Duration(i)*time.Minute), "ana", fmt.Sprintf("SELECT %d", i))
	}
	// Only the rows these tests insert (protocol pg): the CLI adds its own.
	total := func() float64 { return count(t, root, "duct.duct_audit WHERE protocol = 'pg'") }
	exported := func() float64 { return count(t, root, governance.AuditLogTable+" WHERE protocol = 'pg'") }
	out := mustDuct(t, "audit", "export", "--project", root)
	again := mustDuct(t, "audit", "export", "--project", root) // nothing new in between
	if !strings.Contains(out, "exported") || !strings.Contains(again, "exported 0 ") || exported() != total() {
		t.Fatalf("export: %s / %s (%v of %v)", out, again, exported(), total())
	}
	// A late row (older than the watermark, like a long statement finishing) is still picked up once.
	insertAudit(t, root, now.Add(-2*time.Hour), "late", "SELECT late")
	insertAudit(t, root, now.Add(time.Second), "ana", "SELECT fresh")
	mustDuct(t, "audit", "export", "--project", root)
	mustDuct(t, "audit", "export", "--project", root)
	if exported() != total() {
		t.Errorf("after late rows: %v exported of %v", exported(), total())
	}
	if d := queryRows(t, root, "SELECT count(*) - count(DISTINCT audit_id) AS d FROM "+governance.AuditLogTable)[0]["d"].(float64); d != 0 {
		t.Errorf("duplicates in the lake: %v", d)
	}
	// Governed like any table: a role sees nothing unless granted.
	mustDuct(t, "role", "create", "analistas", "--project", root)
	if out := queryRows(t, root, "SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = 'role_analistas'"); out[0]["n"].(float64) != 0 {
		t.Errorf("role views exist without grants: %v", out)
	}
}

func TestAuditCacheHitsCoalesce(t *testing.T) {
	root := auditProject(t)
	eng, closeEng := openAuditEngine(t, root)
	h := governance.NewHandler(eng, governance.HandlerOptions{})
	rec := contract.AuditRecord{At: time.Now(), User: "ana", Role: "ventas", Protocol: "web", SQL: "-- semantic query (cache hit) {}", Duration: 3 * time.Millisecond, Rows: 7, Cached: true, Hits: 1}
	for range 5 {
		h.Audit(context.Background(), rec)
	}
	other := rec
	other.User = "bob"
	h.Audit(context.Background(), other)
	if err := h.FlushAudit(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	closeEng()
	rows := queryRows(t, root, "SELECT user_name, status, cached, hits, rows_returned FROM duct.duct_audit WHERE protocol = 'web' ORDER BY user_name")
	if len(rows) != 2 {
		t.Fatalf("want one coalesced row per user: %v", rows)
	}
	if rows[0]["user_name"] != "ana" || rows[0]["status"] != "ok" || fmt.Sprint(rows[0]["cached"]) != "1" && fmt.Sprint(rows[0]["cached"]) != "true" || rows[0]["hits"].(float64) != 5 || rows[0]["rows_returned"].(float64) != 7 {
		t.Errorf("ana: %v", rows[0])
	}
	if rows[1]["hits"].(float64) != 1 {
		t.Errorf("bob: %v", rows[1])
	}
	if out := mustDuct(t, "audit", "query", "--project", root, "--user", "ana"); !strings.Contains(out, "ok*") {
		t.Errorf("audit query does not flag cache hits:\n%s", out)
	}
	if got := queryRows(t, root, "SELECT count(*) AS n FROM duct.duct_audit WHERE user_name = 'ana' AND cached")[0]["n"]; got.(float64) != 1 {
		t.Errorf("cached filter: %v", got)
	}
}

// TestAuditTimestampMigration: a state database from before migration 9
// (ragged text timestamps, no audit_id/cached/hits) is converted in place.
func TestAuditTimestampMigration(t *testing.T) {
	root := auditProject(t)
	q := func(s string) { mustDuct(t, "query", "--project", root, s) }
	// Back to v8.
	for _, s := range []string{
		"ALTER TABLE duct.duct_audit DROP COLUMN audit_id", "ALTER TABLE duct.duct_audit DROP COLUMN cached", "ALTER TABLE duct.duct_audit DROP COLUMN hits",
		"DROP TABLE duct.duct_audit_state", "DELETE FROM duct.duct_migrations WHERE version = 9", "DELETE FROM duct.duct_audit",
	} {
		q(s)
	}
	for _, at := range []string{"2026-10-01 10:00:00.5", "2026-10-01 10:00:00", "2026-09-30 23:59:59.123", "2026-10-02 00:00:00"} {
		q("INSERT INTO duct.duct_audit VALUES (CAST('" + at + "' AS TIMESTAMP), 'u', 'r', 'pg', 's', 1, 1, 'ok', NULL)")
	}
	if out := mustDuct(t, "migrate", "--project", root); !strings.Contains(out, "9") {
		t.Fatalf("migrate: %s", out)
	}
	rows := queryRows(t, root, "SELECT occurred_at FROM duct.duct_audit WHERE protocol = 'pg' ORDER BY occurred_at")
	want := []string{"2026-09-30 23:59:59.123000", "2026-10-01 10:00:00.000000", "2026-10-01 10:00:00.500000", "2026-10-02 00:00:00.000000"}
	if len(rows) != len(want) {
		t.Fatalf("rows: %v", rows)
	}
	for i, r := range rows {
		if got := fmt.Sprint(r["occurred_at"]); got != want[i] {
			t.Errorf("row %d: %q want %q", i, got, want[i])
		}
	}
	// Readable as a timestamp, range filters work, and the indexes exist.
	if n := count(t, root, "duct.duct_audit WHERE protocol = 'pg' AND CAST(occurred_at AS TIMESTAMP) >= TIMESTAMP '2026-10-01'"); n != 3 {
		t.Errorf("timestamp filter: %v", n)
	}
	idx := queryRows(t, root, "SELECT count(*) AS n FROM sqlite_query('duct', 'SELECT name FROM sqlite_master WHERE type = ''index'' AND name LIKE ''duct_audit_%''')")
	if idx[0]["n"].(float64) != 2 {
		t.Errorf("audit indexes: %v", idx)
	}
	// The default catalog was left alone by the index step.
	if cur := queryRows(t, root, "SELECT current_catalog() AS c")[0]["c"]; cur != "lake" {
		t.Errorf("current catalog %v", cur)
	}
}

// TestAuditMaintenanceLoop: what `duct serve` runs in the background prunes by
// retention and exports, and exports once more when it is stopped.
func TestAuditMaintenanceLoop(t *testing.T) {
	root := auditProject(t)
	now := time.Now()
	insertAudit(t, root, now.Add(-10*24*time.Hour), "old", "SELECT 1")
	insertAudit(t, root, now.Add(-time.Hour), "recent", "SELECT 2")
	eng, closeEng := openAuditEngine(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		governance.RunAuditMaintenance(ctx, eng, governance.MaintenanceOptions{Retention: 24 * time.Hour, Export: true, PruneEvery: 100 * time.Millisecond, ExportEvery: 100 * time.Millisecond})
	}()
	time.Sleep(time.Second)
	insertAudit(t, root, now, "last", "SELECT 3") // exported by the final run at stop
	cancel()
	<-done
	closeEng()
	if count(t, root, "duct.duct_audit WHERE user_name = 'old'") != 0 || count(t, root, "duct.duct_audit WHERE user_name = 'recent'") != 1 {
		t.Error("retention not applied by the loop")
	}
	if got := count(t, root, governance.AuditLogTable+" WHERE user_name IN ('recent', 'last')"); got != 2 {
		t.Errorf("exported by the loop: %v, want 2", got)
	}
}
