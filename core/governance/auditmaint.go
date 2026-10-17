package governance

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
)

// AuditLogTable is the lake table `[audit] export = true` appends to. It is
// an ordinary bronze table: no role reads it unless an admin grants it.
const AuditLogTable = "lake.bronze.duct_audit_log"

const (
	exportWatermarkKey = "audit_export_watermark"
	// exportMargin re-reads this much before the watermark: a record carries
	// the time its statement started but is written when it ends, so a long
	// statement lands behind newer rows. Rows already exported are skipped by
	// audit_id, which makes the overlap harmless.
	exportMargin   = 24 * time.Hour
	epochWatermark = "0001-01-01 00:00:00.000000"
)

// occurredText is occurred_at in project.AuditTimeLayout on either engine.
const occurredText = "strftime(CAST(occurred_at AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f')"

// auditIDExpr is the export key: audit_id, or a hash of the row for rows
// written before audit_id existed.
const auditIDExpr = `COALESCE(audit_id, md5(concat_ws('|', ` + occurredText + `, user_name, role, protocol, sql_text, CAST(duration_ms AS VARCHAR))))`

const createAuditLog = `CREATE TABLE IF NOT EXISTS ` + AuditLogTable + ` (
	audit_id VARCHAR, occurred_at TIMESTAMP, user_name VARCHAR, role VARCHAR, protocol VARCHAR,
	sql_text VARCHAR, duration_ms BIGINT, rows_returned BIGINT, status VARCHAR, error VARCHAR,
	cached BOOLEAN, hits BIGINT)`

// ExportAudit appends the audit rows not yet in AuditLogTable and returns how
// many it added. Safe to repeat and to run from several processes: rows are
// keyed by audit_id and the watermark in duct.duct_audit_state only narrows
// the scan (it moves after the append, so a crash in between re-reads, never
// loses).
func ExportAudit(ctx context.Context, eng contract.Engine) (int64, error) {
	if err := eng.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS lake.bronze"); err != nil {
		return 0, fmt.Errorf("audit export: %w", err)
	}
	if err := eng.Exec(ctx, createAuditLog); err != nil {
		return 0, fmt.Errorf("audit export: %w", err)
	}
	wm, err := auditScalar(ctx, eng, `SELECT state_value FROM duct.duct_audit_state WHERE state_key = ?`, exportWatermarkKey)
	if err != nil {
		return 0, fmt.Errorf("audit export: %w", err)
	}
	from := epochWatermark
	if t, perr := time.Parse(project.AuditTimeLayout, wm); perr == nil {
		from = t.Add(-exportMargin).Format(project.AuditTimeLayout)
	}
	cand := `SELECT ` + auditIDExpr + ` AS audit_id, CAST(occurred_at AS TIMESTAMP) AS occurred_at, user_name, role, protocol, sql_text,
		duration_ms, rows_returned, status, error, COALESCE(cached, false) AS cached, COALESCE(hits, 1) AS hits
		FROM duct.duct_audit WHERE occurred_at > ?`
	fresh := `SELECT * FROM (` + cand + `) s WHERE NOT EXISTS (SELECT 1 FROM ` + AuditLogTable + ` b
		WHERE b.audit_id = s.audit_id AND b.occurred_at > CAST(? AS TIMESTAMP))`
	n, err := auditScalar(ctx, eng, `SELECT CAST(count(*) AS VARCHAR) FROM (`+fresh+`)`, from, from)
	if err != nil {
		return 0, fmt.Errorf("audit export: %w", err)
	}
	var added int64
	_, _ = fmt.Sscan(n, &added)
	if added == 0 {
		return 0, nil
	}
	if err := eng.Exec(ctx, `INSERT INTO `+AuditLogTable+` `+fresh, from, from); err != nil {
		return 0, fmt.Errorf("audit export: %w", err)
	}
	newWM, err := auditScalar(ctx, eng, `SELECT `+occurredText+` FROM duct.duct_audit ORDER BY occurred_at DESC LIMIT 1`)
	if err != nil || newWM == "" {
		return added, err
	}
	err = retryLocked(func() error {
		return eng.ExecTx(ctx,
			`DELETE FROM duct.duct_audit_state WHERE state_key = '`+exportWatermarkKey+`'`,
			`INSERT INTO duct.duct_audit_state (state_key, state_value, updated_at) VALUES ('`+exportWatermarkKey+`', '`+newWM+`', CAST('`+time.Now().UTC().Format(project.AuditTimeLayout)+`' AS TIMESTAMP))`)
	})
	return added, err
}

func auditScalar(ctx context.Context, eng contract.Engine, q string, args ...any) (string, error) {
	st, err := eng.Query(ctx, q, args...)
	if err != nil {
		return "", err
	}
	defer st.Release()
	out := ""
	for st.Next() {
		if r := st.RecordBatch(); r.NumRows() > 0 && out == "" && !r.Column(0).IsNull(0) {
			out = strings.Clone(r.Column(0).ValueStr(0))
		}
	}
	return out, st.Err()
}

// MaintenanceOptions tune RunAuditMaintenance.
type MaintenanceOptions struct {
	Retention   time.Duration // 0: keep forever
	Export      bool
	PruneEvery  time.Duration // default 1 h
	ExportEvery time.Duration // default 1 min
}

// RunAuditMaintenance prunes the audit log (at start and every PruneEvery)
// and, if Export is set, appends it to the lake every ExportEvery, until ctx
// ends. Errors are logged and retried on the next tick. A final export runs
// on the way out so a clean shutdown leaves nothing behind.
func RunAuditMaintenance(ctx context.Context, eng contract.Engine, opt MaintenanceOptions) {
	if opt.PruneEvery <= 0 {
		opt.PruneEvery = time.Hour
	}
	if opt.ExportEvery <= 0 {
		opt.ExportEvery = time.Minute
	}
	prune := func() {
		if opt.Retention <= 0 {
			return
		}
		n, err := project.PruneAudit(ctx, eng, time.Now().Add(-opt.Retention), 0)
		if err != nil && ctx.Err() == nil {
			slog.Warn("audit prune failed", "error", err)
		}
		if n > 0 {
			slog.Info("audit pruned", "rows", n)
		}
	}
	export := func(c context.Context) {
		if !opt.Export {
			return
		}
		if n, err := ExportAudit(c, eng); err != nil {
			if c.Err() == nil {
				slog.Warn("audit export failed", "error", err)
			}
		} else if n > 0 {
			slog.Info("audit exported", "rows", n)
		}
	}
	prune()
	export(ctx)
	pt, et := time.NewTicker(opt.PruneEvery), time.NewTicker(opt.ExportEvery)
	defer pt.Stop()
	defer et.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			export(fctx)
			cancel()
			return
		case <-pt.C:
			prune()
		case <-et.C:
			export(ctx)
		}
	}
}

// Recorder writes the audit rows of local CLI commands (protocol "cli", role
// "local", user = the OS user). Failures never reach the command: a project
// that has not run `duct migrate` since the audit table changed simply gets
// no rows.
type Recorder struct {
	a    *auditLog
	user string
}

// NewRecorder returns a Recorder over eng. Close it before closing eng.
func NewRecorder(eng contract.Engine) *Recorder {
	a := &auditLog{eng: eng, in: make(chan auditItem, 64), done: make(chan struct{}), quiet: true}
	go a.run()
	return &Recorder{a: a, user: OSUser()}
}

// OSUser is $USER, or the account name of the process, or "unknown".
func OSUser() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	return "unknown"
}

// Record audits one finished command: sql is the statement or a description
// ("duct run silver.orders"), start when it began, rows what it returned.
func (r *Recorder) Record(sql string, start time.Time, rows int64, err error) {
	rec := contract.AuditRecord{At: start.UTC(), User: r.user, Role: "local", Protocol: "cli", SQL: sql, Duration: time.Since(start), Rows: rows}
	if err != nil {
		rec.Err = err.Error()
	}
	r.a.add(rec)
}

// Close writes what is pending.
func (r *Recorder) Close() { _ = r.a.close() }

// AuditFilter selects rows for QueryAudit.
type AuditFilter struct {
	User   string
	Status string
	Since  time.Duration // 0: no lower bound
	Limit  int           // default 100
	// Protocol, From, To and Offset serve the web console's paginated viewer.
	Protocol string
	From, To time.Time // zero: no bound
	Offset   int
}

// AuditRow is one row of the audit log as `duct audit query` shows it.
type AuditRow struct {
	At         string `json:"occurred_at"`
	User       string `json:"user"`
	Role       string `json:"role"`
	Protocol   string `json:"protocol"`
	Status     string `json:"status"`
	DurationMS int64  `json:"duration_ms"`
	Rows       int64  `json:"rows"`
	Cached     bool   `json:"cached"`
	Hits       int64  `json:"hits"`
	SQL        string `json:"sql"`
	Error      string `json:"error,omitempty"`
}

// where renders the conditions of f (with a leading AND each) and their args.
func (f AuditFilter) where() (string, []any) {
	var q string
	var args []any
	if f.User != "" {
		q += " AND user_name = ?"
		args = append(args, f.User)
	}
	if f.Status != "" {
		q += " AND status = ?"
		args = append(args, f.Status)
	}
	if f.Protocol != "" {
		q += " AND protocol = ?"
		args = append(args, f.Protocol)
	}
	if f.Since > 0 {
		q += " AND occurred_at >= ?"
		args = append(args, time.Now().Add(-f.Since).UTC().Format(project.AuditTimeLayout))
	}
	if !f.From.IsZero() {
		q += " AND occurred_at >= ?"
		args = append(args, f.From.UTC().Format(project.AuditTimeLayout))
	}
	if !f.To.IsZero() {
		q += " AND occurred_at < ?"
		args = append(args, f.To.UTC().Format(project.AuditTimeLayout))
	}
	return q, args
}

// CountAudit returns how many rows match f (Limit and Offset are ignored).
func CountAudit(ctx context.Context, eng contract.Engine, f AuditFilter) (int64, error) {
	where, args := f.where()
	v, err := auditScalar(ctx, eng, "SELECT CAST(count(*) AS VARCHAR) FROM duct.duct_audit WHERE true"+where, args...)
	if err != nil {
		return 0, err
	}
	var n int64
	_, _ = fmt.Sscan(v, &n)
	return n, nil
}

// QueryAudit returns the newest rows matching f. SQL and errors are
// redacted again on the way out, so rows written by older versions are safe.
func QueryAudit(ctx context.Context, eng contract.Engine, f AuditFilter) ([]AuditRow, error) {
	if f.Limit <= 0 {
		f.Limit = 100
	}
	where, args := f.where()
	q := `SELECT ` + occurredText + `, user_name, role, protocol, status, duration_ms, rows_returned,
		CAST(COALESCE(cached, false) AS VARCHAR), COALESCE(hits, 1), sql_text, COALESCE(error, '') FROM duct.duct_audit WHERE true` + where
	q += fmt.Sprintf(" ORDER BY occurred_at DESC LIMIT %d", f.Limit)
	if f.Offset > 0 {
		q += fmt.Sprintf(" OFFSET %d", f.Offset)
	}
	st, err := eng.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out []AuditRow
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			v := func(c int) string { return strings.Clone(rec.Column(c).ValueStr(i)) }
			var r AuditRow
			r.At, r.User, r.Role, r.Protocol, r.Status = v(0), v(1), v(2), v(3), v(4)
			_, _ = fmt.Sscan(v(5), &r.DurationMS)
			_, _ = fmt.Sscan(v(6), &r.Rows)
			r.Cached = v(7) == "true" || v(7) == "1"
			_, _ = fmt.Sscan(v(8), &r.Hits)
			r.SQL, r.Error = RedactSQL(v(9)), RedactSQL(v(10))
			out = append(out, r)
		}
	}
	return out, st.Err()
}
