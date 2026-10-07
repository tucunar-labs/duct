package project

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// AuditTimeLayout is how occurred_at is written and compared: fixed width
// (microseconds, UTC), so on SQLite, where DuckDB can only declare time
// columns as VARCHAR, text order is time order. On Postgres the column is a
// native TIMESTAMP and the same literal is cast by the engine.
const AuditTimeLayout = "2006-01-02 15:04:05.000000"

// auditTimeSQL renders a stored occurred_at in AuditTimeLayout on either engine.
const auditTimeSQL = "strftime(CAST(occurred_at AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f')"

// stateBackend is "sqlite" or "postgres": the engine behind the attached
// Duct state alias.
func stateBackend(ctx context.Context, eng contract.Engine, alias string) (string, error) {
	st, err := eng.Query(ctx, "SELECT CAST(type AS VARCHAR) FROM duckdb_databases() WHERE database_name = '"+strings.ReplaceAll(alias, "'", "''")+"'")
	if err != nil {
		return "", err
	}
	defer st.Release()
	out := ""
	for st.Next() {
		if r := st.RecordBatch(); r.NumRows() > 0 && out == "" {
			out = strings.Clone(r.Column(0).ValueStr(0))
		}
	}
	return out, st.Err()
}

// auditBefore is the engine-specific part of migration 9. DuckDB's SQLite
// writer declares every time type as VARCHAR (verified: TIMESTAMP, TIMESTAMPTZ,
// DATE all become VARCHAR), so occurred_at cannot be a SQLite TIMESTAMP: the
// legacy ragged text ("10:00:00.5", "10:00:00") is rewritten to the fixed
// layout so ordering, the index and range filters are correct. On Postgres the
// column already is a TIMESTAMP and only the indexes are added.
func auditBefore(ctx context.Context, eng contract.Engine, alias string) error {
	backend, err := stateBackend(ctx, eng, alias)
	if err != nil {
		return err
	}
	switch backend {
	case "sqlite":
		if err := eng.Exec(ctx, "UPDATE "+alias+".duct_audit SET occurred_at = "+auditTimeSQL+" WHERE length(occurred_at) <> 26"); err != nil {
			return fmt.Errorf("normalise audit timestamps: %w", err)
		}
		ensureAuditIndexesSQLite(ctx, eng, alias)
	case "postgres":
		for _, ix := range auditIndexes {
			q := "CREATE INDEX IF NOT EXISTS " + ix.name + " ON " + alias + ".duct_audit (" + ix.cols + ")"
			if err := eng.Exec(ctx, "CALL postgres_execute('"+alias+"', '"+strings.ReplaceAll(q, "'", "''")+"')"); err != nil {
				slog.Warn("audit index not created", "index", ix.name, "error", err)
			}
		}
	}
	return nil
}

var auditIndexes = []struct{ name, cols string }{
	{"duct_audit_at", "occurred_at"},
	{"duct_audit_user_at", "user_name, occurred_at"},
}

// ensureAuditIndexesSQLite: DuckDB only creates an index in a SQLite file when
// that file is the current catalog, so the statements run in one transaction
// (one connection) that switches to it and back. Best effort: the log works
// without indexes, only slower.
func ensureAuditIndexesSQLite(ctx context.Context, eng contract.Engine, alias string) {
	stmts := []string{"USE " + alias}
	for _, ix := range auditIndexes {
		stmts = append(stmts, "CREATE INDEX IF NOT EXISTS "+ix.name+" ON duct_audit ("+ix.cols+")")
	}
	var err error
	for i := range 5 {
		if err = eng.ExecTx(ctx, stmts...); err == nil {
			break
		}
		time.Sleep(time.Duration(50<<i) * time.Millisecond)
	}
	// Whatever happened, make sure the connection used goes back to the lake.
	for range 3 {
		if eng.ExecTx(ctx, "USE lake") == nil {
			break
		}
	}
	if err != nil {
		slog.Warn("audit indexes not created", "error", err)
	}
}

// PruneAudit deletes audit rows older than cutoff in batches of about batch
// rows (time-ordered, so each DELETE is short and the state database is never
// locked for long). It returns the rows deleted.
func PruneAudit(ctx context.Context, eng contract.Engine, cutoff time.Time, batch int) (int64, error) {
	if batch <= 0 {
		batch = 5000
	}
	cut := cutoff.UTC().Format(AuditTimeLayout)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		// The time of the batch-th oldest row bounds this batch (ties may add a few).
		upper, err := scalarString(ctx, eng, `SELECT `+auditTimeSQL+` FROM duct.duct_audit WHERE occurred_at < ? ORDER BY occurred_at LIMIT 1 OFFSET `+fmt.Sprint(batch-1), cut)
		if err != nil {
			return total, err
		}
		inclusive := true
		if upper == "" { // fewer than batch rows left: take them all
			upper, inclusive = cut, false
		}
		op := "<"
		if inclusive {
			op = "<="
		}
		n, err := scalarString(ctx, eng, `SELECT CAST(count(*) AS VARCHAR) FROM duct.duct_audit WHERE occurred_at `+op+` ?`, upper)
		if err != nil {
			return total, err
		}
		var c int64
		_, _ = fmt.Sscan(n, &c)
		if c == 0 {
			return total, nil
		}
		if err := retryDB(func() error {
			return eng.Exec(ctx, `DELETE FROM duct.duct_audit WHERE occurred_at `+op+` ?`, upper)
		}); err != nil {
			return total, fmt.Errorf("prune audit: %w", err)
		}
		total += c
		if !inclusive {
			return total, nil
		}
	}
}

func scalarString(ctx context.Context, eng contract.Engine, q string, args ...any) (string, error) {
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

// retryDB repeats fn while SQLite answers "database is locked".
func retryDB(fn func() error) error {
	var err error
	for i := range 8 {
		if err = fn(); err == nil || !strings.Contains(err.Error(), "database is locked") {
			return err
		}
		time.Sleep(time.Duration(50<<i) * time.Millisecond)
	}
	return err
}
