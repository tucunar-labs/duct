package project

import (
	"context"
	"fmt"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// GCOptions tunes GC. The zero value uses the defaults.
type GCOptions struct {
	// KeepSnapshots is how long snapshots stay available for time travel
	// (default 7 days).
	KeepSnapshots time.Duration
}

// GCStep is the outcome of one maintenance step.
type GCStep struct {
	Name   string
	Detail string
}

// DefaultKeepSnapshots is the retention of GCOptions.KeepSnapshots.
const DefaultKeepSnapshots = 7 * 24 * time.Hour

// GC runs maintenance over the lake: flush inlined data to Parquet, merge
// adjacent small files, expire old snapshots, delete files no snapshot
// references, and checkpoint the SQLite catalog WAL (ADR 0001). The engine must
// be unlocked; steps run in this order and stop at the first error.
func (p *Project) GC(ctx context.Context, eng contract.Engine, opts GCOptions) ([]GCStep, error) {
	keep := opts.KeepSnapshots
	if keep <= 0 {
		keep = DefaultKeepSnapshots
	}
	cutoff := fmt.Sprintf("now() - INTERVAL '%d seconds'", int64(keep.Seconds()))
	calls := []struct{ name, sql string }{
		{"flush inlined data", "CALL ducklake_flush_inlined_data('lake')"},
		{"merge adjacent files", "CALL ducklake_merge_adjacent_files('lake')"},
		{"expire snapshots", "CALL ducklake_expire_snapshots('lake', older_than => " + cutoff + ")"},
		{"cleanup old files", "CALL ducklake_cleanup_old_files('lake', older_than => " + cutoff + ")"},
		{"delete orphaned files", "CALL ducklake_delete_orphaned_files('lake', older_than => " + cutoff + ")"},
	}
	var steps []GCStep
	for _, c := range calls {
		n, err := countRows(ctx, eng, c.sql)
		if err != nil {
			return steps, fmt.Errorf("%s: %w", c.name, err)
		}
		steps = append(steps, GCStep{c.name, fmt.Sprintf("%d rows", n)})
	}
	if p.CatalogIsPostgres() {
		return append(steps, GCStep{"wal checkpoint", "n/a (Postgres catalog)"}), nil
	}
	detail, err := p.checkpointWAL(ctx, eng)
	if err != nil {
		return steps, fmt.Errorf("wal checkpoint: %w", err)
	}
	return append(steps, GCStep{"wal checkpoint", detail}), nil
}

// checkpointWAL runs PRAGMA wal_checkpoint(TRUNCATE) on the catalog through a
// temporary plain-SQLite attach (DuckLake's own attach exposes no PRAGMA).
func (p *Project) checkpointWAL(ctx context.Context, eng contract.Engine) (string, error) {
	const alias = "duct_gc_catalog"
	q := "ATTACH '" + strings.ReplaceAll(p.CatalogPath(), "'", "''") + "' AS " + alias + " (TYPE sqlite, BUSY_TIMEOUT 5000)"
	if err := eng.Exec(ctx, q); err != nil {
		return "", err
	}
	defer func() { _ = eng.Exec(ctx, "DETACH "+alias) }()
	st, err := eng.Query(ctx, "SELECT * FROM sqlite_query('"+alias+"', 'PRAGMA wal_checkpoint(TRUNCATE)')")
	if err != nil {
		return "", err
	}
	defer st.Release()
	if !st.Next() {
		return "", fmt.Errorf("no result (%v)", st.Err())
	}
	rec := st.RecordBatch()
	vals := make([]string, 0, rec.NumCols())
	for i := range int(rec.NumCols()) {
		vals = append(vals, rec.ColumnName(i)+"="+strings.Clone(rec.Column(i).ValueStr(0)))
	}
	if rec.NumCols() > 0 && rec.Column(0).ValueStr(0) != "0" {
		return strings.Join(vals, " "), fmt.Errorf("checkpoint blocked by another connection (%s)", strings.Join(vals, " "))
	}
	return strings.Join(vals, " "), nil
}

func countRows(ctx context.Context, eng contract.Engine, sql string) (int64, error) {
	st, err := eng.Query(ctx, sql)
	if err != nil {
		return 0, err
	}
	defer st.Release()
	var n int64
	for st.Next() {
		n += st.RecordBatch().NumRows()
	}
	return n, st.Err()
}
