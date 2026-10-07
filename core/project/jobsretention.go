package project

import (
	"context"
	"fmt"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// JobsSettings is the [jobs] table of duct.toml (ADR 0005):
//
//	[jobs]
//	history_retention = "30d"   # finished runs older than this are deleted hourly by `duct serve` and by `duct gc`; unset or 0 = keep
type JobsSettings struct {
	HistoryRetention any `toml:"history_retention"`
}

// RetentionDuration is the history retention; 0 keeps everything.
func (j JobsSettings) RetentionDuration() (time.Duration, error) {
	switch v := j.HistoryRetention.(type) {
	case nil:
		return 0, nil
	case int64:
		if v == 0 {
			return 0, nil
		}
		return 0, fmt.Errorf("[jobs] history_retention: write a duration with a unit (\"30d\"), or 0")
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, nil
		}
		d, err := ParseAuditDuration(v)
		if err != nil {
			return 0, fmt.Errorf("[jobs] history_retention: %w", err)
		}
		return d, nil
	}
	return 0, fmt.Errorf("[jobs] history_retention: want a string like \"30d\" or 0, got %v", j.HistoryRetention)
}

// PruneJobsHistory deletes the runs of jobs that finished before cutoff, with
// their task attempts, and returns how many runs went. Running runs are never
// touched. The fire keys that make a tick start once are kept for at least 7
// days whatever the retention.
func PruneJobsHistory(ctx context.Context, eng contract.Engine, cutoff time.Time) (int64, error) {
	ts := fmt.Sprintf("make_timestamp(%d)", cutoff.UTC().UnixMicro())
	old := "SELECT id FROM duct.duct_jobs_runs WHERE status <> 'running' AND finished_at IS NOT NULL AND CAST(finished_at AS TIMESTAMP) < " + ts
	n, err := countRows(ctx, eng, old)
	if err != nil || n == 0 {
		return 0, err
	}
	fires := ts
	if floor := time.Now().UTC().Add(-7 * 24 * time.Hour); cutoff.After(floor) {
		fires = fmt.Sprintf("make_timestamp(%d)", floor.UnixMicro())
	}
	err = eng.ExecTx(ctx,
		"DELETE FROM duct.duct_task_runs WHERE run_id IN ("+old+")",
		"DELETE FROM duct.duct_jobs_fires WHERE CAST(fired_at AS TIMESTAMP) < "+fires,
		"DELETE FROM duct.duct_jobs_runs WHERE id IN ("+old+")",
	)
	return n, err
}
