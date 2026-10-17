package orchestrator

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/txretry"
)

// Run and task statuses.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusSuccess   = "success"
	StatusFailed    = "failed"
	StatusSkipped   = "skipped" // an upstream task did not succeed
	StatusCancelled = "cancelled"
)

// Trigger kinds recorded in a run.
const (
	TriggerCron     = "cron"
	TriggerManual   = "manual"
	TriggerSnapshot = "snapshot"
	TriggerFile     = "file"
)

const (
	runsT  = "duct.duct_jobs_runs"
	tasksT = "duct.duct_task_runs"
	firesT = "duct.duct_jobs_fires"
	stateT = "duct.duct_jobs_state"

	maxErrorLen = 2048
)

// RunRow is a row of duct_jobs_runs.
type RunRow struct {
	ID           int64
	Job          string
	Trigger      string
	Detail       string
	Status       string
	ScheduledFor time.Time
	StartedAt    time.Time
	FinishedAt   time.Time
	Error        string
	Tasks        int // tasks of the run
	Attempts     int // attempts made so far across its tasks
}

// TaskRow is a row of duct_task_runs.
type TaskRow struct {
	RunID       int64
	ID          string
	Job         string
	Type        string
	DependsOn   []string
	Status      string
	Attempts    int
	MaxAttempts int
	NotBefore   time.Time
	Owner       string
	StartedAt   time.Time
	FinishedAt  time.Time
	Duration    time.Duration
	Error       string
	Output      string
	pinned      string // run pinned to one process (manual runs)
}

// store is the SQL layer over the Duct state. Every statement is retried on
// commit conflicts (SQLite busy, Postgres serialization) with core/txretry.
type store struct {
	eng contract.Engine
}

func tsLit(t time.Time) string { return fmt.Sprintf("make_timestamp(%d)", t.UTC().UnixMicro()) }

func sqlQ(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func nullQ(s string) string {
	if s == "" {
		return "NULL"
	}
	return sqlQ(s)
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// epoch reads a timestamp column as microseconds since the epoch (NULL -> "").
func epoch(col string) string { return "CAST(epoch_us(CAST(" + col + " AS TIMESTAMP)) AS BIGINT)" }

func micros(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMicro(n).UTC()
}

func (s *store) exec(ctx context.Context, sql string) error {
	return txretry.Do(ctx, txretry.Policy{}, func(ctx context.Context) error { return s.eng.Exec(ctx, sql) })
}

func (s *store) execTx(ctx context.Context, stmts ...string) error {
	return txretry.Do(ctx, txretry.Policy{}, func(ctx context.Context) error { return s.eng.ExecTx(ctx, stmts...) })
}

// rows runs a query and returns every cell as a string (NULL as "").
func (s *store) rows(ctx context.Context, sql string, args ...any) ([][]string, error) {
	var out [][]string
	err := txretry.Do(ctx, txretry.Policy{}, func(ctx context.Context) error {
		out = out[:0]
		st, err := s.eng.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer st.Release()
		for st.Next() {
			rec := st.RecordBatch()
			for i := range int(rec.NumRows()) {
				row := make([]string, rec.NumCols())
				for c := range row {
					if col := rec.Column(c); col.IsValid(i) {
						row[c] = strings.Clone(col.ValueStr(i))
					}
				}
				out = append(out, row)
			}
		}
		return st.Err()
	})
	return out, err
}

func (s *store) scalar(ctx context.Context, sql string, args ...any) (string, error) {
	r, err := s.rows(ctx, sql, args...)
	if err != nil || len(r) == 0 {
		return "", err
	}
	return r[0][0], nil
}

// newID is unique across processes sharing one state (see pipeline.newRunID).
func newID() int64 { return time.Now().UnixMicro()*1000 + rand.Int64N(1000) }

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = cryptorand.Read(b)
	return hex.EncodeToString(b)
}

// newRun describes a run to create.
type newRun struct {
	Job          Job
	Trigger      string
	FireKey      string // "" = no dedup (manual runs)
	ScheduledFor time.Time
	Detail       string
	PinnedTo     string
	Now          time.Time
}

// createRun inserts the run and its tasks (and the fire key) in one
// transaction. created=false means another process already fired the same key.
func (s *store) createRun(ctx context.Context, nr newRun) (id int64, created bool, err error) {
	id = newID()
	stmts := make([]string, 0, 2+len(nr.Job.Tasks))
	if nr.FireKey != "" {
		stmts = append(stmts, fmt.Sprintf("INSERT INTO %s (fire_key, run_id, fired_at) VALUES (%s, %d, %s)",
			firesT, sqlQ(nr.FireKey), id, tsLit(nr.Now)))
	}
	sched := "NULL"
	if !nr.ScheduledFor.IsZero() {
		sched = tsLit(nr.ScheduledFor)
	}
	hb := "NULL"
	if nr.PinnedTo != "" {
		hb = tsLit(nr.Now)
	}
	stmts = append(stmts, fmt.Sprintf(
		"INSERT INTO %s (id, job, trigger_kind, detail, status, scheduled_for, started_at, pinned_to, heartbeat_at) VALUES (%d, %s, %s, %s, 'running', %s, %s, %s, %s)",
		runsT, id, sqlQ(nr.Job.Name), sqlQ(nr.Trigger), nullQ(nr.Detail), sched, tsLit(nr.Now), nullQ(nr.PinnedTo), hb))
	for _, t := range nr.Job.Tasks {
		deps, _ := json.Marshal(append([]string{}, t.DependsOn...))
		stmts = append(stmts, fmt.Sprintf(
			"INSERT INTO %s (run_id, task_id, job, task_type, depends_on, status, attempts, max_attempts) VALUES (%d, %s, %s, %s, %s, 'pending', 0, %d)",
			tasksT, id, sqlQ(t.ID), sqlQ(nr.Job.Name), sqlQ(t.Type), sqlQ(string(deps)), t.MaxAttempts()))
	}
	if err := s.execTx(ctx, stmts...); err != nil {
		if nr.FireKey != "" {
			// Lost the race (duplicate key) or a real error: the key tells which.
			if n, qerr := s.scalar(ctx, "SELECT count(*) FROM "+firesT+" WHERE fire_key = ?", nr.FireKey); qerr == nil && n != "0" {
				return 0, false, nil
			}
		}
		return 0, false, fmt.Errorf("create run of %s: %w", nr.Job.Name, err)
	}
	return id, true, nil
}

const taskCols = `run_id, task_id, job, task_type, depends_on, status, attempts, max_attempts, ` +
	`CAST(epoch_us(CAST(not_before AS TIMESTAMP)) AS BIGINT), COALESCE(owner, ''), CAST(epoch_us(CAST(started_at AS TIMESTAMP)) AS BIGINT), ` +
	`CAST(epoch_us(CAST(finished_at AS TIMESTAMP)) AS BIGINT), COALESCE(duration_ms, 0), COALESCE(error, ''), COALESCE(output, '')`

func parseTask(r []string) TaskRow {
	t := TaskRow{ID: r[1], Job: r[2], Type: r[3], Status: r[5], NotBefore: micros(r[8]), Owner: r[9],
		StartedAt: micros(r[10]), FinishedAt: micros(r[11]), Error: r[13], Output: r[14]}
	t.RunID, _ = strconv.ParseInt(r[0], 10, 64)
	_ = json.Unmarshal([]byte(r[4]), &t.DependsOn)
	t.Attempts, _ = strconv.Atoi(r[6])
	t.MaxAttempts, _ = strconv.Atoi(r[7])
	ms, _ := strconv.ParseInt(r[12], 10, 64)
	t.Duration = time.Duration(ms) * time.Millisecond
	return t
}

// activeTasks returns every task of every running run (the work queue plus the
// statuses needed to resolve dependencies).
func (s *store) activeTasks(ctx context.Context) ([]TaskRow, error) {
	r, err := s.rows(ctx, "SELECT "+taskCols+" FROM "+tasksT+" WHERE run_id IN (SELECT id FROM "+runsT+" WHERE status = 'running')")
	if err != nil {
		return nil, err
	}
	pins, err := s.rows(ctx, "SELECT id, COALESCE(pinned_to, '') FROM "+runsT+" WHERE status = 'running'")
	if err != nil {
		return nil, err
	}
	pinned := map[string]string{}
	for _, p := range pins {
		pinned[p[0]] = p[1]
	}
	out := make([]TaskRow, len(r))
	for i, row := range r {
		out[i] = parseTask(row)
		out[i].pinned = pinned[row[0]]
	}
	return out, nil
}

func taskKey(runID int64, task string) string {
	return fmt.Sprintf("run_id = %d AND task_id = %s", runID, sqlQ(task))
}

// claim moves one pending task to running for token. Several processes may try
// at once: the conditional UPDATE lets one change the row (on Postgres the
// others get a serialization failure and txretry re-evaluates the condition),
// and the read-back of owner tells who won.
func (s *store) claim(ctx context.Context, runID int64, task, token string, now time.Time) (bool, error) {
	err := s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = 'running', owner = %s, heartbeat_at = %s, started_at = %s, "+
		"finished_at = NULL, attempts = attempts + 1 WHERE %s AND status = 'pending'",
		tasksT, sqlQ(token), tsLit(now), tsLit(now), taskKey(runID, task)))
	if err != nil {
		return false, err
	}
	owner, err := s.scalar(ctx, "SELECT COALESCE(owner, '') FROM "+tasksT+" WHERE "+taskKey(runID, task))
	return owner == token, err
}

// finishTask records the end of an attempt by the claim holder. retryAt non-zero
// puts the task back in the queue for another attempt at that time.
func (s *store) finishTask(ctx context.Context, runID int64, task, token, status, errMsg, output string, started, now time.Time, retryAt time.Time) error {
	ms := now.Sub(started).Milliseconds()
	if retryAt.IsZero() {
		return s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = %s, finished_at = %s, duration_ms = %d, error = %s, output = %s "+
			"WHERE %s AND owner = %s AND status = 'running'",
			tasksT, sqlQ(status), tsLit(now), ms, nullQ(clip(errMsg, maxErrorLen)), nullQ(clip(output, 4096)),
			taskKey(runID, task), sqlQ(token)))
	}
	return s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = 'pending', owner = NULL, not_before = %s, duration_ms = %d, error = %s, output = %s "+
		"WHERE %s AND owner = %s AND status = 'running'",
		tasksT, tsLit(retryAt), ms, nullQ(clip(errMsg, maxErrorLen)), nullQ(clip(output, 4096)), taskKey(runID, task), sqlQ(token)))
}

func (s *store) skipTask(ctx context.Context, runID int64, task, reason string, now time.Time) error {
	return s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = 'skipped', finished_at = %s, error = %s WHERE %s AND status = 'pending'",
		tasksT, tsLit(now), sqlQ(reason), taskKey(runID, task)))
}

// finalizeRun closes a run exactly once: the conditional UPDATE plus the
// finalizer read-back elect one process, which then sends the alerts.
func (s *store) finalizeRun(ctx context.Context, runID int64, status, errMsg, token string, now time.Time) (bool, error) {
	err := s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = %s, finished_at = %s, error = %s, finalizer = %s WHERE id = %d AND status = 'running'",
		runsT, sqlQ(status), tsLit(now), nullQ(clip(errMsg, maxErrorLen)), sqlQ(token), runID))
	if err != nil {
		return false, err
	}
	f, err := s.scalar(ctx, "SELECT COALESCE(finalizer, '') FROM "+runsT+" WHERE id = ?", runID)
	return f == token, err
}

// touch renews the heartbeat of what this process owns: its running tasks and
// the pinned runs it drives.
func (s *store) touch(ctx context.Context, instance string, now time.Time) error {
	if err := s.exec(ctx, fmt.Sprintf("UPDATE %s SET heartbeat_at = %s WHERE status = 'running' AND owner LIKE %s",
		tasksT, tsLit(now), sqlQ(instance+"/%"))); err != nil {
		return err
	}
	return s.exec(ctx, fmt.Sprintf("UPDATE %s SET heartbeat_at = %s WHERE status = 'running' AND pinned_to = %s",
		runsT, tsLit(now), sqlQ(instance)))
}

// reap handles what dead processes left: running tasks without heartbeat go
// back to the queue (or fail when their attempts are used up), and the open
// tasks of a pinned run whose driver is gone fail.
func (s *store) reap(ctx context.Context, cutoff, now time.Time) error {
	if err := s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END, "+
		"error = 'worker lost (no heartbeat)', owner = NULL, not_before = %s, finished_at = CASE WHEN attempts < max_attempts THEN NULL ELSE %s END "+
		"WHERE status = 'running' AND CAST(heartbeat_at AS TIMESTAMP) < %s", tasksT, tsLit(now), tsLit(now), tsLit(cutoff))); err != nil {
		return err
	}
	return s.exec(ctx, fmt.Sprintf("UPDATE %s SET status = 'failed', finished_at = %s, error = 'the process driving this run is gone' "+
		"WHERE status IN ('pending', 'running') AND run_id IN (SELECT id FROM %s WHERE status = 'running' AND pinned_to IS NOT NULL AND CAST(heartbeat_at AS TIMESTAMP) < %s)",
		tasksT, tsLit(now), runsT, tsLit(cutoff)))
}

func (s *store) activeRuns(ctx context.Context, job string) (int, error) {
	n, err := s.scalar(ctx, "SELECT count(*) FROM "+runsT+" WHERE job = ? AND status = 'running'", job)
	if err != nil {
		return 0, err
	}
	v, _ := strconv.Atoi(n)
	return v, nil
}

// previousStatus is the status of the last finished run of job before runID.
func (s *store) previousStatus(ctx context.Context, job string, runID int64) (string, error) {
	return s.scalar(ctx, "SELECT status FROM "+runsT+" WHERE job = ? AND id < ? AND status IN ('success', 'failed') ORDER BY id DESC LIMIT 1", job, runID)
}

func (s *store) getState(ctx context.Context, key string) (string, bool, error) {
	r, err := s.rows(ctx, "SELECT state_value FROM "+stateT+" WHERE state_key = ?", key)
	if err != nil || len(r) == 0 {
		return "", false, err
	}
	return r[0][0], true, nil
}

func (s *store) setState(ctx context.Context, key, value string, now time.Time) error {
	return s.execTx(ctx,
		"DELETE FROM "+stateT+" WHERE state_key = "+sqlQ(key),
		fmt.Sprintf("INSERT INTO %s (state_key, state_value, updated_at) VALUES (%s, %s, %s)", stateT, sqlQ(key), sqlQ(value), tsLit(now)))
}

func (s *store) pruneFires(ctx context.Context, before time.Time) error {
	return s.exec(ctx, "DELETE FROM "+firesT+" WHERE CAST(fired_at AS TIMESTAMP) < "+tsLit(before))
}

// History returns the most recent runs, newest first, of job ("" = every job).
func History(ctx context.Context, eng contract.Engine, job string, limit int) ([]RunRow, error) {
	s := &store{eng: eng}
	q := "SELECT " + runCols + " FROM " + runsT + " r"
	if job != "" {
		q += " WHERE r.job = " + sqlQ(job)
	}
	q += " ORDER BY r.id DESC LIMIT " + strconv.Itoa(max(limit, 1))
	r, err := s.rows(ctx, q)
	if err != nil {
		return nil, err
	}
	return parseRuns(r), nil
}

// runCols are the columns of RunRow, for a query over runsT aliased r.
var runCols = "r.id, r.job, r.trigger_kind, COALESCE(r.detail, ''), r.status, " + epoch("r.scheduled_for") + ", " + epoch("r.started_at") + ", " +
	epoch("r.finished_at") + ", COALESCE(r.error, ''), " +
	"(SELECT count(*) FROM " + tasksT + " t WHERE t.run_id = r.id), (SELECT COALESCE(sum(t.attempts), 0) FROM " + tasksT + " t WHERE t.run_id = r.id)"

func parseRuns(r [][]string) []RunRow {
	out := make([]RunRow, len(r))
	for i, x := range r {
		out[i] = RunRow{Job: x[1], Trigger: x[2], Detail: x[3], Status: x[4], ScheduledFor: micros(x[5]),
			StartedAt: micros(x[6]), FinishedAt: micros(x[7]), Error: x[8]}
		out[i].ID, _ = strconv.ParseInt(x[0], 10, 64)
		out[i].Tasks, _ = strconv.Atoi(x[9])
		out[i].Attempts, _ = strconv.Atoi(x[10])
	}
	return out
}

// GetRun returns one run and its tasks (in id order); ok=false if there is none.
func GetRun(ctx context.Context, eng contract.Engine, id int64) (RunRow, []TaskRow, bool, error) {
	s := &store{eng: eng}
	r, err := s.rows(ctx, "SELECT "+runCols+" FROM "+runsT+" r WHERE r.id = ?", id)
	if err != nil || len(r) == 0 {
		return RunRow{}, nil, false, err
	}
	tr, err := s.rows(ctx, "SELECT "+taskCols+" FROM "+tasksT+" WHERE run_id = ? ORDER BY COALESCE(CAST(started_at AS TIMESTAMP), make_timestamp(0)), task_id", id)
	if err != nil {
		return RunRow{}, nil, false, err
	}
	tasks := make([]TaskRow, len(tr))
	for i, x := range tr {
		tasks[i] = parseTask(x)
	}
	return parseRuns(r)[0], tasks, true, nil
}

// LastRuns maps job name to its latest run (for `duct jobs list`).
func LastRuns(ctx context.Context, eng contract.Engine) (map[string]RunRow, error) {
	runs, err := History(ctx, eng, "", 500)
	if err != nil {
		return nil, err
	}
	out := map[string]RunRow{}
	for _, r := range runs { // newest first
		if _, ok := out[r.Job]; !ok {
			out[r.Job] = r
		}
	}
	return out, nil
}
