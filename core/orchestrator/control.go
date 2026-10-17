package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Controls the web console (and any operator) uses on top of the scheduler:
// pause a job, start a run now, cancel a run. They live in the shared Duct
// state (duct_jobs_state, the run tables), so they take effect in every
// process of a deployment and need no new migration.

const (
	disabledPrefix = "disabled:" // state key: the job's schedule and triggers are paused
	cancelPrefix   = "cancel:"   // state key: a user asked to cancel this run id
)

func disabledKey(job string) string { return disabledPrefix + job }

// JobDisabled reports whether the schedule and triggers of job are paused.
func JobDisabled(ctx context.Context, eng contract.Engine, job string) (bool, error) {
	_, ok, err := (&store{eng: eng}).getState(ctx, disabledKey(job))
	return ok, err
}

// DisabledJobs lists the paused jobs.
func DisabledJobs(ctx context.Context, eng contract.Engine) (map[string]bool, error) {
	r, err := (&store{eng: eng}).rows(ctx, "SELECT state_key FROM "+stateT+" WHERE state_key LIKE 'disabled:%'")
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, x := range r {
		out[strings.TrimPrefix(x[0], disabledPrefix)] = true
	}
	return out, nil
}

// SetJobEnabled pauses (false) or resumes (true) the cron schedule and the
// triggers of job. A paused job still runs when started by hand. Ticks and
// trigger events that happen while paused are dropped, not replayed.
func SetJobEnabled(ctx context.Context, eng contract.Engine, job string, enabled bool) error {
	s := &store{eng: eng}
	if enabled {
		return s.exec(ctx, "DELETE FROM "+stateT+" WHERE state_key = "+sqlQ(disabledKey(job)))
	}
	return s.setState(ctx, disabledKey(job), "1", time.Now().UTC())
}

// StartRun creates a run of job now with trigger "manual", without waiting for
// it: a worker of any `duct serve` picks it up. by (a user name) goes to the
// run's detail. It returns the run id.
func StartRun(ctx context.Context, eng contract.Engine, job Job, by string) (int64, error) {
	id, _, err := (&store{eng: eng}).createRun(ctx, newRun{Job: job, Trigger: TriggerManual, Detail: by, Now: time.Now().UTC()})
	return id, err
}

// ErrNotRunning is returned by CancelRun for a run that is already over.
var ErrNotRunning = errors.New("the run is not running")

// CancelRun asks to cancel a running run: its pending tasks are cancelled at
// once and the process executing a running task interrupts it within about a
// poll interval (Options.Poll); the run then ends as cancelled.
func CancelRun(ctx context.Context, eng contract.Engine, runID int64, by string) error {
	s := &store{eng: eng}
	st, err := s.scalar(ctx, "SELECT status FROM "+runsT+" WHERE id = ?", runID)
	if err != nil {
		return err
	}
	if st == "" {
		return fmt.Errorf("run %d does not exist", runID)
	}
	if st != StatusRunning {
		return ErrNotRunning
	}
	now := time.Now().UTC()
	return s.execTx(ctx,
		fmt.Sprintf("UPDATE %s SET status = 'cancelled', finished_at = %s, error = %s WHERE run_id = %d AND status = 'pending'",
			tasksT, tsLit(now), sqlQ("cancelled by "+by), runID),
		fmt.Sprintf("DELETE FROM %s WHERE state_key = %s", stateT, sqlQ(fmt.Sprintf("%s%d", cancelPrefix, runID))),
		fmt.Sprintf("INSERT INTO %s (state_key, state_value, updated_at) VALUES (%s, %s, %s)", stateT,
			sqlQ(fmt.Sprintf("%s%d", cancelPrefix, runID)), sqlQ(by), tsLit(now)))
}

// runningTask is a task attempt this process executes, so a cancel request
// can interrupt it.
type runningTask struct {
	run    int64
	cancel context.CancelFunc
	user   atomic.Bool // cancelled on request, not by shutdown or timeout
}

func (o *Orchestrator) track(token string, rt *runningTask) {
	o.mu.Lock()
	o.tasks[token] = rt
	o.mu.Unlock()
}

func (o *Orchestrator) untrack(token string) {
	o.mu.Lock()
	delete(o.tasks, token)
	o.mu.Unlock()
}

// checkCancels interrupts the local task attempts whose run has a cancel
// request. It costs nothing when no task runs here.
func (o *Orchestrator) checkCancels(ctx context.Context) {
	o.mu.Lock()
	n := len(o.tasks)
	o.mu.Unlock()
	if n == 0 {
		return
	}
	r, err := o.st.rows(ctx, "SELECT state_key FROM "+stateT+" WHERE state_key LIKE 'cancel:%'")
	if err != nil || len(r) == 0 {
		return
	}
	asked := map[string]bool{}
	for _, x := range r {
		asked[strings.TrimPrefix(x[0], cancelPrefix)] = true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, rt := range o.tasks {
		if asked[fmt.Sprint(rt.run)] && !rt.user.Load() {
			rt.user.Store(true)
			rt.cancel()
		}
	}
}

// Wake makes the queue loop look for work now (after StartRun from this process).
func (o *Orchestrator) Wake() { o.wake() }

// Job returns the current definition of name from the loaded catalog.
func (o *Orchestrator) Job(name string) (Job, bool) {
	j, ok := o.cat.Load().jobs[name]
	return j, ok
}

// Jobs returns the loaded definitions, sorted by name.
func (o *Orchestrator) Jobs() []Job {
	c := o.cat.Load()
	out := make([]Job, len(c.names))
	for i, n := range c.names {
		out[i] = c.jobs[n]
	}
	return out
}

// Console implements contract.JobsConsole over the Duct state. Orch returns the
// running orchestrator of this process (nil when there is none: --no-jobs, or
// no job yet), in which case definitions are read from the project's jobs/.
// Authorisation and audit are the caller's (the governance handler).
type Console struct {
	Eng  contract.Engine
	Root string
	Orch func() *Orchestrator
}

var _ contract.JobsConsole = (*Console)(nil)

func (c *Console) defs() ([]Job, error) {
	if c.Orch != nil {
		if o := c.Orch(); o != nil {
			return o.Jobs(), nil
		}
	}
	return Load(c.Root)
}

func (c *Console) def(name string) (Job, error) {
	js, err := c.defs()
	if err != nil {
		return Job{}, err
	}
	for _, j := range js {
		if j.Name == name {
			return j, nil
		}
	}
	return Job{}, fmt.Errorf("job %q does not exist", name)
}

func runInfo(r RunRow) contract.JobRunInfo {
	return contract.JobRunInfo{ID: r.ID, Job: r.Job, Trigger: r.Trigger, Detail: r.Detail, Status: r.Status,
		ScheduledFor: r.ScheduledFor, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt, Error: r.Error, Tasks: r.Tasks, Attempts: r.Attempts}
}

// Jobs implements contract.JobsConsole.
func (c *Console) Jobs(ctx context.Context) ([]contract.JobInfo, error) {
	defs, err := c.defs()
	if err != nil {
		return nil, err
	}
	last, err := LastRuns(ctx, c.Eng)
	if err != nil {
		return nil, err
	}
	off, err := DisabledJobs(ctx, c.Eng)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	out := make([]contract.JobInfo, 0, len(defs))
	for _, j := range defs {
		in := contract.JobInfo{
			Name: j.Name, Schedule: j.Schedule, Timezone: j.Timezone, Catchup: j.Catchup,
			OnSnapshot: j.SnapshotTables(), OnFile: j.Triggers.OnFile, Enabled: !off[j.Name],
		}
		for _, a := range j.Alerts.OnFailure {
			in.Alerts = append(in.Alerts, a.Channel)
		}
		for _, t := range j.Tasks {
			in.Tasks = append(in.Tasks, contract.JobTaskDef{ID: t.ID, Type: t.Type, DependsOn: append([]string(nil), t.DependsOn...),
				Retries: t.Retries, Timeout: t.timeout()})
		}
		if in.Enabled {
			if n, ok := NextFire(j, now); ok {
				in.NextFire = n
			}
		}
		if r, ok := last[j.Name]; ok {
			ri := runInfo(r)
			in.LastRun = &ri
		}
		out = append(out, in)
	}
	return out, nil
}

// JobRuns implements contract.JobsConsole.
func (c *Console) JobRuns(ctx context.Context, job string, limit int) ([]contract.JobRunInfo, error) {
	rs, err := History(ctx, c.Eng, job, limit)
	if err != nil {
		return nil, err
	}
	out := make([]contract.JobRunInfo, len(rs))
	for i, r := range rs {
		out[i] = runInfo(r)
	}
	return out, nil
}

// JobRun implements contract.JobsConsole.
func (c *Console) JobRun(ctx context.Context, runID int64) (contract.JobRunInfo, []contract.JobTaskRun, bool, error) {
	r, ts, ok, err := GetRun(ctx, c.Eng, runID)
	if err != nil || !ok {
		return contract.JobRunInfo{}, nil, false, err
	}
	out := make([]contract.JobTaskRun, len(ts))
	for i, t := range ts {
		d := t.Duration
		if d == 0 && !t.StartedAt.IsZero() && t.FinishedAt.IsZero() && t.Status == StatusRunning {
			d = time.Since(t.StartedAt)
		}
		out[i] = contract.JobTaskRun{ID: t.ID, Type: t.Type, DependsOn: t.DependsOn, Status: t.Status, Attempts: t.Attempts,
			MaxAttempts: t.MaxAttempts, StartedAt: t.StartedAt, FinishedAt: t.FinishedAt, Duration: d, Error: t.Error, Output: t.Output}
	}
	return runInfo(r), out, true, nil
}

// RunJob implements contract.JobsConsole.
func (c *Console) RunJob(ctx context.Context, id contract.Identity, job string) (int64, error) {
	j, err := c.def(job)
	if err != nil {
		return 0, err
	}
	rid, err := StartRun(ctx, c.Eng, j, id.User)
	if err != nil {
		return 0, err
	}
	if c.Orch != nil {
		if o := c.Orch(); o != nil {
			o.Wake()
		}
	}
	return rid, nil
}

// CancelJobRun implements contract.JobsConsole.
func (c *Console) CancelJobRun(ctx context.Context, id contract.Identity, runID int64) error {
	return CancelRun(ctx, c.Eng, runID, id.User)
}

// SetJobEnabled implements contract.JobsConsole.
func (c *Console) SetJobEnabled(ctx context.Context, _ contract.Identity, job string, enabled bool) error {
	if _, err := c.def(job); err != nil {
		return err
	}
	return SetJobEnabled(ctx, c.Eng, job, enabled)
}
