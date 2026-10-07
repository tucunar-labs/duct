package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
)

// Options configure an Orchestrator. Zero values mean the defaults.
type Options struct {
	// Root is the project directory (jobs' relative paths resolve against it).
	Root string
	// Env is added to the environment of every task process, typically DUCT_URL
	// and DUCT_TOKEN (how user code reaches duct serve). Never lake credentials.
	Env map[string]string
	// Workers bounds the tasks this process runs at once (default 4).
	Workers int
	// Poll is how often the queue is looked at (default 500ms).
	Poll time.Duration
	// TriggerPoll is how often snapshot and file triggers are checked (default 2s).
	TriggerPoll time.Duration
	// HeartbeatTTL: a running task whose owner has not renewed its heartbeat
	// for this long is considered lost (default 30s; renewed every TTL/3).
	HeartbeatTTL time.Duration
	// FileSettle: a file modified more recently than this is not "new" yet (default 2s).
	FileSettle time.Duration
	// HistoryRetention: finished runs (and their task attempts) older than this
	// are deleted at start and hourly (0 keeps everything). `duct gc` applies
	// the same setting.
	HistoryRetention time.Duration
	// Now is the clock of the cron scheduler (default time.Now).
	Now func() time.Time
	// Logger receives the operational log (default slog.Default()).
	Logger *slog.Logger
	// Notify delivers an alert; the default instantiates the KindAlertChannel
	// module named in the spec.
	Notify func(ctx context.Context, spec AlertSpec, a contract.Alert) error
}

func (o Options) withDefaults() Options {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	if o.Poll <= 0 {
		o.Poll = 500 * time.Millisecond
	}
	if o.TriggerPoll <= 0 {
		o.TriggerPoll = 2 * time.Second
	}
	if o.HeartbeatTTL <= 0 {
		o.HeartbeatTTL = 30 * time.Second
	}
	if o.FileSettle <= 0 {
		o.FileSettle = 2 * time.Second
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Orchestrator runs jobs against the Duct state reachable through eng (an
// unlocked engine: tasks write the lake). Create it with New; call Run to
// serve schedules and triggers, or RunJob for one synchronous manual run.
type Orchestrator struct {
	eng      contract.Engine
	st       *store
	opt      Options
	cat      atomic.Pointer[catalog] // the jobs now defined; Reload swaps it
	instance string
	seq      atomic.Int64
	log      *slog.Logger

	kick  chan struct{}
	sem   chan struct{}
	wg    sync.WaitGroup // running tasks
	ready chan struct{}

	mu     sync.Mutex
	cursor map[string]time.Time // next cron tick per job
	known  map[string]Job       // every job ever loaded: runs in flight outlive an edit or removal
}

// catalog is an immutable set of validated jobs.
type catalog struct {
	jobs   map[string]Job
	names  []string
	scheds map[string]Schedule
}

func newCatalog(jobs []Job) (*catalog, error) {
	c := &catalog{jobs: map[string]Job{}, scheds: map[string]Schedule{}}
	for _, j := range jobs {
		if err := j.Validate(); err != nil {
			return nil, fmt.Errorf("job %s: %w", j.Name, err)
		}
		if _, dup := c.jobs[j.Name]; dup {
			return nil, fmt.Errorf("job %s defined twice", j.Name)
		}
		c.jobs[j.Name] = j
		c.names = append(c.names, j.Name)
		if j.Schedule != "" {
			sc, _ := ParseSchedule(j.Schedule, j.Timezone)
			c.scheds[j.Name] = sc
		}
	}
	sort.Strings(c.names)
	return c, nil
}

// job returns the current definition of name, or the last one seen when it was
// removed while a run of it was still going.
func (o *Orchestrator) job(name string) (Job, bool) {
	if j, ok := o.cat.Load().jobs[name]; ok {
		return j, true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	j, ok := o.known[name]
	return j, ok
}

func (o *Orchestrator) remember(c *catalog) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for n, j := range c.jobs {
		o.known[n] = j
	}
}

// New validates the jobs and builds an orchestrator. The Duct tables must
// already be migrated (project.Migrate).
func New(eng contract.Engine, jobs []Job, opt Options) (*Orchestrator, error) {
	opt = opt.withDefaults()
	host, _ := os.Hostname()
	o := &Orchestrator{
		eng: eng, st: &store{eng: eng}, opt: opt, log: opt.Logger,
		cursor: map[string]time.Time{}, known: map[string]Job{},
		instance: fmt.Sprintf("%s-%d-%s", host, os.Getpid(), randHex(3)),
		kick:     make(chan struct{}, 1), sem: make(chan struct{}, opt.Workers), ready: make(chan struct{}),
	}
	c, err := newCatalog(jobs)
	if err != nil {
		return nil, err
	}
	o.cat.Store(c)
	o.remember(c)
	return o, nil
}

// Instance is the id this process uses as owner of claims and pinned runs.
func (o *Orchestrator) Instance() string { return o.instance }

// Ready is closed once Run took its trigger baselines and schedule cursors.
func (o *Orchestrator) Ready() <-chan struct{} { return o.ready }

func (o *Orchestrator) now() time.Time { return o.opt.Now().UTC() }

func (o *Orchestrator) wake() {
	select {
	case o.kick <- struct{}{}:
	default:
	}
}

// NextFire is the next scheduled tick of job after now, if it has a schedule.
func NextFire(j Job, now time.Time) (time.Time, bool) {
	if j.Schedule == "" {
		return time.Time{}, false
	}
	sc, err := ParseSchedule(j.Schedule, j.Timezone)
	if err != nil {
		return time.Time{}, false
	}
	n := sc.Next(now)
	return n, !n.IsZero()
}

// fire creates a run unless another process already fired key. It reports
// whether this process started it.
func (o *Orchestrator) fire(ctx context.Context, job Job, trigger, key string, scheduled time.Time, detail string) (bool, error) {
	_, created, err := o.st.createRun(ctx, newRun{Job: job, Trigger: trigger, FireKey: key, ScheduledFor: scheduled, Detail: detail, Now: o.now()})
	if err != nil {
		return false, err
	}
	if created {
		o.log.Info("job started", "job", job.Name, "trigger", trigger)
		o.wake()
	}
	return created, nil
}

// cronKey is the Duct state key holding the last cron tick of job that was
// handled (fired, skipped or deliberately forgotten): where a restart resumes.
func cronKey(job string) string { return "cron:" + job }

func policyOf(j Job) string {
	if j.Catchup == "" {
		return CatchupLast
	}
	return j.Catchup
}

// initCursor sets where the cron loop of job resumes: right after the last
// tick handled before (so ticks missed while nothing was running are seen
// again, then dealt with by the catchup policy), or after now when the job is
// new or its policy is none.
func (o *Orchestrator) initCursor(ctx context.Context, j Job, sc Schedule, now time.Time) {
	next := sc.Next(now)
	if policyOf(j) != CatchupNone {
		if v, ok, err := o.st.getState(ctx, cronKey(j.Name)); err == nil && ok {
			if sec, err := strconv.ParseInt(v, 10, 64); err == nil {
				next = sc.Next(time.Unix(sec, 0).UTC())
			}
		} else if err != nil {
			o.log.Warn("cron cursor", "job", j.Name, "err", err)
		}
	}
	o.mu.Lock()
	o.cursor[j.Name] = next
	o.mu.Unlock()
}

// dueTicks lists the ticks of sc from cursor up to now, oldest first, keeping
// only the last max (and none older than CatchupHorizon). next is the cursor
// after all of them.
func dueTicks(sc Schedule, cursor, now time.Time, max int) (ticks []time.Time, next time.Time) {
	if floor := now.Add(-CatchupHorizon); cursor.Before(floor) {
		cursor = sc.Next(floor)
	}
	next = cursor
	for !next.IsZero() && !next.After(now) {
		ticks = append(ticks, next)
		if len(ticks) > max {
			ticks = ticks[1:]
		}
		next = sc.Next(next)
	}
	return ticks, next
}

// fireDue starts the runs of every cron job whose tick has come, and of the
// ticks missed during downtime according to the job's catchup policy.
func (o *Orchestrator) fireDue(ctx context.Context) {
	now := o.now()
	cat := o.cat.Load()
	for _, name := range cat.names {
		sc, ok := cat.scheds[name]
		if !ok {
			continue
		}
		job := cat.jobs[name]
		o.mu.Lock()
		cur := o.cursor[name]
		o.mu.Unlock()
		if cur.IsZero() || cur.After(now) {
			continue
		}
		active, err := o.st.activeRuns(ctx, name)
		if err != nil {
			o.log.Warn("cron", "job", name, "err", err)
			continue
		}
		if policyOf(job) == CatchupAll {
			if active > 0 {
				continue // one at a time: the backlog waits
			}
			ticks, _ := dueTicks(sc, cur, now, MaxCatchup)
			if len(ticks) == 0 {
				continue
			}
			if dropped := o.skipped(cur, ticks[0], sc); dropped > 0 {
				o.log.Warn("cron catch-up: ticks dropped (too many or too old)", "job", name, "dropped", dropped)
			}
			o.advanceTo(ctx, name, ticks[0], sc.Next(ticks[0]))
			o.fireTick(ctx, job, ticks[0], len(ticks) > 1 || now.Sub(ticks[0]) > time.Minute)
			continue
		}
		tick, next, due := advance(sc, cur, now)
		if !due {
			continue
		}
		o.advanceTo(ctx, name, tick, next)
		if active > 0 {
			o.log.Info("cron tick skipped: previous run still active", "job", name, "tick", tick)
			continue
		}
		o.fireTick(ctx, job, tick, now.Sub(tick) > time.Minute)
	}
}

// skipped counts the ticks between cursor and first that catch-up dropped.
func (o *Orchestrator) skipped(cursor, first time.Time, sc Schedule) int {
	n := 0
	for c := cursor; !c.IsZero() && c.Before(first) && n < 1_000_000; c = sc.Next(c) {
		n++
	}
	return n
}

// advanceTo moves the in-memory cursor and records the last handled tick.
func (o *Orchestrator) advanceTo(ctx context.Context, job string, handled, next time.Time) {
	o.mu.Lock()
	o.cursor[job] = next
	o.mu.Unlock()
	if err := o.st.setState(ctx, cronKey(job), strconv.FormatInt(handled.Unix(), 10), o.now()); err != nil && ctx.Err() == nil {
		o.log.Warn("cron cursor not saved", "job", job, "err", err)
	}
}

func (o *Orchestrator) fireTick(ctx context.Context, job Job, tick time.Time, late bool) {
	detail := ""
	if late {
		detail = "catch-up"
	}
	if created, err := o.fire(ctx, job, TriggerCron, "cron:"+job.Name+":"+strconv.FormatInt(tick.Unix(), 10), tick, detail); err != nil && ctx.Err() == nil {
		o.log.Warn("cron fire", "job", job.Name, "err", err)
	} else if created && late {
		o.log.Info("cron catch-up", "job", job.Name, "tick", tick)
	}
}

// nextWake is how long the cron loop may sleep.
func (o *Orchestrator) nextWake() time.Duration {
	d := time.Second
	now := o.now()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, n := range o.cursor {
		if n.IsZero() {
			continue
		}
		w := n.Sub(now)
		if w <= 0 {
			w = 500 * time.Millisecond // a catch-up backlog waiting for the active run
		}
		d = min(d, w)
	}
	return max(d, 10*time.Millisecond)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Run serves schedules, triggers and the work queue until ctx is cancelled.
// On cancellation running tasks receive the cancellation, their attempts are
// recorded as cancelled and Run returns once they stopped.
func (o *Orchestrator) Run(ctx context.Context) error {
	now := o.now()
	cat := o.cat.Load()
	for _, name := range cat.names {
		if sc, ok := cat.scheds[name]; ok {
			o.initCursor(ctx, cat.jobs[name], sc, now)
		}
	}
	o.prune(ctx)
	o.pollTriggers(ctx) // baselines
	close(o.ready)

	var loops sync.WaitGroup
	// The cron loop always runs: Reload may bring the first schedule later.
	loops.Add(1)
	go func() {
		defer loops.Done()
		for ctx.Err() == nil {
			o.fireDue(ctx)
			sleep(ctx, o.nextWake())
		}
	}()
	loops.Add(1)
	go func() {
		defer loops.Done()
		last := time.Now()
		for ctx.Err() == nil {
			sleep(ctx, o.opt.TriggerPoll)
			if o.hasTriggers() {
				o.pollTriggers(ctx)
			}
			if time.Since(last) > time.Hour {
				last = time.Now()
				o.prune(ctx)
			}
		}
	}()

	hb := time.Now()
	for ctx.Err() == nil {
		if time.Since(hb) >= o.opt.HeartbeatTTL/3 {
			hb = time.Now()
			if err := o.st.touch(ctx, o.instance, o.now()); err != nil && ctx.Err() == nil {
				o.log.Warn("heartbeat", "err", err)
			}
			if err := o.st.reap(ctx, o.now().Add(-o.opt.HeartbeatTTL), o.now()); err != nil && ctx.Err() == nil {
				o.log.Warn("reap", "err", err)
			}
		}
		claimed, err := o.step(ctx, 0, true)
		if err != nil && ctx.Err() == nil {
			o.log.Warn("queue", "err", err)
		}
		if claimed {
			continue
		}
		select {
		case <-ctx.Done():
		case <-o.kick:
		case <-time.After(o.opt.Poll):
		}
	}
	loops.Wait()
	o.wg.Wait()
	// Close what the cancelled attempts left behind (cascade skips, final status).
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if _, err := o.step(fctx, 0, false); err != nil {
		o.log.Warn("shutdown reconcile", "err", err)
	}
	return nil
}

// step looks at the queue once: skips tasks whose upstream failed, closes
// finished runs (alerts included) and, if claim is set, starts every ready
// task a free worker slot allows. only != 0 restricts it to one run.
func (o *Orchestrator) step(ctx context.Context, only int64, claim bool) (claimed bool, err error) {
	rows, err := o.st.activeTasks(ctx)
	if err != nil {
		return false, err
	}
	if only != 0 {
		kept := rows[:0]
		for _, r := range rows {
			if r.RunID == only {
				kept = append(kept, r)
			}
		}
		rows = kept
	}
	if sk := toSkip(rows); len(sk) > 0 {
		for _, s := range sk {
			if err := o.st.skipTask(ctx, s.RunID, s.Task, s.Reason, o.now()); err != nil {
				return false, err
			}
		}
		if rows, err = o.reload(ctx, only); err != nil {
			return false, err
		}
	}
	for _, f := range finishedRuns(rows) {
		won, err := o.st.finalizeRun(ctx, f.RunID, f.Status, f.Error, o.instance+"/f", o.now())
		if err != nil {
			return false, err
		}
		if won {
			o.onFinished(ctx, f, rows)
		}
	}
	if !claim {
		return false, nil
	}
	for _, t := range readyTasks(rows, o.instance, o.now()) {
		select {
		case o.sem <- struct{}{}:
		default:
			return claimed, nil // all workers busy
		}
		token := o.instance + "/" + strconv.FormatInt(o.seq.Add(1), 10)
		ok, err := o.st.claim(ctx, t.RunID, t.ID, token, o.now())
		if err != nil || !ok {
			<-o.sem
			if err != nil {
				return claimed, err
			}
			continue
		}
		claimed = true
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			defer func() { <-o.sem }()
			o.execute(ctx, t, token)
			o.wake()
		}()
	}
	return claimed, nil
}

func (o *Orchestrator) reload(ctx context.Context, only int64) ([]TaskRow, error) {
	rows, err := o.st.activeTasks(ctx)
	if err != nil || only == 0 {
		return rows, err
	}
	kept := rows[:0]
	for _, r := range rows {
		if r.RunID == only {
			kept = append(kept, r)
		}
	}
	return kept, nil
}

// RunJob starts job now (trigger "manual"), drives it to the end in this
// process, whatever else is running, and returns the final run and tasks.
// Cancelling ctx cancels the running tasks; the run ends as cancelled.
func (o *Orchestrator) RunJob(ctx context.Context, name string) (RunRow, []TaskRow, error) {
	job, ok := o.cat.Load().jobs[name]
	if !ok {
		return RunRow{}, nil, fmt.Errorf("no job %q in %s/", name, Dir)
	}
	id, _, err := o.st.createRun(ctx, newRun{Job: job, Trigger: TriggerManual, PinnedTo: o.instance, Now: o.now()})
	if err != nil {
		return RunRow{}, nil, err
	}
	hb := time.Now()
	for ctx.Err() == nil {
		if time.Since(hb) >= o.opt.HeartbeatTTL/3 {
			hb = time.Now()
			_ = o.st.touch(ctx, o.instance, o.now())
		}
		claimed, err := o.step(ctx, id, true)
		if err != nil && ctx.Err() == nil {
			return RunRow{}, nil, err
		}
		if run, _, ok, err := GetRun(ctx, o.eng, id); err == nil && ok && run.Status != StatusRunning {
			break
		}
		if !claimed {
			select {
			case <-ctx.Done():
			case <-o.kick:
			case <-time.After(o.opt.Poll):
			}
		}
	}
	o.wg.Wait()
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	for range 3 { // cancelled attempts -> skips -> final status
		if _, err := o.step(fctx, id, false); err != nil {
			return RunRow{}, nil, err
		}
	}
	run, tasks, ok, err := GetRun(fctx, o.eng, id)
	if err != nil {
		return RunRow{}, nil, err
	}
	if !ok {
		return RunRow{}, nil, errors.New("run vanished")
	}
	return run, tasks, ctx.Err()
}

// prune deletes what is old: the fire keys (7 days) and, with a retention
// configured, finished runs and their attempts.
func (o *Orchestrator) prune(ctx context.Context) {
	if err := o.st.pruneFires(ctx, o.now().Add(-fireKeepFor)); err != nil && ctx.Err() == nil {
		o.log.Warn("prune fires", "err", err)
	}
	if o.opt.HistoryRetention > 0 {
		n, err := project.PruneJobsHistory(ctx, o.eng, o.now().Add(-o.opt.HistoryRetention))
		switch {
		case err != nil && ctx.Err() == nil:
			o.log.Warn("jobs history retention", "err", err)
		case n > 0:
			o.log.Info("jobs history pruned", "runs", n, "older_than", o.opt.HistoryRetention)
		}
	}
}

// fireKeepFor is how long fire keys are kept (CatchupHorizon must stay below).
const fireKeepFor = 7 * 24 * time.Hour

// Reload swaps in a new set of jobs while running (hot reload of jobs/*.yaml).
// The whole set is validated first; on error nothing changes. Runs in flight
// finish (a removed job's runs with its last known definition; an edited job's
// remaining tasks take the new parameters). A new job, or one whose schedule
// changed, starts its cron from now; a job whose schedule did not change keeps
// its cursor. Triggers of a new job take their baseline on the next poll, so
// existing snapshots or files never start a run.
func (o *Orchestrator) Reload(ctx context.Context, jobs []Job) error {
	c, err := newCatalog(jobs)
	if err != nil {
		return err
	}
	old := o.cat.Load()
	now := o.now()
	o.remember(c)
	o.cat.Store(c)
	o.mu.Lock()
	for name := range o.cursor {
		if _, ok := c.scheds[name]; !ok {
			delete(o.cursor, name)
		}
	}
	o.mu.Unlock()
	for _, name := range c.names {
		sc, ok := c.scheds[name]
		if !ok {
			continue
		}
		j := c.jobs[name]
		if prev, had := old.jobs[name]; had && prev.Schedule == j.Schedule && prev.Timezone == j.Timezone {
			continue
		}
		next := sc.Next(now)
		o.mu.Lock()
		o.cursor[name] = next
		o.mu.Unlock()
		// Handled up to now: a restart does not replay ticks of the old schedule.
		_ = o.st.setState(ctx, cronKey(name), strconv.FormatInt(now.Unix(), 10), now)
	}
	o.wake()
	return nil
}

// JobNames lists the jobs currently loaded.
func (o *Orchestrator) JobNames() []string { return append([]string(nil), o.cat.Load().names...) }
