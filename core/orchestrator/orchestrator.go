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
	jobs     map[string]Job
	names    []string
	scheds   map[string]Schedule
	instance string
	seq      atomic.Int64
	log      *slog.Logger

	kick  chan struct{}
	sem   chan struct{}
	wg    sync.WaitGroup // running tasks
	ready chan struct{}

	mu     sync.Mutex
	cursor map[string]time.Time // next cron tick per job
}

// New validates the jobs and builds an orchestrator. The Duct tables must
// already be migrated (project.Migrate).
func New(eng contract.Engine, jobs []Job, opt Options) (*Orchestrator, error) {
	opt = opt.withDefaults()
	host, _ := os.Hostname()
	o := &Orchestrator{
		eng: eng, st: &store{eng: eng}, opt: opt, log: opt.Logger,
		jobs: map[string]Job{}, scheds: map[string]Schedule{}, cursor: map[string]time.Time{},
		instance: fmt.Sprintf("%s-%d-%s", host, os.Getpid(), randHex(3)),
		kick:     make(chan struct{}, 1), sem: make(chan struct{}, opt.Workers), ready: make(chan struct{}),
	}
	for _, j := range jobs {
		j := j
		if err := j.Validate(); err != nil {
			return nil, fmt.Errorf("job %s: %w", j.Name, err)
		}
		if _, dup := o.jobs[j.Name]; dup {
			return nil, fmt.Errorf("job %s defined twice", j.Name)
		}
		o.jobs[j.Name] = j
		o.names = append(o.names, j.Name)
		if j.Schedule != "" {
			sc, _ := ParseSchedule(j.Schedule, j.Timezone)
			o.scheds[j.Name] = sc
		}
	}
	sort.Strings(o.names)
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

// fireDue starts the runs of every cron job whose tick has come.
func (o *Orchestrator) fireDue(ctx context.Context) {
	now := o.now()
	for _, name := range o.names {
		sc, ok := o.scheds[name]
		if !ok {
			continue
		}
		o.mu.Lock()
		tick, next, due := advance(sc, o.cursor[name], now)
		o.cursor[name] = next
		o.mu.Unlock()
		if !due {
			continue
		}
		if n, err := o.st.activeRuns(ctx, name); err != nil {
			o.log.Warn("cron", "job", name, "err", err)
			continue
		} else if n > 0 {
			o.log.Info("cron tick skipped: previous run still active", "job", name, "tick", tick)
			continue
		}
		if _, err := o.fire(ctx, o.jobs[name], TriggerCron, "cron:"+name+":"+strconv.FormatInt(tick.Unix(), 10), tick, ""); err != nil && ctx.Err() == nil {
			o.log.Warn("cron fire", "job", name, "err", err)
		}
	}
}

// nextWake is how long the cron loop may sleep.
func (o *Orchestrator) nextWake() time.Duration {
	d := time.Second
	now := o.now()
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, n := range o.cursor {
		if !n.IsZero() && n.Sub(now) < d {
			d = n.Sub(now)
		}
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
	o.mu.Lock()
	for name, sc := range o.scheds {
		o.cursor[name] = sc.Next(now)
	}
	o.mu.Unlock()
	o.pollTriggers(ctx) // baselines
	close(o.ready)

	var loops sync.WaitGroup
	if len(o.scheds) > 0 {
		loops.Add(1)
		go func() {
			defer loops.Done()
			for ctx.Err() == nil {
				o.fireDue(ctx)
				sleep(ctx, o.nextWake())
			}
		}()
	}
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
				_ = o.st.pruneFires(ctx, o.now().Add(-7*24*time.Hour))
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
	job, ok := o.jobs[name]
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
