package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contracts"
	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/pipeline"
	"github.com/alonsoburon/duct/core/registry"
)

// execute runs one claimed attempt and records how it ended: success, a retry
// after backoff, a final failure, or cancellation by shutdown.
func (o *Orchestrator) execute(ctx context.Context, row TaskRow, token string) {
	start := o.now()
	attempt := row.Attempts + 1
	var (
		res contract.TaskResult
		err error
	)
	job, jok := o.job(row.Job)
	task, tok := job.Task(row.ID)
	var actx context.Context
	var cancel context.CancelFunc = func() {}
	if !jok || !tok {
		err = fmt.Errorf("task %s/%s is not defined in this process's %s/ (jobs changed during the run?)", row.Job, row.ID, Dir)
	} else {
		actx, cancel = context.WithTimeout(ctx, task.timeout())
		res, err = o.runTask(actx, job, task, row.RunID)
	}
	defer cancel()

	status, msg, retryAt := StatusSuccess, "", time.Time{}
	if err != nil {
		msg = err.Error()
		switch {
		case ctx.Err() != nil:
			status, msg = StatusCancelled, "interrupted: "+ctx.Err().Error()
		case actx != nil && errors.Is(actx.Err(), context.DeadlineExceeded):
			status, msg = StatusFailed, fmt.Sprintf("timeout after %s", task.timeout())
		default:
			status = StatusFailed
		}
		if status == StatusFailed && tok && attempt < row.MaxAttempts {
			retryAt = o.now().Add(task.RetryDelay(attempt))
			o.log.Warn("task failed, will retry", "job", row.Job, "task", row.ID, "attempt", attempt, "in", task.RetryDelay(attempt), "err", firstLine(msg))
		}
	}
	// Own context: the outcome must be recorded even during shutdown.
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer rcancel()
	if ferr := o.st.finishTask(rctx, row.RunID, row.ID, token, status, msg, res.Output, start, o.now(), retryAt); ferr != nil {
		o.log.Error("recording task outcome", "job", row.Job, "task", row.ID, "err", ferr)
	}
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	return s
}

// baseEnv is the allow-list of the parent environment tasks inherit. Lake and
// catalog credentials (DUCT_S3_*, AWS_*, DUCT_CATALOG_*) are not in it.
var baseEnv = []string{"PATH", "HOME", "USER", "LANG", "LC_ALL", "TMPDIR", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR"}

// taskEnv builds the complete environment of a task process.
func (o *Orchestrator) taskEnv(job Job, runID int64, t Task) map[string]string {
	env := map[string]string{}
	for _, k := range baseEnv {
		if v, ok := os.LookupEnv(k); ok {
			env[k] = v
		}
	}
	for k, v := range o.opt.Env {
		env[k] = v
	}
	env["DUCT_JOB"], env["DUCT_TASK"], env["DUCT_RUN_ID"] = job.Name, t.ID, strconv.FormatInt(runID, 10)
	if m, ok := t.Params["env"].(map[string]any); ok {
		for k, v := range m {
			env[k] = fmt.Sprint(v)
		}
	}
	return env
}

func (o *Orchestrator) runTask(ctx context.Context, job Job, t Task, runID int64) (contract.TaskResult, error) {
	switch t.Type {
	case TypePipeline:
		return o.runPipeline(ctx, t)
	case TypeIngest:
		return o.runIngest(ctx, t)
	}
	info, ok := registry.Lookup(contract.KindTaskType, t.Type)
	if !ok {
		return contract.TaskResult{}, fmt.Errorf("task type %q is not linked into this binary (see `duct modules`)", t.Type)
	}
	mod, ok := info.New().(contract.TaskType)
	if !ok {
		return contract.TaskResult{}, fmt.Errorf("module task/%s does not implement contract.TaskType", t.Type)
	}
	if err := mod.Init(ctx, contract.Config{"root": o.opt.Root}); err != nil {
		_ = mod.Close()
		return contract.TaskResult{}, fmt.Errorf("task %s: %w", t.Type, err)
	}
	defer func() { _ = mod.Close() }()
	params := contract.Config{}
	for k, v := range t.Params {
		params[k] = v
	}
	params["root"] = o.opt.Root
	params["env"] = o.taskEnv(job, runID, t)
	return mod.Run(ctx, contract.TaskSpec{Name: t.ID, Params: params})
}

func stringList(v any) []string {
	l, _ := v.([]any)
	out := make([]string, 0, len(l))
	for _, x := range l {
		out = append(out, fmt.Sprint(x))
	}
	return out
}

func (o *Orchestrator) runPipeline(ctx context.Context, t Task) (contract.TaskResult, error) {
	tables, err := pipeline.Load(o.opt.Root)
	if err != nil {
		return contract.TaskResult{}, err
	}
	only, _ := t.Params["only"].(bool)
	ref, err := contracts.RunPipeline(ctx, o.eng, o.opt.Root, tables, pipeline.Options{Targets: stringList(t.Params["targets"]), Only: only})
	rep := ref.Report
	var b strings.Builder
	var failed []string
	if rep != nil {
		for _, r := range rep.Results {
			fmt.Fprintf(&b, "%s %s %d rows\n", r.Status, r.ID, r.Rows)
			if r.Status != pipeline.StatusSuccess {
				failed = append(failed, fmt.Sprintf("%s (%s): %v", r.ID, r.Status, r.Err))
			}
		}
	}
	if err != nil {
		return contract.TaskResult{Output: b.String()}, err
	}
	for _, l := range ref.Log {
		b.WriteString(l + "\n")
	}
	for _, w := range ref.Warnings {
		fmt.Fprintf(&b, "warning: %s\n", w)
	}
	if len(failed) > 0 {
		return contract.TaskResult{Output: b.String()}, fmt.Errorf("pipeline: %s", strings.Join(failed, "; "))
	}
	return contract.TaskResult{Output: strings.TrimSpace(b.String())}, nil
}

func (o *Orchestrator) runIngest(ctx context.Context, t Task) (contract.TaskResult, error) {
	all, err := ingest.Load(o.opt.Root)
	if err != nil {
		return contract.TaskResult{}, err
	}
	selected := all
	if names := stringList(t.Params["names"]); len(names) > 0 {
		by := map[string]ingest.Config{}
		for _, c := range all {
			by[c.Name] = c
		}
		selected = nil
		for _, n := range names {
			c, ok := by[n]
			if !ok {
				return contract.TaskResult{}, fmt.Errorf("no ingestion %q in %s/", n, ingest.Dir)
			}
			selected = append(selected, c)
		}
	}
	var b strings.Builder
	var failed []string
	for _, c := range selected {
		res, err := ingest.Run(ctx, o.eng, o.opt.Root, c)
		if err != nil {
			if ctx.Err() != nil {
				return contract.TaskResult{Output: b.String()}, ctx.Err()
			}
			failed = append(failed, fmt.Sprintf("%s: %v", c.Name, err))
			continue
		}
		fmt.Fprintf(&b, "%s -> %s %d rows\n", c.Name, res.Table, res.Rows)
	}
	if len(failed) > 0 {
		return contract.TaskResult{Output: b.String()}, fmt.Errorf("ingest: %s", strings.Join(failed, "; "))
	}
	return contract.TaskResult{Output: strings.TrimSpace(b.String())}, nil
}

// onFinished tells the job's alert channels about a run that this process
// closed: firing when it failed, resolved when it succeeds after a failure.
func (o *Orchestrator) onFinished(ctx context.Context, f finished, rows []TaskRow) {
	var name string
	for _, r := range rows {
		if r.RunID == f.RunID {
			name = r.Job
			break
		}
	}
	o.log.Info("job finished", "job", name, "run", f.RunID, "status", f.Status)
	job, ok := o.job(name)
	if !ok || len(job.Alerts.OnFailure) == 0 {
		return
	}
	labels := map[string]string{"job": name, "run_id": strconv.FormatInt(f.RunID, 10)}
	var a contract.Alert
	switch f.Status {
	case StatusFailed:
		a = contract.Alert{Rule: "job:" + name, Key: name, State: contract.AlertFiring, Labels: labels, At: o.now(),
			Message: fmt.Sprintf("job %s: run %d failed: %s", name, f.RunID, firstLine(f.Error))}
	case StatusSuccess:
		prev, err := o.st.previousStatus(ctx, name, f.RunID)
		if err != nil || prev != StatusFailed {
			return
		}
		a = contract.Alert{Rule: "job:" + name, Key: name, State: contract.AlertResolved, Labels: labels, At: o.now(),
			Message: fmt.Sprintf("job %s: run %d succeeded after a failure", name, f.RunID)}
	default:
		return
	}
	// Own context: the alert must go out even while shutting down.
	nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	for _, spec := range job.Alerts.OnFailure {
		if err := o.notify(nctx, spec, a); err != nil {
			o.log.Error("alert not delivered", "job", name, "channel", spec.Channel, "err", err)
		}
	}
}

func (o *Orchestrator) notify(ctx context.Context, spec AlertSpec, a contract.Alert) error {
	if o.opt.Notify != nil {
		return o.opt.Notify(ctx, spec, a)
	}
	return Notify(ctx, spec, a)
}

// Notify delivers a through the KindAlertChannel module spec.Channel,
// initialised with spec.Config. It is the helper other parts of Duct (the
// alerts engine) can reuse to reach a channel declared in YAML.
func Notify(ctx context.Context, spec AlertSpec, a contract.Alert) error {
	info, ok := registry.Lookup(contract.KindAlertChannel, spec.Channel)
	if !ok {
		return fmt.Errorf("alert channel %q is not linked into this binary", spec.Channel)
	}
	ch, ok := info.New().(contract.AlertChannel)
	if !ok {
		return fmt.Errorf("module alert.channel/%s does not implement contract.AlertChannel", spec.Channel)
	}
	if err := ch.Init(ctx, contract.Config(spec.Config)); err != nil {
		_ = ch.Close()
		return err
	}
	defer func() { _ = ch.Close() }()
	return ch.Notify(ctx, a)
}
