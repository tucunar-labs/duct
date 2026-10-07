package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/alonsoburon/duct/core/orchestrator"
	"github.com/alonsoburon/duct/core/project"
)

// runJobs wires `duct jobs` (own entry point, like ingest).
func runJobs(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := cmdJobs(ctx, args, stdout, stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct jobs: %v\n", err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct jobs: %v\n", err)
		return 1
	}
}

const jobsUsage = "usage: duct jobs list | run <job> | history [job] [--limit N] [--run ID]"

func cmdJobs(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageErr("%s", jobsUsage)
	}
	switch args[0] {
	case "list":
		return jobsList(ctx, args[1:], stdout, stderr)
	case "run":
		return jobsRun(ctx, args[1:], stdout, stderr)
	case "history":
		return jobsHistory(ctx, args[1:], stdout, stderr)
	}
	return usageErr("unknown subcommand %q; %s", args[0], jobsUsage)
}

// splitArgs separates positionals from flags, allowing flags after them.
func splitArgs(fs *flag.FlagSet, args []string) (pos []string, err error) {
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return nil, err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		pos, rest = append(pos, rest[0]), rest[1:]
	}
	return pos, nil
}

// jobsEnv is what a manual run hands to task processes: the server to talk to.
func jobsEnv() map[string]string {
	env := map[string]string{}
	if v := os.Getenv("DUCT_URL"); v != "" {
		env["DUCT_URL"] = v
	}
	if v := firstNonEmpty(os.Getenv("DUCT_JOBS_TOKEN"), os.Getenv("DUCT_TOKEN")); v != "" {
		env["DUCT_TOKEN"] = v
	}
	return env
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

func jobsList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("jobs list", stderr)
	if _, err := splitArgs(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	jobs, err := orchestrator.Load(p.Root)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		_, _ = fmt.Fprintf(stdout, "No jobs: add %s/<name>.yaml\n", orchestrator.Dir)
		return nil
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		return err
	}
	last, err := orchestrator.LastRuns(ctx, eng)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "JOB\tSCHEDULE\tNEXT\tTRIGGERS\tTASKS\tLAST RUN")
	for _, j := range jobs {
		sched, next := "-", "-"
		if j.Schedule != "" {
			sched = j.Schedule
			if j.Timezone != "" {
				sched += " " + j.Timezone
			}
			if n, ok := orchestrator.NextFire(j, time.Now()); ok {
				next = n.Local().Format("2006-01-02 15:04:05")
			}
		}
		var trig []string
		if len(j.Triggers.OnSnapshot) > 0 {
			trig = append(trig, "snapshot:"+strings.Join(j.SnapshotTables(), ","))
		}
		if j.Triggers.OnFile != "" {
			trig = append(trig, "file:"+j.Triggers.OnFile)
		}
		t := "-"
		if len(trig) > 0 {
			t = strings.Join(trig, " ")
		}
		l := "-"
		if r, ok := last[j.Name]; ok {
			l = fmt.Sprintf("%s (%s, %s)", r.Status, r.Trigger, r.StartedAt.Local().Format("01-02 15:04:05"))
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n", j.Name, sched, next, t, len(j.Tasks), l)
	}
	return tw.Flush()
}

func jobsRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("jobs run", stderr)
	pos, err := splitArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usageErr("jobs run needs one job name")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	jobs, err := orchestrator.Load(p.Root)
	if err != nil {
		return err
	}
	eng, err := openEngineUDF(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		return err
	}
	o, err := orchestrator.New(eng, jobs, orchestrator.Options{Root: p.Root, Env: jobsEnv()})
	if err != nil {
		return err
	}
	run, tasks, err := o.RunJob(ctx, pos[0])
	if run.ID != 0 {
		printRun(stdout, run, tasks)
	}
	if err != nil {
		return err
	}
	if run.Status != orchestrator.StatusSuccess {
		return fmt.Errorf("run %d %s", run.ID, run.Status)
	}
	return nil
}

func jobsHistory(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("jobs history", stderr)
	limit := fs.Int("limit", 20, "how many runs to show")
	runID := fs.Int64("run", 0, "show one run with its tasks")
	pos, err := splitArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) > 1 {
		return usageErr("jobs history takes at most one job name")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		return err
	}
	if *runID != 0 {
		run, tasks, ok, err := orchestrator.GetRun(ctx, eng, *runID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no run %d", *runID)
		}
		printRun(stdout, run, tasks)
		return nil
	}
	job := ""
	if len(pos) == 1 {
		job = pos[0]
	}
	runs, err := orchestrator.History(ctx, eng, job, *limit)
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		_, _ = fmt.Fprintln(stdout, "No runs yet.")
		return nil
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "RUN\tJOB\tTRIGGER\tSTATUS\tSTARTED\tDURATION\tERROR")
	for _, r := range runs {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", r.ID, r.Job, r.Trigger, r.Status,
			r.StartedAt.Local().Format("01-02 15:04:05"), runDuration(r), oneLine(r.Error, 80))
	}
	return tw.Flush()
}

func runDuration(r orchestrator.RunRow) string {
	if r.FinishedAt.IsZero() {
		return "-"
	}
	return r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond).String()
}

func oneLine(s string, n int) string {
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > n {
		s = s[:n-1] + "…"
	}
	return s
}

func printRun(w io.Writer, run orchestrator.RunRow, tasks []orchestrator.TaskRow) {
	_, _ = fmt.Fprintf(w, "run %d  job %s  %s (%s)  %s\n", run.ID, run.Job, strings.ToUpper(run.Status), run.Trigger, runDuration(run))
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, t := range tasks {
		_, _ = fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\n", t.ID, t.Type, strings.ToUpper(t.Status),
			"attempts "+strconv.Itoa(t.Attempts)+"/"+strconv.Itoa(t.MaxAttempts), t.Duration.Round(time.Millisecond))
	}
	_ = tw.Flush()
	for _, t := range tasks {
		if t.Error != "" {
			_, _ = fmt.Fprintf(w, "  %s: %s\n", t.ID, indent(t.Error))
		}
	}
}

func indent(s string) string { return strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n    ") }
