package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/orchestrator"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/reload"
)

// startJobs runs the orchestrator of jobs/*.yaml inside `duct serve` until ctx
// is cancelled. The returned channel closes when it has fully stopped (running
// tasks cancelled and recorded). With no jobs, or noJobs, it does nothing.
//
// Tasks that run user code get DUCT_URL (this server's HTTP protocol) and
// DUCT_TOKEN (DUCT_JOBS_TOKEN, else DUCT_TOKEN, from the server's environment):
// they talk to the lake as that governed user, never with lake credentials.
func startJobs(ctx context.Context, p *project.Project, httpURL string, noJobs bool, holder *orchHolder) (<-chan struct{}, error) {
	done := make(chan struct{})
	if noJobs {
		close(done)
		return done, nil
	}
	// The baseline of the watcher is taken before reading: an edit in between
	// is seen as a change and reloaded (harmlessly), never missed.
	baseline, _ := reload.Signature(filepath.Join(p.Root, orchestrator.Dir), "*.yaml", "*.yml")
	jobs, err := orchestrator.Load(p.Root)
	if err != nil {
		close(done)
		return done, err
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})) // serve silences the default logger
	retention, _ := p.Config.Jobs.RetentionDuration()
	env := map[string]string{}
	if httpURL != "" {
		env["DUCT_URL"] = httpURL
	}
	if v := firstNonEmpty(os.Getenv("DUCT_JOBS_TOKEN"), os.Getenv("DUCT_TOKEN")); v != "" {
		env["DUCT_TOKEN"] = v
	}
	js := &jobsServer{ctx: ctx, p: p, log: logger, holder: holder, opt: orchestrator.Options{Root: p.Root, Env: env, Logger: logger, HistoryRetention: retention}}
	if len(jobs) > 0 {
		if err := js.start(jobs); err != nil {
			close(done)
			return done, err
		}
		select { // trigger baselines are taken before serve reports ready
		case <-js.o.Ready():
		case <-ctx.Done():
		case <-time.After(30 * time.Second):
		}
	}
	go func() {
		defer close(done)
		js.watch(baseline)
		js.wait()
	}()
	return done, nil
}

// jobsServer is the orchestrator of `duct serve` plus the hot reload of
// jobs/*.yaml: the folder is polled, a changed set is parsed and validated
// as a whole and swapped in (Orchestrator.Reload); an invalid edit is logged
// and the jobs that were running keep running. The orchestrator (and its
// engine) is only created once there is a first job, so a project may add
// its first job file while serve is up.
type jobsServer struct {
	ctx context.Context
	p   *project.Project
	log *slog.Logger
	opt orchestrator.Options

	o        *orchestrator.Orchestrator
	finished chan struct{}
	holder   *orchHolder // shares o with the web console (console.go); may be nil
}

func (j *jobsServer) start(jobs []orchestrator.Job) error {
	eng, err := openEngineUDF(j.ctx, j.p, false) // tasks write the lake: not the locked serving engine
	if err != nil {
		return err
	}
	o, err := orchestrator.New(eng, jobs, j.opt)
	if err != nil {
		_ = eng.Close()
		return err
	}
	j.o, j.finished = o, make(chan struct{})
	if j.holder != nil {
		j.holder.o.Store(o)
	}
	go func() {
		defer close(j.finished)
		defer func() { _ = eng.Close() }()
		_ = o.Run(j.ctx)
	}()
	return nil
}

func (j *jobsServer) wait() {
	if j.finished != nil {
		<-j.finished
	}
}

func (j *jobsServer) watch(baseline string) {
	reload.Poll(j.ctx, jobsReloadEvery, filepath.Join(j.p.Root, orchestrator.Dir), []string{"*.yaml", "*.yml"}, baseline, func(string) {
		jobs, err := orchestrator.Load(j.p.Root)
		if err != nil {
			j.log.Error("jobs: reload rejected, keeping the previous jobs", "error", err)
			return
		}
		switch {
		case j.o == nil && len(jobs) == 0:
		case j.o == nil:
			if err := j.start(jobs); err != nil {
				j.log.Error("jobs: cannot start the orchestrator", "error", err)
				return
			}
			j.log.Info("jobs: loaded", "jobs", len(jobs))
		default:
			if err := j.o.Reload(j.ctx, jobs); err != nil {
				j.log.Error("jobs: reload rejected, keeping the previous jobs", "error", err)
				return
			}
			j.log.Info("jobs: reloaded", "jobs", len(jobs))
		}
	})
}

// jobsReloadEvery is how often serve looks for changes in jobs/.
var jobsReloadEvery = reload.DefaultInterval

// httpURLOf is the base URL of the HTTP protocol, if one is served.
func httpURLOf(p *project.Project, names []string, protos []contract.Protocol) string {
	for i, n := range names {
		if n != "http" {
			continue
		}
		a, ok := protos[i].(contract.Addresser)
		if !ok {
			return ""
		}
		host, port, err := net.SplitHostPort(a.Addr())
		if err != nil {
			return ""
		}
		if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
			host = "127.0.0.1"
		}
		scheme := "http"
		if m := p.Config.Serve.Modules["http"]; m != nil && m["tls_cert"] != nil {
			scheme = "https"
		}
		return scheme + "://" + net.JoinHostPort(host, port)
	}
	return ""
}
