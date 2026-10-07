package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/orchestrator"
	"github.com/alonsoburon/duct/core/project"
)

// startJobs runs the orchestrator of jobs/*.yaml inside `duct serve` until ctx
// is cancelled. The returned channel closes when it has fully stopped (running
// tasks cancelled and recorded). With no jobs, or noJobs, it does nothing.
//
// Tasks that run user code get DUCT_URL (this server's HTTP protocol) and
// DUCT_TOKEN (DUCT_JOBS_TOKEN, else DUCT_TOKEN, from the server's environment):
// they talk to the lake as that governed user, never with lake credentials.
func startJobs(ctx context.Context, p *project.Project, httpURL string, noJobs bool) (<-chan struct{}, error) {
	done := make(chan struct{})
	if noJobs {
		close(done)
		return done, nil
	}
	jobs, err := orchestrator.Load(p.Root)
	if err != nil || len(jobs) == 0 {
		close(done)
		return done, err
	}
	eng, err := openEngineUDF(ctx, p, false) // tasks write the lake: not the locked serving engine
	if err != nil {
		close(done)
		return done, err
	}
	env := map[string]string{}
	if httpURL != "" {
		env["DUCT_URL"] = httpURL
	}
	if v := firstNonEmpty(os.Getenv("DUCT_JOBS_TOKEN"), os.Getenv("DUCT_TOKEN")); v != "" {
		env["DUCT_TOKEN"] = v
	}
	o, err := orchestrator.New(eng, jobs, orchestrator.Options{
		Root: p.Root, Env: env,
		// serve silences the default logger (duckgres logs every statement).
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
	})
	if err != nil {
		_ = eng.Close()
		close(done)
		return done, err
	}
	go func() {
		defer close(done)
		defer func() { _ = eng.Close() }()
		_ = o.Run(ctx)
	}()
	select { // trigger baselines are taken before serve reports ready
	case <-o.Ready():
	case <-ctx.Done():
	case <-time.After(30 * time.Second):
	}
	return done, nil
}

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
