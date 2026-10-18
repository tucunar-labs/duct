package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/reload"
	"github.com/alonsoburon/duct/core/udf"
)

// functionsReloadEvery is how often serve looks for changes in functions/.
var functionsReloadEvery = reload.DefaultInterval

// startFunctionsReload follows functions/*.py and functions/*.toml while `duct
// serve` runs (ADR 0005). A changed folder is discovered as a whole first; if that
// fails (syntax error, bad type, a changed signature) the error is logged and
// the previous functions keep serving. Otherwise udf.Manager.Reload swaps the
// bodies, registers new names and replaces the Python workers once their
// in-flight batches finish. The admin-only guard learns the new names before the
// functions become callable. baseline is functionsSignature taken before the server reported ready, so a file
// written right after that is seen as a change. The returned function waits for the watcher to stop.
func startFunctionsReload(ctx context.Context, p *project.Project, m *udf.Manager, handler *governance.Handler, baseline string) func() {
	if m == nil {
		return func() {}
	}
	m.SetGuard(handler.SetAdminOnlyFunctions)
	dir := filepath.Join(p.Root, udf.DirName)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		reload.Poll(ctx, functionsReloadEvery, dir, []string{"*.py", "*.toml"}, baseline, func(string) {
			start := time.Now()
			ch, err := m.Reload(ctx)
			if err != nil {
				slog.Error("functions: reload rejected, keeping the previous functions", "error", err)
				return
			}
			slog.Info("functions: reloaded", "added", ch.Added, "removed", ch.Removed, "took", time.Since(start).Round(time.Millisecond))
		})
	}()
	return wg.Wait
}

// functionsSignature is the baseline of the functions/ watcher.
func functionsSignature(p *project.Project) string {
	sig, _ := reload.Signature(filepath.Join(p.Root, udf.DirName), "*.py", "*.toml")
	return sig
}
