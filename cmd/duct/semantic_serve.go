package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/semantic"
)

// preaggRefreshEvery is how often serve checks whether the preaggregates are
// stale (a new lake snapshot after their source) and rebuilds them.
var preaggRefreshEvery = 15 * time.Second

// startSemantic keeps the semantic model of the project live inside `duct
// serve`: semantic/*.ossie.yaml are re-read when they change (validated first;
// an invalid edit is logged and the previous model keeps serving), every
// consumer follows through live, and the preaggregates are refreshed after
// each change of the lake or of the model. The returned func waits for it to
// stop after ctx ends.
func startSemantic(ctx context.Context, p *project.Project, live *semantic.Live, handler *governance.Handler) func() {
	kick := make(chan struct{}, 1)
	live.OnChange(func(m *semantic.Model) {
		handler.SetSemanticModel(m)
		select {
		case kick <- struct{}{}:
		default:
		}
	})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); live.Watch(ctx, 0, slog.Default()) }()
	go func() {
		defer wg.Done()
		t := time.NewTicker(preaggRefreshEvery)
		defer t.Stop()
		for {
			if m := live.Model(); m != nil && len(m.Preaggs()) > 0 {
				refreshPreaggs(ctx, p, m)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			case <-kick:
			}
		}
	}()
	return wg.Wait
}

// refreshPreaggs rebuilds the stale on_snapshot preaggregates with an engine
// of its own (the serving one is locked and cannot write the lake).
func refreshPreaggs(ctx context.Context, p *project.Project, m *semantic.Model) {
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		slog.Warn("semantic preaggregates: cannot open the engine", "error", err)
		return
	}
	defer func() { _ = eng.Close() }()
	rep, err := governance.RefreshPreaggs(ctx, eng, m, governance.PreaggOptions{})
	switch {
	case ctx.Err() != nil:
	case err != nil:
		slog.Warn("semantic preaggregates: refresh failed", "error", err)
	case rep.Built > 0 || rep.Dropped > 0:
		slog.Info("semantic preaggregates refreshed", "built", rep.Built, "fresh", rep.Current, "dropped", rep.Dropped)
	}
}
