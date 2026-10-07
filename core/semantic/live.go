package semantic

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alonsoburon/duct/core/reload"
)

// Live is the semantic model of a project folder that can be reloaded while
// `duct serve` runs: a new version is parsed, validated and resolved first
// and only then swapped in; if it is invalid the previous model keeps serving
// and the error is logged.
type Live struct {
	dir string
	cur atomic.Pointer[Model]
	sig atomic.Value // string

	mu   sync.Mutex
	subs []func(*Model)
}

var modelGlobs = []string{"*.ossie.yaml", "*.ossie.yml"}

// NewLive loads dir. A folder without models is fine (Model returns nil until
// one appears); an invalid model is an error.
func NewLive(dir string) (*Live, error) {
	l := &Live{dir: dir}
	sig, err := reload.Signature(dir, modelGlobs...)
	if err != nil {
		return nil, err
	}
	m, err := LoadDir(dir)
	switch {
	case err == nil:
		l.cur.Store(m)
	case errors.Is(err, ErrNoModel):
	default:
		return nil, err
	}
	l.sig.Store(sig)
	return l, nil
}

// Model is the current model (nil when there is none).
func (l *Live) Model() *Model { return l.cur.Load() }

// OnChange registers fn, called with the new model (nil if every file was
// removed) after each successful swap.
func (l *Live) OnChange(fn func(*Model)) {
	l.mu.Lock()
	l.subs = append(l.subs, fn)
	l.mu.Unlock()
}

// Reload re-reads the folder now. It reports whether the model changed. On an
// invalid model it returns the error and keeps the previous one.
func (l *Live) Reload() (bool, error) {
	sig, err := reload.Signature(l.dir, modelGlobs...)
	if err != nil {
		return false, err
	}
	if prev, _ := l.sig.Load().(string); prev == sig {
		return false, nil
	}
	m, err := LoadDir(l.dir)
	if err != nil && !errors.Is(err, ErrNoModel) {
		l.sig.Store(sig) // do not retry the same broken content
		return false, err
	}
	if errors.Is(err, ErrNoModel) {
		m = nil
	}
	l.sig.Store(sig)
	l.cur.Store(m)
	l.mu.Lock()
	subs := append([]func(*Model){}, l.subs...)
	l.mu.Unlock()
	for _, fn := range subs {
		fn(m)
	}
	return true, nil
}

// Watch reloads the folder every interval (0: 2 s) until ctx ends, logging
// each outcome.
func (l *Live) Watch(ctx context.Context, interval time.Duration, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = reload.DefaultInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		changed, err := l.Reload()
		switch {
		case err != nil:
			log.Error("semantic model: reload rejected, keeping the previous model", "dir", filepath.Clean(l.dir), "error", err)
		case changed:
			if m := l.Model(); m != nil {
				log.Info("semantic model reloaded", "datasets", len(m.Datasets), "metrics", len(m.Metrics), "preaggregates", len(m.Preaggs()))
			} else {
				log.Warn("semantic model removed: no *.ossie.yaml left")
			}
		}
	}
}
