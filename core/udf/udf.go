// Package udf loads the user-defined functions of a project (ADR 0005) and
// registers them in an engine:
//
//   - functions/*.py: Python functions declared with @duct.function. DuckDB
//     cannot run Python from Go, so each call hands a whole Arrow batch to a
//     long-lived worker process (`python -m duct.udf.worker`) over stdin/stdout.
//   - functions/*.toml: executables (ClickHouse "executable UDF" style): a
//     command that reads one row per line on stdin and writes one result per
//     line on stdout. No Python involved.
//
// The processes inherit a small allow-listed environment, never the lake or
// catalog credentials of the Go process.
package udf

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// DirName is the folder of a project that holds the functions.
const DirName = "functions"

// Spec describes one function found in a project.
type Spec struct {
	Name       string
	ArgTypes   []string
	ReturnType string
	Kind       string // "python" or "exec"
	File       string // source file, relative to the project
	Vectorized bool   // python only
	Table      bool   // a table function: Columns instead of ReturnType
	Columns    []contract.TableColumn
	exec       *execSpec
}

// callFunc and tableCallFunc are the bodies behind a registered function.
type (
	callFunc      = func(context.Context, []arrow.Array) (arrow.Array, error)
	tableCallFunc = func(context.Context, []arrow.Array) ([]arrow.RecordBatch, error)
)

// signature is what DuckDB fixed at registration: it cannot change while the
// process lives.
func (s Spec) signature() string {
	if s.Table {
		cols := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			cols[i] = c.Name + " " + c.Type
		}
		return fmt.Sprintf("table(%s) -> (%s)", strings.Join(s.ArgTypes, ", "), strings.Join(cols, ", "))
	}
	return fmt.Sprintf("(%s) -> %s", strings.Join(s.ArgTypes, ", "), s.ReturnType)
}

// Options configure discovery and the worker processes.
type Options struct {
	// Root is the project directory.
	Root string
	// Python is the argv prefix that starts an interpreter that can import
	// duct (default: python3). `uv run --project <root> python` is typical.
	Python []string
	// Workers caps the Python worker processes (default 2); they start lazily.
	Workers int
	// Timeout caps one batch (default 60 s).
	Timeout time.Duration
	// StartTimeout caps one Python worker cold start (interpreter + imports),
	// separate from Timeout (default 2 min).
	StartTimeout time.Duration
	// ExtraEnv is added to the allow-listed environment of the children.
	ExtraEnv map[string]string
}

func (o Options) withDefaults() Options {
	if len(o.Python) == 0 {
		o.Python = []string{"python3"}
	}
	if o.Workers <= 0 {
		o.Workers = 2
	}
	if o.StartTimeout <= 0 {
		o.StartTimeout = 2 * time.Minute
	}
	if o.Timeout <= 0 {
		o.Timeout = time.Minute
	}
	return o
}

// Exists reports whether the project has any function file.
func Exists(root string) bool {
	for _, pat := range []string{"*.py", "*.toml"} {
		if m, _ := filepath.Glob(filepath.Join(root, DirName, pat)); len(m) > 0 {
			return true
		}
	}
	return false
}

// Discover lists the functions of the project without registering them.
func Discover(ctx context.Context, opt Options) ([]Spec, error) {
	opt = opt.withDefaults()
	dir := filepath.Join(opt.Root, DirName)
	var specs []Spec
	tomls, _ := filepath.Glob(filepath.Join(dir, "*.toml"))
	sort.Strings(tomls)
	for _, f := range tomls {
		s, err := loadExecSpec(f)
		if err != nil {
			return nil, err
		}
		s.File, _ = filepath.Rel(opt.Root, f)
		specs = append(specs, s)
	}
	if pys, _ := filepath.Glob(filepath.Join(dir, "*.py")); len(pys) > 0 {
		ps, err := discoverPython(ctx, opt)
		if err != nil {
			return nil, err
		}
		specs = append(specs, ps...)
	}
	seen := map[string]string{}
	for _, s := range specs {
		k := strings.ToLower(s.Name)
		if prev, dup := seen[k]; dup {
			return nil, fmt.Errorf("function %s is defined twice (%s and %s)", s.Name, prev, s.Kind)
		}
		seen[k] = s.Kind
	}
	return specs, nil
}

// Attach discovers the project's functions and registers them in eng. It
// returns their names (lower case). The processes start on first use and stop
// when eng closes. It does nothing when there is no functions/ folder; it
// fails when eng cannot register functions. Use AttachManaged to reload them.
func Attach(ctx context.Context, eng contract.Engine, opt Options) ([]string, error) {
	m, err := AttachManaged(ctx, eng, opt)
	if m == nil || err != nil {
		return nil, err
	}
	return m.Names(), nil
}

// slot is one function registered in the engine. DuckDB cannot unregister or
// re-register a function, so the registered body is a stub that forwards to the
// current implementation, which a reload swaps.
type slot struct {
	spec    Spec
	impl    atomic.Pointer[impl]
	removed bool // guarded by Manager.mu
}

type impl struct {
	call  callFunc
	tcall tableCallFunc
}

// Manager owns the functions registered by AttachManaged and can reload them
// (ADR 0005, hot reload).
type Manager struct {
	eng    contract.Engine
	opt    Options
	scalar contract.ScalarRegistrar
	table  contract.TableRegistrar

	guard func(names []string)

	mu     sync.Mutex // serialises Reload and guards the fields below
	slots  map[string]*slot
	order  []string
	pool   *pyPool
	hooked bool
	closed bool
}

// AttachManaged is Attach returning the Manager, which stays valid for Reload
// even when functions/ is empty or missing at the start. It returns nil (and no
// error) when there are no functions and eng cannot register any.
func AttachManaged(ctx context.Context, eng contract.Engine, opt Options) (*Manager, error) {
	m := &Manager{eng: eng, opt: opt.withDefaults(), slots: map[string]*slot{}}
	m.scalar, _ = eng.(contract.ScalarRegistrar)
	m.table, _ = eng.(contract.TableRegistrar)
	if m.scalar == nil {
		if Exists(opt.Root) {
			return nil, errors.New("udf: this engine cannot register scalar functions")
		}
		return nil, nil
	}
	if !Exists(opt.Root) {
		return m, nil
	}
	if _, err := m.Reload(ctx); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}

// SetGuard registers f, called by every Reload with the complete list of names
// BEFORE a new function is registered in the engine, so an access guard (the
// admin-only list of `duct serve`) already covers it when it becomes callable.
func (m *Manager) SetGuard(f func(names []string)) {
	m.mu.Lock()
	m.guard = f
	m.mu.Unlock()
}

// Names lists every function ever registered (lower case), including the ones a
// later reload no longer finds: DuckDB keeps their name, they only answer with an
// error, and the governance guard must keep them admin-only.
func (m *Manager) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.order...)
}

// Close stops the Python workers (the engine calls it when it closes).
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.pool != nil {
		_ = m.pool.Close()
	}
	return nil
}

// Change is what a Reload did.
type Change struct {
	Added, Updated, Removed []string // function names
}

// Empty reports whether the reload changed the set of names.
func (c Change) Empty() bool { return len(c.Added)+len(c.Removed) == 0 }

// Reload re-reads functions/ and applies it without restarting the engine:
//
//   - the whole folder is discovered first (python -m duct.registry); on any error
//     nothing changes and the old functions keep working;
//   - a new name is registered in the engine; an existing one gets its new body;
//   - a name that disappeared stays registered (DuckDB cannot drop it) but fails
//     with "removed" until the process restarts;
//   - a changed signature (types, columns) cannot be applied: registering it again
//     would add an overload and leave the old signature callable, so the reload
//     fails and asks for a restart;
//   - the Python workers are replaced: calls in flight finish on the old ones,
//     which then stop; new calls start fresh workers (lazily).
func (m *Manager) Reload(ctx context.Context) (Change, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ch Change
	if m.closed {
		return ch, errors.New("udf: engine closed")
	}
	var specs []Spec
	if Exists(m.opt.Root) {
		var err error
		if specs, err = Discover(ctx, m.opt); err != nil {
			return ch, err
		}
	}
	for _, s := range specs {
		key := strings.ToLower(s.Name)
		if old, ok := m.slots[key]; ok && old.spec.signature() != s.signature() {
			return ch, fmt.Errorf("function %s changed its signature from %s to %s: DuckDB would keep the old signature as an overload, restart duct serve",
				s.Name, old.spec.signature(), s.signature())
		}
		if s.Table && m.table == nil {
			return ch, fmt.Errorf("function %s: this engine cannot register table functions", s.Name)
		}
	}
	var pool *pyPool
	build := func(s Spec) *impl {
		if s.Kind != "python" {
			return &impl{call: s.exec.caller(m.opt, s)}
		}
		if pool == nil {
			pool = newPyPool(m.opt)
		}
		if s.Table {
			return &impl{tcall: pool.tableCaller(s)}
		}
		return &impl{call: pool.caller(s)}
	}
	impls := make([]*impl, len(specs))
	for i, s := range specs {
		impls[i] = build(s)
	}
	if m.guard != nil {
		all := append([]string(nil), m.order...)
		for _, s := range specs {
			if key := strings.ToLower(s.Name); m.slots[key] == nil && !contains(all, key) {
				all = append(all, key)
			}
		}
		m.guard(all)
	}
	// Register the new names first: if the engine refuses one, nothing was swapped.
	seen := map[string]bool{}
	for i, s := range specs {
		key := strings.ToLower(s.Name)
		seen[key] = true
		if _, ok := m.slots[key]; ok {
			continue
		}
		sl := &slot{spec: s}
		sl.impl.Store(impls[i])
		if err := m.register(ctx, sl); err != nil {
			if pool != nil {
				_ = pool.Close()
			}
			return ch, fmt.Errorf("function %s (%s): %w", s.Name, s.File, err)
		}
		m.slots[key] = sl
		m.order = append(m.order, key)
		ch.Added = append(ch.Added, key)
	}
	for i, s := range specs {
		key := strings.ToLower(s.Name)
		sl := m.slots[key]
		if !contains(ch.Added, key) {
			sl.spec, sl.removed = s, false
			sl.impl.Store(impls[i])
			ch.Updated = append(ch.Updated, key)
		}
	}
	for _, key := range m.order {
		if seen[key] {
			continue
		}
		sl := m.slots[key]
		name := sl.spec.Name
		if !sl.removed {
			sl.removed = true
			sl.impl.Store(&impl{
				call: func(context.Context, []arrow.Array) (arrow.Array, error) {
					return nil, fmt.Errorf("function %s was removed from functions/ (restart duct serve to drop it)", name)
				},
				tcall: func(context.Context, []arrow.Array) ([]arrow.RecordBatch, error) {
					return nil, fmt.Errorf("function %s was removed from functions/ (restart duct serve to drop it)", name)
				},
			})
			ch.Removed = append(ch.Removed, key)
		}
	}
	old := m.pool
	m.pool = pool
	if old != nil {
		_ = old.Close() // in-flight batches finish; the workers stop as they come back
	}
	return ch, nil
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// register hands a slot to the engine. The Manager closes with the first one.
func (m *Manager) register(ctx context.Context, sl *slot) error {
	var closeHook func() error
	if !m.hooked {
		closeHook = m.Close
	}
	s := sl.spec
	if s.Table {
		f := contract.TableFunction{Name: s.Name, ArgTypes: s.ArgTypes, Columns: s.Columns, Close: closeHook,
			Call: func(ctx context.Context, args []arrow.Array) ([]arrow.RecordBatch, error) {
				return forward(sl, func(i *impl) ([]arrow.RecordBatch, error) { return i.tcall(ctx, args) })
			}}
		if err := m.table.RegisterTable(ctx, f); err != nil {
			return err
		}
	} else {
		f := contract.ScalarFunction{Name: s.Name, ArgTypes: s.ArgTypes, ReturnType: s.ReturnType, Close: closeHook,
			Call: func(ctx context.Context, args []arrow.Array) (arrow.Array, error) {
				return forward(sl, func(i *impl) (arrow.Array, error) { return i.call(ctx, args) })
			}}
		if err := m.scalar.RegisterScalar(ctx, f); err != nil {
			return err
		}
	}
	m.hooked = true
	return nil
}

// forward runs call against the slot's current implementation. If a reload
// closed that implementation's worker pool between the lookup and the
// acquisition, it retries against the new one.
func forward[T any](sl *slot, call func(*impl) (T, error)) (T, error) {
	var zero T
	for range 3 {
		cur := sl.impl.Load()
		res, err := call(cur)
		if errors.Is(err, errPoolClosed) && sl.impl.Load() != cur {
			continue
		}
		return res, err
	}
	return zero, errPoolClosed
}

// SafeEnv returns the allow-listed part of the process environment plus extra.
func SafeEnv(extra map[string]string) []string {
	allow := []string{"PATH", "HOME", "USER", "LANG", "LC_ALL", "TMPDIR", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR",
		"VIRTUAL_ENV", "PYTHONPATH", "PYTHONHOME", "UV_CACHE_DIR", "UV_PROJECT_ENVIRONMENT", "XDG_CACHE_HOME",
		"XDG_DATA_HOME", "XDG_CONFIG_HOME", "MLFLOW_TRACKING_URI", "MLFLOW_ARTIFACT_ROOT"}
	var env []string
	for _, k := range allow {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	keys := make([]string, 0, len(extra))
	for k := range extra {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		env = append(env, k+"="+extra[k])
	}
	return env
}
