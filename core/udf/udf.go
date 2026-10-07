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
	"time"

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
	exec       *execSpec
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
// fails when eng cannot register functions.
func Attach(ctx context.Context, eng contract.Engine, opt Options) ([]string, error) {
	if !Exists(opt.Root) {
		return nil, nil
	}
	reg, ok := eng.(contract.ScalarRegistrar)
	if !ok {
		return nil, errors.New("udf: this engine cannot register scalar functions")
	}
	opt = opt.withDefaults()
	specs, err := Discover(ctx, opt)
	if err != nil {
		return nil, err
	}
	var pool *pyPool
	hooked := false
	var names []string
	for _, s := range specs {
		f := contract.ScalarFunction{Name: s.Name, ArgTypes: s.ArgTypes, ReturnType: s.ReturnType}
		switch s.Kind {
		case "python":
			if pool == nil {
				pool = newPyPool(opt)
			}
			f.Call = pool.caller(s)
			if !hooked { // the engine closes the pool with the first python function
				f.Close, hooked = pool.Close, true
			}
		case "exec":
			f.Call = s.exec.caller(opt, s)
		}
		if err := reg.RegisterScalar(ctx, f); err != nil {
			if pool != nil {
				_ = pool.Close()
			}
			return nil, fmt.Errorf("function %s (%s): %w", s.Name, s.File, err)
		}
		names = append(names, strings.ToLower(s.Name))
	}
	return names, nil
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
