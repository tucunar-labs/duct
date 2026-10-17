package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/udf"
)

// errEngine marks a failure to open the engine itself (not the functions).
var errEngine = errors.New("engine")

func udfOptions(p *project.Project) udf.Options {
	return udf.Options{Root: p.Root, Python: p.PythonCommand(), Workers: p.Config.Python.Workers}
}

// openEngineUDF is openEngine plus the project's functions/ (ADR 0005): Python
// functions and executables become scalar functions of the engine. They are
// reserved to the local CLI and to the admin role of `duct serve`.
//
// A functions/ folder that cannot be loaded (no Python environment, a typo) is a
// warning here, so unrelated queries and pipelines keep working; `duct serve`
// is strict (openEngineUDFManager).
func openEngineUDF(ctx context.Context, p *project.Project, locked bool) (contract.Engine, error) {
	eng, _, err := openEngineUDFManager(ctx, p, locked)
	if err != nil && udf.Exists(p.Root) && !errors.Is(err, errEngine) {
		_, _ = fmt.Fprintf(os.Stderr, "warning: functions/ not loaded: %v\n", err)
		eng, err = openEngine(ctx, p, locked)
	}
	if err == nil && !locked { // the MLflow database is only readable by the local CLI
		if aerr := attachMLflow(ctx, p, eng); aerr != nil {
			_ = eng.Close()
			return nil, aerr
		}
	}
	return eng, err
}

// openEngineUDFManager also returns the manager of the registered functions
// (nil when the engine cannot take any and there are none), which `duct serve`
// uses to know their names and to reload them.
func openEngineUDFManager(ctx context.Context, p *project.Project, locked bool) (contract.Engine, *udf.Manager, error) {
	eng, err := openEngine(ctx, p, locked)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", errEngine, err)
	}
	m, err := udf.AttachManaged(ctx, eng, udfOptions(p))
	if err != nil {
		_ = eng.Close()
		return nil, nil, err
	}
	return eng, m, nil
}

// cmdFunctions is `duct functions list|check|bench`.
func cmdFunctions(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("functions", stderr)
	rows := fs.Int("rows", 100000, "bench: rows to push through each function")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usageErr("functions needs a subcommand: list | check | bench [name...]")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	opt := udfOptions(p)
	switch pos[0] {
	case "list", "check":
		specs, err := udf.Discover(ctx, opt)
		if err != nil {
			return err
		}
		if len(specs) == 0 {
			_, _ = fmt.Fprintf(stdout, "no functions (put *.py with @duct.function or *.toml in %s/)\n", udf.DirName)
			return nil
		}
		if pos[0] == "check" {
			_, _ = fmt.Fprintf(stdout, "%d functions OK\n", len(specs))
			return nil
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "NAME\tKIND\tARGS\tRETURNS\tSOURCE")
		for _, s := range specs {
			kind := s.Kind
			if s.Vectorized {
				kind += " (vectorized)"
			}
			ret := s.ReturnType
			if s.Table {
				kind += " (table)"
				ret = "TABLE("
				for i, c := range s.Columns {
					if i > 0 {
						ret += ", "
					}
					ret += c.Name + " " + c.Type
				}
				ret += ")"
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\t(%s)\t%s\t%s\n", s.Name, kind, join(s.ArgTypes), ret, s.File)
		}
		return tw.Flush()
	case "bench":
		return benchFunctions(ctx, p, pos[1:], *rows, stdout)
	}
	return usageErr("unknown functions subcommand %q", pos[0])
}

func join(s []string) string {
	out := ""
	for i, v := range s {
		if i > 0 {
			out += ", "
		}
		out += v
	}
	return out
}

// benchFunctions times each function over generated rows, against the same
// expression computed by DuckDB, to show the per-row cost of leaving the engine.
func benchFunctions(ctx context.Context, p *project.Project, only []string, rows int, stdout io.Writer) error {
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	specs, err := udf.Discover(ctx, udfOptions(p))
	if err != nil {
		return err
	}
	if _, err := udf.Attach(ctx, eng, udfOptions(p)); err != nil {
		return err
	}
	want := map[string]bool{}
	for _, n := range only {
		want[n] = true
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "FUNCTION\tROWS\tTIME\tPER ROW\tPER 2048-ROW BATCH")
	for _, s := range specs {
		if len(want) > 0 && !want[s.Name] {
			continue
		}
		if s.Table { // no per-row cost to compare: it runs once per query
			continue
		}
		args := ""
		for i, t := range s.ArgTypes {
			if i > 0 {
				args += ", "
			}
			args += benchArg(t)
		}
		q := fmt.Sprintf("SELECT count(%s(%s)) FROM range(%d) t(i)", s.Name, args, rows)
		start := time.Now()
		st, err := eng.Query(ctx, q)
		if err != nil {
			return fmt.Errorf("%s: %w", s.Name, err)
		}
		for st.Next() {
		}
		err = st.Err()
		st.Release()
		if err != nil {
			return fmt.Errorf("%s: %w", s.Name, err)
		}
		d := time.Since(start)
		per := d / time.Duration(rows)
		_, _ = fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n", s.Name, rows, d.Round(time.Millisecond), per, (per * 2048).Round(time.Microsecond))
	}
	return tw.Flush()
}

func benchArg(t string) string {
	switch t {
	case "VARCHAR":
		return "CAST(i AS VARCHAR)"
	case "BOOLEAN":
		return "i % 2 = 0"
	case "DATE":
		return "DATE '2026-01-01' + CAST(i % 100 AS INTEGER)"
	case "TIMESTAMP":
		return "TIMESTAMP '2026-01-01' + to_seconds(i)"
	default:
		return "CAST(i % 100 AS " + t + ")" // numbers, DECIMAL(p,s); lists and structs need a hand-written query
	}
}
