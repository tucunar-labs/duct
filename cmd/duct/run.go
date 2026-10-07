package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/alonsoburon/duct/core/contracts"
	"github.com/alonsoburon/duct/core/pipeline"
	"github.com/alonsoburon/duct/core/project"
)

// cmdRun is `duct run [--only] [target...]`: full refresh of the pipelines/
// tables in dependency order.
func cmdRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("run", stderr)
	only := fs.Bool("only", false, "run only the given targets, not their upstream")
	var targets []string
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		targets, rest = append(targets, rest[0]), rest[1:]
	}
	if *only && len(targets) == 0 {
		return usageErr("--only needs at least one target")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	tables, err := pipeline.Load(p.Root)
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
	ref, err := contracts.RunPipeline(ctx, eng, p.Root, tables, pipeline.Options{Targets: targets, Only: *only})
	rep := ref.Report
	if rep != nil {
		printReport(stdout, rep)
	}
	for _, l := range ref.Log {
		_, _ = fmt.Fprintln(stdout, l)
	}
	for _, w := range ref.Warnings {
		_, _ = fmt.Fprintf(stderr, "warning: %s\n", w)
	}
	if err != nil {
		return err
	}
	if rep.Failed() {
		return errors.New("run finished with failures")
	}
	return nil
}

func printReport(w io.Writer, rep *pipeline.Report) {
	_, _ = fmt.Fprintf(w, "run %d\n", rep.RunID)
	ok, bad := 0, 0
	for _, r := range rep.Results {
		line := fmt.Sprintf("%-8s %-24s", strings.ToUpper(r.Status), r.ID)
		if r.Status == pipeline.StatusSuccess {
			ok++
			line += fmt.Sprintf(" %d rows", r.Rows)
			if r.Dropped > 0 {
				line += fmt.Sprintf(" (%d dropped)", r.Dropped)
			}
		} else {
			bad++
		}
		line += fmt.Sprintf("  %dms", r.Duration.Milliseconds())
		_, _ = fmt.Fprintln(w, line)
		for _, v := range r.Violations {
			if v.Count > 0 {
				_, _ = fmt.Fprintf(w, "    expectation %s [%s]: %d violations\n", v.Name, v.Action, v.Count)
			}
		}
		if r.Err != nil {
			_, _ = fmt.Fprintf(w, "    %v\n", r.Err)
		}
	}
	_, _ = fmt.Fprintf(w, "%d refreshed, %d failed or skipped\n", ok, bad)
}
