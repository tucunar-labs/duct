package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/dashboards"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/semantic"
)

const dashboardUsage = "usage: duct dashboard lint [file...] | list"

// runDashboard wires `duct dashboard` (own entry point, like contract).
func runDashboard(args []string, stdout, stderr io.Writer) int {
	err := cmdDashboard(context.Background(), args, stdout, stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errFindings):
		return 1
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct dashboard: %v\n", err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct dashboard: %v\n", err)
		return 1
	}
}

func cmdDashboard(_ context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageErr("%s", dashboardUsage)
	}
	switch args[0] {
	case "lint":
		return dashboardLint(args[1:], stdout, stderr)
	case "list":
		return dashboardList(args[1:], stdout, stderr)
	}
	return usageErr("unknown subcommand %q; %s", args[0], dashboardUsage)
}

// dashboardLint validates dashboards/*.yaml (or the files given): format,
// then, when the project has a semantic model, that every name resolves and
// every filter can reach the charts it applies to.
func dashboardLint(args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("dashboard lint", stderr)
	files, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		if files, err = dashboardFiles(p); err != nil {
			return err
		}
	}
	var model *semantic.Model
	switch m, err := semantic.LoadDir(filepath.Join(p.Root, semanticDirName)); {
	case err == nil:
		model = m
	case errors.Is(err, semantic.ErrNoModel):
		_, _ = fmt.Fprintln(stdout, "warning: no semantic model; names are not checked against it")
	default:
		return fmt.Errorf("semantic model: %w", err)
	}
	failed := 0
	for _, f := range files {
		if err := lintDashboardFile(f, model); err != nil {
			failed++
			_, _ = fmt.Fprintf(stdout, "%s: %s\n", f, strings.ReplaceAll(err.Error(), "\n", "\n  "))
		}
	}
	_, _ = fmt.Fprintf(stdout, "%d dashboards checked, %d with errors\n", len(files), failed)
	if failed > 0 {
		return errFindings
	}
	return nil
}

func lintDashboardFile(path string, model *semantic.Model) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	d, err := dashboards.Parse(data)
	if err != nil {
		return err
	}
	if model == nil {
		return nil
	}
	sc := semantic.Scope{Role: "lint"} // nil Tables: names and joins, not grants
	return d.Check(func(r contract.SemanticRequest) error {
		req := semantic.Request{Dims: r.Dims, Measures: r.Measures, Limit: r.Limit}
		for _, f := range r.Filters {
			req.Filters = append(req.Filters, semantic.Filter{Field: f.Field, Op: f.Op, Value: f.Value})
		}
		for _, o := range r.Order {
			req.Order = append(req.Order, semantic.Order{By: o.By, Desc: o.Desc})
		}
		_, err := model.Query(sc, req)
		return err
	})
}

func dashboardFiles(p *project.Project) ([]string, error) {
	var out []string
	for _, pat := range []string{"*.yaml", "*.yml"} {
		m, err := filepath.Glob(filepath.Join(p.Root, dashboards.Dir, pat))
		if err != nil {
			return nil, err
		}
		out = append(out, m...)
	}
	return out, nil
}

func dashboardList(args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("dashboard list", stderr)
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	ok, bad, err := dashboards.Load(filepath.Join(p.Root, dashboards.Dir))
	if err != nil {
		return err
	}
	for _, d := range ok {
		_, _ = fmt.Fprintf(stdout, "%s\t%s\t%d charts\n", d.Name, d.Title, len(d.Charts))
	}
	for name, e := range bad {
		_, _ = fmt.Fprintf(stdout, "%s\tINVALID: %s\n", name, strings.SplitN(e.Error(), "\n", 2)[0])
	}
	return nil
}
