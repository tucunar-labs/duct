package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
)

// openEngine instantiates the DuckDB engine module from the registry.
func openEngine(ctx context.Context, p *project.Project, locked bool) (contract.Engine, error) {
	info, ok := registry.Lookup(contract.KindEngine, "duckdb")
	if !ok {
		return nil, errors.New("engine module duckdb is not linked into this binary")
	}
	eng, ok := info.New().(contract.Engine)
	if !ok {
		return nil, errors.New("module engine/duckdb does not implement contract.Engine")
	}
	if err := eng.Init(ctx, p.EngineConfig(locked)); err != nil {
		_ = eng.Close()
		return nil, err
	}
	return eng, nil
}

// runProject runs the commands that need a project.
func runProject(cmd string, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch cmd {
	case "init":
		err = cmdInit(ctx, args, stdout, stderr)
	case "query":
		err = cmdQuery(ctx, args, stdout, stderr)
	case "migrate":
		err = cmdMigrate(ctx, args, stdout, stderr)
	case "catalog":
		err = cmdCatalog(ctx, args, stdout, stderr)
	case "gc":
		err = cmdGC(ctx, args, stdout, stderr)
	case "run":
		err = cmdRun(ctx, args, stdout, stderr)
	case "serve":
		err = cmdServe(ctx, args, stdout, stderr)
	case "role":
		err = cmdRole(ctx, args, stdout, stderr)
	case "user":
		err = cmdUser(ctx, args, stdout, stderr)
	case "token":
		err = cmdToken(ctx, args, stdout, stderr)
	case "grant", "revoke":
		err = cmdGrant(ctx, cmd, args, stdout, stderr)
	case "policy":
		err = cmdPolicy(ctx, args, stdout, stderr)
	}
	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct %s: %v\n", cmd, err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct %s: %v\n", cmd, err)
		return 1
	}
}

var errUsage = errors.New("usage error")

func usageErr(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errUsage}, a...)...)
}

func newFlags(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet("duct "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("project", "", "project directory (default: nearest folder with duct.toml above the current one)")
	return fs, dir
}

func findProject(dir string) (*project.Project, error) {
	if dir != "" {
		return project.Load(dir)
	}
	return project.Find(".")
}

func parse(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageErr("%v", err)
	}
	return nil
}

func cmdInit(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duct init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	force := fs.Bool("force", false, "create the project even on a network or synchronised folder")
	catalog := fs.String("catalog", "", "Postgres catalog (postgres://user@host/db or postgres:dbname=... host=...); default: SQLite in .duct/. Password via DUCT_CATALOG_PASSWORD")
	// Accept flags after the directory too: duct init demo --force.
	var dir string
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		if dir != "" {
			return usageErr("init takes a single directory")
		}
		dir, rest = rest[0], rest[1:]
	}
	if dir == "" {
		return usageErr("init needs a directory: duct init <dir>")
	}
	if !*force {
		if err := project.CheckLocalFolder(dir); err != nil {
			return fmt.Errorf("%w\n(use --force to create it anyway)", err)
		}
	}
	if *catalog != "" && !pgconn.IsDSN(*catalog) {
		return usageErr("--catalog must be a postgres connection string (postgres://user@host/db)")
	}
	p, stripped, err := project.ScaffoldWithCatalog(dir, *catalog)
	if err != nil {
		return err
	}
	eng, err := openEngine(ctx, p, false) // ATTACH creates the DuckLake catalog
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		return err
	}
	if p.CatalogIsPostgres() {
		_, _ = fmt.Fprintf(stdout, "Created Duct project in %s\n  duct.toml  lake/  catalog and Duct state in %s (schema duct)\n", p.Root, p.CatalogRef())
		if stripped {
			_, _ = fmt.Fprintf(stdout, "The password was not written to duct.toml: export %s (or %s) before running duct here.\n", project.EnvCatalogPassword, project.EnvCatalogURL)
		}
		_, _ = fmt.Fprintf(stdout, "Next: cd %s && duct query \"CREATE TABLE t (id INTEGER)\"\n", dir)
		return nil
	}
	_, _ = fmt.Fprintf(stdout, "Created Duct project in %s\n  duct.toml  lake/  .duct/catalog.sqlite  .duct/duct.sqlite\nNext: cd %s && duct query \"CREATE TABLE t (id INTEGER)\"\n", p.Root, dir)
	return nil
}

func cmdQuery(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("query", stderr)
	format := fs.String("format", "table", "output format: table, csv or json")
	locked := fs.Bool("locked", false, "run with the server restrictions (no external access)")
	var sqlText string
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		if sqlText != "" {
			return usageErr("query takes a single SQL string (quote it)")
		}
		sqlText, rest = rest[0], rest[1:]
	}
	if sqlText == "" {
		return usageErr(`query needs SQL: duct query "SELECT 1"`)
	}
	if _, err := newWriter(*format, io.Discard); err != nil {
		return usageErr("%v", err)
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	eng, err := openEngine(ctx, p, *locked)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	st, err := eng.Query(ctx, sqlText)
	if err != nil {
		return err
	}
	defer st.Release()
	return writeStream(st, *format, stdout)
}

func cmdMigrate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("migrate", stderr)
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	applied, err := project.Migrate(ctx, eng)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		_, _ = fmt.Fprintln(stdout, "Already up to date")
		return nil
	}
	_, _ = fmt.Fprintf(stdout, "Applied migrations: %v\n", applied)
	return nil
}

func cmdGC(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("gc", stderr)
	keep := fs.Duration("keep-snapshots", project.DefaultKeepSnapshots, "keep snapshots newer than this (time travel window)")
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	start := time.Now()
	steps, err := p.GC(ctx, eng, project.GCOptions{KeepSnapshots: *keep})
	for _, s := range steps {
		_, _ = fmt.Fprintf(stdout, "%-22s %s\n", s.Name, s.Detail)
	}
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "gc done in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}
