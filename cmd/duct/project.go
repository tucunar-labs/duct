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
	case "sql":
		err = cmdSQL(ctx, args, stdout, stderr)
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
	case "semantic":
		err = cmdSemantic(ctx, args, stdout, stderr)
	case "functions":
		err = cmdFunctions(ctx, args, stdout, stderr)
	case "notebook":
		err = cmdNotebook(ctx, args, stdout, stderr)
	case "audit":
		err = cmdAudit(ctx, args, stdout, stderr)
	case "mlflow":
		err = cmdMLflow(ctx, args, stdout, stderr)
	case "enable":
		err = cmdEnable(ctx, args, stdout, stderr)
	case "share":
		err = cmdShare(ctx, args, stdout, stderr)
	case "recipient":
		err = cmdRecipient(ctx, args, stdout, stderr)
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
	storage := fs.String("storage", "", "lake location: a local folder (default ./lake) or s3://bucket/prefix (credentials from DUCT_S3_* or AWS_* variables)")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint host[:port] (RustFS, Garage...); empty = AWS S3")
	s3Region := fs.String("s3-region", "", "S3 region (default us-east-1)")
	s3Style := fs.String("s3-url-style", "", "S3 URL style: path (default with an endpoint) or vhost")
	s3NoSSL := fs.Bool("s3-no-ssl", false, "reach the S3 endpoint over plain http")
	s3Role := fs.String("s3-role-arn", "", "STS role to assume with a session policy scoped to the lake prefix")
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
	opts := project.ScaffoldOptions{Catalog: *catalog, Storage: *storage,
		S3: project.S3Config{Endpoint: *s3Endpoint, Region: *s3Region, URLStyle: *s3Style, RoleARN: *s3Role}}
	if *s3NoSSL {
		off := false
		opts.S3.UseSSL = &off
	}
	p, stripped, err := project.ScaffoldWith(dir, opts)
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
	if p.StorageIsS3() {
		_, _ = fmt.Fprintf(stdout, "Created Duct project in %s\n  duct.toml  lake in %s  catalog in %s\n", p.Root, p.Config.Storage, p.CatalogRef())
		if stripped {
			_, _ = fmt.Fprintf(stdout, "The password was not written to duct.toml: export %s (or %s) before running duct here.\n", project.EnvCatalogPassword, project.EnvCatalogURL)
		}
		_, _ = fmt.Fprintf(stdout, "Credentials come from DUCT_S3_ACCESS_KEY_ID / DUCT_S3_SECRET_ACCESS_KEY (or AWS_*), never from duct.toml.\n")
		return nil
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
	eng, err := openEngineUDF(ctx, p, *locked)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	return auditedQuery(ctx, p, eng, sqlText, *format, stdout)
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
