package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

const catalogUsage = `usage: duct catalog <subcommand>

  info                                   show which catalog the project uses (no secrets)
  migrate --to postgres://user@host/db   copy the SQLite catalog and Duct state to Postgres,
                                         verify counts, then switch duct.toml
          [--dry-run]                    only report; write nothing
`

func cmdCatalog(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, catalogUsage)
		return usageErr("catalog needs a subcommand")
	}
	switch args[0] {
	case "info":
		return cmdCatalogInfo(args[1:], stdout, stderr)
	case "migrate":
		return cmdCatalogMigrate(ctx, args[1:], stdout, stderr)
	case "-h", "-help", "--help":
		_, _ = fmt.Fprint(stdout, catalogUsage)
		return nil
	}
	_, _ = fmt.Fprint(stderr, catalogUsage)
	return usageErr("unknown catalog subcommand %q", args[0])
}

func cmdCatalogInfo(args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("catalog info", stderr)
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if p.CatalogIsPostgres() {
		_, _ = fmt.Fprintf(stdout, "catalog: %s (DuckLake metadata in public, Duct state in schema duct)\n", p.CatalogRef())
		return nil
	}
	_, _ = fmt.Fprintf(stdout, "catalog: sqlite %s (Duct state in %s)\n", p.CatalogPath(), p.StatePath())
	return nil
}

func cmdCatalogMigrate(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("catalog migrate", stderr)
	to := fs.String("to", "", "target Postgres connection string (postgres://user@host:5432/db); password via DUCT_CATALOG_PASSWORD")
	dry := fs.Bool("dry-run", false, "inspect source and target and report; write nothing")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *to == "" || !pgconn.IsDSN(*to) {
		return usageErr("catalog migrate needs --to postgres://user@host/db")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if p.CatalogIsPostgres() {
		return errors.New("the catalog is already Postgres")
	}
	full, err := pgconn.WithPassword(*to, os.Getenv(project.EnvCatalogPassword))
	if err != nil {
		return err
	}
	clean, hadPassword, err := pgconn.StripPassword(*to)
	if err != nil {
		return err
	}
	if !hadPassword {
		// Written to duct.toml as given; the password (if any) came from the env.
		clean = *to
	}

	info, ok := registry.Lookup(contract.KindEngine, "duckdb")
	if !ok {
		return errors.New("engine module duckdb is not linked into this binary")
	}
	eng, ok := info.New().(contract.Engine)
	if !ok {
		return errors.New("module engine/duckdb does not implement contract.Engine")
	}
	cfg := p.EngineConfig(false)
	cfg["extensions"] = []string{"postgres"}
	if err := eng.Init(ctx, cfg); err != nil {
		_ = eng.Close()
		return err
	}
	defer func() { _ = eng.Close() }()

	rep, err := p.MigrateCatalogToPostgres(ctx, eng, project.CatalogMigrateOptions{DSN: full, DryRun: *dry})
	if rep != nil {
		printCatalogReport(stdout, rep)
	}
	if err != nil {
		return err
	}
	if *dry {
		_, _ = fmt.Fprintln(stdout, "Dry run: nothing was written; duct.toml unchanged.")
		return nil
	}
	if !rep.Verified() {
		return errors.New("verification did not pass; duct.toml unchanged")
	}
	if err := switchCatalog(p, clean); err != nil {
		return fmt.Errorf("copied and verified, but could not update duct.toml: %w (set catalog = %q by hand)", err, clean)
	}
	_, _ = fmt.Fprintf(stdout, "duct.toml now points at Postgres. The SQLite files in .duct/ are untouched: undo by restoring the commented catalog line.\n")
	if hadPassword || os.Getenv(project.EnvCatalogPassword) == "" {
		_, _ = fmt.Fprintf(stdout, "Export %s (or %s) on every machine that uses this project: the password is not in duct.toml.\n", project.EnvCatalogPassword, project.EnvCatalogURL)
	}
	return nil
}

func printCatalogReport(w io.Writer, r *project.CatalogMigrateReport) {
	_, _ = fmt.Fprintf(w, "Target: %s (%s)\n", r.Target, r.TargetState)
	if r.Flushed {
		_, _ = fmt.Fprintln(w, "Flushed inlined rows of the source to Parquet first.")
	} else if r.Inlined > 0 {
		_, _ = fmt.Fprintf(w, "Source has %d inlined rows: a real run flushes them to Parquet first.\n", r.Inlined)
	}
	bad := 0
	for _, c := range r.Checks {
		switch {
		case r.DryRun:
			_, _ = fmt.Fprintf(w, "  %-44s %d\n", c.Name, c.Source)
		case c.Source == c.Target:
			_, _ = fmt.Fprintf(w, "  ok   %-40s %d\n", c.Name, c.Source)
		default:
			bad++
			_, _ = fmt.Fprintf(w, "  FAIL %-40s source=%d target=%d\n", c.Name, c.Source, c.Target)
		}
	}
	if !r.DryRun && bad == 0 && len(r.Checks) > 0 {
		_, _ = fmt.Fprintf(w, "Verified %d counts.\n", len(r.Checks))
	}
}

var catalogLine = regexp.MustCompile(`(?m)^catalog\s*=.*$`)

// switchCatalog rewrites the catalog line of duct.toml (keeping the old one as
// a comment for rollback), atomically.
func switchCatalog(p *project.Project, dsn string) error {
	path := filepath.Join(p.Root, project.ConfigFile)
	raw, err := os.ReadFile(path) //nolint:gosec // project file
	if err != nil {
		return err
	}
	if !catalogLine.Match(raw) {
		return errors.New("no catalog line found")
	}
	done := false
	out := catalogLine.ReplaceAllFunc(raw, func(old []byte) []byte {
		if done {
			return old
		}
		done = true
		return []byte(fmt.Sprintf("# was: %s\ncatalog = %q  # DuckLake catalog in Postgres; password in DUCT_CATALOG_PASSWORD", old, dsn))
	})
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(p.Root, ".duct.toml.*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(out); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), st.Mode().Perm()); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
