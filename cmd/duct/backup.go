package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/alonsoburon/duct/core/backup"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
)

// runBackupCmd wires `duct backup` and `duct restore` (own entry points: they do
// not go through runProject, which would need a project for restore).
func runBackupCmd(cmd string, args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if cmd == "backup" {
		err = cmdBackup(ctx, args, stdout, stderr)
	} else {
		err = cmdRestore(ctx, args, stdout, stderr)
	}
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct %s: %v\n", cmd, err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct %s: %v\n", cmd, err)
		return 1
	}
}

// cmdBackup is `duct backup --to <dir|s3://bucket/prefix> [--catalog-only] [--keep N]`
// and `duct backup list --to ...`.
func cmdBackup(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("backup", stderr)
	to := fs.String("to", "", "backup root: a folder or s3://bucket/prefix (one subfolder per backup)")
	catalogOnly := fs.Bool("catalog-only", false, "leave the lake data files out (only for lakes on s3:// storage)")
	keep := fs.Int("keep", 0, "after a good backup, keep only the newest N complete backups under --to (0: keep all)")
	var list bool
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		if rest[0] != "list" || list {
			return usageErr("unexpected argument %q (use: duct backup --to X, or duct backup list --to X)", rest[0])
		}
		list, rest = true, rest[1:]
	}
	if *to == "" {
		return usageErr("backup needs --to <folder|s3://bucket/prefix>")
	}
	if *keep < 0 {
		return usageErr("--keep must be 0 or more")
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if list {
		root, err := backup.OpenDestination(p, *to)
		if err != nil {
			return err
		}
		entries, err := backup.List(ctx, root)
		if err != nil {
			return err
		}
		for _, e := range entries {
			state := "complete"
			if !e.Complete {
				state = "INCOMPLETE"
			}
			_, _ = fmt.Fprintf(stdout, "%s  %s\n", e.ID, state)
		}
		return nil
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	start := time.Now()
	m, err := backup.Run(ctx, p, eng, backup.Options{
		To: *to, CatalogOnly: *catalogOnly, Keep: *keep, Version: version,
		Log: func(f string, a ...any) { _, _ = fmt.Fprintf(stdout, f+"\n", a...) },
	})
	if err != nil {
		return err
	}
	var stored int64
	for _, f := range m.Files {
		stored += f.Size
	}
	mode := "with data files"
	if m.CatalogOnly {
		mode = "catalog only"
	}
	_, _ = fmt.Fprintf(stdout, "backup %s done in %s (%s): snapshot %d, %d tables, %d files, %d bytes stored\n  to %s/%s\n",
		m.ID, time.Since(start).Round(time.Millisecond), mode, m.SnapshotID, len(m.Tables), len(m.Files), stored, *to, m.ID)
	return nil
}

var (
	tomlStorageLine = regexp.MustCompile(`(?m)^storage\s*=.*$`)
	tomlCatalogLine = regexp.MustCompile(`(?m)^catalog\s*=.*$`)
)

// cmdRestore is `duct restore --from <backup|root> [--id ID] <dir>`.
func cmdRestore(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("duct restore", flag.ContinueOnError)
	fs.SetOutput(stderr)
	from := fs.String("from", "", "a backup folder, or a root holding backups (folder or s3://bucket/prefix)")
	id := fs.String("id", "", "backup to restore from a root (default: the newest complete one)")
	catalog := fs.String("catalog", "", "restore into this (empty) Postgres catalog instead of the one in the backup's duct.toml; password via DUCT_CATALOG_PASSWORD or in the string")
	storage := fs.String("storage", "", "restore the lake to this folder or s3://bucket/prefix instead of the backup's")
	force := fs.Bool("force", false, "create the project even on a network or synchronised folder")
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
			return usageErr("restore takes a single target folder")
		}
		dir, rest = rest[0], rest[1:]
	}
	if *from == "" || dir == "" {
		return usageErr("usage: duct restore --from <backup|root> [--id ID] [--catalog postgres://...] [--storage ...] <empty folder>")
	}
	if *catalog != "" && !pgconn.IsDSN(*catalog) {
		return usageErr("--catalog must be a postgres connection string")
	}
	if !*force {
		if err := project.CheckLocalFolder(dir); err != nil {
			return fmt.Errorf("%w\n(use --force to restore there anyway)", err)
		}
	}
	if ents, err := os.ReadDir(dir); err == nil && len(ents) > 0 {
		return fmt.Errorf("%s is not empty: restore into a new or empty folder", dir)
	}
	logf := func(f string, a ...any) { _, _ = fmt.Fprintf(stdout, f+"\n", a...) }

	src, err := backup.OpenSource(ctx, *from, *id)
	if err != nil {
		return err
	}
	m := src.Manifest
	logf("backup %s (duct %s, %s catalog, snapshot %d, %d tables)", m.ID, m.DuctVersion, m.CatalogType, m.SnapshotID, len(m.Tables))
	cfg, err := src.ConfigFile(ctx)
	if err != nil {
		return fmt.Errorf("the backup has no duct.toml: %w", err)
	}
	fullCatalog := ""
	if *catalog != "" {
		clean, had, err := pgconn.StripPassword(*catalog)
		if err != nil {
			return err
		}
		if had {
			fullCatalog = *catalog
		}
		cfg = tomlCatalogLine.ReplaceAll(cfg, []byte(fmt.Sprintf("catalog = %q", clean)))
	}
	if *storage != "" {
		cfg = tomlStorageLine.ReplaceAll(cfg, []byte(fmt.Sprintf("storage = %q", *storage)))
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, project.StateDir), 0o750); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, project.ConfigFile), cfg, 0o600); err != nil {
		return err
	}
	p, err := project.Load(root)
	if err != nil {
		return err
	}
	if fullCatalog != "" {
		if err := p.UseCatalogURL(fullCatalog); err != nil {
			return err
		}
	}
	if !p.StorageIsS3() {
		if err := os.MkdirAll(p.DataPath(), 0o750); err != nil {
			return err
		}
	}

	start := time.Now()
	eng, err := openEngine(ctx, p, false) // ATTACH creates an empty DuckLake
	if err != nil {
		return err
	}
	if _, err := project.Migrate(ctx, eng); err != nil {
		_ = eng.Close()
		return err
	}
	err = backup.Restore(ctx, p, eng, src, backup.RestoreOptions{Log: logf})
	if cerr := eng.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}

	eng, err = openEngine(ctx, p, false) // fresh: DuckLake caches the catalog
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	rep, err := backup.Verify(ctx, p, eng, m)
	if err != nil {
		return err
	}
	if !rep.OK() {
		for _, pr := range rep.Problems {
			_, _ = fmt.Fprintf(stderr, "verification: %s\n", pr)
		}
		return fmt.Errorf("restored into %s but verification failed (%d problems)", root, len(rep.Problems))
	}
	logf("verified: snapshot %d, %d tables, %d rows, %d lake files present, Duct tables match", rep.SnapshotID, rep.Tables, rep.Rows, rep.LakeFiles)
	logf("restore done in %s: %s (catalog %s, lake %s)", time.Since(start).Round(time.Millisecond), root, p.CatalogRef(), p.Config.Storage)
	if p.CatalogIsPostgres() && fullCatalog == "" {
		logf("export %s (or %s) before running duct in that folder", project.EnvCatalogPassword, project.EnvCatalogURL)
	}
	return nil
}
