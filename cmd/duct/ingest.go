package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/project"
)

// runIngest wires `duct ingest` (own entry point: it does not go through runProject).
func runIngest(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	err := cmdIngest(ctx, args, stdout, stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct ingest: %v\n", err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct ingest: %v\n", err)
		return 1
	}
}

// cmdIngest is `duct ingest [name...]`: runs the named ingestions (all if none
// is named) in name order and prints the rows loaded per table. A failing
// ingestion does not stop the others; the exit code is 1 if any failed.
func cmdIngest(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("ingest", stderr)
	var names []string
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		names, rest = append(names, rest[0]), rest[1:]
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	all, err := ingest.Load(p.Root)
	if err != nil {
		return err
	}
	selected := all
	if len(names) > 0 {
		byName := map[string]ingest.Config{}
		for _, c := range all {
			byName[c.Name] = c
		}
		selected = nil
		for _, n := range names {
			c, ok := byName[n]
			if !ok {
				return usageErr("no ingestion %q in %s/ (have %d)", n, ingest.Dir, len(all))
			}
			selected = append(selected, c)
		}
	}
	if len(selected) == 0 {
		_, _ = fmt.Fprintf(stdout, "No ingestions: add %s/<name>.yaml\n", ingest.Dir)
		return nil
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil { // duct_ingest_state may be new
		return err
	}
	failed := 0
	for _, c := range selected {
		start := time.Now()
		res, err := ingest.Run(ctx, eng, p.Root, c)
		if err != nil {
			failed++
			_, _ = fmt.Fprintf(stdout, "%-20s FAILED: %v\n", c.Name, err)
			continue
		}
		_, _ = fmt.Fprintf(stdout, "%-20s %-24s %8d rows  %s\n", c.Name, res.Table, res.Rows, time.Since(start).Round(time.Millisecond))
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d ingestions failed", failed, len(selected))
	}
	return nil
}
