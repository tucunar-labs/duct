package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/stream"
)

// runStream wires `duct stream` (own entry point, like ingest).
func runStream(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := cmdStream(ctx, args, stdout, stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct stream: %v\n", err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct stream: %v\n", err)
		return 1
	}
}

// kafkaConfig is [kafka] of duct.toml plus the SASL credentials from the
// environment.
func kafkaConfig(p *project.Project) stream.KafkaConfig {
	k := p.Config.Kafka
	return stream.KafkaConfig{Brokers: k.Brokers, TLS: k.TLS, Mechanism: k.Mechanism,
		User: os.Getenv(project.EnvKafkaUser), Password: os.Getenv(project.EnvKafkaPassword), ClientID: "duct"}
}

// cmdStream is `duct stream [name...] [-v]`: one consumer group per
// streams/*.yaml, running until interrupted.
func cmdStream(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("stream", stderr)
	verbose := fs.Bool("v", false, "print every committed batch")
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
	cfgs, err := selectStreams(p, names)
	if err != nil {
		return err
	}
	if len(cfgs) == 0 {
		_, _ = fmt.Fprintf(stdout, "No streams: add %s/<name>.yaml\n", stream.Dir)
		return nil
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if _, err := project.Migrate(ctx, eng); err != nil {
		return err
	}
	for _, c := range cfgs {
		_, _ = fmt.Fprintf(stdout, "streaming %-20s %s -> %s (group %s)\n", c.Name, c.Topic, c.Target, c.Group)
	}
	return runStreamWriters(ctx, p, eng, cfgs, slog.Default(), func(b stream.BatchInfo) {
		if *verbose {
			_, _ = fmt.Fprintf(stdout, "%s  %-20s %6d rows %4d rejected %4d skipped  write %s  age %s\n",
				time.Now().Format("15:04:05.000"), b.Stream, b.Rows, b.Rejected, b.Skipped,
				b.Elapsed.Round(time.Millisecond), b.Age.Round(time.Millisecond))
		}
	})
}

func selectStreams(p *project.Project, names []string) ([]stream.Config, error) {
	all, err := stream.Load(p.Root)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return all, nil
	}
	byName := map[string]stream.Config{}
	for _, c := range all {
		byName[c.Name] = c
	}
	var out []stream.Config
	for _, n := range names {
		c, ok := byName[n]
		if !ok {
			return nil, usageErr("no stream %q in %s/ (have %d)", n, stream.Dir, len(all))
		}
		out = append(out, c)
	}
	return out, nil
}

// runStreamWriters runs one writer per stream until ctx ends or one fails.
func runStreamWriters(ctx context.Context, p *project.Project, eng contract.Engine, cfgs []stream.Config, log *slog.Logger, onBatch func(stream.BatchInfo)) error {
	kcfg := kafkaConfig(p)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for _, c := range cfgs {
		cons, err := stream.NewKafka(kcfg, stream.ConsumerOptions{Group: c.Group, Topics: []string{c.Topic}, Start: c.Start})
		if err != nil {
			cancel()
			wg.Wait()
			return fmt.Errorf("stream %s: %w", c.Name, err)
		}
		w := &stream.Writer{Eng: eng, Cfg: c, Consumer: cons, Log: log, OnBatch: onBatch}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cons.Close()
			if err := w.Run(ctx); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("stream %s: %w", c.Name, err)
				}
				mu.Unlock()
				cancel()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// startStreaming runs, inside `duct serve`, the writers and the alert rules
// the project configures ([stream] / [alerts] tables). It returns a function
// that waits for them to stop and reports the first failure; a failure also
// calls cancel so the server stops with a clear error instead of silently
// not ingesting.
func startStreaming(ctx context.Context, cancel context.CancelFunc, p *project.Project) (wait func() error) {
	if p.Config.Stream == nil && p.Config.Alerts == nil {
		return func() error { return nil }
	}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		fail error
	)
	run := func(name string, f func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(ctx); err != nil && ctx.Err() == nil {
				mu.Lock()
				if fail == nil {
					fail = fmt.Errorf("%s: %w", name, err)
				}
				mu.Unlock()
				cancel()
			}
		}()
	}
	log := slog.Default()
	if p.Config.Stream != nil {
		run("stream", func(ctx context.Context) error {
			cfgs, err := stream.Load(p.Root)
			if err != nil || len(cfgs) == 0 {
				return err
			}
			eng, err := openEngine(ctx, p, false)
			if err != nil {
				return err
			}
			defer func() { _ = eng.Close() }()
			if _, err := project.Migrate(ctx, eng); err != nil {
				return err
			}
			return runStreamWriters(ctx, p, eng, cfgs, log, nil)
		})
	}
	if p.Config.Alerts != nil {
		run("alerts", func(ctx context.Context) error { return runAlerts(ctx, p, log, nil) })
	}
	return func() error {
		wg.Wait()
		return fail
	}
}
