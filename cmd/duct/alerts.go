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
	"syscall"
	"time"

	"github.com/alonsoburon/duct/core/alerts"
	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
	"github.com/alonsoburon/duct/core/stream"
)

// defaultAlertsGroup is the Kafka consumer group of `duct alerts`.
const defaultAlertsGroup = "duct-alerts"

// runAlertsCmd wires `duct alerts` (own entry point, like ingest).
func runAlertsCmd(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := cmdAlerts(ctx, args, stdout, stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct alerts: %v\n", err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct alerts: %v\n", err)
		return 1
	}
}

// cmdAlerts is `duct alerts [check]`: evaluates alerts/*.sql over the topics
// of the streams that set a window, until interrupted. `duct alerts check`
// only parses the rules and the channel configuration.
func cmdAlerts(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("alerts", stderr)
	var sub string
	rest := args
	for len(rest) > 0 {
		if err := parse(fs, rest); err != nil {
			return err
		}
		rest = fs.Args()
		if len(rest) == 0 {
			break
		}
		if sub != "" {
			return usageErr("alerts takes at most one subcommand")
		}
		sub, rest = rest[0], rest[1:]
		switch sub { // these only touch the Duct state and parse their own arguments
		case "status", "silences", "silence", "unsilence":
			return alertsOps(ctx, sub, *dir, rest, stdout, stderr)
		}
	}
	if sub != "" && sub != "check" {
		return usageErr("unknown subcommand %q (check | status | silences | silence <rule> | unsilence <id>)", sub)
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	if sub == "check" {
		return checkAlerts(ctx, p, stdout)
	}
	return runAlerts(ctx, p, slog.Default(), func(e alerts.Entry, ds []alerts.Delivery) {
		t := e.Transition
		sum, _ := alerts.Summary(ds)
		_, _ = fmt.Fprintf(stdout, "%s  %-8s %s %s: %s  [%s]\n", t.At.Format("15:04:05.000"), t.State, t.Rule, t.Key, t.Message, sum)
	})
}

// alertInputs loads and cross-checks rules, windowed streams and channels.
func alertInputs(ctx context.Context, p *project.Project) ([]alerts.Rule, []stream.Config, *alerts.Channels, error) {
	rules, err := alerts.LoadRules(p.Root)
	if err != nil {
		return nil, nil, nil, err
	}
	all, err := stream.Load(p.Root)
	if err != nil {
		return nil, nil, nil, err
	}
	var windowed []stream.Config
	for _, s := range all {
		if s.Window > 0 {
			windowed = append(windowed, s)
		}
	}
	var chCfg map[string]map[string]any
	if p.Config.Alerts != nil {
		chCfg = p.Config.Alerts.Channels
	}
	ch, err := alerts.OpenChannels(ctx, chCfg)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := ch.Check(rules); err != nil {
		_ = ch.Close()
		return nil, nil, nil, err
	}
	return rules, windowed, ch, nil
}

func checkAlerts(ctx context.Context, p *project.Project, stdout io.Writer) error {
	rules, windowed, ch, err := alertInputs(ctx, p)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	for _, r := range rules {
		_, _ = fmt.Fprintf(stdout, "%-24s every %-6s severity %-8s key %-12s channels %v\n", r.Name, r.Every, r.Severity, orDash(r.Key), r.Channels)
	}
	_, _ = fmt.Fprintf(stdout, "%d rules, %d streams with a window, channels OK\n", len(rules), len(windowed))
	if len(rules) > 0 && len(windowed) == 0 {
		return errors.New("rules need at least one stream with `window:` (streams/*.yaml)")
	}
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// openWindowEngine starts the in-memory DuckDB that holds the stream windows.
func openWindowEngine(ctx context.Context) (contract.Engine, func(), error) {
	info, ok := registry.Lookup(contract.KindEngine, "duckdb")
	if !ok {
		return nil, nil, errors.New("engine module duckdb is not linked into this binary")
	}
	eng, ok := info.New().(contract.Engine)
	if !ok {
		return nil, nil, errors.New("module engine/duckdb does not implement contract.Engine")
	}
	if err := eng.Init(ctx, contract.Config{"memory": true}); err != nil {
		_ = eng.Close()
		return nil, nil, err
	}
	return eng, func() { _ = eng.Close() }, nil
}

// runAlerts runs the rules until ctx ends.
func runAlerts(ctx context.Context, p *project.Project, log *slog.Logger, onEvent func(alerts.Entry, []alerts.Delivery)) error {
	rules, windowed, ch, err := alertInputs(ctx, p)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if len(rules) == 0 {
		return fmt.Errorf("no rules: add %s/<name>.sql", alerts.Dir)
	}
	if len(windowed) == 0 {
		return errors.New("no stream has a `window:` (streams/*.yaml): the rules would have nothing to read")
	}
	var topics []string
	var longest time.Duration
	for _, s := range windowed {
		topics = append(topics, s.Topic)
		longest = max(longest, s.Window)
	}
	group := defaultAlertsGroup
	if p.Config.Alerts != nil && p.Config.Alerts.Group != "" {
		group = p.Config.Alerts.Group
	}

	lake, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	defer func() { _ = lake.Close() }()
	if _, err := project.Migrate(ctx, lake); err != nil {
		return err
	}
	window, closeWindow, err := openWindowEngine(ctx)
	if err != nil {
		return err
	}
	defer closeWindow()

	// No offsets are ever committed: every start (and every rebalance) replays
	// the last `longest` of each partition, which is exactly the state needed.
	cons, err := stream.NewKafka(kafkaConfig(p), stream.ConsumerOptions{Group: group, Topics: topics, Since: time.Now().Add(-longest)})
	if err != nil {
		return err
	}
	defer cons.Close()
	r := &alerts.Runner{Lake: lake, Window: window, Consumer: cons, Streams: windowed, Rules: rules,
		Channels: ch, Log: log, OnEvent: onEvent}
	r.Instance = os.Getenv("DUCT_ALERTS_INSTANCE")
	if p.Config.Alerts != nil {
		if p.Config.Alerts.Instance != "" {
			r.Instance = p.Config.Alerts.Instance
		}
		if s := p.Config.Alerts.LeaseTTL; s != "" {
			if r.LeaseTTL, err = time.ParseDuration(s); err != nil || r.LeaseTTL < time.Second {
				return fmt.Errorf("[alerts] lease_ttl %q must be a duration of at least 1s", s)
			}
		}
	}
	return r.Run(ctx)
}
