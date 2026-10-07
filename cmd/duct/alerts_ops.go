package main

import (
	"context"
	"fmt"
	"io"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/alonsoburon/duct/core/alerts"
	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
)

// alertsOps are the subcommands that only touch the Duct state (no Kafka):
//
//	duct alerts status
//	duct alerts silences
//	duct alerts silence <rule> [--key K] --for 2h --reason "..."
//	duct alerts unsilence <id>
//
// `rest` are the arguments left after the subcommand word (flags may come
// before or after the positional ones).
func alertsOps(ctx context.Context, sub, dir string, rest []string, stdout, stderr io.Writer) error {
	fs, pdir := newFlags("alerts "+sub, stderr)
	key := fs.String("key", "", "silence: only this alert key (default: every key of the rule)")
	forD := fs.String("for", "", "silence: how long, e.g. 30m, 2h, 1d (required)")
	reason := fs.String("reason", "", "silence: why (required)")
	pos, err := parseInterleaved(fs, rest)
	if err != nil {
		return err
	}
	if *pdir != "" {
		dir = *pdir
	}
	p, err := findProject(dir)
	if err != nil {
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
	now := time.Now().UTC()
	switch sub {
	case "status":
		return alertsStatus(ctx, p, eng, now, stdout)
	case "silences":
		return printSilences(ctx, eng, now, stdout)
	case "silence":
		if len(pos) != 1 {
			return usageErr("alerts silence needs one rule name: duct alerts silence <rule> [--key K] --for 2h --reason ...")
		}
		if *forD == "" || *reason == "" {
			return usageErr("alerts silence needs --for and --reason")
		}
		d, err := project.ParseAuditDuration(*forD)
		if err != nil || d <= 0 {
			return usageErr("--for %q: use a duration like 30m, 2h or 1d", *forD)
		}
		rules, err := alerts.LoadRules(p.Root)
		if err != nil {
			return err
		}
		known := false
		for _, r := range rules {
			known = known || r.Name == pos[0]
		}
		if !known {
			return fmt.Errorf("no rule %q in %s/", pos[0], alerts.Dir)
		}
		by := "cli"
		if u, err := user.Current(); err == nil {
			by = u.Username
		}
		s, err := alerts.AddSilence(ctx, eng, pos[0], *key, *reason, by, d, now)
		if err != nil {
			return err
		}
		what := s.Rule
		if s.Key != "" {
			what += " [" + s.Key + "]"
		}
		_, _ = fmt.Fprintf(stdout, "silenced %s until %s (id %d)\n", what, s.Expires.Format(time.RFC3339), s.ID)
		return nil
	default: // unsilence
		if len(pos) != 1 {
			return usageErr("alerts unsilence needs the id shown by `duct alerts silences`")
		}
		id, err := strconv.ParseInt(pos[0], 10, 64)
		if err != nil {
			return usageErr("silence id %q is not a number", pos[0])
		}
		ok, err := alerts.RemoveSilence(ctx, eng, id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no silence with id %d", id)
		}
		_, _ = fmt.Fprintf(stdout, "silence %d removed\n", id)
		return nil
	}
}

func printSilences(ctx context.Context, eng contract.Engine, now time.Time, stdout io.Writer) error {
	ss, err := alerts.ActiveSilences(ctx, eng, now)
	if err != nil {
		return err
	}
	if len(ss) == 0 {
		_, _ = fmt.Fprintln(stdout, "no active silences")
		return nil
	}
	for _, s := range ss {
		key := s.Key
		if key == "" {
			key = "*"
		}
		_, _ = fmt.Fprintf(stdout, "%d\t%s\t%s\tuntil %s (in %s)\tby %s\t%s\n", s.ID, s.Rule, key,
			s.Expires.Format(time.RFC3339), s.Expires.Sub(now).Round(time.Second), s.By, s.Reason)
	}
	return nil
}

// alertsStatus: the rules, the alerts in the shared state and the silences.
func alertsStatus(ctx context.Context, p *project.Project, eng contract.Engine, now time.Time, stdout io.Writer) error {
	rules, err := alerts.LoadRules(p.Root)
	if err != nil {
		return err
	}
	rows, err := alerts.ListState(ctx, eng)
	if err != nil {
		return err
	}
	ss, err := alerts.ActiveSilences(ctx, eng, now)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "%d rules, %d alerts in the state, %d active silences\n", len(rules), len(rows), len(ss))
	for _, r := range rules {
		var extra []string
		if r.For > 0 {
			extra = append(extra, "for "+r.For.String())
		}
		if r.ResolveFor > 0 {
			extra = append(extra, "resolve_for "+r.ResolveFor.String())
		}
		if r.Repeat > 0 {
			extra = append(extra, "repeat "+r.Repeat.String())
		}
		_, _ = fmt.Fprintf(stdout, "rule %-20s every %-6s severity %-8s %s\n", r.Name, r.Every, r.Severity, strings.Join(extra, " "))
	}
	for _, a := range rows {
		silenced := ""
		for _, s := range ss {
			if s.Matches(a.Rule, a.Key) {
				silenced = fmt.Sprintf("  SILENCED #%d until %s: %s", s.ID, s.Expires.Format(time.RFC3339), s.Reason)
				break
			}
		}
		notified := "never"
		if !a.NotifiedAt.IsZero() {
			notified = a.NotifiedAt.Format(time.RFC3339)
		}
		_, _ = fmt.Fprintf(stdout, "%-8s %s [%s] %s since %s, notified %s, holder %s%s\n", a.Status, a.Rule, orDash(a.Key), a.Severity,
			a.Since.Format(time.RFC3339), notified, orDash(a.Holder), silenced)
	}
	if len(ss) > 0 {
		_, _ = fmt.Fprintln(stdout, "silences:")
		return printSilences(ctx, eng, now, stdout)
	}
	return nil
}
