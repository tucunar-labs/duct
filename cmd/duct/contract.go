package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contracts"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/pipeline"
	"github.com/alonsoburon/duct/core/project"
)

const contractUsage = `usage: duct contract <command>

commands:
  lint [--strict] [--json] [file...]   validate (ODCS schema + odcs-go lint + Duct checks against pipelines/ and the lake)
  diff [--base <git-ref>] [--json]     compare contracts/ with a git ref (default HEAD); exit 1 on breaking changes
  test [--json] [table...]             run schema and quality rules against the lake; exit 1 if a critical one fails
  compile [--apply] [table...]         print what the contracts generate; --apply syncs roles, grants, filters and masks
  freshness [--json]                   freshness per contracted table from its SLA; exit 1 if any is stale
`

// runContract wires `duct contract` (own entry point, like ingest).
func runContract(args []string, stdout, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, contractUsage)
		return 2
	}
	var err error
	switch args[0] {
	case "lint":
		err = cmdContractLint(ctx, args[1:], stdout, stderr)
	case "diff":
		err = cmdContractDiff(ctx, args[1:], stdout, stderr)
	case "test":
		err = cmdContractTest(ctx, args[1:], stdout, stderr)
	case "compile":
		err = cmdContractCompile(ctx, args[1:], stdout, stderr)
	case "freshness":
		err = cmdContractFreshness(ctx, args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "duct contract: unknown command %q\n%s", args[0], contractUsage)
		return 2
	}
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errFindings):
		return 1
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "duct contract %s: %v\n", args[0], err)
		return 2
	default:
		_, _ = fmt.Fprintf(stderr, "duct contract %s: %v\n", args[0], err)
		return 1
	}
}

// errFindings marks a clean run that found problems (exit 1, message already printed).
var errFindings = errors.New("")

func contractSession(ctx context.Context, dir string) (*project.Project, contract.Engine, error) {
	p, err := findProject(dir)
	if err != nil {
		return nil, nil, err
	}
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return nil, nil, err
	}
	if _, err := project.Migrate(ctx, eng); err != nil {
		_ = eng.Close()
		return nil, nil, err
	}
	return p, eng, nil
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// selectContracts keeps the contracts matching the given table ids, bare names
// or file paths; none given = all.
func selectContracts(all []*contracts.Contract, names []string) ([]*contracts.Contract, error) {
	if len(names) == 0 {
		return all, nil
	}
	var out []*contracts.Contract
	for _, n := range names {
		found := false
		for _, c := range all {
			if c.File == n || c.Table == pipeline.NormalizeTarget(n) {
				out, found = append(out, c), true
			}
		}
		if !found {
			return nil, fmt.Errorf("no contract for %q", n)
		}
	}
	return out, nil
}

func cmdContractLint(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("contract lint", stderr)
	strict := fs.Bool("strict", false, "fail on warnings too")
	asJSON := fs.Bool("json", false, "print the issues as JSON")
	files, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	p, eng, err := contractSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	if len(files) == 0 {
		if files, err = contracts.Files(p.Root); err != nil {
			return err
		}
	}
	env := contracts.LintEnv{Root: p.Root, Pipelines: map[string]bool{}, Engine: eng}
	if ts, err := pipeline.Load(p.Root); err == nil {
		for _, t := range ts {
			env.Pipelines[t.ID()] = true
		}
	}
	var all []contracts.Issue
	tables := map[string]string{}
	for _, f := range files {
		all = append(all, contracts.Lint(ctx, env, f)...)
		if c, err := contracts.Parse(f, mustRead(p.Root, f)); err == nil {
			if prev, dup := tables[c.Table]; dup {
				all = append(all, contracts.Issue{File: f, Severity: "error", Message: fmt.Sprintf("%s is already governed by %s", c.Table, prev)})
			}
			tables[c.Table] = f
		}
	}
	if *asJSON {
		if all == nil {
			all = []contracts.Issue{}
		}
		if err := printJSON(stdout, all); err != nil {
			return err
		}
	} else {
		for _, i := range all {
			_, _ = fmt.Fprintln(stdout, i)
		}
		_, _ = fmt.Fprintf(stdout, "%d contracts checked\n", len(files))
	}
	bad := contracts.HasErrors(all)
	for _, i := range all {
		bad = bad || *strict && i.Severity == "warning"
	}
	if bad {
		return errFindings
	}
	return nil
}

func mustRead(root, f string) []byte {
	b, _ := os.ReadFile(root + "/" + f)
	return b
}

func cmdContractDiff(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("contract diff", stderr)
	base := fs.String("base", "HEAD", "git ref to compare against")
	asJSON := fs.Bool("json", false, "print the changes as JSON")
	if _, err := parseInterleaved(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	diffs, err := contracts.DiffAgainst(ctx, p.Root, *base)
	if err != nil {
		return err
	}
	breaking := false
	for _, d := range diffs {
		breaking = breaking || d.Breaking()
	}
	if *asJSON {
		if diffs == nil {
			diffs = []contracts.FileDiff{}
		}
		if err := printJSON(stdout, diffs); err != nil {
			return err
		}
	} else {
		for _, d := range diffs {
			switch d.Status {
			case "added", "unchanged":
				_, _ = fmt.Fprintf(stdout, "%s: %s\n", d.File, d.Status)
			case "removed":
				_, _ = fmt.Fprintf(stdout, "%s: removed (BREAKING: the table loses its contract)\n", d.File)
			default:
				_, _ = fmt.Fprintf(stdout, "%s: changed\n", d.File)
				for _, ch := range d.Changes {
					_, _ = fmt.Fprintf(stdout, "  %s\n", ch)
				}
			}
		}
		if breaking {
			_, _ = fmt.Fprintf(stdout, "breaking changes against %s\n", *base)
		}
	}
	if breaking {
		return errFindings
	}
	return nil
}

func cmdContractTest(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("contract test", stderr)
	asJSON := fs.Bool("json", false, "print the results as JSON")
	names, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	p, eng, err := contractSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	all, err := contracts.Load(p.Root)
	if err != nil {
		return err
	}
	cs, err := selectContracts(all, names)
	if err != nil {
		return err
	}
	type jrule struct {
		Name     string  `json:"name"`
		Severity string  `json:"severity"`
		Value    float64 `json:"value"`
		Expect   string  `json:"expect"`
		Pass     bool    `json:"pass"`
		Error    string  `json:"error,omitempty"`
	}
	type jrep struct {
		Contract string   `json:"contract"`
		Table    string   `json:"table"`
		Pass     bool     `json:"pass"`
		Schema   []string `json:"schema_issues,omitempty"`
		Error    string   `json:"error,omitempty"`
		Rules    []jrule  `json:"rules"`
	}
	failed := false
	var js []jrep
	for _, c := range cs {
		rep, err := contracts.Test(ctx, eng, c, contracts.LakeRel(c.Table))
		if err != nil {
			return err
		}
		failed = failed || rep.Failed()
		j := jrep{Contract: rep.Contract, Table: rep.Table, Pass: !rep.Failed(), Schema: rep.Schema, Rules: []jrule{}}
		if rep.Err != nil {
			j.Error = rep.Err.Error()
		}
		if !*asJSON {
			_, _ = fmt.Fprintf(stdout, "%s (%s)\n", rep.Contract, rep.Table)
			if rep.Err != nil {
				_, _ = fmt.Fprintf(stdout, "  FAIL  table: %v\n", rep.Err)
			}
			for _, m := range rep.Schema {
				_, _ = fmt.Fprintf(stdout, "  FAIL  schema: %s\n", m)
			}
		}
		for _, r := range rep.Rules {
			jr := jrule{Name: r.Check.Name, Severity: r.Check.Severity, Value: r.Value, Expect: r.Check.Expect, Pass: !r.Failed()}
			word := "PASS"
			detail := fmt.Sprintf("%s = %s (must be %s)", r.Check.Name, fmtFloat(r.Value), r.Check.Expect)
			if r.Err != nil {
				jr.Error = r.Err.Error()
				detail = fmt.Sprintf("%s: %v", r.Check.Name, r.Err)
			}
			if r.Failed() {
				word = "FAIL"
				if !r.Check.Blocking {
					word = "WARN"
				}
			}
			j.Rules = append(j.Rules, jr)
			if !*asJSON {
				_, _ = fmt.Fprintf(stdout, "  %s  [%s] %s\n", word, r.Check.Severity, detail)
			}
		}
		js = append(js, j)
	}
	if *asJSON {
		if js == nil {
			js = []jrep{}
		}
		if err := printJSON(stdout, js); err != nil {
			return err
		}
	} else if len(cs) == 0 {
		_, _ = fmt.Fprintln(stdout, "no contracts")
	}
	if failed {
		return errFindings
	}
	return nil
}

func fmtFloat(f float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.4f", f), "0"), ".")
}

func cmdContractCompile(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("contract compile", stderr)
	apply := fs.Bool("apply", false, "sync the contract roles into governance (roles, grants, row filters, masks)")
	names, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	p, eng, err := contractSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	all, err := contracts.Load(p.Root)
	if err != nil {
		return err
	}
	cs, err := selectContracts(all, names)
	if err != nil {
		return err
	}
	for _, c := range cs {
		plan, err := c.Plan()
		if err != nil {
			return fmt.Errorf("%s: %w", c.File, err)
		}
		c.Describe(stdout, plan)
	}
	if !*apply {
		return nil
	}
	return applyContractRoles(ctx, eng, cs, stdout, true)
}

// applyContractRoles syncs the roles of each contract and rebuilds the role views.
func applyContractRoles(ctx context.Context, eng contract.Engine, cs []*contracts.Contract, stdout io.Writer, rebuild bool) error {
	st := governance.NewStore(eng)
	var errs []error
	for _, c := range cs {
		log, err := contracts.ApplyRoles(ctx, st, c)
		for _, l := range log {
			_, _ = fmt.Fprintf(stdout, "applied %s: %s\n", c.File, l)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if rebuild {
		if err := regen(ctx, st, stdout); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func cmdContractFreshness(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("contract freshness", stderr)
	asJSON := fs.Bool("json", false, "print the statuses as JSON")
	if _, err := parseInterleaved(fs, args); err != nil {
		return err
	}
	p, eng, err := contractSession(ctx, *dir)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	cs, err := contracts.Load(p.Root)
	if err != nil {
		return err
	}
	sts, err := contracts.FreshnessOf(ctx, eng, cs, time.Now())
	if err != nil {
		return err
	}
	stale := false
	for _, s := range sts {
		stale = stale || s.IsStale()
	}
	if *asJSON {
		if sts == nil {
			sts = []contracts.Status{}
		}
		if err := printJSON(stdout, sts); err != nil {
			return err
		}
	} else {
		for _, s := range sts {
			line := fmt.Sprintf("%-8s %s", strings.ToUpper(s.Status), s.Table)
			if s.LastRefresh != nil {
				line += fmt.Sprintf("  last refresh %s (%s ago)", s.LastRefresh.Format(time.RFC3339), (time.Duration(s.AgeSeconds) * time.Second).String())
			}
			if s.MaxSeconds > 0 {
				line += fmt.Sprintf("  max %s (%s)", (time.Duration(s.MaxSeconds) * time.Second).String(), s.Source)
			}
			_, _ = fmt.Fprintln(stdout, line)
		}
	}
	if stale {
		return errFindings
	}
	return nil
}
