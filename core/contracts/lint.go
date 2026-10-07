package contracts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	odcs "github.com/alonsoburon/odcs-go"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Issue is one finding of Lint.
type Issue struct {
	File     string `json:"file"`
	Severity string `json:"severity"` // error | warning | info
	Path     string `json:"path,omitempty"`
	Message  string `json:"message"`
}

func (i Issue) String() string {
	p := ""
	if i.Path != "" {
		p = " " + i.Path
	}
	return fmt.Sprintf("%s: %s%s: %s", i.File, i.Severity, p, i.Message)
}

// HasErrors reports whether any issue is an error.
func HasErrors(is []Issue) bool {
	for _, i := range is {
		if i.Severity == "error" {
			return true
		}
	}
	return false
}

// LintEnv is what Lint knows about the project.
type LintEnv struct {
	Root      string
	Pipelines map[string]bool // table ids defined in pipelines/
	Engine    contract.Engine // optional: checks the lake table, columns and types
}

// Lint checks one contract file: ODCS schema validation, odcs-go semantic
// lint, and the Duct checks: the table is a pipeline target or exists in the
// lake, its columns exist with compatible types, every rule compiles and its
// SQL binds, roles and extensions are well formed. file is relative to root.
func Lint(ctx context.Context, env LintEnv, file string) []Issue {
	var out []Issue
	add := func(sev, path, format string, a ...any) {
		out = append(out, Issue{File: file, Severity: sev, Path: path, Message: fmt.Sprintf(format, a...)})
	}
	data, err := os.ReadFile(filepath.Join(env.Root, filepath.FromSlash(file))) //nolint:gosec // under the project root
	if err != nil {
		add("error", "", "%v", err)
		return out
	}
	if err := odcs.Validate(data); err != nil {
		var ve *odcs.ValidationError
		if errors.As(err, &ve) {
			for _, e := range ve.Errors {
				add("error", e.Path, "%s", e.Message)
			}
		} else {
			add("error", "", "%v", err)
		}
		return out
	}
	dc, err := odcs.Parse(data)
	if err != nil {
		add("error", "", "%v", err)
		return out
	}
	for _, is := range odcs.Lint(dc) {
		add(is.Severity.String(), is.Path, "%s", is.Message)
	}
	c, err := bind(file, dc)
	if err != nil {
		add("error", "", "%s", strings.TrimPrefix(err.Error(), file+": "))
		return out
	}
	plan, err := c.Plan()
	if err != nil {
		for _, m := range strings.Split(err.Error(), "\n") {
			add("error", "", "%s", m)
		}
	}
	if plan == nil {
		return out
	}
	for _, s := range plan.Skipped {
		add("info", "", "not evaluated: %s", s)
	}
	for _, s := range c.skippedRoles() {
		add("info", "", "%s", s)
	}
	if sch, _, _ := strings.Cut(c.Table, "."); sch == "bronze" || sch == "silver" {
		add("warning", "", "%s is %s: by the single-source rule (ADR 0004) Bronze and Silver carry EXPECT in the pipeline SQL; contracts govern Gold and shared tables", c.Table, sch)
	}
	inPipe := env.Pipelines[c.Table]
	if !inPipe && plan.SLA.Latency+plan.SLA.Frequency > 0 {
		add("warning", "", "%s is not a pipeline target: its freshness will be reported as unknown", c.Table)
	}
	if env.Engine == nil {
		return out
	}
	rel := LakeRel(c.Table)
	actual, err := ActualColumns(ctx, env.Engine, rel)
	if err != nil {
		if inPipe {
			add("info", "", "%s is not materialized yet: schema and rules are checked on the first `duct run`", c.Table)
		} else {
			add("error", "", "%s is neither a pipeline target nor a table in the lake", c.Table)
		}
		return out
	}
	for _, m := range c.SchemaIssues(actual) {
		add("error", "", "%s", m)
	}
	for _, ch := range plan.Checks {
		if _, err := queryRows(ctx, env.Engine, "EXPLAIN "+ch.Query(rel)); err != nil {
			add("error", "", "rule %q does not bind against %s: %v", ch.Name, c.Table, err)
		}
	}
	return out
}
