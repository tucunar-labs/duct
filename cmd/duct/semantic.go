package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/semantic"
)

// semanticDirName is where the Ossie models live inside a project.
const semanticDirName = "semantic"

// hasSemanticModel reports whether the project has semantic/*.ossie.yaml files.
func hasSemanticModel(p *project.Project) bool {
	files, err := semantic.Files(filepath.Join(p.Root, semanticDirName))
	return err == nil && len(files) > 0
}

// cmdSemantic is `duct semantic validate|list|build|query` (ADR 0011).
func cmdSemantic(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageErr("semantic needs a subcommand: validate [file...] | list | build --role R | query --role R --measures m")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "validate":
		return semanticValidate(rest, stdout, stderr)
	case "list":
		return semanticList(ctx, rest, stdout, stderr)
	case "build":
		return semanticBuild(rest, stdout, stderr)
	case "query":
		return semanticQuery(ctx, rest, stdout, stderr)
	}
	return usageErr("unknown semantic subcommand %q", sub)
}

// loadModel loads the explicit files, or semantic/*.ossie.yaml of the project.
func loadModel(dir string, files []string) (*semantic.Model, *project.Project, error) {
	if len(files) > 0 {
		m, err := semantic.LoadFiles(files...)
		return m, nil, err
	}
	p, err := findProject(dir)
	if err != nil {
		return nil, nil, err
	}
	m, err := semantic.LoadDir(filepath.Join(p.Root, semanticDirName))
	if errors.Is(err, semantic.ErrNoModel) {
		return nil, p, fmt.Errorf("no model found: create %s/*.ossie.yaml (Apache Ossie %s)", semanticDirName, semantic.SpecVersion)
	}
	return m, p, err
}

func semanticValidate(args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("semantic validate", stderr)
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	m, _, err := loadModel(*dir, pos)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "ok: %d documents, %d datasets, %d relationships, %d metrics (Ossie %s)\n",
		len(m.Docs), len(m.Datasets), len(m.Relationships), len(m.Metrics), semantic.SpecVersion)
	return nil
}

// roleScope lists the views of the role by asking the governed handler.
func roleScope(ctx context.Context, h *governance.Handler, id contract.Identity) (semantic.Scope, error) {
	st, err := h.Catalog(ctx, id)
	if err != nil {
		return semantic.Scope{}, err
	}
	defer st.Release()
	sc := semantic.Scope{Role: id.Role, Tables: map[string]bool{}}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			sc.Tables[strings.ToLower(strings.Clone(rec.Column(1).ValueStr(i)))] = true
		}
	}
	return sc, st.Err()
}

// openHandler opens the project's engine and a governance handler over it.
func openHandler(ctx context.Context, p *project.Project) (*governance.Handler, func(), error) {
	eng, err := openEngine(ctx, p, false)
	if err != nil {
		return nil, nil, err
	}
	if _, err := project.Migrate(ctx, eng); err != nil {
		_ = eng.Close()
		return nil, nil, err
	}
	h := governance.NewHandler(eng, governance.HandlerOptions{})
	return h, func() {
		_ = h.FlushAudit(context.WithoutCancel(ctx))
		_ = h.Close()
		_ = eng.Close()
	}, nil
}

func semanticList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("semantic list", stderr)
	role := fs.String("role", "", "list only what this role can use (needs its views)")
	if err := parse(fs, args); err != nil {
		return err
	}
	m, p, err := loadModel(*dir, nil)
	if err != nil {
		return err
	}
	sc := semantic.Scope{Role: "any"}
	if *role != "" {
		h, done, err := openHandler(ctx, p)
		if err != nil {
			return err
		}
		defer done()
		if sc, err = roleScope(ctx, h, contract.Identity{User: "cli", Role: *role}); err != nil {
			return err
		}
	}
	for _, mt := range m.ListMetrics(sc) {
		_, _ = fmt.Fprintf(stdout, "%s\t%s\t%s\n", mt.Name, mt.Dataset, mt.Description)
	}
	return nil
}

func semanticBuild(args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("semantic build", stderr)
	role := fs.String("role", "", "role whose views the SQL reads (required)")
	out := fs.String("out", "", "write one <metric>.sql per metric into this folder instead of printing")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *role == "" {
		return usageErr("semantic build needs --role: the SQL reads the views of that role")
	}
	m, _, err := loadModel(*dir, nil)
	if err != nil {
		return err
	}
	if *out != "" {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			return err
		}
	}
	for _, mt := range m.Metrics {
		if mt.Hidden {
			continue
		}
		c, err := m.Query(semantic.Scope{Role: *role}, semantic.Request{Measures: []string{mt.Name}})
		if err != nil {
			return fmt.Errorf("metric %s: %w", mt.Name, err)
		}
		if *out == "" {
			_, _ = fmt.Fprintf(stdout, "-- metric %s\n%s;\n\n", mt.Name, c.SQL)
			continue
		}
		if err := os.WriteFile(filepath.Join(*out, mt.Name+".sql"), []byte(c.SQL+";\n"), 0o644); err != nil {
			return err
		}
	}
	if *out != "" {
		_, _ = fmt.Fprintf(stdout, "wrote %d files to %s\n", len(m.Metrics), *out)
	}
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, "; ") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func semanticQuery(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("semantic query", stderr)
	role := fs.String("role", "", "role to run as (required): the query reads its views")
	dims := fs.String("dims", "", "comma-separated dimensions (dataset.field or field)")
	measures := fs.String("measures", "", "comma-separated metrics")
	var filters multiFlag
	fs.Var(&filters, "filter", "condition, repeatable: 'field=value', field!=v, <, <=, >, >=, 'field~pattern' (LIKE), 'field in a,b', 'field is null'")
	order := fs.String("order", "", "comma-separated sort keys, prefix with - for descending")
	limit := fs.Int("limit", 0, "maximum rows (default from the model)")
	format := fs.String("format", "table", "output format: table, csv or json")
	showSQL := fs.Bool("sql", false, "print the generated SQL instead of running it")
	if err := parse(fs, args); err != nil {
		return err
	}
	if *role == "" {
		return usageErr("semantic query needs --role (it never reads base tables)")
	}
	m, p, err := loadModel(*dir, nil)
	if err != nil {
		return err
	}
	req := semantic.Request{Dims: splitList(*dims), Measures: splitList(*measures), Limit: *limit}
	for _, f := range filters {
		fl, err := parseFilter(f)
		if err != nil {
			return usageErr("%v", err)
		}
		req.Filters = append(req.Filters, fl)
	}
	for _, o := range splitList(*order) {
		req.Order = append(req.Order, semantic.Order{By: strings.TrimPrefix(o, "-"), Desc: strings.HasPrefix(o, "-")})
	}
	h, done, err := openHandler(ctx, p)
	if err != nil {
		return err
	}
	defer done()
	id := contract.Identity{User: "cli", Role: *role}
	sc, err := roleScope(ctx, h, id)
	if err != nil {
		return err
	}
	c, err := m.Query(sc, req)
	if err != nil {
		return err
	}
	if *showSQL {
		_, _ = fmt.Fprintln(stdout, c.SQL)
		return nil
	}
	st, err := h.ForProtocol("cli").Query(ctx, id, c.SQL)
	if err != nil {
		return err
	}
	defer st.Release()
	return writeStream(st, *format, stdout)
}

var filterRe = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*(!=|<=|>=|=|<|>|~)\s*(.*?)\s*$`)
var wordFilterRe = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_.]*)\s+(not in|in|is not null|is null)\s*(.*?)\s*$`)

// parseFilter reads the CLI form of a structured filter (never SQL).
func parseFilter(s string) (semantic.Filter, error) {
	if m := wordFilterRe.FindStringSubmatch(s); m != nil {
		switch op := strings.ToLower(m[2]); op {
		case "is null":
			return semantic.Filter{Field: m[1], Op: "is_null"}, nil
		case "is not null":
			return semantic.Filter{Field: m[1], Op: "is_not_null"}, nil
		default:
			var vals []any
			for _, v := range splitList(m[3]) {
				vals = append(vals, cliValue(v))
			}
			return semantic.Filter{Field: m[1], Op: strings.ReplaceAll(op, " ", "_"), Value: vals}, nil
		}
	}
	if m := filterRe.FindStringSubmatch(s); m != nil {
		op := m[2]
		if op == "~" {
			return semantic.Filter{Field: m[1], Op: "like", Value: m[3]}, nil
		}
		return semantic.Filter{Field: m[1], Op: op, Value: cliValue(m[3])}, nil
	}
	return semantic.Filter{}, fmt.Errorf("filter %q: use field=value, field!=v, <, <=, >, >=, field~pattern, 'field in a,b' or 'field is null'", s)
}

// cliValue types a command-line value: 'quoted' stays text, numbers and
// true/false are typed, everything else is text.
func cliValue(v string) any {
	if len(v) >= 2 && (v[0] == '\'' && v[len(v)-1] == '\'' || v[0] == '"' && v[len(v)-1] == '"') {
		return v[1 : len(v)-1]
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	switch strings.ToLower(v) {
	case "true":
		return true
	case "false":
		return false
	}
	return v
}
