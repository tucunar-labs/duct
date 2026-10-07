// Package dashboards is the declarative dashboard format of Duct (ADR 0008,
// 0012): dashboards/<name>.yaml, one file per dashboard, in git. A dashboard
// is filters plus charts; every chart is a semantic query (dims, measures,
// sort, limit) that the server runs as the viewer's identity, so the same
// numbers appear in dashboards, the MCP tool query_metrics and the CLI.
//
// The package is pure: parsing, validation and the translation of a
// dashboard state (filter values, cross-filter selections) into
// contract.SemanticRequest. It never runs a query. Schema: specs/dashboard.schema.json.
package dashboards

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Dir is the project folder that holds the dashboards.
const Dir = "dashboards"

// Chart and filter types.
const (
	Line    = "line"
	Bar     = "bar"
	Area    = "area"
	Pie     = "pie"
	Table   = "table"
	KPI     = "kpi"
	Scatter = "scatter"
	Heatmap = "heatmap"

	FilterSelect      = "select"
	FilterMultiselect = "multiselect"
	FilterDateRange   = "daterange"
)

var (
	chartTypes  = []string{Line, Bar, Area, Pie, Table, KPI, Scatter, Heatmap}
	filterTypes = []string{FilterSelect, FilterMultiselect, FilterDateRange}
	formats     = []string{"auto", "integer", "decimal", "percent", "compact"}
	nameRe      = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	idRe        = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)
	colorRe     = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)
	dateRe      = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
)

const (
	// MaxLimit caps a chart's limit.
	MaxLimit = 10000
	// GridColumns is the width of the layout grid.
	GridColumns = 12
	maxValues   = 100
	maxValueLen = 200
)

// Dashboard is one dashboards/<name>.yaml file.
type Dashboard struct {
	Title       string   `yaml:"title"`
	Description string   `yaml:"description"`
	Filters     []Filter `yaml:"filters"`
	Charts      []Chart  `yaml:"charts"`

	// Name is the file name without extension (the URL slug); File its path.
	Name string `yaml:"-"`
	File string `yaml:"-"`
}

// Filter is a picker at the top of the dashboard. A select/multiselect lists
// the values of Dimension (a semantic query grouped by it); a daterange
// bounds a time dimension. It applies to the Charts listed (default: all).
type Filter struct {
	ID        string   `yaml:"id"`
	Label     string   `yaml:"label"`
	Type      string   `yaml:"type"`
	Dimension string   `yaml:"dimension"`
	Measure   string   `yaml:"measure"` // metric used to list the values (default: first measure of an applicable chart)
	Charts    []string `yaml:"charts"`
}

// Chart is one panel: a semantic query and how to draw it.
type Chart struct {
	ID       string      `yaml:"id"`
	Title    string      `yaml:"title"`
	Type     string      `yaml:"type"`
	Dims     []string    `yaml:"dims"`
	Measures []string    `yaml:"measures"`
	Sort     []Sort      `yaml:"sort"`
	Limit    int         `yaml:"limit"`
	Where    []Condition `yaml:"where"`
	Layout   Layout      `yaml:"layout"`
	Options  Options     `yaml:"options"`
	Cross    *bool       `yaml:"cross_filter"` // false: neither emits nor receives cross-filters
}

// Sort orders a chart's rows by one of its dims or measures.
type Sort struct {
	By   string `yaml:"by"`
	Desc bool   `yaml:"desc"`
}

// Condition is a static filter of a chart (the structured filter of
// query_metrics; never SQL).
type Condition struct {
	Field string `yaml:"field"`
	Op    string `yaml:"op"`
	Value any    `yaml:"value"`
}

// Layout places a chart on the 12-column grid. X and Y are optional (auto
// flow); W defaults to 6 columns and H to 4 rows.
type Layout struct {
	X *int `yaml:"x"`
	Y *int `yaml:"y"`
	W int  `yaml:"w"`
	H int  `yaml:"h"`
}

// Options is the whitelist of drawing options. They are Duct options, not raw
// ECharts options: no formatter functions, no arbitrary option tree.
type Options struct {
	Legend     *bool    `yaml:"legend" json:"legend,omitempty"`
	Stack      bool     `yaml:"stack" json:"stack,omitempty"`
	Smooth     bool     `yaml:"smooth" json:"smooth,omitempty"`
	Horizontal bool     `yaml:"horizontal" json:"horizontal,omitempty"`
	Donut      bool     `yaml:"donut" json:"donut,omitempty"`
	Labels     *bool    `yaml:"labels" json:"labels,omitempty"`
	Log        bool     `yaml:"log" json:"log,omitempty"`
	XTitle     string   `yaml:"x_title" json:"x_title,omitempty"`
	YTitle     string   `yaml:"y_title" json:"y_title,omitempty"`
	Format     string   `yaml:"format" json:"format,omitempty"`
	Prefix     string   `yaml:"prefix" json:"prefix,omitempty"`
	Suffix     string   `yaml:"suffix" json:"suffix,omitempty"`
	Colors     []string `yaml:"colors" json:"colors,omitempty"`
	YMin       *float64 `yaml:"y_min" json:"y_min,omitempty"`
	YMax       *float64 `yaml:"y_max" json:"y_max,omitempty"`
}

// Emits reports whether a click or brush on the chart adds a cross-filter.
func (c *Chart) Emits() bool {
	if c.Cross != nil && !*c.Cross {
		return false
	}
	switch c.Type {
	case KPI, Table, Scatter:
		return false
	}
	return len(c.Dims) > 0
}

// Receives reports whether cross-filters from other charts apply to it.
func (c *Chart) Receives() bool { return c.Cross == nil || *c.Cross }

// Chart returns the chart with id.
func (d *Dashboard) Chart(id string) (*Chart, bool) {
	for i := range d.Charts {
		if d.Charts[i].ID == id {
			return &d.Charts[i], true
		}
	}
	return nil, false
}

// FilterByID returns the filter with id.
func (d *Dashboard) FilterByID(id string) (*Filter, bool) {
	for i := range d.Filters {
		if d.Filters[i].ID == id {
			return &d.Filters[i], true
		}
	}
	return nil, false
}

// Applies reports whether the filter applies to the chart.
func (f *Filter) Applies(chartID string) bool {
	if len(f.Charts) == 0 {
		return true
	}
	for _, c := range f.Charts {
		if c == chartID {
			return true
		}
	}
	return false
}

// Parse decodes and validates one dashboard (unknown keys are errors).
func Parse(data []byte) (*Dashboard, error) {
	var d Dashboard
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&d); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("empty file")
		}
		return nil, err
	}
	for i := range d.Charts {
		c := &d.Charts[i]
		if c.Layout.W == 0 {
			c.Layout.W = 6
		}
		if c.Layout.H == 0 {
			c.Layout.H = 4
		}
	}
	return &d, d.Validate()
}

// Validate checks everything that does not need the semantic model.
func (d *Dashboard) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	if strings.TrimSpace(d.Title) == "" {
		add("title is required")
	}
	if len(d.Charts) == 0 {
		add("at least one chart is required")
	}
	seen := map[string]bool{}
	for i := range d.Charts {
		c := &d.Charts[i]
		where := fmt.Sprintf("chart %q", c.ID)
		if !idRe.MatchString(c.ID) {
			add("charts[%d]: id %q must match %s", i, c.ID, idRe)
			continue
		}
		if seen[c.ID] {
			add("%s: duplicate id", where)
		}
		seen[c.ID] = true
		errs = append(errs, validateChart(c, where)...)
	}
	fseen := map[string]bool{}
	for i := range d.Filters {
		f := &d.Filters[i]
		where := fmt.Sprintf("filter %q", f.ID)
		if !idRe.MatchString(f.ID) {
			add("filters[%d]: id %q must match %s", i, f.ID, idRe)
			continue
		}
		if fseen[f.ID] {
			add("%s: duplicate id", where)
		}
		fseen[f.ID] = true
		if !contains(filterTypes, f.Type) {
			add("%s: type %q must be one of %s", where, f.Type, strings.Join(filterTypes, ", "))
		}
		if strings.TrimSpace(f.Dimension) == "" {
			add("%s: dimension is required", where)
		}
		if f.Type == FilterDateRange && f.Measure != "" {
			add("%s: measure only applies to select and multiselect", where)
		}
		for _, cid := range f.Charts {
			if !seen[cid] {
				add("%s: charts refers to unknown chart %q", where, cid)
			}
		}
	}
	return errors.Join(errs...)
}

func validateChart(c *Chart, where string) []error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(where+": "+format, a...)) }
	if !contains(chartTypes, c.Type) {
		add("type %q must be one of %s", c.Type, strings.Join(chartTypes, ", "))
		return errs
	}
	nd, nm := len(c.Dims), len(c.Measures)
	if nm == 0 {
		add("at least one measure is required")
	}
	switch c.Type {
	case KPI:
		if nd != 0 || nm != 1 {
			add("kpi takes no dims and exactly one measure")
		}
	case Pie:
		if nd != 1 || nm != 1 {
			add("pie takes one dim and one measure")
		}
	case Heatmap:
		if nd != 2 || nm != 1 {
			add("heatmap takes two dims (x, y) and one measure")
		}
	case Scatter:
		if nd > 1 || nm < 2 || nm > 3 {
			add("scatter takes at most one dim (point label) and two or three measures (x, y, size)")
		}
	case Line, Area, Bar:
		if nd < 1 || nd > 2 {
			add("%s takes one dim (x), plus an optional second dim (series)", c.Type)
		}
		if nd == 2 && nm != 1 {
			add("with two dims there must be exactly one measure")
		}
	}
	for _, s := range c.Sort {
		if !contains(c.Dims, s.By) && !contains(c.Measures, s.By) {
			add("sort by %q: not one of the chart's dims or measures", s.By)
		}
	}
	if c.Limit < 0 || c.Limit > MaxLimit {
		add("limit must be between 0 and %d", MaxLimit)
	}
	for _, w := range c.Where {
		if w.Field == "" || w.Op == "" {
			add("where needs field and op")
		}
	}
	l := c.Layout
	if l.W < 1 || l.W > GridColumns || l.H < 1 || l.H > 12 {
		add("layout w must be 1..%d and h 1..12", GridColumns)
	}
	if l.X != nil && (*l.X < 0 || *l.X+l.W > GridColumns) {
		add("layout x + w must fit in %d columns", GridColumns)
	}
	if l.Y != nil && *l.Y < 0 {
		add("layout y must be >= 0")
	}
	o := c.Options
	if o.Format != "" && !contains(formats, o.Format) {
		add("options.format %q must be one of %s", o.Format, strings.Join(formats, ", "))
	}
	for _, col := range o.Colors {
		if !colorRe.MatchString(col) {
			add("options.colors: %q is not a #rrggbb color", col)
		}
	}
	if len(o.Colors) > 12 {
		add("options.colors: at most 12")
	}
	if len(o.XTitle) > 80 || len(o.YTitle) > 80 || len(o.Prefix) > 8 || len(o.Suffix) > 8 {
		add("options: titles are limited to 80 characters, prefix and suffix to 8")
	}
	if o.YMin != nil && o.YMax != nil && *o.YMin >= *o.YMax {
		add("options: y_min must be below y_max")
	}
	return errs
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// Load reads every dashboards/*.yaml (and .yml) of dir. Files that do not
// parse are returned in bad (name -> error) instead of failing the rest. A
// missing folder yields nothing. Dashboards are sorted by name.
func Load(dir string) (ok []*Dashboard, bad map[string]error, err error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	bad = map[string]error{}
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		if e.IsDir() || (ext != ".yaml" && ext != ".yml") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ext)
		path := filepath.Join(dir, e.Name())
		if !nameRe.MatchString(name) {
			bad[name] = fmt.Errorf("file name must match %s", nameRe)
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			bad[name] = err
			continue
		}
		d, err := Parse(data)
		if err != nil {
			bad[name] = err
			continue
		}
		if prev := findName(ok, name); prev != nil {
			bad[name] = fmt.Errorf("already defined in %s", prev.File)
			continue
		}
		d.Name, d.File = name, path
		ok = append(ok, d)
	}
	sort.Slice(ok, func(i, j int) bool { return ok[i].Name < ok[j].Name })
	return ok, bad, nil
}

func findName(ds []*Dashboard, name string) *Dashboard {
	for _, d := range ds {
		if d.Name == name {
			return d
		}
	}
	return nil
}

// LoadOne reads dashboards/<name>.yaml (or .yml) of dir.
func LoadOne(dir, name string) (*Dashboard, error) {
	if !nameRe.MatchString(name) {
		return nil, os.ErrNotExist
	}
	for _, ext := range []string{".yaml", ".yml"} {
		path := filepath.Join(dir, name+ext)
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		d, err := Parse(data)
		if err != nil {
			return nil, err
		}
		d.Name, d.File = name, path
		return d, nil
	}
	return nil, os.ErrNotExist
}
