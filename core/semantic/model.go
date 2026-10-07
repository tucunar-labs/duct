package semantic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// DuctVendor is the vendor_name of Duct's custom_extensions.
//
// Recognised data (a JSON string, unknown keys are an error):
//   - model:   {"default_limit": 1000, "max_limit": 10000}
//   - dataset, field, metric: {"hidden": true} (kept out of describe/list and
//     unusable as a dimension or filter by clients; metrics may still use it)
const DuctVendor = "DUCT"

// Default and maximum row limits of a query (overridable by the DUCT
// extension of the model).
const (
	DefaultLimit = 1000
	MaxLimit     = 10000
)

// ErrNoModel is returned by LoadDir when there is no *.ossie.yaml file.
var ErrNoModel = errors.New("no semantic model (*.ossie.yaml) found")

// Model is a semantic model: the union of every document loaded, resolved and
// ready to compile queries. Create it with Parse, LoadFiles or LoadDir.
type Model struct {
	Docs          []Doc
	Datasets      []*Dataset
	Relationships []*Relationship
	Metrics       []*Metric

	defaultLimit, maxLimit int
	dsIdx                  map[string]*Dataset
	metIdx                 map[string]*Metric
}

// Doc is the header of one loaded document.
type Doc struct {
	File        string
	Name        string
	Description string
	AIContext   any
}

type document struct {
	Version          string          `yaml:"version"`
	Name             string          `yaml:"name"`
	Description      string          `yaml:"description"`
	AIContext        any             `yaml:"ai_context"`
	Datasets         []*Dataset      `yaml:"datasets"`
	Relationships    []*Relationship `yaml:"relationships"`
	Metrics          []*Metric       `yaml:"metrics"`
	CustomExtensions []Extension     `yaml:"custom_extensions"`
}

// Extension is an Ossie custom_extensions entry.
type Extension struct {
	VendorName string `yaml:"vendor_name"`
	Data       string `yaml:"data"`
}

// Expression is an Ossie multi-dialect expression.
type Expression struct {
	Dialects []DialectExpr `yaml:"dialects"`
}

// DialectExpr is one dialect of an Expression.
type DialectExpr struct {
	Dialect    string `yaml:"dialect"`
	Expression string `yaml:"expression"`
}

// Dataset is a logical dataset: one lake table (or role view).
type Dataset struct {
	Name             string      `yaml:"name"`
	Source           string      `yaml:"source"`
	PrimaryKey       []string    `yaml:"primary_key"`
	UniqueKeys       [][]string  `yaml:"unique_keys"`
	Description      string      `yaml:"description"`
	AIContext        any         `yaml:"ai_context"`
	Fields           []*Field    `yaml:"fields"`
	CustomExtensions []Extension `yaml:"custom_extensions"`

	// Table is the table name of Source ("gold.ventas" -> "ventas"): the name
	// of the view in the role schema.
	Table  string `yaml:"-"`
	Hidden bool   `yaml:"-"`

	file     string
	fieldIdx map[string]*Field
	extra    []string // key columns that are not fields: projected as they are
}

// Field is a row-level attribute.
type Field struct {
	Name             string      `yaml:"name"`
	Expression       Expression  `yaml:"expression"`
	Dimension        *Dimension  `yaml:"dimension"`
	Label            string      `yaml:"label"`
	Description      string      `yaml:"description"`
	Datatype         string      `yaml:"datatype"`
	AIContext        any         `yaml:"ai_context"`
	CustomExtensions []Extension `yaml:"custom_extensions"`

	Hidden bool   `yaml:"-"`
	SQL    string `yaml:"-"` // expression in DuckDB SQL over the source columns
}

// Dimension is Ossie's dimension metadata.
type Dimension struct {
	IsTime *bool `yaml:"is_time"`
}

// IsTime applies the Ossie default: a temporal datatype is a time dimension
// unless is_time says otherwise.
func (f *Field) IsTime() bool {
	if f.Dimension != nil && f.Dimension.IsTime != nil {
		return *f.Dimension.IsTime
	}
	switch f.Datatype {
	case "Date", "Time", "DateTime", "DateTimeTz":
		return true
	}
	return false
}

// Relationship joins the many side (From) to the one side (To).
type Relationship struct {
	Name             string      `yaml:"name"`
	From             string      `yaml:"from"`
	To               string      `yaml:"to"`
	FromColumns      []string    `yaml:"from_columns"`
	ToColumns        []string    `yaml:"to_columns"`
	AIContext        any         `yaml:"ai_context"`
	CustomExtensions []Extension `yaml:"custom_extensions"`

	from, to *Dataset
}

// Metric is an aggregate expression over the fields of one dataset (the
// metric's home, the fact it is computed on) or over other metrics of it.
type Metric struct {
	Name             string      `yaml:"name"`
	Expression       Expression  `yaml:"expression"`
	Description      string      `yaml:"description"`
	Datatype         string      `yaml:"datatype"`
	AIContext        any         `yaml:"ai_context"`
	CustomExtensions []Extension `yaml:"custom_extensions"`

	Hidden bool `yaml:"-"`

	file     string
	segs     []seg // resolved: segRaw and segRef only
	home     *Dataset
	refs     []fieldRef
	resolved bool
	visiting bool
}

type fieldRef struct {
	ds *Dataset
	f  *Field
}

// Home returns the dataset the metric is computed on.
func (m *Metric) Home() *Dataset { return m.home }

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// Parse validates one document against the Ossie schema and resolves it.
func Parse(file string, data []byte) (*Model, error) {
	m, err := parseDoc(file, data)
	if err != nil {
		return nil, err
	}
	return Merge(m)
}

func parseDoc(file string, data []byte) (*Model, error) {
	if err := validateDocument(data); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	var d document
	dec := yaml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	m := &Model{Docs: []Doc{{File: file, Name: d.Name, Description: d.Description, AIContext: d.AIContext}},
		Datasets: d.Datasets, Relationships: d.Relationships, Metrics: d.Metrics}
	for _, x := range d.Datasets {
		x.file = file
	}
	for _, x := range d.Metrics {
		x.file = file
	}
	lim, err := modelExtension(d.CustomExtensions)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	m.defaultLimit, m.maxLimit = lim.DefaultLimit, lim.MaxLimit
	return m, nil
}

// Merge joins models (one per document) into a single resolved model. Dataset,
// relationship and metric names must be unique across all of them.
func Merge(models ...*Model) (*Model, error) {
	out := &Model{}
	for _, m := range models {
		out.Docs = append(out.Docs, m.Docs...)
		out.Datasets = append(out.Datasets, m.Datasets...)
		out.Relationships = append(out.Relationships, m.Relationships...)
		out.Metrics = append(out.Metrics, m.Metrics...)
		if m.defaultLimit > 0 {
			out.defaultLimit = m.defaultLimit
		}
		if m.maxLimit > 0 {
			out.maxLimit = m.maxLimit
		}
	}
	if out.maxLimit == 0 {
		out.maxLimit = MaxLimit
	}
	if out.defaultLimit == 0 {
		out.defaultLimit = min(DefaultLimit, out.maxLimit)
	}
	if out.defaultLimit > out.maxLimit {
		return nil, fmt.Errorf("DUCT extension: default_limit %d exceeds max_limit %d", out.defaultLimit, out.maxLimit)
	}
	if err := out.resolve(); err != nil {
		return nil, err
	}
	return out, nil
}

// LoadFiles loads and merges the given model files.
func LoadFiles(paths ...string) (*Model, error) {
	var models []*Model
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		m, err := parseDoc(filepath.Base(p), data)
		if err != nil {
			return nil, err
		}
		models = append(models, m)
	}
	if len(models) == 0 {
		return nil, ErrNoModel
	}
	return Merge(models...)
}

// Files lists the model files of dir (semantic/*.ossie.yaml), sorted.
func Files(dir string) ([]string, error) {
	var out []string
	for _, pat := range []string{"*.ossie.yaml", "*.ossie.yml"} {
		fs, err := filepath.Glob(filepath.Join(dir, pat))
		if err != nil {
			return nil, err
		}
		out = append(out, fs...)
	}
	sort.Strings(out)
	return out, nil
}

// LoadDir loads every *.ossie.yaml of dir. It returns ErrNoModel when there
// is none (or the folder does not exist).
func LoadDir(dir string) (*Model, error) {
	files, err := Files(dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, ErrNoModel
	}
	return LoadFiles(files...)
}

type modelExt struct {
	DefaultLimit int `json:"default_limit"`
	MaxLimit     int `json:"max_limit"`
}

type nodeExt struct {
	Hidden bool `json:"hidden"`
}

func ductData(exts []Extension) (string, error) {
	var data string
	for _, e := range exts {
		if e.VendorName != DuctVendor {
			continue
		}
		if data != "" {
			return "", errors.New("more than one DUCT extension")
		}
		data = e.Data
	}
	return data, nil
}

func decodeStrict(data string, into any) error {
	dec := json.NewDecoder(strings.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("DUCT extension: %w", err)
	}
	return nil
}

func modelExtension(exts []Extension) (modelExt, error) {
	var e modelExt
	data, err := ductData(exts)
	if err != nil || data == "" {
		return e, err
	}
	if err := decodeStrict(data, &e); err != nil {
		return e, err
	}
	if e.DefaultLimit < 0 || e.MaxLimit < 0 {
		return e, errors.New("DUCT extension: limits must be positive")
	}
	return e, nil
}

func hiddenOf(what string, exts []Extension) (bool, error) {
	data, err := ductData(exts)
	if err != nil || data == "" {
		if err != nil {
			err = fmt.Errorf("%s: %w", what, err)
		}
		return false, err
	}
	var e nodeExt
	if err := decodeStrict(data, &e); err != nil {
		return false, fmt.Errorf("%s: %w", what, err)
	}
	return e.Hidden, nil
}

// pickExpression returns the expression usable by DuckDB: the portable Ossie
// dialect or ANSI SQL.
func pickExpression(e Expression) (string, error) {
	for _, want := range []string{"OSSIE_SQL_2026", "ANSI_SQL"} {
		for _, d := range e.Dialects {
			if d.Dialect == want {
				if strings.TrimSpace(d.Expression) == "" {
					return "", errors.New("empty expression")
				}
				return d.Expression, nil
			}
		}
	}
	var have []string
	for _, d := range e.Dialects {
		have = append(have, d.Dialect)
	}
	return "", fmt.Errorf("no ANSI_SQL or OSSIE_SQL_2026 expression (has %s)", strings.Join(have, ", "))
}

// parseSource turns a dataset source into the table name of its role view.
func parseSource(src string) (string, error) {
	parts := strings.Split(strings.TrimSpace(src), ".")
	if len(parts) > 3 {
		return "", fmt.Errorf("source %q: use schema.table", src)
	}
	for i, p := range parts {
		p = strings.Trim(p, `"`)
		if !identRe.MatchString(p) {
			return "", fmt.Errorf("source %q must be a table reference like gold.ventas (queries are not supported)", src)
		}
		parts[i] = p
	}
	return strings.ToLower(parts[len(parts)-1]), nil
}

func (m *Model) resolve() error {
	m.dsIdx = map[string]*Dataset{}
	m.metIdx = map[string]*Metric{}
	for _, d := range m.Datasets {
		if !identRe.MatchString(d.Name) {
			return fmt.Errorf("dataset %q: the name must be an identifier (letters, digits, _)", d.Name)
		}
		k := strings.ToLower(d.Name)
		if prev, dup := m.dsIdx[k]; dup {
			return fmt.Errorf("dataset %q is defined twice (%s, %s)", d.Name, prev.file, d.file)
		}
		m.dsIdx[k] = d
		var err error
		if d.Table, err = parseSource(d.Source); err != nil {
			return fmt.Errorf("dataset %s: %w", d.Name, err)
		}
		if d.Hidden, err = hiddenOf("dataset "+d.Name, d.CustomExtensions); err != nil {
			return err
		}
		d.fieldIdx = map[string]*Field{}
		for _, f := range d.Fields {
			if !identRe.MatchString(f.Name) {
				return fmt.Errorf("field %s.%s: the name must be an identifier (letters, digits, _)", d.Name, f.Name)
			}
			fk := strings.ToLower(f.Name)
			if _, dup := d.fieldIdx[fk]; dup {
				return fmt.Errorf("field %s.%s is defined twice", d.Name, f.Name)
			}
			d.fieldIdx[fk] = f
			if f.SQL, err = pickExpression(f.Expression); err != nil {
				return fmt.Errorf("field %s.%s: %w", d.Name, f.Name, err)
			}
			if _, err := tokenize(f.SQL); err != nil {
				return fmt.Errorf("field %s.%s: %w", d.Name, f.Name, err)
			}
			if f.Hidden, err = hiddenOf("field "+d.Name+"."+f.Name, f.CustomExtensions); err != nil {
				return err
			}
		}
	}
	if err := m.resolveRelationships(); err != nil {
		return err
	}
	for _, mt := range m.Metrics {
		if !identRe.MatchString(mt.Name) {
			return fmt.Errorf("metric %q: the name must be an identifier (letters, digits, _)", mt.Name)
		}
		k := strings.ToLower(mt.Name)
		if prev, dup := m.metIdx[k]; dup {
			return fmt.Errorf("metric %q is defined twice (%s, %s)", mt.Name, prev.file, mt.file)
		}
		m.metIdx[k] = mt
		var err error
		if mt.Hidden, err = hiddenOf("metric "+mt.Name, mt.CustomExtensions); err != nil {
			return err
		}
	}
	for _, mt := range m.Metrics {
		if err := m.resolveMetric(mt); err != nil {
			return fmt.Errorf("metric %s: %w", mt.Name, err)
		}
	}
	return nil
}

func (m *Model) resolveRelationships() error {
	seen := map[string]bool{}
	for _, r := range m.Relationships {
		k := strings.ToLower(r.Name)
		if seen[k] {
			return fmt.Errorf("relationship %q is defined twice", r.Name)
		}
		seen[k] = true
		var ok bool
		if r.from, ok = m.dsIdx[strings.ToLower(r.From)]; !ok {
			return fmt.Errorf("relationship %s: unknown dataset %q in from", r.Name, r.From)
		}
		if r.to, ok = m.dsIdx[strings.ToLower(r.To)]; !ok {
			return fmt.Errorf("relationship %s: unknown dataset %q in to", r.Name, r.To)
		}
		if r.from == r.to {
			return fmt.Errorf("relationship %s: self relationships are not supported", r.Name)
		}
		if len(r.FromColumns) != len(r.ToColumns) {
			return fmt.Errorf("relationship %s: from_columns and to_columns differ in length", r.Name)
		}
		for _, c := range r.FromColumns {
			r.from.addKeyColumn(c)
		}
		for _, c := range r.ToColumns {
			r.to.addKeyColumn(c)
		}
		// The "one" side must be unique on the join columns, otherwise the
		// join multiplies rows: when the dataset declares its keys, check them.
		if keys := r.to.keySets(); len(keys) > 0 && !coversAKey(r.ToColumns, keys) {
			return fmt.Errorf("relationship %s: to_columns (%s) are not a primary or unique key of %s, the join would duplicate rows",
				r.Name, strings.Join(r.ToColumns, ", "), r.to.Name)
		}
	}
	return nil
}

func (d *Dataset) addKeyColumn(c string) {
	if !identRe.MatchString(c) {
		return
	}
	k := strings.ToLower(c)
	if _, ok := d.fieldIdx[k]; ok {
		return
	}
	for _, e := range d.extra {
		if strings.EqualFold(e, c) {
			return
		}
	}
	d.extra = append(d.extra, c)
}

func (d *Dataset) keySets() [][]string {
	var ks [][]string
	if len(d.PrimaryKey) > 0 {
		ks = append(ks, d.PrimaryKey)
	}
	return append(ks, d.UniqueKeys...)
}

func coversAKey(cols []string, keys [][]string) bool {
	have := map[string]bool{}
	for _, c := range cols {
		have[strings.ToLower(c)] = true
	}
	for _, k := range keys {
		all := len(k) > 0
		for _, c := range k {
			if !have[strings.ToLower(c)] {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// resolveMetric parses the expression of mt, binds dataset.field references,
// and inlines references to other metrics (bare names).
func (m *Model) resolveMetric(mt *Metric) error {
	if mt.resolved {
		return nil
	}
	if mt.visiting {
		return errors.New("metrics reference each other in a cycle")
	}
	mt.visiting = true
	defer func() { mt.visiting = false }()
	src, err := pickExpression(mt.Expression)
	if err != nil {
		return err
	}
	p, err := parseExpr(src)
	if err != nil {
		return err
	}
	hasAgg := p.hasAgg
	var segs []seg
	var home *Dataset
	bind := func(ds *Dataset) error {
		if home != nil && home != ds {
			return fmt.Errorf("it spans datasets %s and %s: a metric must be computed on a single dataset (define it on the fact and bring the other side in with a relationship)", home.Name, ds.Name)
		}
		home = ds
		return nil
	}
	for _, s := range p.segs {
		switch s.kind {
		case segRaw:
			segs = append(segs, s)
		case segRef:
			ds, ok := m.dsIdx[strings.ToLower(s.ds)]
			if !ok {
				return fmt.Errorf("unknown dataset %q in %s.%s", s.ds, s.ds, s.field)
			}
			f, ok := ds.fieldIdx[strings.ToLower(s.field)]
			if !ok {
				return fmt.Errorf("dataset %s has no field %q", ds.Name, s.field)
			}
			if err := bind(ds); err != nil {
				return err
			}
			mt.refs = append(mt.refs, fieldRef{ds, f})
			segs = append(segs, seg{kind: segRef, ds: ds.Name, field: f.Name})
		case segBare:
			other, ok := m.metIdx[strings.ToLower(s.text)]
			if !ok {
				return fmt.Errorf("%q is neither a function, a keyword nor a metric (qualify fields as dataset.field)", s.text)
			}
			if err := m.resolveMetric(other); err != nil {
				return err
			}
			if err := bind(other.home); err != nil {
				return err
			}
			hasAgg = hasAgg || other.hasAggregate()
			segs = append(segs, seg{kind: segRaw, text: "("})
			segs = append(segs, other.segs...)
			segs = append(segs, seg{kind: segRaw, text: ")"})
			mt.refs = append(mt.refs, other.refs...)
		}
	}
	if home == nil {
		return errors.New("it references no dataset field (write e.g. COUNT(orders.id) instead of COUNT(*))")
	}
	if !hasAgg {
		return errors.New("it has no aggregate function (SUM, COUNT, AVG...)")
	}
	mt.segs, mt.home, mt.resolved = segs, home, true
	return nil
}

func (mt *Metric) hasAggregate() bool { return mt.resolved }

// Dataset returns a dataset by name (case-insensitive).
func (m *Model) Dataset(name string) (*Dataset, bool) {
	d, ok := m.dsIdx[strings.ToLower(name)]
	return d, ok
}

// Metric returns a metric by name (case-insensitive).
func (m *Model) Metric(name string) (*Metric, bool) {
	x, ok := m.metIdx[strings.ToLower(name)]
	return x, ok
}

// Field returns a field by name (case-insensitive).
func (d *Dataset) Field(name string) (*Field, bool) {
	f, ok := d.fieldIdx[strings.ToLower(name)]
	return f, ok
}

// Tables returns the lake tables the model reads (view names of the role schema).
func (m *Model) Tables() []string {
	set := map[string]bool{}
	for _, d := range m.Datasets {
		set[d.Table] = true
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
