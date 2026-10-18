// Package metrics is a tiny Prometheus registry: counters, gauges and
// histograms with labels, plus collectors that write series at scrape time. It
// renders the text exposition format (version 0.0.4) and has no dependencies,
// so any package of core or any module can count things without pulling in a
// client library (ADR 0013, operability contract).
//
// Metrics register on a Registry by name; asking for the same name twice
// returns the same metric (so tests and repeated Init calls are harmless) and
// asking with a different type or label set panics, as that is a programming
// error. Default is the registry served by `duct serve` at /metrics.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metrics and collectors.
type Registry struct {
	mu         sync.Mutex
	metrics    map[string]*family
	order      []string
	collectors []func(*Writer)
}

// New returns an empty Registry.
func New() *Registry { return &Registry{metrics: map[string]*family{}} }

var defaultRegistry = New()

// Default is the process-wide registry served at /metrics.
func Default() *Registry { return defaultRegistry }

type kind string

const (
	kindCounter   kind = "counter"
	kindGauge     kind = "gauge"
	kindHistogram kind = "histogram"
)

// DefBuckets are the default histogram buckets (seconds), from 1 ms to 60 s.
var DefBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

type family struct {
	name, help string
	kind       kind
	labels     []string
	buckets    []float64

	mu     sync.Mutex
	series map[string]*series
}

type series struct {
	values []string
	val    float64  // counter, gauge
	counts []uint64 // histogram: per bucket (non-cumulative)
	sum    float64  // histogram
	n      uint64   // histogram count
}

func (r *Registry) family(name, help string, k kind, labels []string, buckets []float64) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.metrics[name]; ok {
		if f.kind != k || strings.Join(f.labels, ",") != strings.Join(labels, ",") {
			panic("metrics: " + name + " registered again with a different type or labels")
		}
		return f
	}
	f := &family{name: name, help: help, kind: k, labels: labels, buckets: buckets, series: map[string]*series{}}
	r.metrics[name] = f
	r.order = append(r.order, name)
	return f
}

func (f *family) get(values []string) *series {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s takes %d label values, got %d", f.name, len(f.labels), len(values)))
	}
	key := strings.Join(values, "\xff")
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.series[key]
	if !ok {
		s = &series{values: append([]string(nil), values...)}
		if f.kind == kindHistogram {
			s.counts = make([]uint64, len(f.buckets))
		}
		f.series[key] = s
	}
	return s
}

// Counter is a monotonically increasing value family.
type Counter struct{ f *family }

// Gauge is a value family that goes up and down.
type Gauge struct{ f *family }

// Histogram observes values into buckets.
type Histogram struct{ f *family }

// Counter registers (or returns) a counter with the given label names.
func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	return &Counter{r.family(name, help, kindCounter, labels, nil)}
}

// Gauge registers (or returns) a gauge with the given label names.
func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r.family(name, help, kindGauge, labels, nil)}
}

// Histogram registers (or returns) a histogram; buckets nil means DefBuckets.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram {
	if buckets == nil {
		buckets = DefBuckets
	}
	return &Histogram{r.family(name, help, kindHistogram, labels, buckets)}
}

// Add adds v (>= 0) to the series with these label values.
func (c *Counter) Add(v float64, labelValues ...string) {
	if v < 0 {
		return
	}
	s := c.f.get(labelValues)
	c.f.mu.Lock()
	s.val += v
	c.f.mu.Unlock()
}

// Inc adds 1.
func (c *Counter) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Value returns the current value (for tests and status pages).
func (c *Counter) Value(labelValues ...string) float64 { return c.f.value(labelValues) }

// Set replaces the value.
func (g *Gauge) Set(v float64, labelValues ...string) {
	s := g.f.get(labelValues)
	g.f.mu.Lock()
	s.val = v
	g.f.mu.Unlock()
}

// Add adds v (may be negative).
func (g *Gauge) Add(v float64, labelValues ...string) {
	s := g.f.get(labelValues)
	g.f.mu.Lock()
	s.val += v
	g.f.mu.Unlock()
}

// Value returns the current value.
func (g *Gauge) Value(labelValues ...string) float64 { return g.f.value(labelValues) }

// Reset removes every series (a gauge rebuilt from scratch on each update).
func (g *Gauge) Reset() {
	g.f.mu.Lock()
	g.f.series = map[string]*series{}
	g.f.mu.Unlock()
}

func (f *family) value(values []string) float64 {
	s := f.get(values)
	f.mu.Lock()
	defer f.mu.Unlock()
	return s.val
}

// Observe records one value.
func (h *Histogram) Observe(v float64, labelValues ...string) {
	s := h.f.get(labelValues)
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	s.sum += v
	s.n++
	for i, b := range h.f.buckets {
		if v <= b {
			s.counts[i]++
			break
		}
	}
}

// Count returns how many values were observed.
func (h *Histogram) Count(labelValues ...string) uint64 {
	s := h.f.get(labelValues)
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	return s.n
}

// Writer lets a collector emit series at scrape time.
type Writer struct {
	w    io.Writer
	seen map[string]bool
}

// Gauge writes one gauge sample. labels is name, value pairs.
func (w *Writer) Gauge(name, help string, v float64, labels ...string) {
	w.sample(name, help, "gauge", v, labels)
}

// Counter writes one counter sample. labels is name, value pairs.
func (w *Writer) Counter(name, help string, v float64, labels ...string) {
	w.sample(name, help, "counter", v, labels)
}

func (w *Writer) sample(name, help, typ string, v float64, labels []string) {
	if !w.seen[name] {
		w.seen[name] = true
		_, _ = fmt.Fprintf(w.w, "# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(help), name, typ)
	}
	var names, vals []string
	for i := 0; i+1 < len(labels); i += 2 {
		names, vals = append(names, labels[i]), append(vals, labels[i+1])
	}
	_, _ = fmt.Fprintf(w.w, "%s%s %s\n", name, labelText(names, vals, "", ""), fmtFloat(v))
}

// AddCollector registers fn, called on every scrape. A collector must be fast
// and must not panic; cache anything that costs a query.
func (r *Registry) AddCollector(fn func(*Writer)) {
	r.mu.Lock()
	r.collectors = append(r.collectors, fn)
	r.mu.Unlock()
}

// WriteText renders the registry in the Prometheus text format.
func (r *Registry) WriteText(w io.Writer) {
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	fams := make([]*family, 0, len(names))
	for _, n := range names {
		fams = append(fams, r.metrics[n])
	}
	collectors := append([]func(*Writer){}, r.collectors...)
	r.mu.Unlock()
	for _, f := range fams {
		f.write(w)
	}
	cw := &Writer{w: w, seen: map[string]bool{}}
	for _, c := range collectors {
		c(cw)
	}
}

// WriteCollectors renders only the given collectors (a server's own series).
func WriteCollectors(w io.Writer, collectors []func(*Writer)) {
	cw := &Writer{w: w, seen: map[string]bool{}}
	for _, c := range collectors {
		c(cw)
	}
}

func (f *family) write(w io.Writer) {
	f.mu.Lock()
	rows := make([]series, 0, len(f.series))
	for _, s := range f.series {
		c := *s
		c.counts = append([]uint64(nil), s.counts...)
		rows = append(rows, c)
	}
	f.mu.Unlock()
	if len(rows) == 0 {
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		return strings.Join(rows[i].values, "\xff") < strings.Join(rows[j].values, "\xff")
	})
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, escapeHelp(f.help), f.name, f.kind)
	for _, s := range rows {
		if f.kind != kindHistogram {
			_, _ = fmt.Fprintf(w, "%s%s %s\n", f.name, labelText(f.labels, s.values, "", ""), fmtFloat(s.val))
			continue
		}
		var cum uint64
		for i, b := range f.buckets {
			cum += s.counts[i]
			_, _ = fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelText(f.labels, s.values, "le", fmtFloat(b)), cum)
		}
		_, _ = fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelText(f.labels, s.values, "le", "+Inf"), s.n)
		_, _ = fmt.Fprintf(w, "%s_sum%s %s\n", f.name, labelText(f.labels, s.values, "", ""), fmtFloat(s.sum))
		_, _ = fmt.Fprintf(w, "%s_count%s %d\n", f.name, labelText(f.labels, s.values, "", ""), s.n)
	}
}

func labelText(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	parts := make([]string, 0, len(names)+1)
	for i, n := range names {
		parts = append(parts, n+`="`+escapeLabel(values[i])+`"`)
	}
	if extraName != "" {
		parts = append(parts, extraName+`="`+extraValue+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func escapeHelp(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s)
}

func fmtFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Each calls fn for every series of the gauge (status pages).
func (g *Gauge) Each(fn func(labelValues []string, v float64)) {
	g.f.mu.Lock()
	rows := make([]series, 0, len(g.f.series))
	for _, s := range g.f.series {
		rows = append(rows, *s)
	}
	g.f.mu.Unlock()
	for _, s := range rows {
		fn(s.values, s.val)
	}
}
