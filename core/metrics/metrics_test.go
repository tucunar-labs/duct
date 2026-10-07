package metrics

import (
	"strings"
	"testing"
)

func TestRegistryText(t *testing.T) {
	r := New()
	c := r.Counter("duct_x_total", "Things.", "kind")
	c.Inc("a")
	c.Add(2, "a")
	c.Inc(`b"q`)
	g := r.Gauge("duct_g", "A gauge.")
	g.Set(3.5)
	h := r.Histogram("duct_h_seconds", "Latency.", []float64{0.1, 1}, "proto")
	h.Observe(0.05, "pg")
	h.Observe(0.5, "pg")
	h.Observe(5, "pg")
	r.AddCollector(func(w *Writer) { w.Gauge("duct_dyn", "Dynamic.", 7, "k", "v") })

	var b strings.Builder
	r.WriteText(&b)
	out := b.String()
	for _, want := range []string{
		"# TYPE duct_x_total counter",
		`duct_x_total{kind="a"} 3`,
		`duct_x_total{kind="b\"q"} 1`,
		"duct_g 3.5",
		`duct_h_seconds_bucket{proto="pg",le="0.1"} 1`,
		`duct_h_seconds_bucket{proto="pg",le="1"} 2`,
		`duct_h_seconds_bucket{proto="pg",le="+Inf"} 3`,
		`duct_h_seconds_count{proto="pg"} 3`,
		`duct_dyn{k="v"} 7`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestSameNameSameMetric(t *testing.T) {
	r := New()
	a := r.Counter("duct_y_total", "y", "l")
	b := r.Counter("duct_y_total", "y", "l")
	a.Inc("x")
	if b.Value("x") != 1 {
		t.Fatal("re-registering must return the same metric")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("different labels must panic")
		}
	}()
	r.Counter("duct_y_total", "y", "other")
}
