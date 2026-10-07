package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alonsoburon/duct/core/admin"
	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/metrics"
	"github.com/alonsoburon/duct/core/project"
)

// mounter is implemented by the HTTP protocol (modules/serve-http).
type mounter interface {
	Mount(pattern string, h http.Handler)
}

// mountOperability gives the HTTP protocol the operability contract of ADR
// 0013: /admin/v1 (admin API), /readyz (readiness) and the Prometheus
// collectors that read the Duct tables. /healthz and /metrics are the module's.
func mountOperability(p *project.Project, eng contract.Engine, h *governance.Handler, auth contract.AuthProvider, protocols map[string]contract.Protocol) {
	m, ok := protocols["http"].(mounter)
	if !ok {
		return
	}
	api := admin.New(admin.Config{
		Auth: auth, Handler: h, Engine: eng, Project: p, Version: version,
		Components: func(ctx context.Context) map[string]any { return componentsStatus(ctx, p, eng) },
	})
	m.Mount(admin.Prefix+"/", api)
	m.Mount("/readyz", &admin.Ready{Engine: eng, Project: p})
	registerDBMetrics(eng)
}

var (
	dbMetricsOnce sync.Once
	dbMetricsEng  atomic.Pointer[contract.Engine]
)

// registerDBMetrics adds the series that live in the shared state tables, so
// they count work done by any process (a cron `duct run`, another server):
// pipeline runs, job and task runs by status, alerts firing, the lake snapshot.
// One query round per 10 s at most.
func registerDBMetrics(eng contract.Engine) {
	dbMetricsEng.Store(&eng)
	dbMetricsOnce.Do(func() {
		var (
			mu    sync.Mutex
			at    time.Time
			cache func(*metrics.Writer)
		)
		metrics.Default().AddCollector(func(w *metrics.Writer) {
			mu.Lock()
			defer mu.Unlock()
			if cache == nil || time.Since(at) > 10*time.Second {
				cache, at = collectDBMetrics(*dbMetricsEng.Load()), time.Now()
			}
			cache(w)
		})
	})
}

func collectDBMetrics(eng contract.Engine) func(*metrics.Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	type sample struct {
		name, help, typ string
		labels          []string
		v               float64
	}
	var out []sample
	group := func(name, help, sql string, labelNames ...string) {
		rows, err := queryAll(ctx, eng, sql)
		if err != nil {
			out = append(out, sample{name: "duct_metrics_collect_errors", help: "Collector queries that failed (this scrape), by series.", typ: "gauge", labels: []string{"series", name}, v: 1})
			return
		}
		for _, r := range rows {
			n, _ := strconv.ParseFloat(r[len(r)-1], 64)
			var ls []string
			for i, ln := range labelNames {
				ls = append(ls, ln, r[i])
			}
			out = append(out, sample{name, help, "counter", ls, n})
		}
	}
	group("duct_pipeline_runs_total", "Pipeline target refreshes by status (success, failed, skipped...), from duct_pipeline_runs.",
		"SELECT status, count(*) FROM duct.duct_pipeline_runs GROUP BY status ORDER BY status", "status")
	group("duct_job_runs_total", "Job runs by status, from duct_jobs_runs.",
		"SELECT status, count(*) FROM duct.duct_jobs_runs GROUP BY status ORDER BY status", "status")
	group("duct_task_runs_total", "Task runs by type and status, from duct_task_runs.",
		"SELECT task_type, status, count(*) FROM duct.duct_task_runs GROUP BY task_type, status ORDER BY 1, 2", "type", "status")
	if rows, err := queryAll(ctx, eng, "SELECT rule, severity, count(*) FROM duct.duct_alert_state GROUP BY rule, severity ORDER BY 1, 2"); err == nil {
		for _, r := range rows {
			n, _ := strconv.ParseFloat(r[2], 64)
			out = append(out, sample{"duct_alerts_firing", "Alerts firing now, by rule and severity, from duct_alert_state.", "gauge", []string{"rule", r[0], "severity", r[1]}, n})
		}
	}
	if rows, err := queryAll(ctx, eng, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')"); err == nil && len(rows) > 0 {
		n, _ := strconv.ParseFloat(rows[0][0], 64)
		out = append(out, sample{"duct_lake_snapshot_id", "Latest DuckLake snapshot id.", "gauge", nil, n})
	}
	if rows, err := queryAll(ctx, eng, "SELECT max(version) FROM duct.duct_migrations"); err == nil && len(rows) > 0 {
		n, _ := strconv.ParseFloat(rows[0][0], 64)
		out = append(out, sample{"duct_migrations_version", "Highest applied Duct migration.", "gauge", nil, n})
	}
	out = append(out, sample{"duct_build_info", "Build information; the value is always 1.", "gauge", []string{"version", version}, 1})
	return func(w *metrics.Writer) {
		for _, s := range out {
			if s.typ == "counter" {
				w.Counter(s.name, s.help, s.v, s.labels...)
			} else {
				w.Gauge(s.name, s.help, s.v, s.labels...)
			}
		}
	}
}

func queryAll(ctx context.Context, eng contract.Engine, sql string) ([][]string, error) {
	st, err := eng.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				if col := rec.Column(c); !col.IsNull(i) {
					row[c] = strings.Clone(col.ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	return out, st.Err()
}

// componentsStatus is the jobs / stream / alerts part of /admin/v1/status.
func componentsStatus(ctx context.Context, p *project.Project, eng contract.Engine) map[string]any {
	out := map[string]any{}

	jobs := map[string]any{"configured": dirHasFiles(filepath.Join(p.Root, "jobs"))}
	if rows, err := queryAll(ctx, eng, "SELECT status, count(*) FROM duct.duct_jobs_runs WHERE started_at > now() - INTERVAL 24 HOUR GROUP BY status"); err == nil {
		by := map[string]int64{}
		for _, r := range rows {
			n, _ := strconv.ParseInt(r[1], 10, 64)
			by[r[0]] = n
		}
		jobs["runs_24h"] = by
	}
	health := "ok"
	if rows, err := queryAll(ctx, eng, "SELECT status FROM duct.duct_jobs_runs ORDER BY started_at DESC LIMIT 1"); err == nil && len(rows) > 0 {
		jobs["last_run_status"] = rows[0][0]
		if rows[0][0] == "failed" || rows[0][0] == "error" {
			health = "degraded"
		}
	}
	if jobs["configured"] == false {
		health = "disabled"
	}
	jobs["health"] = health
	out["jobs"] = jobs

	st := map[string]any{"configured": p.Config.Stream != nil}
	streams := []map[string]any{}
	metrics.Default().Gauge("duct_stream_lag_seconds", "", "stream").Each(func(l []string, v float64) {
		s := map[string]any{"name": l[0], "lag_seconds": v}
		metrics.Default().Gauge("duct_stream_last_commit_timestamp_seconds", "", "stream").Each(func(l2 []string, t float64) {
			if l2[0] == l[0] {
				s["last_commit"] = time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
			}
		})
		streams = append(streams, s)
	})
	st["streams"] = streams
	switch {
	case p.Config.Stream == nil:
		st["health"] = "disabled"
	case len(streams) == 0:
		st["health"] = "idle" // configured, nothing committed by this process yet
	default:
		st["health"] = "ok"
	}
	out["stream"] = st

	al := map[string]any{"configured": p.Config.Alerts != nil}
	firing := int64(0)
	if rows, err := queryAll(ctx, eng, "SELECT count(*) FROM duct.duct_alert_state"); err == nil && len(rows) > 0 {
		firing, _ = strconv.ParseInt(rows[0][0], 10, 64)
	}
	al["firing"] = firing
	switch {
	case p.Config.Alerts == nil:
		al["health"] = "disabled"
	case firing > 0:
		al["health"] = "firing"
	default:
		al["health"] = "ok"
	}
	out["alerts"] = al
	return out
}

func dirHasFiles(dir string) bool {
	ents, err := os.ReadDir(dir)
	return err == nil && len(ents) > 0
}
