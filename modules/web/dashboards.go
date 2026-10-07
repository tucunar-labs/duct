package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/starfederation/datastar-go/datastar"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/dashboards"
)

const (
	maxChartRows    = dashboards.MaxLimit // rows of one chart that travel to the browser
	chartConcurrent = 4                   // chart queries of one dashboard in flight
	cacheEntries    = 256
	cacheBytes      = 32 << 20
)

// chartData is the cached payload of one semantic query: columns and rows
// already encoded as JSON for the island.
type chartData struct {
	Columns []specColumn
	Rows    json.RawMessage
	N       int // number of rows (for the audit record of a cache hit)
}

func (d *chartData) size() int { return len(d.Rows) + 64*len(d.Columns) + 64 }

type specColumn struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// chartSpec is the data-spec attribute of a <duct-chart> (see tools/bundle/web/charts.js).
type chartSpec struct {
	Type     string             `json:"type"`
	Columns  []specColumn       `json:"columns,omitempty"`
	Rows     json.RawMessage    `json:"rows,omitempty"`
	Options  dashboards.Options `json:"options"`
	Selected []string           `json:"selected,omitempty"`
	Emit     bool               `json:"emit,omitempty"`
	Error    string             `json:"error,omitempty"`
}

var errNoSemantic = errors.New("este servidor no tiene capa semántica (semantic/*.ossie.yaml): los dashboards no pueden consultar datos")

func (a *app) semantic() (contract.SemanticQuerier, error) {
	if sq, ok := a.q.(contract.SemanticQuerier); ok {
		return sq, nil
	}
	return nil, errNoSemantic
}

// cacheKey: the latest DuckLake snapshot (data, views and policies all bump
// it), the role (row filters and masks are per role) and the request.
func cacheKey(snapshot, role string, req contract.SemanticRequest) string {
	raw, _ := json.Marshal(req)
	h := sha256.New()
	h.Write([]byte(snapshot + "\x00" + role + "\x00"))
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

// semanticData runs req as who, through the cache when the snapshot is known.
// Errors are never cached.
func (a *app) semanticData(ctx context.Context, sq contract.SemanticQuerier, snapshot string, who contract.Identity, req contract.SemanticRequest) (*chartData, error) {
	key := ""
	if snapshot != "" {
		t0 := time.Now()
		key = cacheKey(snapshot, who.Role, req)
		if d, ok := a.chartCache.get(key); ok {
			a.auditCacheHit(ctx, who, req, d, time.Since(t0), t0)
			return d, nil
		}
	}
	res, err := sq.SemanticQuery(ctx, who, req)
	if err != nil {
		return nil, err
	}
	defer res.Stream.Release()
	kinds := make([]string, len(res.Columns))
	data := &chartData{}
	for i, c := range res.Columns {
		kinds[i] = c.Kind
		data.Columns = append(data.Columns, specColumn{Name: c.Name, Kind: c.Kind})
	}
	rows := make([][]any, 0, 64)
read:
	for res.Stream.Next() {
		b := res.Stream.RecordBatch()
		for i := 0; i < int(b.NumRows()); i++ {
			if len(rows) >= maxChartRows {
				break read
			}
			row := make([]any, b.NumCols())
			for c := range row {
				kind := ""
				if c < len(kinds) {
					kind = kinds[c]
				}
				row[c] = jsonCell(b.Column(c), i, kind == "measure")
			}
			rows = append(rows, row)
		}
	}
	if err := res.Stream.Err(); err != nil {
		return nil, err
	}
	if data.Rows, err = json.Marshal(rows); err != nil {
		return nil, err
	}
	data.N = len(rows)
	if key != "" {
		a.chartCache.put(key, data)
	}
	return data, nil
}

// auditCacheHit records a dashboard answer served from the cache: status ok,
// cached=true, duration of the lookup. The audit log coalesces identical hits
// (same user, role and request) inside a window, so a busy dashboard writes
// one row per widget and viewer per minute with hits = how many.
func (a *app) auditCacheHit(ctx context.Context, who contract.Identity, req contract.SemanticRequest, d *chartData, took time.Duration, at time.Time) {
	au, ok := a.q.(contract.Auditor)
	if !ok {
		return
	}
	raw, _ := json.Marshal(req)
	au.Audit(ctx, contract.AuditRecord{At: at.UTC(), User: who.User, Role: who.Role, SQL: "-- semantic query (cache hit) " + string(raw),
		Duration: took, Rows: int64(d.N), Cached: true, Hits: 1})
}

// jsonCell converts one Arrow value to something encoding/json writes the way
// the island wants: numbers as numbers, everything else as text.
func jsonCell(col arrow.Array, i int, measure bool) any {
	if col.IsNull(i) {
		return nil
	}
	switch c := col.(type) {
	case *array.Int8:
		return int64(c.Value(i))
	case *array.Int16:
		return int64(c.Value(i))
	case *array.Int32:
		return int64(c.Value(i))
	case *array.Int64:
		return c.Value(i)
	case *array.Uint8:
		return uint64(c.Value(i))
	case *array.Uint16:
		return uint64(c.Value(i))
	case *array.Uint32:
		return uint64(c.Value(i))
	case *array.Uint64:
		return c.Value(i)
	case *array.Float32:
		return finite(float64(c.Value(i)))
	case *array.Float64:
		return finite(c.Value(i))
	case *array.Boolean:
		return c.Value(i)
	}
	// ValueStr may alias the Arrow buffer: copy.
	s := strings.Clone(clip(col.ValueStr(i)))
	if measure { // decimals, HUGEINT sums...
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return finite(f)
		}
	}
	return s
}

func finite(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}
	return f
}

// ---- state ----

// crossEvent is what the island reports: the chart and the values clicked or
// brushed on its first dimension. Empty values clear that chart's selection.
type crossEvent struct {
	Chart  string   `json:"chart"`
	Values []string `json:"values"`
}

// readState rebuilds the dashboard state from the signals the browser sent
// and applies the pending cross-filter event, if any. Nothing from the
// browser names a field: filters and charts are looked up in the YAML.
func readState(d *dashboards.Dashboard, sig map[string]any) dashboards.State {
	st := dashboards.State{Filters: map[string][]string{}, Cross: map[string][]string{}}
	for _, f := range d.Filters {
		key := "f_" + f.ID
		switch f.Type {
		case dashboards.FilterDateRange:
			st.Filters[f.ID] = []string{sigString(sig[key+"_from"]), sigString(sig[key+"_to"])}
		case dashboards.FilterMultiselect:
			st.Filters[f.ID] = sigStrings(sig[key])
		default:
			st.Filters[f.ID] = []string{sigString(sig[key])}
		}
	}
	if s, ok := sig["cross"].(string); ok && s != "" {
		var m map[string][]string
		if json.Unmarshal([]byte(s), &m) == nil {
			for id, vals := range m {
				if c, ok := d.Chart(id); ok && c.Emits() && len(vals) > 0 && len(vals) <= 100 {
					st.Cross[id] = vals
				}
			}
		}
	}
	if s, ok := sig["ev"].(string); ok && s != "" {
		var ev crossEvent
		if json.Unmarshal([]byte(s), &ev) == nil {
			if c, ok := d.Chart(ev.Chart); ok && c.Emits() {
				if len(ev.Values) == 0 || len(ev.Values) > 100 || slices.Equal(st.Cross[ev.Chart], ev.Values) {
					delete(st.Cross, ev.Chart) // clearing, or clicking the same selection again
				} else {
					st.Cross[ev.Chart] = ev.Values
				}
			}
		}
	}
	return st
}

func sigString(v any) string {
	s, _ := v.(string)
	return s
}

func sigStrings(v any) []string {
	switch x := v.(type) {
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if x != "" {
			return []string{x}
		}
	}
	return nil
}

// ---- handlers ----

func (a *app) dashboardLinks() ([]dashLink, error) {
	if a.cfg.dashboardsDir == "" {
		return nil, nil
	}
	ok, bad, err := dashboards.Load(a.cfg.dashboardsDir)
	if err != nil {
		return nil, err
	}
	var out []dashLink
	for _, d := range ok {
		out = append(out, dashLink{Name: d.Name, Title: d.Title, Description: d.Description, Charts: len(d.Charts)})
	}
	for name, e := range bad {
		out = append(out, dashLink{Name: name, Err: e.Error()})
	}
	slices.SortFunc(out, func(x, y dashLink) int { return strings.Compare(x.Name, y.Name) })
	return out, nil
}

func (a *app) dashList(w http.ResponseWriter, r *http.Request, sess session) {
	links, err := a.dashboardLinks()
	msg, status := "", http.StatusOK
	if err != nil {
		slog.Warn("web: dashboards", "err", err)
		msg, status = "No se pudieron leer los dashboards: "+err.Error(), http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = dashboardsPage(a.page(sess, "Dashboards", "dashboards"), links, msg).Render(r.Context(), w)
}

func (a *app) loadDashboard(name string) (*dashboards.Dashboard, int, error) {
	if a.cfg.dashboardsDir == "" {
		return nil, http.StatusNotFound, os.ErrNotExist
	}
	d, err := dashboards.LoadOne(a.cfg.dashboardsDir, name)
	switch {
	case err == nil:
		return d, 0, nil
	case errors.Is(err, os.ErrNotExist):
		return nil, http.StatusNotFound, errors.New("dashboard no encontrado")
	default:
		return nil, http.StatusUnprocessableEntity, err
	}
}

func (a *app) dashView(w http.ResponseWriter, r *http.Request, sess session) {
	d, status, err := a.loadDashboard(r.PathValue("name"))
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_ = dashboardsPage(a.page(sess, "Dashboards", "dashboards"), nil, err.Error()).Render(r.Context(), w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	filters := a.filterViews(ctx, d, sess.who)
	sig := dashSignals{"_csrf": a.store.csrf(sess), "cross": "{}", "ev": ""}
	for _, f := range d.Filters {
		switch f.Type {
		case dashboards.FilterDateRange:
			sig["f_"+f.ID+"_from"], sig["f_"+f.ID+"_to"] = "", ""
		case dashboards.FilterMultiselect:
			sig["f_"+f.ID] = []string{}
		default:
			sig["f_"+f.ID] = ""
		}
	}
	p := a.page(sess, d.Title, "dashboards")
	p.Crumbs = []crumb{{Label: "Dashboards", Href: "/dashboards"}, {Label: d.Title}}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = dashboardPage(p, d, filters, sig, "/dashboards/"+d.Name+"/data").Render(r.Context(), w)
}

// filterViews lists the values of every picker (cached, in parallel).
func (a *app) filterViews(ctx context.Context, d *dashboards.Dashboard, who contract.Identity) []filterView {
	out := make([]filterView, len(d.Filters))
	sq, sqErr := a.semantic()
	snap := ""
	if sqErr == nil {
		snap, _ = sq.LakeSnapshot(ctx)
	}
	var wg sync.WaitGroup
	for i := range d.Filters {
		f := d.Filters[i]
		out[i].F = f
		if f.Type == dashboards.FilterDateRange {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sqErr != nil {
				out[i].Err = sqErr.Error()
				return
			}
			req, err := d.ValuesRequest(&f)
			var data *chartData
			if err == nil {
				data, err = a.semanticData(ctx, sq, snap, who, req)
			}
			if err != nil {
				out[i].Err = err.Error()
				return
			}
			out[i].Options = firstColumn(data.Rows)
		}()
	}
	wg.Wait()
	return out
}

func firstColumn(rows json.RawMessage) []string {
	dec := json.NewDecoder(bytes.NewReader(rows))
	dec.UseNumber()
	var all [][]any
	if dec.Decode(&all) != nil {
		return nil
	}
	var out []string
	for _, r := range all {
		if len(r) == 0 || r[0] == nil {
			continue
		}
		switch v := r[0].(type) {
		case string:
			out = append(out, v)
		case json.Number:
			out = append(out, v.String())
		case bool:
			out = append(out, strconv.FormatBool(v))
		}
	}
	return out
}

// dashData answers the dashboard's one SSE endpoint: it applies the state
// the browser sent (pickers, cross-filter event), reports the new
// cross-filters and streams each chart as soon as its query ends.
func (a *app) dashData(w http.ResponseWriter, r *http.Request, sess session) {
	d, status, err := a.loadDashboard(r.PathValue("name"))
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	sig := map[string]any{}
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)
	if err := datastar.ReadSignals(r, &sig); err != nil {
		http.Error(w, "bad signals", http.StatusBadRequest)
		return
	}
	st := readState(d, sig)
	sse := datastar.NewSSE(w, r)
	crossJSON, _ := json.Marshal(st.Cross)
	var mu sync.Mutex // one writer on the SSE stream
	_ = sse.MarshalAndPatchSignals(map[string]any{"cross": string(crossJSON), "ev": ""})
	_ = sse.PatchElementTempl(dashChips(d, st.Cross))

	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	sq, sqErr := a.semantic()
	snap := ""
	if sqErr == nil {
		snap, _ = sq.LakeSnapshot(ctx) // "": no cache this time
	}
	sem := make(chan struct{}, chartConcurrent)
	var wg sync.WaitGroup
	for i := range d.Charts {
		c := &d.Charts[i]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			spec := chartSpec{Type: c.Type, Options: c.Options, Selected: st.Cross[c.ID], Emit: c.Emits()}
			var data *chartData
			var err error
			if sqErr != nil {
				err = sqErr
			} else {
				var req contract.SemanticRequest
				if req, err = d.Request(c, st); err == nil {
					data, err = a.semanticData(ctx, sq, snap, sess.who, req)
				}
			}
			if err != nil {
				slog.Info("web: chart failed", "dashboard", d.Name, "chart", c.ID, "user", sess.who.User, "err", err)
				spec.Error = err.Error()
			} else {
				spec.Columns, spec.Rows = data.Columns, data.Rows
			}
			raw, _ := json.Marshal(spec)
			mu.Lock()
			defer mu.Unlock()
			_ = sse.PatchElementTempl(chartEl(c.ID, string(raw)))
		}()
	}
	wg.Wait()
}
