package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/alerts"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/stream"
)

// hook collects what a webhook receives.
type hook struct {
	mu   sync.Mutex
	got  []map[string]any
	at   []time.Time
	fail bool
}

func (h *hook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var m map[string]any
	_ = json.NewDecoder(r.Body).Decode(&m)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	h.got, h.at = append(h.got, m), append(h.at, time.Now())
}

// find waits for a delivery of state for rule/key and returns when it arrived.
func (h *hook) find(t *testing.T, rule, key, state string, within time.Duration) time.Time {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		for i, m := range h.got {
			if m["rule"] == rule && m["key"] == key && m["state"] == state {
				at := h.at[i]
				h.mu.Unlock()
				return at
			}
		}
		h.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %s %s/%s within %s; got %v", state, rule, key, within, h.snapshot())
	return time.Time{}
}

func (h *hook) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, m := range h.got {
		out = append(out, fmt.Sprintf("%v:%v/%v", m["state"], m["rule"], m["key"]))
	}
	return out
}

func (h *hook) count(rule, key, state string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.got {
		if m["rule"] == rule && m["key"] == key && m["state"] == state {
			n++
		}
	}
	return n
}

const hotSQL = `-- EVERY 100ms
-- KEY sensor_id
-- SEVERITY critical
-- CHANNELS webhook:ops
-- MESSAGE {sensor_id} marca {temp} C
SELECT sensor_id, temp FROM (
  SELECT *, row_number() OVER (PARTITION BY sensor_id ORDER BY _kafka_ts DESC, _kafka_offset DESC) AS rn FROM sensors
) WHERE rn = 1 AND temp > 80`

const silentSQL = `-- EVERY 100ms
-- KEY sensor_id
-- CHANNELS webhook:ops
-- MESSAGE sensor {sensor_id} sin datos
SELECT sensor_id FROM sensors GROUP BY sensor_id HAVING max(_kafka_ts) < now() - INTERVAL 1 SECOND`

type feeder struct {
	f   *fakeConsumer
	off int64
}

func (fd *feeder) send(sensor string, temp float64) {
	fd.f.polls <- []stream.Record{{Topic: "planta.sensores", Offset: fd.off, Timestamp: time.Now(), Key: []byte(sensor),
		Value: []byte(fmt.Sprintf(`{"sensor_id":%q,"temp":%v}`, sensor, temp))}}
	fd.off++
}

// startAlerts runs a Runner over a fake consumer until the returned stop is called.
func startAlerts(t *testing.T, root string, srvURL string) (fd *feeder, stop func()) {
	t.Helper()
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	rules, err := alerts.LoadRules(root)
	if err != nil {
		t.Fatal(err)
	}
	cfgs, err := stream.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := alerts.OpenChannels(context.Background(), map[string]map[string]any{"ops": {"type": "webhook", "url": srvURL}})
	if err != nil {
		t.Fatal(err)
	}
	ch.Retry = []time.Duration{10 * time.Millisecond}
	lake, err := openEngine(context.Background(), p, false)
	if err != nil {
		t.Fatal(err)
	}
	window, closeWindow, err := openWindowEngine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f := newFake()
	r := &alerts.Runner{Lake: lake, Window: window, Consumer: f, Streams: cfgs, Rules: rules, Channels: ch, Log: slog.New(slog.NewTextHandler(testWriter{t}, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		err := r.Run(ctx)
		if err != nil {
			t.Log("RUN ERR", err)
		}
		done <- err
	}()
	return &feeder{f: f}, func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
		_ = ch.Close()
		closeWindow()
		_ = lake.Close()
	}
}

// TestAlertsRunner: a threshold rule and a silent-sensor rule over a fake
// consumer: firing within a second, dedup, resolution, record in Bronze,
// persisted state and no repeat notification after a restart.
func TestAlertsRunner(t *testing.T) {
	root := newProject(t)
	writeFile(t, root+"/streams/sensores.yaml", "name: sensors\ntopic: planta.sensores\nwindow: 1m\n")
	writeFile(t, root+"/alerts/hot.sql", hotSQL)
	writeFile(t, root+"/alerts/silent.sql", silentSQL)
	h := &hook{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	fd, stop := startAlerts(t, root, srv.URL)
	fd.send("s1", 20)
	fd.send("s2", 20)
	time.Sleep(400 * time.Millisecond) // warm-up and first evaluations: nothing fires
	if got := h.snapshot(); len(got) != 0 {
		t.Fatalf("nothing should fire yet: %v", got)
	}

	// Threshold: fires within a second of the reading.
	sent := time.Now()
	fd.send("s1", 95)
	at := h.find(t, "hot", "s1", "firing", time.Second)
	lat := at.Sub(sent)
	t.Logf("threshold alert latency: %s", lat.Round(time.Millisecond))
	if lat > time.Second {
		t.Errorf("latency %s", lat)
	}
	m := h.got[0]
	if m["message"] != "s1 marca 95 C" || m["labels"].(map[string]any)["severity"] != "critical" {
		t.Errorf("payload = %v", m)
	}

	// Dedup: still hot for a while, s2 reports normally; s1 keeps reporting 96.
	for range 6 {
		fd.send("s1", 96)
		fd.send("s2", 21)
		time.Sleep(100 * time.Millisecond)
	}
	if n := h.count("hot", "s1", "firing"); n != 1 {
		t.Errorf("firing notified %d times, want 1 (dedup)", n)
	}

	// Back to normal: resolved.
	sent = time.Now()
	fd.send("s1", 22)
	at = h.find(t, "hot", "s1", "resolved", time.Second)
	t.Logf("resolve latency: %s", at.Sub(sent).Round(time.Millisecond))

	// Silent sensor: s2 stops, s1 keeps talking.
	silentSince := time.Now()
	for time.Since(silentSince) < 1500*time.Millisecond {
		fd.send("s1", 22)
		time.Sleep(100 * time.Millisecond)
	}
	at = h.find(t, "silent", "s2", "firing", 2*time.Second)
	t.Logf("silent sensor fired %s after its last reading", at.Sub(silentSince).Round(time.Millisecond))
	if h.count("silent", "s1", "firing") != 0 {
		t.Error("s1 is talking: must not be silent")
	}
	fd.send("s2", 21)
	h.find(t, "silent", "s2", "resolved", time.Second)

	// Leave s1 hot again and restart: the persisted firing alert is not repeated.
	fd.send("s1", 99)
	for i := 0; i < 100 && h.count("hot", "s1", "firing") < 2; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if n := h.count("hot", "s1", "firing"); n != 2 {
		t.Fatalf("hot/s1 firing count = %d, want 2", n)
	}
	time.Sleep(300 * time.Millisecond) // let the record reach Bronze
	stop()

	fd, stop = startAlerts(t, root, srv.URL)
	for range 6 { // replayed history and live readings: s1 still hot, s2 fine
		fd.send("s1", 99)
		fd.send("s2", 21)
		time.Sleep(200 * time.Millisecond)
	}
	stop()
	if n := h.count("hot", "s1", "firing"); n != 2 {
		t.Errorf("after restart hot/s1 notified again: %d firing notifications", n)
	}

	got := queryCSV(t, root, "SELECT rule, alert_key, state, severity, delivered FROM bronze.duct_alerts ORDER BY occurred_at")
	for _, want := range []string{"hot,s1,firing,critical,true", "hot,s1,resolved,critical,true", "silent,s2,firing,warning,true", "silent,s2,resolved,warning,true"} {
		if !strings.Contains(got, want) {
			t.Errorf("bronze.duct_alerts lacks %q:\n%s", want, got)
		}
	}
	if got := queryCSV(t, root, "SELECT rule, alert_key FROM duct.duct_alert_state"); got != "rule,alert_key\nhot,s1" {
		t.Errorf("state table: %q", got)
	}
}

// TestAlertsDeliveryFailureIsRecorded: a channel that keeps failing is retried
// and the transition is still recorded, with delivered = false and no URL.
func TestAlertsDeliveryFailureIsRecorded(t *testing.T) {
	root := newProject(t)
	writeFile(t, root+"/streams/sensores.yaml", "name: sensors\ntopic: planta.sensores\nwindow: 1m\n")
	writeFile(t, root+"/alerts/hot.sql", hotSQL)
	h := &hook{fail: true}
	srv := httptest.NewServer(h)
	defer srv.Close()
	fd, stop := startAlerts(t, root, srv.URL)
	fd.send("s1", 95)
	deadline := time.Now().Add(20 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		// The table appears with the first record; until then the query fails.
		out, _, code := duct(t, "query", "--project", root, "--format", "csv", "SELECT state, delivered, channels FROM bronze.duct_alerts")
		if got = out; code == 0 && strings.Contains(got, "firing,false") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	if !strings.Contains(got, "firing,false") || strings.Contains(got, "127.0.0.1") || !strings.Contains(got, "after 2 tries") {
		t.Fatalf("record = %q", got)
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(b)))
	return len(b), nil
}
