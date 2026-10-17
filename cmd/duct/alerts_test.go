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
	return startAlertsAs(t, root, srvURL, "", 0)
}

// startAlertsAs is startAlerts for one of several instances (instance names it
// in the shared state; ttl is its lease, 0 = default).
func startAlertsAs(t *testing.T, root string, srvURL string, instance string, ttl time.Duration) (fd *feeder, stop func()) {
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
	r := &alerts.Runner{Lake: lake, Window: window, Consumer: f, Streams: cfgs, Rules: rules, Channels: ch, Log: slog.New(slog.NewTextHandler(testWriter{t}, nil)), Instance: instance, LeaseTTL: ttl}
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
func testAlertsRunner(t *testing.T) {
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
func testAlertsDeliveryFailureIsRecorded(t *testing.T) {
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

// quietRule fires when a sensor's last reading is over 80 (like hotSQL) with extra directives.
func ruleWith(directives string) string {
	return "-- EVERY 100ms\n-- KEY sensor_id\n-- CHANNELS webhook:ops\n-- MESSAGE {sensor_id} marca {temp} C\n" + directives +
		`SELECT sensor_id, temp FROM (
  SELECT *, row_number() OVER (PARTITION BY sensor_id ORDER BY _kafka_ts DESC, _kafka_offset DESC) AS rn FROM sensors
) WHERE rn = 1 AND temp > 80`
}

func alertsProject(t *testing.T, rule string) (root string, h *hook, url string) {
	t.Helper()
	root = newProject(t)
	writeFile(t, root+"/streams/sensores.yaml", "name: sensors\ntopic: planta.sensores\nwindow: 1m\n")
	writeFile(t, root+"/alerts/hot.sql", rule)
	h = &hook{}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return root, h, srv.URL
}

// TestAlertsHysteresisAndReminders: FOR holds the alert back until the
// condition lasted (a blip never fires), RESOLVE_FOR keeps it firing through a
// short dip, and REPEAT re-notifies while it stays firing.
func testAlertsHysteresisAndReminders(t *testing.T) {
	root, h, url := alertsProject(t, ruleWith("-- FOR 1s\n-- RESOLVE_FOR 1s\n-- REPEAT 1s\n"))
	fd, stop := startAlerts(t, root, url)
	defer stop()
	fd.send("s1", 20)
	time.Sleep(400 * time.Millisecond)

	// A blip shorter than FOR: nothing.
	fd.send("s1", 95)
	time.Sleep(400 * time.Millisecond)
	fd.send("s1", 20)
	time.Sleep(600 * time.Millisecond)
	if got := h.snapshot(); len(got) != 0 {
		t.Fatalf("a blip fired: %v", got)
	}

	// Sustained: fires only after FOR (~1s), not before.
	start := time.Now()
	fd.send("s1", 96)
	at := h.find(t, "hot", "s1", "firing", 4*time.Second)
	if d := at.Sub(start); d < 800*time.Millisecond {
		t.Errorf("fired after %s, FOR is 1s", d)
	}
	// Reminders while it stays firing (REPEAT 1s), marked as such.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && h.count("hot", "s1", "firing") < 3 {
		time.Sleep(100 * time.Millisecond)
	}
	if n := h.count("hot", "s1", "firing"); n < 3 {
		t.Fatalf("expected the alert and 2 reminders, got %d notifications", n)
	}
	h.mu.Lock()
	first, second := h.got[0], h.got[1]
	h.mu.Unlock()
	if first["labels"].(map[string]any)["reminder"] != nil || second["labels"].(map[string]any)["reminder"] != "true" || !strings.HasPrefix(second["message"].(string), "[reminder] ") {
		t.Errorf("first=%v second=%v", first, second)
	}
	// A dip shorter than RESOLVE_FOR does not resolve it.
	fd.send("s1", 20)
	time.Sleep(500 * time.Millisecond)
	fd.send("s1", 97)
	time.Sleep(700 * time.Millisecond)
	if n := h.count("hot", "s1", "resolved"); n != 0 {
		t.Fatalf("resolved during a short dip")
	}
	// A real recovery resolves after RESOLVE_FOR.
	rec := time.Now()
	fd.send("s1", 20)
	at = h.find(t, "hot", "s1", "resolved", 4*time.Second)
	if d := at.Sub(rec); d < 800*time.Millisecond {
		t.Errorf("resolved after %s, RESOLVE_FOR is 1s", d)
	}
	reminders := queryCSV(t, root, "SELECT count(*) FROM bronze.duct_alerts WHERE state = 'reminder'")
	if strings.TrimSpace(strings.TrimPrefix(reminders, "count_star()\n")) == "0" {
		t.Errorf("reminders not recorded in Bronze: %q", reminders)
	}
}

// TestAlertsSilences: a silence mutes notifications (firing, reminders and
// resolved) of a rule or one key, is recorded in Bronze, shows in `status`
// and ends by itself or with `unsilence`; the state keeps tracking meanwhile.
func testAlertsSilences(t *testing.T) {
	root, h, url := alertsProject(t, ruleWith("-- REPEAT 1s\n"))
	if out, errs, code := duct(t, "alerts", "silence", "nope", "--for", "1h", "--reason", "x", "--project", root); code == 0 {
		t.Fatalf("silencing an unknown rule worked: %s %s", out, errs)
	}
	if _, _, code := duct(t, "alerts", "silence", "hot", "--project", root); code == 0 {
		t.Error("silence without --for/--reason accepted")
	}
	out := mustDuct(t, "alerts", "silence", "hot", "--key", "s1", "--for", "1h", "--reason", "mantención planta", "--project", root)
	if !strings.Contains(out, "silenced hot [s1]") {
		t.Fatalf("silence: %q", out)
	}
	fd, stop := startAlerts(t, root, url)
	defer stop()
	fd.send("s1", 20)
	fd.send("s2", 20)
	time.Sleep(400 * time.Millisecond)
	fd.send("s1", 95) // silenced
	fd.send("s2", 95) // not
	h.find(t, "hot", "s2", "firing", 3*time.Second)
	time.Sleep(2500 * time.Millisecond) // s1 stays firing for 2 reminder periods, silently
	if n := h.count("hot", "s1", "firing"); n != 0 {
		t.Errorf("silenced key notified %d times", n)
	}
	status := mustDuct(t, "alerts", "status", "--project", root)
	if !strings.Contains(status, "SILENCED") || !strings.Contains(status, "mantención planta") || !strings.Contains(status, "hot [s1]") {
		t.Errorf("status: %s", status)
	}
	if lst := mustDuct(t, "alerts", "silences", "--project", root); !strings.Contains(lst, "hot") || !strings.Contains(lst, "s1") {
		t.Errorf("silences: %s", lst)
	}
	// Resolved while silenced: nothing sent either, but recorded.
	fd.send("s1", 20)
	time.Sleep(600 * time.Millisecond)
	if n := h.count("hot", "s1", "resolved"); n != 0 {
		t.Errorf("silenced resolved was sent")
	}
	var rec string
	waitFor(t, "the silenced transitions in Bronze", 20*time.Second, func() bool {
		rec = queryCSV(t, root, "SELECT state, channels FROM bronze.duct_alerts WHERE alert_key = 's1' ORDER BY occurred_at")
		return strings.Contains(rec, "firing,silenced #") && strings.Contains(rec, "resolved,silenced #")
	})
	if !strings.Contains(rec, "firing,silenced #") || !strings.Contains(rec, "resolved,silenced #") {
		t.Errorf("silenced transitions not recorded:\n%s", rec)
	}
	// Lifting the silence: the next alert is delivered.
	id := strings.Fields(strings.TrimSpace(mustDuct(t, "alerts", "silences", "--project", root)))[0]
	if o := mustDuct(t, "alerts", "unsilence", id, "--project", root); !strings.Contains(o, "removed") {
		t.Errorf("unsilence: %q", o)
	}
	fd.send("s1", 96)
	h.find(t, "hot", "s1", "firing", 3*time.Second)
}

// TestAlertsActiveActive: two instances watching the same data (as two
// consumer groups would) notify each transition exactly once; when the
// instance that holds an alert dies, the other one takes it over and resolves it.
func testAlertsActiveActive(t *testing.T) {
	root, h, url := alertsProject(t, ruleWith("-- REPEAT 1s\n"))
	fa, stopA := startAlertsAs(t, root, url, "a", 3*time.Second)
	fb, stopB := startAlertsAs(t, root, url, "b", 3*time.Second)
	defer stopB()
	stoppedA := false
	defer func() {
		if !stoppedA {
			stopA()
		}
	}()
	both := func(sensor string, temp float64) { fa.send(sensor, temp); fb.send(sensor, temp) }
	both("s1", 20)
	time.Sleep(500 * time.Millisecond)
	for i := range 4 { // fire, stay hot (reminders every 1s), several keys
		both("s1", 95)
		both(fmt.Sprintf("k%d", i), 91)
		time.Sleep(100 * time.Millisecond)
	}
	h.find(t, "hot", "s1", "firing", 3*time.Second)
	for i := range 4 {
		h.find(t, "hot", fmt.Sprintf("k%d", i), "firing", 3*time.Second)
	}
	// ~2.5 s of reminders, both instances evaluating every 100 ms.
	for range 25 {
		both("s1", 95)
		time.Sleep(100 * time.Millisecond)
	}
	if n := h.count("hot", "s1", "firing"); n < 1 || n > 4 {
		t.Errorf("s1: %d notifications (1 firing + ~2 reminders expected, never doubled): %v", n, h.snapshot())
	}
	for i := range 4 {
		k := fmt.Sprintf("k%d", i)
		// the first notification is state firing and unlabelled; reminders carry the label
		h.mu.Lock()
		firsts := 0
		for _, m := range h.got {
			if m["rule"] == "hot" && m["key"] == k && m["state"] == "firing" && m["labels"].(map[string]any)["reminder"] == nil {
				firsts++
			}
		}
		h.mu.Unlock()
		if firsts != 1 {
			t.Errorf("%s: %d initial firing notifications", k, firsts)
		}
	}
	// Recovery seen by both: one resolved.
	both("s1", 20)
	h.find(t, "hot", "s1", "resolved", 5*time.Second)
	time.Sleep(1500 * time.Millisecond)
	if n := h.count("hot", "s1", "resolved"); n != 1 {
		t.Errorf("s1 resolved %d times", n)
	}

	// Failover: whoever holds k0 goes away; the survivor still tracks it and resolves it.
	stopA()
	stoppedA = true
	both("k0", 20) // only b is listening (fa has no consumer anymore)
	h.find(t, "hot", "k0", "resolved", 8*time.Second)
	time.Sleep(1500 * time.Millisecond)
	if n := h.count("hot", "k0", "resolved"); n != 1 {
		t.Errorf("k0 resolved %d times after a failover", n)
	}
	if t.Failed() {
		t.Logf("transitions:\n%s", queryCSV(t, root, "SELECT occurred_at, alert_key, state, channels FROM bronze.duct_alerts WHERE alert_key = 's1' ORDER BY occurred_at"))
	}
	rows := queryCSV(t, root, "SELECT alert_key, state, count(*) FROM bronze.duct_alerts WHERE state IN ('firing','resolved') GROUP BY ALL ORDER BY ALL")
	for _, line := range strings.Split(strings.TrimSpace(rows), "\n")[1:] {
		if !strings.HasSuffix(line, ",1") {
			t.Errorf("Bronze has a duplicated transition: %s\n%s", line, rows)
		}
	}
}
