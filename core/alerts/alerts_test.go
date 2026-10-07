package alerts

import (
	"strings"
	"sync"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

const hotRule = `-- Temperatura alta por sensor.
-- EVERY 200ms
-- KEY sensor_id
-- SEVERITY critical
-- CHANNELS ntfy:incendios, webhook:ops, pagerduty:main
-- MESSAGE {sensor_id} marca {temp} C ({rule})
SELECT sensor_id, max(temp) AS temp FROM sensors
WHERE _kafka_ts > now() - INTERVAL 30 SECOND GROUP BY 1 HAVING max(temp) > 80;
`

func TestParseRule(t *testing.T) {
	r, err := ParseRule("hot", hotRule)
	if err != nil {
		t.Fatal(err)
	}
	if r.Every != 200*time.Millisecond || r.Key != "sensor_id" || r.Severity != "critical" || r.Message != "{sensor_id} marca {temp} C ({rule})" {
		t.Fatalf("rule = %+v", r)
	}
	want := []ChannelRef{{"ntfy", "incendios"}, {"webhook", "ops"}, {"pagerduty", "main"}}
	if len(r.Channels) != 3 || r.Channels[0] != want[0] || r.Channels[1] != want[1] || r.Channels[2] != want[2] {
		t.Fatalf("channels = %v", r.Channels)
	}
	if strings.HasSuffix(r.SQL, ";") || !strings.Contains(r.SQL, "FROM sensors") {
		t.Fatalf("sql = %q", r.SQL)
	}
}

func TestParseRuleDefaults(t *testing.T) {
	r, err := ParseRule("silent", "SELECT 'x' AS key WHERE false")
	if err != nil {
		t.Fatal(err)
	}
	if r.Every != DefaultEvery || r.Severity != DefaultSeverity || r.Message != "{rule} {key}" || len(r.Channels) != 0 || r.Key != "" {
		t.Fatalf("rule = %+v", r)
	}
	// An ordinary comment that starts with a capital word is not a directive.
	if _, err := ParseRule("c", "-- Every sensor must report\nSELECT 1 AS key"); err != nil {
		t.Fatal(err)
	}
}

func TestParseRuleErrors(t *testing.T) {
	for name, src := range map[string]string{
		"every":    "-- EVERY soon\nSELECT 1",
		"too fast": "-- EVERY 1ms\nSELECT 1",
		"twice":    "-- EVERY 1s\n-- EVERY 2s\nSELECT 1",
		"severity": "-- SEVERITY meh\nSELECT 1",
		"channel":  "-- CHANNELS ntfy:Bad-Name\nSELECT 1",
		"key":      "-- KEY a;b\nSELECT 1",
		"empty":    "-- KEY\nSELECT 1",
		"not sel":  "-- EVERY 1s\nDROP TABLE sensors",
		"multi":    "SELECT 1; DELETE FROM sensors",
	} {
		if _, err := ParseRule("r", src); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := ParseRule("Bad Name", "SELECT 1"); err == nil {
		t.Error("bad rule name accepted")
	}
}

func TestRender(t *testing.T) {
	got := Render("{a} y {b} {missing} {", map[string]string{"a": "1", "b": "dos"})
	if got != "1 y dos {missing} {" {
		t.Fatalf("got %q", got)
	}
}

func hits(keys ...string) []Hit {
	var out []Hit
	for _, k := range keys {
		out = append(out, Hit{Key: k, Row: map[string]string{"sensor_id": k, "temp": "90"}})
	}
	return out
}

func states(ts []Transition) string {
	var s []string
	for _, t := range ts {
		s = append(s, string(t.State)+":"+t.Key)
	}
	return strings.Join(s, ",")
}

func TestTrackerFiringDedupResolve(t *testing.T) {
	r, _ := ParseRule("hot", hotRule)
	tr := NewTracker(r, nil)
	t0 := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)

	if got := states(tr.Apply(t0, nil)); got != "" {
		t.Fatalf("nothing firing: %q", got)
	}
	ts := tr.Apply(t0, hits("a", "b"))
	if got := states(ts); got != "firing:a,firing:b" {
		t.Fatalf("first: %q", got)
	}
	if ts[0].Message != "a marca 90 C (hot)" {
		t.Fatalf("message = %q", ts[0].Message)
	}
	// Same keys again: deduplicated, no notification, however often.
	for i := 1; i <= 5; i++ {
		if got := states(tr.Apply(t0.Add(time.Duration(i)*time.Second), hits("a", "b"))); got != "" {
			t.Fatalf("dedup: %q", got)
		}
	}
	// "a" recovers, "c" starts.
	ts = tr.Apply(t0.Add(10*time.Second), hits("b", "c"))
	if got := states(ts); got != "firing:c,resolved:a" {
		t.Fatalf("mixed: %q", got)
	}
	if res := ts[1]; !res.Since.Equal(t0) || res.Message != "a marca 90 C (hot)" {
		t.Fatalf("resolved keeps what it had: %+v", res)
	}
	// Fires again after resolving: a new alert.
	tr.Apply(t0.Add(11*time.Second), nil)
	if got := states(tr.Apply(t0.Add(12*time.Second), hits("a"))); got != "firing:a" {
		t.Fatalf("refire: %q", got)
	}
	// Duplicate keys in one result count once.
	tr = NewTracker(r, nil)
	if got := states(tr.Apply(t0, hits("x", "x"))); got != "firing:x" {
		t.Fatalf("dup keys: %q", got)
	}
	if tr.Len() != 1 {
		t.Fatal("Len")
	}
}

func TestTrackerSeedDoesNotRenotify(t *testing.T) {
	r, _ := ParseRule("hot", hotRule)
	since := time.Date(2026, 10, 7, 11, 0, 0, 0, time.UTC)
	tr := NewTracker(r, []Firing{{Key: "a", Since: since, Row: map[string]string{"sensor_id": "a", "temp": "95"}}, {Key: "gone", Since: since}})
	ts := tr.Apply(time.Now(), hits("a"))
	if got := states(ts); got != "resolved:gone" {
		t.Fatalf("after restart: %q", got)
	}
}

func TestGlobalAlertHasEmptyKey(t *testing.T) {
	r, _ := ParseRule("down", "-- MESSAGE nada llega\nSELECT 1 WHERE true")
	tr := NewTracker(r, nil)
	ts := tr.Apply(time.Now(), []Hit{{Key: "", Row: map[string]string{}}})
	if len(ts) != 1 || ts[0].Key != "" || ts[0].State != contract.AlertFiring || ts[0].Message != "nada llega" {
		t.Fatalf("ts = %+v", ts)
	}
}

func TestResolveEnv(t *testing.T) {
	env := map[string]string{"TOK": "s3cret", "EMPTY": ""}
	get := func(k string) string { return env[k] }
	got, err := ResolveEnv(map[string]any{"topic": "t", "token_env": "TOK", "timeout": "5s"}, get)
	if err != nil || got["token"] != "s3cret" || got["topic"] != "t" || got["timeout"] != "5s" {
		t.Fatalf("got %v err %v", got, err)
	}
	if _, has := got["token_env"]; has {
		t.Fatal("the *_env key must not reach the module")
	}
	if _, err := ResolveEnv(map[string]any{"token_env": "EMPTY"}, get); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("unset var must fail: %v", err)
	}
	if _, err := ResolveEnv(map[string]any{"token_env": 3}, get); err == nil {
		t.Fatal("non-string *_env must fail")
	}
}

func TestSummaryHidesURLs(t *testing.T) {
	sum, ok := Summary([]Delivery{
		{Channel: "ntfy:a", Attempts: 1},
		{Channel: "webhook:ops", Attempts: 3, Err: errString(`webhook: Post "https://hooks.slack.com/services/T0/B0/XXXX": dial tcp: refused`)},
	})
	if ok || strings.Contains(sum, "hooks.slack.com") || !strings.Contains(sum, "ntfy:a: ok") || !strings.Contains(sum, "after 3 tries") {
		t.Fatalf("summary = %q ok=%v", sum, ok)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestSerialKeepsOrderPerKey(t *testing.T) {
	var s serial
	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		s.Do("k", func() {
			defer wg.Done()
			if i == 0 {
				time.Sleep(20 * time.Millisecond) // a slow first job must not be overtaken
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		})
	}
	wg.Wait()
	for i, v := range order {
		if v != i {
			t.Fatalf("order = %v", order)
		}
	}
}
