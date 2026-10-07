//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestAlertsActiveActive: two `duct alerts` processes in one consumer group
// (Kafka splits the partitions) over one Postgres catalog. Every alert is
// notified once even though both processes evaluate rules and share state; one
// process is killed with kill -9 while the alerts fire, the survivor takes the
// partitions over, keeps reminding and resolves each alert exactly once.
func TestAlertsActiveActive(t *testing.T) {
	pg := startPG(t)
	pg.createDB(t, "alertsha")
	k := startKafka(t)
	k.createTopic(t, "planta.ha", 2)
	hook := &received{}
	srv := httptest.NewServer(hook.handler(200))
	t.Cleanup(srv.Close)

	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), pwEnv, "init", root, "--catalog", pg.url("alertsha"))
	f, err := os.OpenFile(filepath.Join(root, "duct.toml"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(f, "\n[kafka]\nbrokers = [%q]\n[alerts]\nlease_ttl = \"3s\"\n[alerts.channels.ops]\ntype = \"webhook\"\nurl = %q\n", k.addr, srv.URL)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(root, "streams", "ha.yaml"), "name: ha\ntopic: planta.ha\nwindow: 5m\nbatch: {max_interval: 200ms}\n")
	write(t, filepath.Join(root, "alerts", "hot.sql"), `-- EVERY 200ms
-- KEY sensor_id
-- CHANNELS webhook:ops
-- REPEAT 2s
-- MESSAGE {sensor_id} marca {temp} C
SELECT sensor_id, temp FROM (
  SELECT *, row_number() OVER (PARTITION BY sensor_id ORDER BY _kafka_ts DESC, _kafka_offset DESC) AS rn FROM ha
) WHERE rn = 1 AND temp > 80
`)

	// Sensors keep reporting their current value every 300 ms (keyed: each one
	// always lands on the same partition).
	const sensors = 8
	var vmu sync.Mutex
	temps := map[string]int{}
	for i := range sensors {
		temps[fmt.Sprintf("s%d", i)] = 20
	}
	setAll := func(v int) {
		vmu.Lock()
		for s := range temps {
			temps[s] = v
		}
		vmu.Unlock()
	}
	cl := k.client(t)
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			vmu.Lock()
			cur := map[string]int{}
			for s, v := range temps {
				cur[s] = v
			}
			vmu.Unlock()
			for s, v := range cur {
				cl.ProduceSync(context.Background(), &kgo.Record{Topic: "planta.ha", Key: []byte(s),
					Value: []byte(fmt.Sprintf(`{"sensor_id":%q,"temp":%d}`, s, v))})
			}
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
			}
		}
	}()
	defer func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		bg.Wait()
	}()

	env := func(name string) []string { return append(append([]string{}, pwEnv...), "DUCT_ALERTS_INSTANCE="+name) }
	a := startDuct(t, root, env("a"), "alerts")
	b := startDuct(t, root, env("b"), "alerts")
	time.Sleep(8 * time.Second) // join the group, replay, warm up
	if n := hook.count(func(rcv) bool { return true }); n != 0 {
		t.Fatalf("%d notifications before anything is wrong", n)
	}

	setAll(95)
	isKey := func(key, state string) func(rcv) bool {
		return func(x rcv) bool { return x.body["key"] == key && x.body["state"] == state }
	}
	initial := func(key string) func(rcv) bool { // the alert itself, not a reminder
		return func(x rcv) bool {
			l, _ := x.body["labels"].(map[string]any)
			return x.body["key"] == key && x.body["state"] == "firing" && l["reminder"] == nil
		}
	}
	for i := range sensors {
		hook.await(t, fmt.Sprintf("s%d firing", i), 15*time.Second, initial(fmt.Sprintf("s%d", i)))
	}
	time.Sleep(7 * time.Second) // reminders every 2 s
	for i := range sensors {
		if n := hook.count(initial(fmt.Sprintf("s%d", i))); n != 1 {
			t.Errorf("s%d: %d initial notifications, want 1", i, n)
		}
	}
	checkSpacing := func(when string) {
		hook.mu.Lock()
		defer hook.mu.Unlock()
		last := map[any]time.Time{}
		for _, x := range hook.list {
			if x.body["state"] != "firing" {
				continue
			}
			key := x.body["key"]
			if prev, ok := last[key]; ok && x.at.Sub(prev) < 1200*time.Millisecond {
				t.Errorf("%s: %v notified twice within %s (REPEAT is 2s): two instances sent the same reminder", when, key, x.at.Sub(prev))
			}
			last[key] = x.at
		}
	}
	checkSpacing("both alive")
	holders := pg.sql(t, "alertsha", "SELECT count(DISTINCT holder) FROM duct.duct_alert_state")
	t.Logf("distinct holders while both run: %s", holders)

	// kill -9 one instance: the survivor takes the partitions over (session
	// timeout 10 s), adopts the firing alerts without announcing them again and
	// keeps reminding.
	a.signal(syscall.SIGKILL)
	before := hook.count(func(x rcv) bool { return x.body["state"] == "firing" })
	time.Sleep(35 * time.Second)
	for i := range sensors {
		if n := hook.count(initial(fmt.Sprintf("s%d", i))); n != 1 {
			t.Errorf("after the failover s%d has %d initial notifications, want 1", i, n)
		}
	}
	if after := hook.count(func(x rcv) bool { return x.body["state"] == "firing" }); after <= before {
		t.Errorf("no reminders after the failover (%d -> %d)", before, after)
	}
	checkSpacing("after the failover")

	// Everything recovers: each alert resolves exactly once.
	setAll(20)
	for i := range sensors {
		hook.await(t, fmt.Sprintf("s%d resolved", i), 20*time.Second, isKey(fmt.Sprintf("s%d", i), "resolved"))
	}
	time.Sleep(3 * time.Second)
	for i := range sensors {
		if n := hook.count(isKey(fmt.Sprintf("s%d", i), "resolved")); n != 1 {
			t.Errorf("s%d resolved %d times, want 1", i, n)
		}
	}
	close(stop)
	bg.Wait()
	time.Sleep(2 * time.Second) // Bronze trails the notifications
	rows := csvQuery(t, root, pwEnv, "SELECT alert_key, state, count(*) AS n FROM bronze.duct_alerts WHERE state IN ('firing','resolved') GROUP BY ALL ORDER BY ALL")
	lines := strings.Split(strings.TrimSpace(rows), "\n")
	if len(lines) != 1+2*sensors {
		t.Errorf("Bronze has %d (key, state) groups, want %d:\n%s", len(lines)-1, 2*sensors, rows)
	}
	for _, l := range lines[1:] {
		if !strings.HasSuffix(l, ",1") {
			t.Errorf("a transition was recorded more than once in Bronze: %s", l)
		}
	}
	if left := pg.sql(t, "alertsha", "SELECT count(*) FROM duct.duct_alert_state"); strings.TrimSpace(left) != "0" {
		t.Errorf("%s alerts left in the shared state after everything resolved", left)
	}
	b.signal(syscall.SIGINT)
}
