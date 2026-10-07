//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka (apache/kafka, KRaft, one node) in rootless Podman bound to
// 127.0.0.1. Override the image with DUCT_TEST_KAFKA_IMAGE, or use an existing
// broker with DUCT_TEST_KAFKA_BROKER. These tests drive the real `duct`
// binary: `duct stream` (Kafka -> Bronze) and `duct alerts` (ADR 0003, 0009).

const kafkaImage = "docker.io/apache/kafka:latest"

type kafkaBroker struct {
	id   string // container id ("" for an external broker)
	addr string // host:port
}

func startKafka(t *testing.T) *kafkaBroker {
	t.Helper()
	for _, name := range []string{"DUCT_TEST_KAFKA_BROKERS", "DUCT_TEST_KAFKA_BROKER"} {
		if b := os.Getenv(name); b != "" {
			return waitKafka(t, &kafkaBroker{addr: strings.Split(b, ",")[0]}, 60*time.Second)
		}
	}
	if _, err := exec.LookPath("podman"); err != nil {
		t.Skip("podman not available")
	}
	image := kafkaImage
	if v := os.Getenv("DUCT_TEST_KAFKA_IMAGE"); v != "" {
		image = v
	}
	port := freePort(t)
	id := podman(t, "run", "-d", "--rm",
		"-p", fmt.Sprintf("127.0.0.1:%d:%d", port, port),
		"-e", "KAFKA_NODE_ID=1",
		"-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", fmt.Sprintf("KAFKA_LISTENERS=PLAINTEXT://:%d,CONTROLLER://:9093", port),
		"-e", fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:%d", port),
		"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		"-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
		image)
	t.Cleanup(func() { _ = exec.Command("podman", "rm", "-f", id).Run() })
	return waitKafka(t, &kafkaBroker{id: id, addr: fmt.Sprintf("127.0.0.1:%d", port)}, 90*time.Second)
}

// waitKafka waits until the broker answers a metadata request.
func waitKafka(t *testing.T, b *kafkaBroker, within time.Duration) *kafkaBroker {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		cl, err := kgo.NewClient(kgo.SeedBrokers(b.addr))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err = kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
			cancel()
			cl.Close()
			if err == nil {
				return b
			}
		}
		if time.Now().After(deadline) {
			logs := ""
			if b.id != "" {
				out, _ := exec.Command("podman", "logs", "--tail", "30", b.id).CombinedOutput()
				logs = string(out)
			}
			t.Fatalf("kafka did not start: %v\n%s", err, logs)
		}
		time.Sleep(time.Second)
	}
}

func (b *kafkaBroker) client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(b.addr), kgo.ProducerLinger(0)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func (b *kafkaBroker) createTopic(t *testing.T, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, partitions, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, b.client(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range resp.Topics {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
}

// groupLag is the total lag of a consumer group (log end offset minus the
// committed offset, summed over its partitions), asked over the Kafka protocol
// so it works with a Podman broker and with an external one. -1 if the group
// has no committed offsets.
func (b *kafkaBroker) groupLag(t *testing.T, group string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := b.client(t)
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group // v0-v7
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group // v8+; no topics = every committed topic
	req.Groups = append(req.Groups, g)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	type tp struct {
		topic string
		part  int32
	}
	committed := map[tp]int64{}
	for _, tt := range resp.Topics {
		for _, p := range tt.Partitions {
			committed[tp{tt.Topic, p.Partition}] = p.Offset
		}
	}
	for _, gr := range resp.Groups {
		for _, tt := range gr.Topics {
			for _, p := range tt.Partitions {
				committed[tp{tt.Topic, p.Partition}] = p.Offset
			}
		}
	}
	lreq := kmsg.NewPtrListOffsetsRequest()
	lreq.ReplicaID = -1
	byTopic := map[string]*kmsg.ListOffsetsRequestTopic{}
	for k, off := range committed {
		if off < 0 {
			continue
		}
		rt := byTopic[k.topic]
		if rt == nil {
			t := kmsg.NewListOffsetsRequestTopic()
			t.Topic = k.topic
			rt = &t
			byTopic[k.topic] = rt
		}
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp = k.part, -1 // latest
		rt.Partitions = append(rt.Partitions, p)
	}
	if len(byTopic) == 0 {
		return -1
	}
	for _, rt := range byTopic {
		lreq.Topics = append(lreq.Topics, *rt)
	}
	lresp, err := lreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}
	var total int64
	for _, tt := range lresp.Topics {
		for _, p := range tt.Partitions {
			total += p.Offset - committed[tp{tt.Topic, p.Partition}]
		}
	}
	return total
}

// proc is a background `duct` subprocess.
type proc struct {
	cmd  *exec.Cmd
	out  *lockedBuf
	done chan struct{}
}

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func startDuct(t *testing.T, root string, env []string, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(ductBinary(t), args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), env...)
	out := &lockedBuf{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, out: out, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		p.signal(syscall.SIGKILL)
		if t.Failed() {
			t.Logf("output of duct %v:\n%s", args, out.String())
		}
	})
	return p
}

func (p *proc) signal(s syscall.Signal) {
	select {
	case <-p.done:
	default:
		_ = p.cmd.Process.Signal(s)
		select {
		case <-p.done:
		case <-time.After(20 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
}

// waitFor polls f until it returns true.
func waitFor(t *testing.T, what string, within time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var streamRowsRe = regexp.MustCompile(`(?m)^\S+\s+\S+\s+(\d+) rows `)

// rowsInOutput sums the rows of the batch lines `duct stream -v` printed.
func rowsInOutput(out string) int {
	n := 0
	for _, m := range streamRowsRe.FindAllStringSubmatch(out, -1) {
		n += atoi(m[1])
	}
	return n
}

func atoi(s string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(s))
	return n
}

func newKafkaProject(t *testing.T, b *kafkaBroker, extraToml string) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "proj")
	mustDuct(t, t.TempDir(), nil, "init", root)
	t.Cleanup(func() { // keep the project of a failed run for a post-mortem
		if d := os.Getenv("DUCT_TEST_KEEP_FAILED"); d != "" && t.Failed() {
			_ = exec.Command("cp", "-a", root, filepath.Join(d, t.Name())).Run()
		}
	})
	f, err := os.OpenFile(filepath.Join(root, "duct.toml"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := fmt.Fprintf(f, "\n[kafka]\nbrokers = [%q]\n%s\n", b.addr, extraToml); err != nil {
		t.Fatal(err)
	}
	return root
}

// TestStreamKafkaToBronze: produce JSON, `duct stream` lands it in Bronze
// within about a second, a kill -9 in the middle of a 4000-message flow loses
// nothing and duplicates nothing, and the consumer group ends with lag 0.
func TestStreamKafkaToBronze(t *testing.T) {
	k := startKafka(t)
	k.createTopic(t, "planta.sensores", 2)
	root := newKafkaProject(t, k, "")
	write(t, filepath.Join(root, "streams", "sensores.yaml"),
		"name: sensores\ntopic: planta.sensores\nbatch: {max_rows: 2000, max_interval: 200ms}\ninline_rows: 50\n")
	cl := k.client(t)
	ctx := context.Background()
	produce := func(i int) {
		rec := &kgo.Record{Topic: "planta.sensores", Key: []byte(fmt.Sprintf("s%d", i%8)),
			Value: []byte(fmt.Sprintf(`{"sensor_id":"s%d","seq":%d,"temp":%d.5,"ts":%q}`, i%8, i, 20+i%5, time.Now().UTC().Format(time.RFC3339Nano)))}
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce: %v", err)
			}
		})
	}
	q := func(sql string) string { return csvQuery(t, root, nil, sql) }
	count := func() int {
		r := duct(t, root, nil, "query", "--format", "csv", "SELECT count(*) FROM bronze.sensores")
		if r.err != nil {
			return 0 // table not created yet
		}
		return atoi(strings.TrimPrefix(strings.TrimSpace(r.stdout), "count_star()"))
	}

	stream := startDuct(t, root, nil, "stream", "-v")

	// 1. Latency: a handful of messages, one at a time, then look at
	// _ingested_at - _kafka_ts (produce -> committed in the lake).
	for i := range 10 {
		produce(i)
		if err := cl.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	waitFor(t, "10 rows in bronze", 60*time.Second, func() bool { return count() == 10 })
	lat := q("SELECT CAST(round(max(epoch_ms(_ingested_at) - epoch_ms(_kafka_ts))) AS BIGINT) AS ms, CAST(round(median(epoch_ms(_ingested_at) - epoch_ms(_kafka_ts))) AS BIGINT) AS med FROM bronze.sensores")
	t.Logf("produce -> bronze latency (ms, max,median): %s", strings.ReplaceAll(lat, "\n", " | "))
	ms := atoi(strings.Split(strings.Split(lat, "\n")[1], ",")[0])
	if ms > 2000 {
		t.Errorf("slowest message took %d ms to land, want <= 2000", ms)
	}
	if got := q("SELECT typeof(ts) FROM bronze.sensores LIMIT 1"); !strings.Contains(got, "TIMESTAMP WITH TIME ZONE") {
		t.Errorf("ts type: %q", got)
	}

	// 2. kill -9 in the middle of a flow, restart, nothing lost or repeated.
	// The producer is throttled (~1000 msg/s, so ~4 s) and the kill is based on
	// the progress `duct stream -v` reports (rows committed to the lake): as
	// soon as 10% of the flow landed, long before 80%, whatever the speed of
	// the machine. Polling the lake with `duct query` is too slow for that.
	total := 10
	const bulk = 4000
	all := total + bulk
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 10; i < 10+bulk; i++ {
			produce(i)
			if i%50 == 0 {
				_ = cl.Flush(ctx)
				time.Sleep(50 * time.Millisecond)
			}
		}
		_ = cl.Flush(ctx)
	}()
	landed := func() int { return rowsInOutput(stream.out.String()) }
	waitFor(t, "10% of the flow landed", 60*time.Second, func() bool { return landed() >= all/10 })
	stream.signal(syscall.SIGKILL)
	atKill := landed()
	t.Logf("kill -9 with ~%d of %d rows committed (stream output)", atKill, all)
	if atKill > all*8/10 {
		t.Logf("the machine landed more than 80%% before the kill; the guarantees below still apply")
	}
	<-done
	stream = startDuct(t, root, nil, "stream", "-v")
	waitFor(t, "all rows after restart", 120*time.Second, func() bool { return count() >= all })
	// No loss, no duplicates (also when everything had landed before the kill:
	// then the restart has nothing to process and must still converge).
	if got := q("SELECT count(*) AS n, count(DISTINCT (_kafka_partition, _kafka_offset)) AS d, count(DISTINCT seq) AS s FROM bronze.sensores"); got != fmt.Sprintf("n,d,s\n%d,%d,%d", all, all, all) {
		t.Fatalf("after crash+restart (killed at ~%d): %q", atKill, got)
	}
	// Per partition the offsets are contiguous from 0: nothing skipped.
	if got := q("SELECT count(*) FROM (SELECT _kafka_partition, count(*) c, max(_kafka_offset) m FROM bronze.sensores GROUP BY 1) WHERE c <> m + 1"); got != "count_star()\n0" {
		t.Fatalf("gaps in offsets: %q", got)
	}
	// Offsets are committed after landing: the group catches up to lag 0.
	waitFor(t, "lag 0", 30*time.Second, func() bool { return k.groupLag(t, "duct-stream-sensores") == 0 })
	stream.signal(syscall.SIGINT)

	// 3. Not JSON: set aside in <target>_rejects, the rest flows on.
	stream = startDuct(t, root, nil, "stream")
	cl.ProduceSync(ctx, &kgo.Record{Topic: "planta.sensores", Value: []byte("esto no es json")}, &kgo.Record{Topic: "planta.sensores", Value: []byte(`{"sensor_id":"s9","seq":99999}`)})
	waitFor(t, "reject", 60*time.Second, func() bool {
		r := duct(t, root, nil, "query", "--format", "csv", "SELECT count(*) FROM bronze.sensores_rejects")
		return r.err == nil && strings.Contains(r.stdout, "\n1")
	})
	waitFor(t, "good row after the bad one", 30*time.Second, func() bool { return count() == total+bulk+1 })
	stream.signal(syscall.SIGINT)
	if got := q("SELECT raw, error FROM bronze.sensores_rejects"); !strings.Contains(got, "esto no es json,") {
		t.Errorf("rejects: %q", got)
	}
}

type received struct {
	mu   sync.Mutex
	list []rcv
}

type rcv struct {
	at   time.Time
	path string
	hdr  http.Header
	body map[string]any
}

func (r *received) handler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var m map[string]any
		_ = json.NewDecoder(req.Body).Decode(&m)
		r.mu.Lock()
		r.list = append(r.list, rcv{time.Now(), req.URL.Path, req.Header.Clone(), m})
		r.mu.Unlock()
		w.WriteHeader(status)
	})
}

// first returns the first request for which match is true.
func (r *received) first(match func(rcv) bool) (rcv, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, x := range r.list {
		if match(x) {
			return x, true
		}
	}
	return rcv{}, false
}

func (r *received) await(t *testing.T, what string, within time.Duration, match func(rcv) bool) rcv {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if x, ok := r.first(match); ok {
			return x
		}
		if time.Now().After(deadline) {
			r.mu.Lock()
			defer r.mu.Unlock()
			t.Fatalf("timeout waiting for %s; received %d requests", what, len(r.list))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *received) count(match func(rcv) bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, x := range r.list {
		if match(x) {
			n++
		}
	}
	return n
}

// TestAlertsKafka: `duct alerts` over a real topic. A threshold rule and a
// "silent sensor" rule fire in under a second, notify webhook, ntfy,
// PagerDuty and WhatsApp (all against httptest servers), resolve when the
// sensor recovers, and are recorded in bronze.duct_alerts.
func TestAlertsKafka(t *testing.T) {
	k := startKafka(t)
	k.createTopic(t, "planta.alertas", 1)
	hook, ntfy, pd, wa := &received{}, &received{}, &received{}, &received{}
	var urls [4]string
	for i, r := range []*received{hook, ntfy, pd, wa} {
		srv := httptest.NewServer(r.handler(http.StatusOK))
		t.Cleanup(srv.Close)
		urls[i] = srv.URL
	}
	root := newKafkaProject(t, k, fmt.Sprintf(`
[alerts.channels.ops]
type = "webhook"
url = %q
[alerts.channels.incendios]
type = "ntfy"
url = %q
topic = "planta-incendios"
token_env = "TEST_NTFY_TOKEN"
[alerts.channels.main]
type = "pagerduty"
url = %q
routing_key_env = "TEST_PD_KEY"
[alerts.channels.oncall]
type = "whatsapp"
api_url = %q
phone_number_id = "1055"
token_env = "TEST_WA_TOKEN"
template = "duct_alerta"
to = "56911112222"
`, urls[0], urls[1], urls[2], urls[3]))
	env := []string{"TEST_NTFY_TOKEN=tk_test", "TEST_PD_KEY=pd-routing-test", "TEST_WA_TOKEN=wa-token-test"}

	write(t, filepath.Join(root, "streams", "alertas.yaml"), "name: alertas\ntopic: planta.alertas\nwindow: 5m\nbatch: {max_interval: 200ms}\n")
	write(t, filepath.Join(root, "alerts", "hot.sql"), `-- Temperatura sobre el umbral, por sensor (ultima lectura).
-- EVERY 200ms
-- KEY sensor_id
-- SEVERITY critical
-- CHANNELS ntfy:incendios, webhook:ops, pagerduty:main, whatsapp:oncall
-- MESSAGE {sensor_id} marca {temp} C
SELECT sensor_id, temp FROM (
  SELECT *, row_number() OVER (PARTITION BY sensor_id ORDER BY _kafka_ts DESC, _kafka_offset DESC) AS rn FROM alertas
) WHERE rn = 1 AND temp > 80
`)
	write(t, filepath.Join(root, "alerts", "silent.sql"), `-- Sensor mudo: sin mensajes por mas de 2 s.
-- EVERY 200ms
-- KEY sensor_id
-- SEVERITY warning
-- CHANNELS webhook:ops, pagerduty:main
-- MESSAGE el sensor {sensor_id} no reporta
SELECT sensor_id FROM alertas GROUP BY sensor_id HAVING max(_kafka_ts) < now() - INTERVAL 2 SECOND
`)
	if out := mustDuct(t, root, env, "alerts", "check"); !strings.Contains(out, "2 rules, 1 streams with a window, channels OK") {
		t.Fatalf("alerts check: %s", out)
	}
	// A rule that points to a channel nobody configured is refused up front.
	write(t, filepath.Join(root, "alerts", "bad.sql"), "-- CHANNELS slack:nadie\nSELECT 1 AS key")
	if r := duct(t, root, env, "alerts", "check"); r.err == nil || !strings.Contains(r.stderr, "not configured") {
		t.Fatalf("bad channel: err=%v stderr=%s", r.err, r.stderr)
	}
	if err := os.Remove(filepath.Join(root, "alerts", "bad.sql")); err != nil {
		t.Fatal(err)
	}

	cl := k.client(t)
	ctx := context.Background()
	send := func(sensor string, temp int) time.Time {
		cl.ProduceSync(ctx, &kgo.Record{Topic: "planta.alertas", Key: []byte(sensor),
			Value: []byte(fmt.Sprintf(`{"sensor_id":%q,"temp":%d}`, sensor, temp))})
		return time.Now()
	}
	// Baseline readings before the process starts: it replays them (window).
	send("s1", 20)
	send("s2", 20)

	alerts := startDuct(t, root, env, "alerts")
	// Sensors keep reporting their current value every 300 ms, as real ones
	// do; the test changes the values. A sensor with value 0 is silent.
	var vmu sync.Mutex
	temps := map[string]int{"s1": 21, "s2": 21}
	setTemp := func(sensor string, v int) { vmu.Lock(); temps[sensor] = v; vmu.Unlock() }
	stop := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				vmu.Lock()
				cur := map[string]int{}
				for k, v := range temps {
					cur[k] = v
				}
				vmu.Unlock()
				for s, v := range cur {
					if v != 0 {
						send(s, v)
					}
				}
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
	time.Sleep(6 * time.Second) // join the group, replay, warm up
	if n := hook.count(func(rcv) bool { return true }); n != 0 {
		t.Fatalf("%d notifications before anything is wrong", n)
	}

	// Threshold.
	setTemp("s1", 95)
	sent := send("s1", 95)
	isHot := func(state string) func(rcv) bool {
		return func(x rcv) bool { return x.body["rule"] == "hot" && x.body["key"] == "s1" && x.body["state"] == state }
	}
	x := hook.await(t, "webhook firing", 5*time.Second, isHot("firing"))
	latWebhook := x.at.Sub(sent)
	t.Logf("threshold alert: webhook %s after the message was produced", latWebhook.Round(time.Millisecond))
	if latWebhook > time.Second {
		t.Errorf("webhook latency %s, want < 1s", latWebhook)
	}
	if x.body["message"] != "s1 marca 95 C" {
		t.Errorf("webhook payload: %v", x.body)
	}
	nt := ntfy.await(t, "ntfy firing", 5*time.Second, func(x rcv) bool { return x.body["title"] == "FIRING: hot s1" })
	t.Logf("threshold alert: ntfy %s", nt.at.Sub(sent).Round(time.Millisecond))
	if nt.hdr.Get("Authorization") != "Bearer tk_test" || nt.body["topic"] != "planta-incendios" || nt.body["priority"] != float64(5) {
		t.Errorf("ntfy request: %v %v", nt.hdr, nt.body)
	}
	p := pd.await(t, "pagerduty trigger", 5*time.Second, func(x rcv) bool { return x.body["dedup_key"] == "hot:s1" && x.body["event_action"] == "trigger" })
	t.Logf("threshold alert: pagerduty %s", p.at.Sub(sent).Round(time.Millisecond))
	if p.body["routing_key"] != "pd-routing-test" || p.body["payload"].(map[string]any)["severity"] != "critical" {
		t.Errorf("pagerduty event: %v", p.body)
	}
	w := wa.await(t, "whatsapp template", 5*time.Second, func(x rcv) bool { return x.body["type"] == "template" })
	t.Logf("threshold alert: whatsapp %s", w.at.Sub(sent).Round(time.Millisecond))
	if w.path != "/v23.0/1055/messages" || w.hdr.Get("Authorization") != "Bearer wa-token-test" || w.body["to"] != "56911112222" {
		t.Errorf("whatsapp request: %s %v %v", w.path, w.hdr, w.body)
	}

	// Dedup: s1 stays hot for a few seconds (it keeps reporting 95): one notification.
	time.Sleep(2 * time.Second)
	if n := hook.count(isHot("firing")); n != 1 {
		t.Errorf("firing notified %d times, want 1", n)
	}

	// Resolution: back to normal.
	setTemp("s1", 22)
	sent = send("s1", 22)
	x = hook.await(t, "webhook resolved", 5*time.Second, isHot("resolved"))
	t.Logf("resolution: webhook %s after the normal reading", x.at.Sub(sent).Round(time.Millisecond))
	pd.await(t, "pagerduty resolve", 5*time.Second, func(x rcv) bool { return x.body["dedup_key"] == "hot:s1" && x.body["event_action"] == "resolve" })
	ntfy.await(t, "ntfy resolved", 5*time.Second, func(x rcv) bool { return x.body["title"] == "RESOLVED: hot s1" })

	// Silent sensor: s2 stops talking (s1 does not).
	setTemp("s2", 0)
	lastS2 := time.Now() // its last reading went out within the last 300 ms
	isSilent := func(state string) func(rcv) bool {
		return func(x rcv) bool {
			return x.body["rule"] == "silent" && x.body["key"] == "s2" && x.body["state"] == state
		}
	}
	x = hook.await(t, "silent sensor firing", 8*time.Second, isSilent("firing"))
	t.Logf("silent sensor: webhook %s after s2's last reading (threshold 2s)", x.at.Sub(lastS2).Round(time.Millisecond))
	if hook.count(func(x rcv) bool { return x.body["rule"] == "silent" && x.body["key"] == "s1" }) != 0 {
		t.Error("s1 talks all the time: it must not be silent")
	}
	setTemp("s2", 21)
	sent = send("s2", 21)
	x = hook.await(t, "silent sensor resolved", 5*time.Second, isSilent("resolved"))
	t.Logf("silent sensor back: resolved %s after it spoke", x.at.Sub(sent).Round(time.Millisecond))
	close(stop)
	bg.Wait()

	time.Sleep(time.Second) // the Bronze records trail the notifications
	alerts.signal(syscall.SIGINT)
	got := csvQuery(t, root, nil, "SELECT rule, alert_key, state, severity, delivered FROM bronze.duct_alerts ORDER BY occurred_at")
	for _, want := range []string{"hot,s1,firing,critical,true", "hot,s1,resolved,critical,true", "silent,s2,firing,warning,true", "silent,s2,resolved,warning,true"} {
		if !strings.Contains(got, want) {
			t.Errorf("bronze.duct_alerts lacks %q:\n%s", want, got)
		}
	}
	if got := csvQuery(t, root, nil, "SELECT count(*) FROM duct.duct_alert_state"); got != "count_star()\n0" {
		t.Errorf("firing set at the end: %q", got)
	}
	// The consumer group never commits: alerts replay the window on start.
	if lag := k.groupLag(t, "duct-alerts"); lag > 0 {
		t.Logf("duct-alerts group lag %d (no offsets are committed by design)", lag)
	}
	if strings.Contains(alerts.out.String(), "wa-token-test") || strings.Contains(alerts.out.String(), "pd-routing-test") || strings.Contains(alerts.out.String(), "tk_test") {
		t.Errorf("a secret leaked into the output:\n%s", alerts.out.String())
	}
}

// TestServeRunsStreamAndAlerts: with [stream] and [alerts] in duct.toml,
// `duct serve` runs the writers and the rules in the same process.
func TestServeRunsStreamAndAlerts(t *testing.T) {
	k := startKafka(t)
	k.createTopic(t, "planta.serve", 1)
	hook := &received{}
	srv := httptest.NewServer(hook.handler(http.StatusOK))
	defer srv.Close()
	root := newKafkaProject(t, k, fmt.Sprintf("[stream]\n[alerts]\n[alerts.channels.ops]\ntype = \"webhook\"\nurl = %q\n", srv.URL))
	write(t, filepath.Join(root, "streams", "serve.yaml"), "name: serve\ntopic: planta.serve\nwindow: 1m\nbatch: {max_interval: 200ms}\n")
	write(t, filepath.Join(root, "alerts", "hot.sql"), "-- EVERY 200ms\n-- KEY sensor_id\n-- CHANNELS webhook:ops\nSELECT sensor_id FROM serve WHERE temp > 80 AND _kafka_ts > now() - INTERVAL 5 SECOND GROUP BY 1\n")
	startServe(t, root, nil)
	cl := k.client(t)
	ctx := context.Background()
	for i := range 20 {
		cl.ProduceSync(ctx, &kgo.Record{Topic: "planta.serve", Value: []byte(fmt.Sprintf(`{"sensor_id":"s1","temp":%d}`, 70+i))})
		time.Sleep(250 * time.Millisecond)
	}
	hook.await(t, "alert from duct serve", 10*time.Second, func(x rcv) bool { return x.body["rule"] == "hot" && x.body["state"] == "firing" })
	waitFor(t, "rows from duct serve", 20*time.Second, func() bool {
		r := duct(t, root, nil, "query", "--format", "csv", "SELECT count(*) FROM bronze.serve")
		return r.err == nil && strings.Contains(r.stdout, "\n20")
	})
}
