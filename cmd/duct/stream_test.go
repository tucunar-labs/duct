package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/stream"
)

// fakeConsumer replays scripted polls and records what is committed.
type fakeConsumer struct {
	mu        sync.Mutex
	polls     chan []stream.Record
	committed []stream.Record
	onCommit  func([]stream.Record)
	assigned  func(string, []int32)
}

func newFake() *fakeConsumer { return &fakeConsumer{polls: make(chan []stream.Record, 16)} }

func (f *fakeConsumer) Poll(ctx context.Context, max int) ([]stream.Record, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-f.polls:
		if len(r) > max {
			f.polls <- r[max:]
			r = r[:max]
		}
		return r, nil
	}
}

func (f *fakeConsumer) Commit(_ context.Context, recs []stream.Record) error {
	if f.onCommit != nil {
		f.onCommit(recs)
	}
	f.mu.Lock()
	f.committed = append(f.committed, recs...)
	f.mu.Unlock()
	return nil
}

func (f *fakeConsumer) OnAssigned(fn func(string, []int32)) { f.assigned = fn }
func (f *fakeConsumer) Close()                              {}

func (f *fakeConsumer) committedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

func rec(off int64, json string) stream.Record {
	return stream.Record{Topic: "t", Partition: 0, Offset: off, Value: []byte(json), Key: []byte("k"),
		Timestamp: time.Date(2026, 10, 7, 12, 0, int(off), 0, time.UTC)}
}

// runWriter runs a writer until the fake has committed want records.
func runWriter(t *testing.T, p *project.Project, cfg stream.Config, f *fakeConsumer, want int) {
	t.Helper()
	eng, err := openEngine(context.Background(), p, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = eng.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := &stream.Writer{Eng: eng, Cfg: cfg, Consumer: f, Log: slog.New(slog.DiscardHandler)}
	go func() { done <- w.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for f.committedCount() < want {
		select {
		case err := <-done:
			t.Fatalf("writer stopped early: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("committed %d of %d records", f.committedCount(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func streamCfg(t *testing.T, yaml string) stream.Config {
	t.Helper()
	c, err := stream.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestStreamWriter: JSON -> bronze with metadata, nested JSON, schema
// evolution, rejects, offsets committed only after the rows are visible, and
// redelivery after a "crash" does not duplicate rows.
func TestStreamWriter(t *testing.T) {
	t.Parallel()
	root := newProject(t)
	p, err := project.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	cfg := streamCfg(t, "name: sensors\ntopic: t\nbatch: {max_rows: 100, max_interval: 50ms}\n")

	f := newFake()
	f.polls <- []stream.Record{
		rec(0, `{"id":"s1","temp":20.5,"ok":true,"ts":"2026-10-07T12:00:00Z","meta":{"a":1}}`),
		rec(1, `{"id":"s2","temp":21,"ok":false,"ts":"2026-10-07T12:00:01.5Z","meta":{"a":2}}`),
		rec(2, `not json`),
		rec(3, `[1,2]`),
	}
	f.polls <- []stream.Record{rec(4, `{"id":"s3","temp":22.0,"extra":"new column"}`)}

	// Commit-after-write: at commit time the rows must already be queryable.
	var violations []string
	f.onCommit = func(recs []stream.Record) {
		hi := recs[len(recs)-1].Offset
		got := queryCSV(t, root, "SELECT coalesce(max(_kafka_offset), -1) AS m FROM bronze.sensors")
		if hi == 4 && got != "m\n4" {
			violations = append(violations, "offset 4 committed but table max is "+got)
		}
	}
	runWriter(t, p, cfg, f, 5)
	if len(violations) > 0 {
		t.Fatal(violations)
	}

	if got := queryCSV(t, root, "SELECT id, temp, ok, extra FROM bronze.sensors ORDER BY _kafka_offset"); got != "id,temp,ok,extra\ns1,20.5,true,\ns2,21,false,\ns3,22,,new column" {
		t.Fatalf("rows: %q", got)
	}
	if got := queryCSV(t, root, "SELECT extra FROM bronze.sensors WHERE id = 's3'"); got != "extra\nnew column" {
		t.Fatalf("schema evolution: %q", got)
	}
	if got := queryCSV(t, root, "SELECT typeof(ts), typeof(meta), typeof(_kafka_ts), CAST(meta->>'$.a' AS INTEGER) FROM bronze.sensors WHERE id = 's1'"); !strings.Contains(got, "TIMESTAMP WITH TIME ZONE,JSON,TIMESTAMP WITH TIME ZONE,1") {
		t.Fatalf("types: %q", got)
	}
	if got := queryCSV(t, root, "SELECT _kafka_offset, error FROM bronze.sensors_rejects ORDER BY 1"); !strings.Contains(got, "2,") || !strings.Contains(got, "3,message is not a JSON object") {
		t.Fatalf("rejects: %q", got)
	}

	// Crash after the lake commit but before the Kafka commit: a new process
	// gets the same records again (plus one new) and must not duplicate.
	f2 := newFake()
	f2.polls <- []stream.Record{
		rec(1, `{"id":"s2","temp":21,"ok":false}`),
		rec(4, `{"id":"s3","temp":22.0}`),
		rec(5, `{"id":"s4","temp":23.0}`),
	}
	runWriter(t, p, cfg, f2, 3)
	if got := queryCSV(t, root, "SELECT count(*) AS n, count(DISTINCT _kafka_offset) AS d FROM bronze.sensors"); got != "n,d\n4,4" {
		t.Fatalf("redelivery duplicated rows: %q", got)
	}
}
