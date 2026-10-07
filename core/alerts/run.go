package alerts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/stream"
)

// The window engine is a plain in-memory DuckDB (engine config "memory": true):
// its default catalog is "memory", so rules say `FROM sensors`.
const windowCatalog, windowSchema = "memory", "main"

// caughtUpAge is how recent the newest message of a poll must be for the
// consumer to count as caught up with the window.
const caughtUpAge = 2 * time.Second

// maxWarmup bounds how long rules wait for the consumer to catch up.
const maxWarmup = 10 * time.Second

// Runner wires the pieces of `duct alerts`.
type Runner struct {
	// Lake holds bronze.duct_alerts and duct.duct_alert_state.
	Lake contract.Engine
	// Window is a separate engine whose memory catalog holds the last minutes
	// of every stream; it must implement contract.Transactor.
	Window   contract.Engine
	Consumer stream.Consumer
	// Streams are the streams to follow (only those with a Window).
	Streams  []stream.Config
	Rules    []Rule
	Channels *Channels
	Log      *slog.Logger
	// OnEvent, if set, is called after a transition was delivered (before it
	// is recorded in Bronze).
	OnEvent func(Entry, []Delivery)

	wtx     contract.Transactor
	store   Store
	serial  serial
	queue   chan Entry
	warm    chan struct{}
	warmOne sync.Once
}

// Run follows the streams and evaluates the rules until ctx ends (returning
// nil). Delivery semantics: a transition is notified once per firing/resolved
// change of an alert (deduplicated by rule + key), each channel retried a few
// times; the notification goes out before the state is saved, so a crash can
// repeat a notification but never lose one. Rules wait for the consumer to
// catch up with the window before their first evaluation (an empty poll or a
// poll whose newest message is under 2 s old; at most 10 s), so a
// restart neither re-notifies alerts that keep firing nor resolves them by
// mistake.
func (r *Runner) Run(ctx context.Context) error {
	if r.Log == nil {
		r.Log = slog.Default()
	}
	wtx, ok := r.Window.(contract.Transactor)
	if !ok {
		return errors.New("window engine does not implement contract.Transactor")
	}
	r.wtx = wtx
	r.store = Store{Eng: r.Lake}
	r.queue = make(chan Entry, 1024)
	r.warm = make(chan struct{})
	if err := r.Channels.Check(r.Rules); err != nil {
		return err
	}
	if err := r.store.Init(ctx); err != nil {
		return err
	}
	persisted, err := r.store.Firing(ctx)
	if err != nil {
		return err
	}
	trackers := make(map[string]*Tracker, len(r.Rules))
	for _, rule := range r.Rules {
		trackers[rule.Name] = NewTracker(rule, persisted[rule.Name])
		delete(persisted, rule.Name)
	}
	for gone := range persisted { // rule deleted while firing: nobody will resolve it
		r.Log.Warn("rule no longer exists; dropping its firing alerts", "rule", gone)
		if err := r.store.Forget(ctx, gone); err != nil {
			return err
		}
	}

	var recorder sync.WaitGroup
	recorder.Add(1)
	go func() { defer recorder.Done(); r.record() }()

	var loops, deliveries sync.WaitGroup
	for _, rule := range r.Rules {
		loops.Add(1)
		go func() {
			defer loops.Done()
			r.ruleLoop(ctx, rule, trackers[rule.Name], &deliveries)
		}()
	}
	err = r.ingest(ctx)
	loops.Wait()
	deliveries.Wait()
	close(r.queue)
	recorder.Wait()
	return err
}

func (r *Runner) markWarm() {
	r.warmOne.Do(func() {
		r.Log.Info("windows warm: evaluating rules", "rules", len(r.Rules), "streams", len(r.Streams))
		close(r.warm)
	})
}

// ingest keeps the in-memory windows up to date from Kafka.
func (r *Runner) ingest(ctx context.Context) error {
	byTopic := map[string]stream.Config{}
	for _, s := range r.Streams {
		byTopic[s.Topic] = s
	}
	timer := time.AfterFunc(maxWarmup, r.markWarm)
	defer timer.Stop()
	nextSweep := time.Now().Add(time.Second)
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		recs, err := r.Consumer.Poll(pctx, 5000)
		cancel()
		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, context.DeadlineExceeded):
			r.markWarm() // nothing new for 100 ms: the backlog is drained
		case err != nil:
			r.Log.Warn("kafka poll failed", "err", err)
			sleepCtx(ctx, time.Second)
		default:
			if err := r.land(ctx, byTopic, recs); err != nil {
				return err
			}
			// A busy topic never goes quiet: the replay is over once the
			// newest message of a poll is (nearly) current.
			newest := recs[0].Timestamp
			for _, rec := range recs {
				if rec.Timestamp.After(newest) {
					newest = rec.Timestamp
				}
			}
			if time.Since(newest) < caughtUpAge {
				r.markWarm()
			}
		}
		if time.Now().After(nextSweep) {
			r.sweep(ctx)
			nextSweep = time.Now().Add(time.Second)
		}
	}
	return nil
}

// land decodes the records of each stream and appends them to its window.
func (r *Runner) land(ctx context.Context, byTopic map[string]stream.Config, recs []stream.Record) error {
	groups := map[string][]stream.Record{}
	now := time.Now()
	for _, rec := range recs {
		s, ok := byTopic[rec.Topic]
		if !ok || now.Sub(rec.Timestamp) > s.Window {
			continue // not a followed topic, or older than the window (replay)
		}
		groups[rec.Topic] = append(groups[rec.Topic], rec)
	}
	for topic, rs := range groups {
		s := byTopic[topic]
		d, err := stream.Decode(rs, nil)
		if err != nil {
			r.Log.Warn("window: undecodable batch skipped", "stream", s.Name, "err", err)
			continue
		}
		if d.Rec == nil {
			continue
		}
		t := stream.Table{Catalog: windowCatalog, Schema: windowSchema, Name: s.Name}
		rr, err := stream.Reader(d.Rec)
		if err == nil {
			err = r.wtx.InTx(ctx, rr, func(ctx context.Context, tx contract.Tx) error {
				_, err := stream.Land(ctx, tx, t, d)
				return err
			})
			rr.Release()
		}
		d.Rec.Release()
		if err != nil && ctx.Err() == nil {
			r.Log.Warn("window: batch not landed", "stream", s.Name, "err", err)
		}
	}
	return nil
}

// sweep drops rows older than each stream's window.
func (r *Runner) sweep(ctx context.Context) {
	for _, s := range r.Streams {
		t := stream.Table{Catalog: windowCatalog, Schema: windowSchema, Name: s.Name}
		err := r.Window.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s < now() - to_seconds(%d)", t.SQL(), stream.ColTimestamp, int64(s.Window.Seconds())))
		if err != nil && ctx.Err() == nil && !isMissingTable(err) {
			r.Log.Warn("window: sweep failed", "stream", s.Name, "err", err)
		}
	}
}

func isMissingTable(err error) bool {
	m := err.Error()
	return strings.Contains(m, "does not exist") && strings.Contains(m, "Table")
}

// ruleLoop evaluates one rule every rule.Every once the windows are warm.
func (r *Runner) ruleLoop(ctx context.Context, rule Rule, tr *Tracker, deliveries *sync.WaitGroup) {
	select {
	case <-ctx.Done():
		return
	case <-r.warm:
	}
	tick := time.NewTicker(rule.Every)
	defer tick.Stop()
	var lastErr string
	var lastLog time.Time
	for {
		hits, err := r.evaluate(ctx, rule)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil:
			lastErr = ""
			for _, t := range tr.Apply(time.Now(), hits) {
				r.dispatch(ctx, rule, t, deliveries)
			}
		case isMissingTable(err):
			// the stream has not delivered anything yet: nothing to judge
		default:
			if m := err.Error(); m != lastErr || time.Since(lastLog) > 30*time.Second {
				r.Log.Warn("rule evaluation failed; state unchanged", "rule", rule.Name, "err", err)
				lastErr, lastLog = m, time.Now()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// evaluate runs the rule over the windows and returns the rows as hits.
func (r *Runner) evaluate(ctx context.Context, rule Rule) ([]Hit, error) {
	var hits []Hit
	err := r.wtx.InTx(ctx, nil, func(ctx context.Context, tx contract.Tx) error {
		st, err := tx.Query(ctx, rule.SQL)
		if err != nil {
			return err
		}
		defer st.Release()
		fields := st.Schema().Fields()
		keyCol := -1
		want := rule.Key
		if want == "" {
			want = "key"
		}
		for i, f := range fields {
			if strings.EqualFold(f.Name, want) {
				keyCol = i
			}
		}
		if keyCol < 0 && rule.Key != "" {
			return fmt.Errorf("the result has no column %q (KEY)", rule.Key)
		}
		for st.Next() {
			rec := st.RecordBatch()
			for i := range int(rec.NumRows()) {
				row := make(map[string]string, len(fields))
				for c, f := range fields {
					if rec.Column(c).IsValid(i) {
						row[f.Name] = strings.Clone(rec.Column(c).ValueStr(i))
					} else {
						row[f.Name] = ""
					}
				}
				key := ""
				if keyCol >= 0 {
					key = row[fields[keyCol].Name]
				}
				hits = append(hits, Hit{Key: key, Row: row})
			}
		}
		return st.Err()
	})
	return hits, err
}

// dispatch notifies the channels of one transition and queues its record.
// Transitions of the same alert are handled in order; different alerts and
// channels do not wait for each other.
func (r *Runner) dispatch(ctx context.Context, rule Rule, t Transition, wg *sync.WaitGroup) {
	wg.Add(1)
	r.serial.Do(rule.Name+"\x00"+t.Key, func() {
		defer wg.Done()
		// Finish in-flight deliveries even when the runner is stopping.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		labels := make(map[string]string, len(t.Row)+1)
		for k, v := range t.Row {
			labels[k] = v
		}
		labels["severity"] = rule.Severity
		ds := r.Channels.Send(dctx, rule.Channels, contract.Alert{
			Rule: rule.Name, Key: t.Key, State: t.State, Message: t.Message, Labels: labels, At: t.At})
		sum, ok := Summary(ds)
		observeTransition(rule, t, ok)
		e := Entry{Transition: t, Severity: rule.Severity, Channels: sum, Delivered: ok}
		if !ok {
			r.Log.Warn("alert not delivered", "rule", rule.Name, "key", t.Key, "state", t.State, "detail", sum)
		}
		if r.OnEvent != nil {
			r.OnEvent(e, ds)
		}
		r.queue <- e
	})
}

// record is the single writer of bronze.duct_alerts and the state table.
func (r *Runner) record() {
	for e := range r.queue {
		batch := []Entry{e}
	drain:
		for len(batch) < 100 {
			select {
			case e, ok := <-r.queue:
				if !ok {
					break drain
				}
				batch = append(batch, e)
			default:
				break drain
			}
		}
		var err error
		for attempt := 1; attempt <= 5; attempt++ {
			if err = r.store.Record(context.Background(), batch); err == nil {
				break
			}
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		if err != nil {
			r.Log.Error("alerts not recorded in Bronze", "count", len(batch), "err", err)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// serial runs functions in order per key, each key on its own goroutine that
// ends when its queue is empty.
type serial struct {
	mu sync.Mutex
	q  map[string][]func()
}

// Do queues f behind earlier calls with the same key.
func (s *serial) Do(key string, f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.q == nil {
		s.q = map[string][]func(){}
	}
	pending, running := s.q[key]
	s.q[key] = append(pending, f)
	if running {
		return
	}
	go func() {
		for {
			s.mu.Lock()
			fs := s.q[key]
			if len(fs) == 0 {
				delete(s.q, key)
				s.mu.Unlock()
				return
			}
			next := fs[0]
			s.q[key] = fs[1:]
			s.mu.Unlock()
			next()
		}
	}()
}
