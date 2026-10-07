package alerts

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

// DefaultLeaseTTL is Runner.LeaseTTL when unset.
const DefaultLeaseTTL = 15 * time.Second

// ttlOf is the lease of a rule: never shorter than three evaluations.
func (r *Runner) ttlOf(rule Rule) time.Duration { return max(r.LeaseTTL, 3*rule.Every) }

func (r *Runner) orphanAge(ttl time.Duration) time.Duration {
	if r.OrphanAfter > 0 {
		return r.OrphanAfter
	}
	return max(4*ttl, time.Minute)
}

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
	// Instance names this process in the shared state (leases, who confirmed
	// an alert). Unique among the instances running at once; default: random.
	Instance string
	// LeaseTTL is how long a claim or a confirmation holds (default 15 s; a
	// rule that evaluates every E uses at least 3E). A dead instance is
	// replaced after about this long.
	LeaseTTL time.Duration
	// OrphanAfter is how long an alert may go unconfirmed by every instance
	// before one of them resolves it as gone (default: 4 leases, at least 60 s:
	// longer than a Kafka rebalance, or a dead instance's partitions would be
	// resolved just before the survivor starts seeing their data again).
	OrphanAfter time.Duration
	// OnEvent, if set, is called after a transition was delivered (before it
	// is recorded in Bronze).
	OnEvent func(Entry, []Delivery)

	wtx     contract.Transactor
	coord   *Coord
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
	if r.Instance == "" {
		host, _ := os.Hostname()
		b := make([]byte, 3)
		_, _ = rand.Read(b)
		r.Instance = fmt.Sprintf("%s-%d-%x", host, os.Getpid(), b)
	}
	if r.LeaseTTL <= 0 {
		r.LeaseTTL = DefaultLeaseTTL
	}
	r.coord = &Coord{Eng: r.Lake, ID: r.Instance}
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
	ttl := r.ttlOf(rule)
	var lastErr string
	var lastLog time.Time
	confirmed := map[string]time.Time{}
	lastSeen := map[string]time.Time{} // key -> last evaluation that had it in the result
	nextSweep := time.Now().Add(ttl)
	for {
		hits, err := r.evaluate(ctx, rule)
		switch {
		case ctx.Err() != nil:
			return
		case err == nil:
			lastErr = ""
			now := time.Now()
			for _, t := range tr.Apply(now, hits) {
				r.handle(ctx, rule, t, ttl, lastSeen[t.Key], deliveries)
				if t.State == contract.AlertFiring {
					confirmed[t.Key] = now
				}
				if t.State == contract.AlertResolved {
					delete(confirmed, t.Key)
				}
			}
			// Tell the shared state this instance still sees the alerts firing,
			// so no other instance resolves them (or fires them again).
			for _, h := range tr.hitsFiring(hits) {
				if now.Sub(confirmed[h.Key]) < ttl/3 {
					continue
				}
				confirmed[h.Key] = now
				t := tr.transition(contract.AlertFiring, h.Key, h.Row, now, tr.firing[h.Key].Since)
				notify, err := r.coord.Confirm(ctx, rule, t, ttl)
				switch {
				case err != nil && ctx.Err() == nil:
					r.Log.Warn("alert coordination: confirm failed", "rule", rule.Name, "key", h.Key, "err", err)
				case notify:
					r.dispatch(ctx, rule, t, ttl, deliveries)
				}
			}
			for _, h := range hits {
				lastSeen[h.Key] = now
			}
			for k := range confirmed {
				if !tr.IsFiring(k) {
					delete(confirmed, k)
				}
			}
			for k := range lastSeen {
				if !tr.IsFiring(k) && now.Sub(lastSeen[k]) > ttl {
					delete(lastSeen, k)
				}
			}
			if now.After(nextSweep) {
				nextSweep = now.Add(ttl)
				r.sweepOrphans(ctx, rule, tr, ttl, deliveries)
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

// hitsFiring are the hits whose key is firing (not one that just fired).
func (t *Tracker) hitsFiring(hits []Hit) []Hit {
	var out []Hit
	seen := map[string]bool{}
	for _, h := range hits {
		if t.IsFiring(h.Key) && !seen[h.Key] {
			seen[h.Key] = true
			out = append(out, h)
		}
	}
	return out
}

// handle decides, with the shared state, whether this instance sends the
// notification of a transition the local tracker produced. When another
// instance already did (or owns the alert now), nothing is sent and the local
// tracker simply goes along. The claim runs in the alert's own queue, after
// the delivery of the previous transition of the same alert has closed its
// claim (a resolved still going out must not look like "already firing").
func (r *Runner) handle(ctx context.Context, rule Rule, t Transition, ttl time.Duration, lastSeen time.Time, wg *sync.WaitGroup) {
	wg.Add(1)
	r.serial.Do(rule.Name+"\x00"+t.Key, func() {
		defer wg.Done()
		// Transitions already produced are settled even when the runner stops.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		var won bool
		var err error
		switch t.State {
		case contract.AlertFiring:
			won, err = r.coord.ClaimFiring(cctx, rule, t, ttl)
		case StateReminder:
			won, err = r.coord.ClaimReminder(cctx, rule, t.Key)
		default:
			won, err = r.coord.ClaimResolve(cctx, rule, t.Key, ttl, lastSeen)
		}
		if err != nil {
			// Cannot coordinate: better a duplicate than a lost alert.
			r.Log.Warn("alert coordination failed; notifying anyway", "rule", rule.Name, "key", t.Key, "state", t.State, "err", err)
			won = true
		}
		if !won {
			r.Log.Debug("transition handled by another instance", "rule", rule.Name, "key", t.Key, "state", t.State)
			return
		}
		r.deliver(ctx, rule, t, ttl)
	})
}

// sweepOrphans resolves the alerts of rule that nobody has seen firing for two
// leases and this instance does not track: their owner died or the data that
// fired them went away.
func (r *Runner) sweepOrphans(ctx context.Context, rule Rule, tr *Tracker, ttl time.Duration, deliveries *sync.WaitGroup) {
	stale, err := r.coord.Stale(ctx, rule.Name, r.orphanAge(ttl))
	if err != nil {
		if ctx.Err() == nil {
			r.Log.Warn("alert coordination: stale scan failed", "rule", rule.Name, "err", err)
		}
		return
	}
	for _, f := range stale {
		if tr.IsFiring(f.Key) {
			continue
		}
		if won, err := r.coord.ClaimResolve(ctx, rule, f.Key, ttl, time.Time{}); err != nil || !won {
			continue
		}
		now := time.Now()
		r.dispatch(ctx, rule, tr.transition(contract.AlertResolved, f.Key, f.Row, now, f.Since), ttl, deliveries)
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

// dispatch notifies the channels of one transition (unless it is silenced),
// completes the claim in the shared state and queues its record. Transitions of
// the same alert are handled in order; different alerts and channels do not
// wait for each other.
func (r *Runner) dispatch(ctx context.Context, rule Rule, t Transition, ttl time.Duration, wg *sync.WaitGroup) {
	wg.Add(1)
	r.serial.Do(rule.Name+"\x00"+t.Key, func() {
		defer wg.Done()
		r.deliver(ctx, rule, t, ttl)
	})
}

// deliver sends a transition this instance has claimed.
func (r *Runner) deliver(ctx context.Context, rule Rule, t Transition, ttl time.Duration) {
	{
		// Finish in-flight deliveries even when the runner is stopping.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		labels := make(map[string]string, len(t.Row)+2)
		for k, v := range t.Row {
			labels[k] = v
		}
		labels["severity"] = rule.Severity
		state, msg := t.State, t.Message
		if state == StateReminder {
			state, msg = contract.AlertFiring, "[reminder] "+t.Message
			labels["reminder"] = "true"
		}
		var (
			ds  []Delivery
			sum string
			ok  bool
		)
		if sil, err := SilencedBy(dctx, r.Lake, rule.Name, t.Key, time.Now()); err != nil {
			r.Log.Warn("silences not readable; notifying", "rule", rule.Name, "err", err)
		} else if sil != nil {
			sum, ok = fmt.Sprintf("silenced #%d until %s (%s)", sil.ID, sil.Expires.Format(time.RFC3339), sil.Reason), true
			silencedTotal.Inc(string(t.State))
		}
		if sum == "" {
			ds = r.Channels.Send(dctx, rule.Channels, contract.Alert{
				Rule: rule.Name, Key: t.Key, State: state, Message: msg, Labels: labels, At: t.At})
			sum, ok = Summary(ds)
			observeTransition(rule, t, ok)
		}
		switch t.State { // the notification went out: close the claim
		case contract.AlertFiring:
			if err := r.coord.FiringDone(dctx, rule, t.Key, ttl); err != nil {
				r.Log.Warn("alert coordination: firing not closed", "rule", rule.Name, "key", t.Key, "err", err)
			}
		case contract.AlertResolved:
			if err := r.coord.ResolveDone(dctx, rule, t.Key); err != nil {
				r.Log.Warn("alert coordination: resolve not closed", "rule", rule.Name, "key", t.Key, "err", err)
			}
		}
		t.Message = msg
		e := Entry{Transition: t, Severity: rule.Severity, Channels: sum, Delivered: ok}
		if !ok {
			r.Log.Warn("alert not delivered", "rule", rule.Name, "key", t.Key, "state", t.State, "detail", sum)
		}
		if r.OnEvent != nil {
			r.OnEvent(e, ds)
		}
		r.queue <- e
	}
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
