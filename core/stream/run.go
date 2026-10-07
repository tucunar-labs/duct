package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/metrics"
	"github.com/alonsoburon/duct/core/txretry"
)

// BatchInfo describes one micro-batch after its offsets were committed.
type BatchInfo struct {
	Stream   string
	Rows     int           // rows landed in the target table
	Rejected int           // messages diverted to <target>_rejects
	Skipped  int           // redelivered messages already in the table
	Elapsed  time.Duration // write time (all attempts)
	Age      time.Duration // oldest message timestamp -> commit
}

var (
	streamRows    = metrics.Default().Counter("duct_stream_rows_total", "Rows landed in Bronze, by stream.", "stream")
	streamRejects = metrics.Default().Counter("duct_stream_rejects_total", "Messages set aside in <target>_rejects, by stream.", "stream")
	streamBatches = metrics.Default().Counter("duct_stream_batches_total", "Micro-batches committed, by stream.", "stream")
	streamLag     = metrics.Default().Gauge("duct_stream_lag_seconds", "Age of the oldest message of the last committed batch (Kafka timestamp to commit), by stream.", "stream")
	streamLast    = metrics.Default().Gauge("duct_stream_last_commit_timestamp_seconds", "Unix time of the last committed batch, by stream.", "stream")
)

func observeBatch(i BatchInfo) {
	streamRows.Add(float64(i.Rows), i.Stream)
	streamRejects.Add(float64(i.Rejected), i.Stream)
	streamBatches.Inc(i.Stream)
	streamLag.Set(i.Age.Seconds(), i.Stream)
	streamLast.Set(float64(time.Now().Unix()), i.Stream)
}

// Writer is one stream: consumer -> micro-batch -> DuckLake -> offset commit.
type Writer struct {
	Eng      contract.Engine
	Cfg      Config
	Consumer Consumer
	Log      *slog.Logger
	// OnBatch, if set, is called after every committed batch.
	OnBatch func(BatchInfo)

	tr   contract.Transactor
	mu   sync.Mutex
	high map[int32]int64 // highest offset known to be in the target, per partition
}

// Run consumes until ctx ends (returning nil) or something unrecoverable
// happens. Delivery semantics:
//
//   - A batch is written in one lake transaction and the Kafka offsets are
//     committed only after that transaction committed. A crash in between
//     redelivers the batch: at-least-once from Kafka.
//   - Landed rows carry _kafka_partition/_kafka_offset. Before writing, the
//     writer drops messages whose offset is not above the highest one already
//     in the table for that partition (read once per assignment), so a
//     redelivery does not duplicate rows: effectively once into Bronze. Only a
//     producer that publishes the same event twice (different offsets) can
//     still create duplicates; deduplicate those downstream.
//   - A failing write (catalog down, type conflict...) is retried with
//     backoff and never skipped: the stream stalls and the data waits in
//     Kafka. Only messages that are not JSON objects are set aside, in
//     <target>_rejects.
func (w *Writer) Run(ctx context.Context) error {
	if w.Log == nil {
		w.Log = slog.Default()
	}
	log := w.Log.With("stream", w.Cfg.Name)
	tr, ok := w.Eng.(contract.Transactor)
	if !ok {
		return errors.New("engine does not implement contract.Transactor")
	}
	w.tr = tr
	w.high = map[int32]int64{}
	if err := ingest.EnsureSchema(ctx, w.Eng, BronzeSchema); err != nil {
		return err
	}
	if n := w.Cfg.InlineRows; n > 0 {
		if err := w.Eng.Exec(ctx, fmt.Sprintf("CALL lake.set_option('data_inlining_row_limit', %d, schema => '%s')", n, BronzeSchema)); err != nil {
			return fmt.Errorf("set data_inlining_row_limit: %w", err)
		}
	}
	w.Consumer.OnAssigned(func(topic string, parts []int32) {
		if topic != w.Cfg.Topic {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, p := range parts {
			delete(w.high, p) // another member may have written since: re-read
		}
	})
	for ctx.Err() == nil {
		recs, err := w.collect(ctx, log)
		if err != nil || len(recs) == 0 {
			continue
		}
		info, err := w.handle(ctx, log, recs)
		if err != nil {
			return err // only context cancellation gets here
		}
		observeBatch(info)
		if w.OnBatch != nil {
			w.OnBatch(info)
		}
	}
	return nil
}

// collect gathers one micro-batch: it ends at MaxRows messages or MaxInterval
// after the first one.
func (w *Writer) collect(ctx context.Context, log *slog.Logger) ([]Record, error) {
	var recs []Record
	var deadline time.Time
	for len(recs) < w.Cfg.Batch.MaxRows {
		pctx, cancel := ctx, context.CancelFunc(func() {})
		if !deadline.IsZero() {
			pctx, cancel = context.WithDeadline(ctx, deadline)
		}
		got, err := w.Consumer.Poll(pctx, w.Cfg.Batch.MaxRows-len(recs))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || len(recs) > 0 {
				return recs, nil
			}
			log.Warn("kafka poll failed", "err", err)
			sleep(ctx, time.Second)
			continue
		}
		if len(recs) == 0 {
			deadline = time.Now().Add(w.Cfg.Batch.MaxInterval)
		}
		recs = append(recs, got...)
	}
	return recs, nil
}

// handle writes one batch (retrying until it works or ctx ends) and commits
// its offsets.
func (w *Writer) handle(ctx context.Context, log *slog.Logger, recs []Record) (BatchInfo, error) {
	info := BatchInfo{Stream: w.Cfg.Name}
	start := time.Now()
	oldest := recs[0].Timestamp
	for _, r := range recs {
		if r.Timestamp.Before(oldest) {
			oldest = r.Timestamp
		}
	}
	wait := 250 * time.Millisecond
	for {
		rows, rejected, skipped, err := w.write(ctx, recs)
		if err == nil {
			info.Rows, info.Rejected, info.Skipped = rows, rejected, skipped
			break
		}
		if ctx.Err() != nil {
			return info, ctx.Err()
		}
		log.Warn("batch not written, retrying (offsets stay uncommitted)", "err", err, "in", wait)
		sleep(ctx, wait)
		if wait *= 2; wait > 30*time.Second {
			wait = 30 * time.Second
		}
	}
	info.Elapsed = time.Since(start)
	w.advance(recs)
	// The data is safe in the lake; a failed commit only means redelivery
	// (filtered out above) and the next successful commit covers these offsets.
	for attempt := 0; attempt < 3; attempt++ {
		err := w.Consumer.Commit(ctx, recs)
		if err == nil || ctx.Err() != nil {
			break
		}
		log.Warn("offset commit failed", "err", err, "attempt", attempt+1)
		sleep(ctx, 200*time.Millisecond)
	}
	info.Age = time.Since(oldest)
	log.Debug("batch committed", "rows", info.Rows, "rejected", info.Rejected, "skipped", info.Skipped, "elapsed", info.Elapsed)
	return info, nil
}

// write lands the batch in one lake transaction.
func (w *Writer) write(ctx context.Context, recs []Record) (rows, rejected, skipped int, err error) {
	fresh := make([]Record, 0, len(recs))
	for _, r := range recs {
		hw, err := w.highWater(ctx, r.Partition)
		if err != nil {
			return 0, 0, 0, err
		}
		if r.Offset <= hw {
			skipped++
			continue
		}
		fresh = append(fresh, r)
	}
	if len(fresh) == 0 {
		return 0, 0, skipped, nil
	}
	d, err := Decode(fresh, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	if d.Rec != nil {
		defer d.Rec.Release()
	}
	target := LakeTable(w.Cfg.Target)
	err = txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		var input contract.Stream
		if d.Rec != nil {
			rr, err := Reader(d.Rec)
			if err != nil {
				return err
			}
			defer rr.Release()
			input = rr
		}
		return w.tr.InTx(ctx, input, func(ctx context.Context, tx contract.Tx) error {
			n, err := Land(ctx, tx, target, d)
			if err != nil {
				return err
			}
			rows = int(n)
			return w.reject(ctx, tx, d.Rejects)
		})
	})
	return rows, len(d.Rejects), skipped, err
}

const rejectsDDL = `CREATE TABLE IF NOT EXISTS %s (
	_kafka_partition INTEGER, _kafka_offset BIGINT, _kafka_ts TIMESTAMPTZ,
	raw VARCHAR, error VARCHAR, rejected_at TIMESTAMPTZ)`

func (w *Writer) reject(ctx context.Context, tx contract.Tx, rejects []Reject) error {
	if len(rejects) == 0 {
		return nil
	}
	t := Table{Schema: BronzeSchema, Name: w.Cfg.Table() + "_rejects"}
	if err := tx.Exec(ctx, fmt.Sprintf(rejectsDDL, t.SQL())); err != nil {
		return err
	}
	for _, r := range rejects {
		err := tx.Exec(ctx, "INSERT INTO "+t.SQL()+" VALUES (?, ?, ?, ?, ?, now())",
			r.Rec.Partition, r.Rec.Offset, r.Rec.Timestamp.UTC(),
			strings.ToValidUTF8(string(r.Rec.Value), "�"), r.Err)
		if err != nil {
			return err
		}
	}
	return nil
}

// highWater is the highest offset of a partition already in the target (-1 if
// none), read from the table once per assignment.
func (w *Writer) highWater(ctx context.Context, p int32) (int64, error) {
	w.mu.Lock()
	hw, ok := w.high[p]
	w.mu.Unlock()
	if ok {
		return hw, nil
	}
	hw = -1
	t := LakeTable(w.Cfg.Target)
	tx := contract.Tx(w.Eng)
	exists, err := t.Exists(ctx, tx)
	if err != nil {
		return 0, err
	}
	if exists {
		v, err := scalar(ctx, tx, fmt.Sprintf("SELECT coalesce(max(%s), -1) FROM %s WHERE %s = %d", ColOffset, t.SQL(), ColPartition, p))
		if err != nil {
			return 0, err
		}
		if _, err := fmt.Sscan(v, &hw); err != nil {
			return 0, fmt.Errorf("high-water mark %q: %w", v, err)
		}
	}
	w.mu.Lock()
	w.high[p] = hw
	w.mu.Unlock()
	return hw, nil
}

func (w *Writer) advance(recs []Record) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range recs {
		if cur, ok := w.high[r.Partition]; !ok || r.Offset > cur {
			w.high[r.Partition] = r.Offset
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
