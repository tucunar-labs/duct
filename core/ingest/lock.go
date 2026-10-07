package ingest

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/alonsoburon/duct/core/txretry"
	"math/rand/v2"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Per-source lock. The incremental watermark lives in Duct state, the rows in
// the lake: two catalogs, so no single transaction can cover both and two
// processes could read the same watermark and append the same rows. The fix
// is to serialize runs of the same source with a lease row in
// duct.duct_ingest_state itself (no extra table, no migration):
//
//   - name = "lock:<source>" (a source name never contains ':', see nameRe),
//     watermark = owner id, updated_at = last heartbeat.
//   - Acquire is INSERT; the primary key on name makes exactly one succeed,
//     on SQLite and on Postgres alike. A row whose heartbeat is older than
//     leaseTTL belongs to a dead process and is deleted first.
//   - A goroutine renews updated_at every leaseTTL/3 while the run lasts.
//   - Release is DELETE ... WHERE owner = me.
//
// Runs of different sources never contend. Timestamps are computed by the
// process (UTC), so clocks of machines sharing a catalog must agree within a
// small fraction of leaseTTL.
var (
	leaseTTL  = 2 * time.Minute
	lockWait  = 15 * time.Minute // how long a run queues behind another of the same source
	lockPoll  = 100 * time.Millisecond
	lockPollM = 2 * time.Second
)

const lockPrefix = "lock:"

// Lease is a held per-source lock; Release it when the run ends.
type Lease struct {
	eng   contract.Engine
	key   string // state row name
	owner string
	stop  chan struct{}
	done  chan struct{}
}

func ts(t time.Time) string { return fmt.Sprintf("make_timestamp(%d)", t.UnixMicro()) }

func sqlQ(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Acquire blocks until it holds the lock of source name, ctx ends or
// lockWait passes.
func Acquire(ctx context.Context, eng contract.Engine, name string) (*Lease, error) {
	var b [8]byte
	_, _ = cryptorand.Read(b[:])
	l := &Lease{eng: eng, key: lockPrefix + name, owner: hex.EncodeToString(b[:]), stop: make(chan struct{}), done: make(chan struct{})}
	deadline := time.Now().Add(lockWait)
	wait := lockPoll
	for {
		now := time.Now().UTC()
		// Take over a lease nobody renews.
		if err := txretry.Do(ctx, txretry.Policy{}, func(ctx context.Context) error {
			return eng.Exec(ctx, "DELETE FROM duct.duct_ingest_state WHERE name = "+sqlQ(l.key)+
				" AND CAST(updated_at AS TIMESTAMP) < "+ts(now.Add(-leaseTTL)))
		}); err != nil {
			return nil, fmt.Errorf("ingest lock %s: %w", name, err)
		}
		// A busy SQLite state DB is retried; a duplicate key is the lock being held.
		err := txretry.Do(ctx, txretry.Policy{}, func(ctx context.Context) error {
			return eng.Exec(ctx, "INSERT INTO duct.duct_ingest_state (name, watermark, rows_loaded, updated_at) VALUES ("+
				sqlQ(l.key)+", "+sqlQ(l.owner)+", 0, "+ts(now)+")")
		})
		if err == nil {
			go l.heartbeat()
			return l, nil
		}
		// Not acquired: is it because someone else holds it, or a real error?
		holder, herr := holderOf(ctx, eng, l.key)
		if herr != nil || (holder == "" && !isDuplicate(err)) {
			return nil, fmt.Errorf("ingest lock %s: %w", name, err)
		}
		// (holder == "" with a duplicate-key error: it was released between
		// our INSERT and the SELECT; just try again.)
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("ingest %s: another run of the same source holds the lock after %s", name, lockWait)
		}
		t := time.NewTimer(time.Duration(rand.Float64()*float64(wait)) + time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		if wait *= 2; wait > lockPollM {
			wait = lockPollM
		}
	}
}

func isDuplicate(err error) bool {
	m := strings.ToLower(err.Error())
	return strings.Contains(m, "duplicate key") || strings.Contains(m, "unique constraint") || strings.Contains(m, "primary key")
}

func holderOf(ctx context.Context, eng contract.Engine, key string) (string, error) {
	st, err := eng.Query(ctx, "SELECT watermark FROM duct.duct_ingest_state WHERE name = ?", key)
	if err != nil {
		return "", err
	}
	defer st.Release()
	var out string
	for st.Next() {
		rec := st.RecordBatch()
		if rec.NumRows() > 0 && rec.Column(0).IsValid(0) && out == "" {
			out = strings.Clone(rec.Column(0).ValueStr(0))
		}
	}
	return out, st.Err()
}

func (l *Lease) heartbeat() {
	defer close(l.done)
	t := time.NewTicker(leaseTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), leaseTTL/3)
			_ = l.eng.Exec(ctx, "UPDATE duct.duct_ingest_state SET updated_at = "+ts(time.Now().UTC())+
				" WHERE name = "+sqlQ(l.key)+" AND watermark = "+sqlQ(l.owner))
			cancel()
		}
	}
}

// Release frees the lock (own context: it must go even if the run's ctx was cancelled).
func (l *Lease) Release(ctx context.Context) error {
	close(l.stop)
	<-l.done
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	err := txretry.Do(rctx, txretry.Policy{}, func(ctx context.Context) error {
		return l.eng.Exec(rctx, "DELETE FROM duct.duct_ingest_state WHERE name = "+sqlQ(l.key)+" AND watermark = "+sqlQ(l.owner))
	})
	if err != nil {
		return errors.Join(errors.New("release ingest lock (it expires by itself)"), err)
	}
	return nil
}
