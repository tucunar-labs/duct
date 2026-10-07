// Package txretry retries units of work that lost a race against another
// writer of the same catalog. With a shared (Postgres) catalog DuckLake
// retries snapshot-id races by itself, but a logical conflict (two
// transactions touching the same table or schema) fails the loser with
// "Transaction conflict"; running the whole unit again on the new snapshot
// is the correct answer.
//
// Only conflicts are retried. Expectation failures, SQL errors, permission
// errors and cancellation are returned at once. The unit must be safe to run
// again from scratch: Do gives no guarantee about what a failed attempt left
// behind other than "its transaction was rolled back".
package txretry

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"
)

// conflictFragments are the lower-case messages that mean "another
// transaction won; try again": DuckLake logical conflicts, the Postgres
// serialization family (the catalog may surface them verbatim) and SQLite's
// busy errors. busy_timeout does not cover a read transaction that must
// upgrade to write after another writer committed (SQLITE_BUSY_SNAPSHOT):
// SQLite fails at once and only a retry of the whole transaction helps.
var conflictFragments = []string{
	"transaction conflict",
	"database is locked",
	"database is busy",
	"could not serialize access",
	"serialization failure",
	"deadlock detected",
	"concurrent update",
}

// Retryable reports whether err is a commit conflict worth retrying.
func Retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	m := strings.ToLower(err.Error())
	for _, f := range conflictFragments {
		if strings.Contains(m, f) {
			return true
		}
	}
	return false
}

// Policy bounds the retries. The zero value means Default.
type Policy struct {
	Attempts int           // total tries, first included
	Base     time.Duration // wait before the 2nd try; doubles each time
	Max      time.Duration // cap of one wait
}

// Default: 8 tries, 50 ms doubling to a 2 s cap, with full jitter: enough
// for a handful of writers contending on one table.
var Default = Policy{Attempts: 8, Base: 50 * time.Millisecond, Max: 2 * time.Second}

// Do runs f until it succeeds, fails with a non-retryable error, the
// attempts run out (the last error is returned) or ctx ends (the last error
// from f is returned if there is one, else ctx.Err()). Waits use full jitter:
// a uniform delay in [0, min(Max, Base*2^n)], so contenders spread out.
func Do(ctx context.Context, p Policy, f func(ctx context.Context) error) error {
	return do(ctx, p, f, rand.Float64, time.NewTimer)
}

func do(ctx context.Context, p Policy, f func(ctx context.Context) error, rnd func() float64, newTimer func(time.Duration) *time.Timer) error {
	if p.Attempts <= 0 {
		p = Default
	}
	wait := p.Base
	var err error
	for i := 0; i < p.Attempts; i++ {
		if cerr := ctx.Err(); cerr != nil {
			if err != nil {
				return err
			}
			return cerr
		}
		if err = f(ctx); err == nil || !Retryable(err) {
			return err
		}
		if i == p.Attempts-1 {
			break
		}
		t := newTimer(time.Duration(rnd() * float64(wait)))
		select {
		case <-ctx.Done():
			t.Stop()
			return err
		case <-t.C:
		}
		if wait *= 2; p.Max > 0 && wait > p.Max {
			wait = p.Max
		}
	}
	return err
}
