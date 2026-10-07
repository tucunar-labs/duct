package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alonsoburon/duct/core/ingest"
)

// TestIngestLock: one holder at a time per source, independent sources do not
// block each other, and a lease nobody renews is taken over.
func TestIngestLock(t *testing.T) {
	e := openRunEngine(t)
	ctx := context.Background()

	var inside, maxInside atomic.Int32
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := ingest.Acquire(ctx, e, "same")
			if err != nil {
				t.Error(err)
				return
			}
			n := inside.Add(1)
			for {
				m := maxInside.Load()
				if n <= m || maxInside.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			inside.Add(-1)
			if err := l.Release(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if maxInside.Load() != 1 {
		t.Fatalf("%d holders at once", maxInside.Load())
	}
	if n := scalar(t, e, "SELECT count(*) FROM duct.duct_ingest_state WHERE name LIKE 'lock:%'"); n != 0 {
		t.Fatalf("%d lock rows left", n)
	}

	a, err := ingest.Acquire(ctx, e, "a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ingest.Acquire(ctx, e, "b") // different source: immediate
	if err != nil {
		t.Fatal(err)
	}
	// A waiter gives up with its context while a holds the lock.
	cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := ingest.Acquire(cctx, e, "a"); err == nil {
		t.Fatal("acquired a held lock")
	}
	// Dead holder: a stale heartbeat is taken over.
	if err := e.Exec(ctx, "UPDATE duct.duct_ingest_state SET updated_at = TIMESTAMP '2000-01-01' WHERE name = 'lock:a'"); err != nil {
		t.Fatal(err)
	}
	a2, err := ingest.Acquire(ctx, e, "a")
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	_ = a.Release(ctx) // the old owner must not free the new owner's row
	if n := scalar(t, e, "SELECT count(*) FROM duct.duct_ingest_state WHERE name = 'lock:a'"); n != 1 {
		t.Fatalf("old owner released the new lease (%d rows)", n)
	}
	_ = a2.Release(ctx)
	_ = b.Release(ctx)
}
