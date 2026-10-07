package txretry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var fast = Policy{Attempts: 5, Base: time.Millisecond, Max: 4 * time.Millisecond}

func TestRetryable(t *testing.T) {
	yes := []string{
		"Transaction conflict - attempting to insert into table with index 3 but it has been altered",
		"ERROR: could not serialize access due to concurrent update",
		"deadlock detected",
	}
	no := []string{
		"expectation FAIL UPDATE violated: x; previous version kept",
		"Parser Error: syntax error at or near FROM",
		"permission denied for table x",
		"Catalog Error: Table with name t does not exist",
		"Constraint Error: duplicate key",
	}
	for _, s := range yes {
		if !Retryable(errors.New(s)) || !Retryable(fmt.Errorf("wrap: %w", errors.New(s))) {
			t.Errorf("should retry: %s", s)
		}
	}
	for _, s := range no {
		if Retryable(errors.New(s)) {
			t.Errorf("must not retry: %s", s)
		}
	}
	if Retryable(nil) || Retryable(context.Canceled) || Retryable(fmt.Errorf("Transaction conflict: %w", context.Canceled)) {
		t.Error("nil and cancellation are not retryable")
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	n := 0
	err := Do(context.Background(), fast, func(context.Context) error {
		if n++; n < 3 {
			return errors.New("Transaction conflict")
		}
		return nil
	})
	if err != nil || n != 3 {
		t.Fatalf("err=%v attempts=%d", err, n)
	}
}

func TestDoStopsOnPermanentError(t *testing.T) {
	n := 0
	boom := errors.New("syntax error")
	if err := Do(context.Background(), fast, func(context.Context) error { n++; return boom }); !errors.Is(err, boom) || n != 1 {
		t.Fatalf("err=%v attempts=%d", err, n)
	}
}

func TestDoGivesUpAfterAttempts(t *testing.T) {
	n := 0
	err := Do(context.Background(), fast, func(context.Context) error { n++; return errors.New("Transaction conflict") })
	if err == nil || n != fast.Attempts {
		t.Fatalf("err=%v attempts=%d", err, n)
	}
}

func TestDoCancelDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	slow := Policy{Attempts: 10, Base: time.Hour, Max: time.Hour}
	n := 0
	done := make(chan error, 1)
	go func() {
		done <- do(ctx, slow, func(context.Context) error {
			n++
			cancel()
			return errors.New("Transaction conflict")
		}, func() float64 { return 1 }, time.NewTimer)
	}()
	select {
	case err := <-done:
		if err == nil || n != 1 {
			t.Fatalf("err=%v attempts=%d", err, n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not honour cancellation")
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	var waits []time.Duration
	nt := func(d time.Duration) *time.Timer { waits = append(waits, d); return time.NewTimer(0) }
	_ = do(context.Background(), Policy{Attempts: 6, Base: 10 * time.Millisecond, Max: 50 * time.Millisecond},
		func(context.Context) error { return errors.New("Transaction conflict") },
		func() float64 { return 1 }, nt)
	want := []time.Duration{10, 20, 40, 50, 50}
	if len(waits) != len(want) {
		t.Fatalf("waits=%v", waits)
	}
	for i, w := range want {
		if waits[i] != w*time.Millisecond {
			t.Fatalf("waits=%v", waits)
		}
	}
}
