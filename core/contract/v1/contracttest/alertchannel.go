// Package contracttest provides reusable conformance suites for the
// contract/v1 interfaces. Every implementation must pass its suite.
package contracttest

import (
	"context"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// AlertChannelFactory returns a fresh, uninitialised channel and a Config
// that makes it work against a test double (e.g. an httptest server). The
// factory registers its own cleanup via t.Cleanup.
type AlertChannelFactory func(t *testing.T) (contract.AlertChannel, contract.Config)

// TestAlertChannel runs the AlertChannel conformance suite.
func TestAlertChannel(t *testing.T, factory AlertChannelFactory) {
	t.Helper()

	newReady := func(t *testing.T) contract.AlertChannel {
		t.Helper()
		ch, cfg := factory(t)
		if ch.Name() == "" {
			t.Fatal("Name must be non-empty")
		}
		if err := ch.Init(context.Background(), cfg); err != nil {
			t.Fatalf("Init: %v", err)
		}
		t.Cleanup(func() { _ = ch.Close() })
		return ch
	}

	t.Run("notify firing and resolved", func(t *testing.T) {
		ch := newReady(t)
		for _, st := range []contract.AlertState{contract.AlertFiring, contract.AlertResolved} {
			a := contract.Alert{Rule: "r", Key: "k", State: st, Message: "m", At: time.Now()}
			if err := ch.Notify(context.Background(), a); err != nil {
				t.Fatalf("Notify(%s): %v", st, err)
			}
		}
	})

	t.Run("zero optional fields", func(t *testing.T) {
		ch := newReady(t)
		if err := ch.Notify(context.Background(), contract.Alert{Rule: "r", State: contract.AlertFiring}); err != nil {
			t.Fatalf("Notify with zero fields: %v", err)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ch := newReady(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := ch.Notify(ctx, contract.Alert{Rule: "r", State: contract.AlertFiring}); err == nil {
			t.Fatal("Notify with cancelled ctx must return an error")
		}
	})

	t.Run("close idempotent", func(t *testing.T) {
		ch, cfg := factory(t)
		if err := ch.Close(); err != nil {
			t.Fatalf("Close without Init: %v", err)
		}
		if err := ch.Init(context.Background(), cfg); err != nil {
			t.Fatalf("Init: %v", err)
		}
		for range 2 {
			if err := ch.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}
	})
}
