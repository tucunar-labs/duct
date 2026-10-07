package contracttest

import (
	"context"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// TaskFixture is a fresh, uninitialised TaskType plus params for three
// scenarios against test doubles: OK succeeds quickly, Fail returns an error,
// Block runs "forever" until the context ends.
type TaskFixture struct {
	Task  contract.TaskType
	OK    contract.Config
	Fail  contract.Config
	Block contract.Config
}

// TaskTypeFactory builds a fixture; it registers its own cleanup via t.Cleanup.
type TaskTypeFactory func(t *testing.T) TaskFixture

// TestTaskType runs the TaskType conformance suite.
func TestTaskType(t *testing.T, factory TaskTypeFactory) {
	t.Helper()
	ready := func(t *testing.T) TaskFixture {
		t.Helper()
		f := factory(t)
		if f.Task.Name() == "" {
			t.Fatal("Name must be non-empty")
		}
		if err := f.Task.Init(context.Background(), contract.Config{}); err != nil {
			t.Fatalf("Init: %v", err)
		}
		t.Cleanup(func() { _ = f.Task.Close() })
		return f
	}

	t.Run("success", func(t *testing.T) {
		f := ready(t)
		if _, err := f.Task.Run(context.Background(), contract.TaskSpec{Name: "t", Params: f.OK}); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
	t.Run("failure is an error", func(t *testing.T) {
		f := ready(t)
		if _, err := f.Task.Run(context.Background(), contract.TaskSpec{Name: "t", Params: f.Fail}); err == nil {
			t.Fatal("Run must fail")
		}
	})
	t.Run("cancellation stops the work", func(t *testing.T) {
		f := ready(t)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := f.Task.Run(ctx, contract.TaskSpec{Name: "t", Params: f.Block})
		if err == nil {
			t.Fatal("Run must return the context error")
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("Run took %s to stop", d)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		f := ready(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := f.Task.Run(ctx, contract.TaskSpec{Name: "t", Params: f.OK}); err == nil {
			t.Fatal("Run with a cancelled context must fail")
		}
	})
	t.Run("close is idempotent and safe without Init", func(t *testing.T) {
		f := factory(t)
		if err := f.Task.Close(); err != nil {
			t.Fatalf("Close without Init: %v", err)
		}
		if err := f.Task.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
	})
}
