package engineduckdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

func TestRegisterScalar(t *testing.T) {
	for _, locked := range []bool{false, true} {
		e, _ := open(t, locked)
		var calls, closed int
		var mu sync.Mutex
		err := e.RegisterScalar(context.Background(), contract.ScalarFunction{
			Name: "plus_one", ArgTypes: []string{"BIGINT", "DOUBLE"}, ReturnType: "DOUBLE",
			Call: func(_ context.Context, args []arrow.Array) (arrow.Array, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				a, b := args[0].(*array.Int64), args[1].(*array.Float64)
				bld := array.NewFloat64Builder(memory.DefaultAllocator)
				defer bld.Release()
				for i := range a.Len() {
					if i%7 == 0 { // a NULL result is allowed too
						bld.AppendNull()
						continue
					}
					bld.Append(float64(a.Value(i)) + b.Value(i) + 1)
				}
				return bld.NewArray(), nil
			},
			Close: func() error { closed++; return nil },
		})
		if err != nil {
			t.Fatalf("locked=%v register: %v", locked, err)
		}
		// 5000 rows: several chunks, a NULL argument and NULL results.
		got := scalar(t, e, `SELECT count(*) || ',' || count(plus_one(CASE WHEN i = 3 THEN NULL ELSE i END, 0.5)) || ',' || sum(plus_one(i, 0.5))
			FROM range(5000) t(i)`)
		if calls < 3 {
			t.Fatalf("calls = %d, want one per chunk", calls)
		}
		if !strings.HasPrefix(got, "5000,") {
			t.Fatalf("got %q", got)
		}
		if got := scalar(t, e, "SELECT plus_one(2, 0.5) IS NULL"); got != "true" { // row 0 -> NULL by the function
			t.Fatalf("row 0: %q", got)
		}
		// Seen from another pooled connection, concurrently.
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if got := scalar(t, e, "SELECT sum(plus_one(i + 1, 1.0)) IS NOT NULL FROM range(100) t(i)"); got != "true" {
					t.Errorf("concurrent: %q", got)
				}
			}()
		}
		wg.Wait()
		_ = e.Close()
		if closed != 1 {
			t.Fatalf("Close hook ran %d times", closed)
		}
	}
}

func TestRegisterScalarErrors(t *testing.T) {
	e, _ := open(t, false)
	ctx := context.Background()
	if err := e.RegisterScalar(ctx, contract.ScalarFunction{Name: "x", ArgTypes: []string{"JSON"}, ReturnType: "DOUBLE",
		Call: func(context.Context, []arrow.Array) (arrow.Array, error) { return nil, nil }}); err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("unsupported arg type: %v", err)
	}
	reg := func(name string, f func(context.Context, []arrow.Array) (arrow.Array, error)) {
		t.Helper()
		if err := e.RegisterScalar(ctx, contract.ScalarFunction{Name: name, ArgTypes: []string{"BIGINT"}, ReturnType: "BIGINT", Call: f}); err != nil {
			t.Fatal(err)
		}
	}
	reg("fails", func(context.Context, []arrow.Array) (arrow.Array, error) { return nil, errors.New("worker exploded") })
	reg("short", func(_ context.Context, a []arrow.Array) (arrow.Array, error) { return array.NewSlice(a[0], 0, 1), nil })
	reg("wrongtype", func(_ context.Context, a []arrow.Array) (arrow.Array, error) {
		b := array.NewFloat64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(make([]float64, a[0].Len()), nil)
		return b.NewArray(), nil
	})
	for name, want := range map[string]string{"fails": "worker exploded", "short": "returned 1 values for 3 rows", "wrongtype": "returned float64"} {
		st, err := e.Query(ctx, "SELECT "+name+"(i) FROM range(3) t(i)")
		if err == nil {
			for st.Next() {
			}
			err = st.Err()
			st.Release()
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", name, err, want)
		}
	}
}
