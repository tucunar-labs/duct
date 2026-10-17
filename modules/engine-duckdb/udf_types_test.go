package engineduckdb

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// TestUDFComplexTypes: DATE, TIMESTAMP, DECIMAL, lists and structs cross the
// Go <-> Arrow boundary both ways (identity functions), with NULLs at every level.
func TestUDFComplexTypes(t *testing.T) {
	e, _ := open(t, false)
	ctx := context.Background()
	cases := []struct{ typ, expr, want string }{
		{"DATE", "DATE '2026-01-31'", "2026-01-31"},
		{"TIMESTAMP", "TIMESTAMP '2026-01-31 12:30:45.123456'", "2026-01-31T12:30:45.123456"},
		{"DECIMAL(10,2)", "CAST(-12345.67 AS DECIMAL(10,2))", "-12345.67"},
		{"DECIMAL(38,10)", "CAST('1234567890123456789.0123456789' AS DECIMAL(38,10))", "1234567890123456789.0123456789"},
		{"BIGINT[]", "[1, NULL, 3]", "[1,null,3]"},
		{"VARCHAR[]", "['a', NULL, 'c']", `["a",null,"c"]`},
		{"DATE[]", "[DATE '2026-02-01', NULL]", `["2026-02-01",null]`},
		{"DECIMAL(6,1)[]", "[1.5::DECIMAL(6,1), 2.5]", `["1.5","2.5"]`},
		{"STRUCT(a BIGINT, b VARCHAR[])", "{'a': 1, 'b': ['x', NULL]}", `{"a":1,"b":["x",null]}`},
		{"STRUCT(a BIGINT, b VARCHAR)[]", "[{'a': 1, 'b': 'x'}, {'a': NULL, 'b': 'y'}]", `[{"a":1,"b":"x"},{"a":null,"b":"y"}]`},
		{"STRUCT(d DATE, t TIMESTAMP, p DECIMAL(5,2))", "{'d': DATE '2026-01-02', 't': TIMESTAMP '2026-01-02 03:04:05', 'p': 1.25::DECIMAL(5,2)}",
			`{"d":"2026-01-02","p":"1.25","t":"2026-01-02T03:04:05"}`},
	}
	for i, c := range cases {
		name := fmt.Sprintf("ident%d", i)
		err := e.RegisterScalar(ctx, contract.ScalarFunction{Name: name, ArgTypes: []string{c.typ}, ReturnType: c.typ,
			Call: func(_ context.Context, a []arrow.Array) (arrow.Array, error) { a[0].Retain(); return a[0], nil }})
		if err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		if got := scalar(t, e, fmt.Sprintf("SELECT %s(%s)", name, c.expr)); got != c.want {
			t.Errorf("%s(%s) = %q, want %q", c.typ, c.expr, got, c.want)
		}
		// Exact round trip, nested NULLs included, and a NULL argument stays NULL.
		if got := scalar(t, e, fmt.Sprintf("SELECT %s(%s) IS NOT DISTINCT FROM (%s)", name, c.expr, c.expr)); got != "true" {
			t.Errorf("%s: not identical after the round trip", c.typ)
		}
		if got := scalar(t, e, fmt.Sprintf("SELECT %s(NULL::%s) IS NULL", name, c.typ)); got != "true" {
			t.Errorf("%s: NULL in did not give NULL", c.typ)
		}
	}
	// Many chunks with variable-length lists.
	if got := scalar(t, e, "SELECT count(ident4(range(i % 20))) FROM range(6000) t(i)"); got != "6000" {
		t.Errorf("chunks: %q", got)
	}
}

// TestRegisterTable: a table function with constant arguments, several chunks,
// NULLs and a complex column; also used inside a join.
func TestRegisterTable(t *testing.T) {
	e, _ := open(t, false)
	ctx := context.Background()
	var closed int
	err := e.RegisterTable(ctx, contract.TableFunction{
		Name: "gen", ArgTypes: []string{"BIGINT", "VARCHAR"},
		Columns: []contract.TableColumn{{Name: "n", Type: "BIGINT"}, {Name: "tag", Type: "VARCHAR"}, {Name: "xs", Type: "BIGINT[]"}},
		Call: func(_ context.Context, args []arrow.Array) ([]arrow.RecordBatch, error) {
			n := args[0].(*array.Int64).Value(0)
			prefix := args[1].(*array.String).Value(0)
			sc := arrow.NewSchema([]arrow.Field{
				{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "tag", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "xs", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
			}, nil)
			var out []arrow.RecordBatch
			for lo := int64(0); lo < n; lo += 1500 { // two batches of unequal size
				hi := min(lo+1500, n)
				b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
				for i := lo; i < hi; i++ {
					b.Field(0).(*array.Int64Builder).Append(i)
					if i%10 == 0 {
						b.Field(1).AppendNull()
					} else {
						b.Field(1).(*array.StringBuilder).Append(prefix)
					}
					lb := b.Field(2).(*array.ListBuilder)
					lb.Append(true)
					lb.ValueBuilder().(*array.Int64Builder).Append(i)
				}
				out = append(out, b.NewRecordBatch())
				b.Release()
			}
			return out, nil
		},
		Close: func() error { closed++; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := scalar(t, e, "SELECT count(*) || ',' || count(tag) || ',' || sum(n) || ',' || sum(xs[1]) FROM gen(5000, 'x')"); got != "5000,4500,12497500,12497500" {
		t.Fatalf("gen = %q", got)
	}
	if got := scalar(t, e, "SELECT count(*) FROM gen(0, 'x')"); got != "0" {
		t.Fatalf("empty = %q", got)
	}
	if got := scalar(t, e, "SELECT count(*) FROM gen(3, 'a') a JOIN gen(5, 'b') b USING (n)"); got != "3" {
		t.Fatalf("join = %q", got)
	}
	_ = e.Close()
	if closed != 1 {
		t.Fatalf("Close hook ran %d times", closed)
	}
}

// TestRegisterTwice records what DuckDB does when a name is registered again: the
// same signature replaces the function, another one adds an overload and keeps the
// old one callable. That is why core/udf refuses a signature change on reload
// (the stale overload would stay) and swaps bodies behind a stub otherwise.
func TestRegisterTwice(t *testing.T) {
	e, _ := open(t, false)
	ctx := context.Background()
	mk := func(v int64, arg string) contract.ScalarFunction {
		return contract.ScalarFunction{Name: "twice", ArgTypes: []string{arg}, ReturnType: "BIGINT",
			Call: func(_ context.Context, a []arrow.Array) (arrow.Array, error) {
				b := array.NewInt64Builder(memory.DefaultAllocator)
				defer b.Release()
				for range a[0].Len() {
					b.Append(v)
				}
				return b.NewArray(), nil
			}}
	}
	for _, f := range []contract.ScalarFunction{mk(1, "BIGINT"), mk(2, "BIGINT"), mk(3, "VARCHAR")} {
		if err := e.RegisterScalar(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	if got := scalar(t, e, "SELECT twice(0) || ',' || twice('x')"); got != "2,3" {
		t.Fatalf("twice(0), twice('x') = %s, want 2,3 (replaced, then overloaded)", got)
	}
}
