package udf

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

func TestParseSQLType(t *testing.T) {
	for in, want := range map[string]string{
		"int":                              "INTEGER",
		"decimal(10, 2)":                   "DECIMAL(10,2)",
		"NUMERIC":                          "DECIMAL(18,3)",
		"text[]":                           "VARCHAR[]",
		"struct(a int, \"b c\" date[])[]":  `STRUCT(a INTEGER, "b c" DATE[])[]`,
		"timestamp":                        "TIMESTAMP",
		"STRUCT(x DECIMAL(5), y DOUBLE[])": "STRUCT(x DECIMAL(5,0), y DOUBLE[])",
	} {
		got, err := contract.ParseSQLType(in)
		if err != nil || got.String() != want {
			t.Errorf("%q = %v, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "JSON", "DECIMAL(40,2)", "DECIMAL(5,6)", "STRUCT()", "STRUCT(a)", "BIGINT[", "INT junk", "STRUCT(a INT"} {
		if got, err := contract.ParseSQLType(bad); err == nil {
			t.Errorf("%q parsed as %v, want an error", bad, got)
		}
	}
}

func dates(days ...int32) arrow.Array {
	b := array.NewDate32Builder(memory.DefaultAllocator)
	defer b.Release()
	for _, d := range days {
		b.Append(arrow.Date32(d))
	}
	return b.NewArray()
}

func decimals(scale int32, vals ...int64) arrow.Array {
	b := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 10, Scale: scale})
	defer b.Release()
	for _, v := range vals {
		b.Append(decimal128.FromI64(v))
	}
	return b.NewArray()
}

func timestamps(us ...int64) arrow.Array {
	b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer b.Release()
	for _, v := range us {
		b.Append(arrow.Timestamp(v))
	}
	return b.NewArray()
}

// TestExecComplexTypes: DATE, TIMESTAMP and DECIMAL in tsv and json, lists and
// structs in json (and refused in tsv).
func TestExecComplexTypes(t *testing.T) {
	root := t.TempDir()
	f := func(name, body string) { write(t, filepath.Join(root, "functions", name+".toml"), body) }
	f("tsvtypes", "command = [\"sed\", \"s/$/!/\"]\nargs = [\"DATE\", \"TIMESTAMP\", \"DECIMAL(10,2)\"]\nreturns = \"VARCHAR\"\n")
	f("nextday", "command = [\"sh\", \"-c\", \"cut -f1 | sed 's/2026-01-31/2026-02-01/'\"]\nargs = [\"DATE\", \"TIMESTAMP\", \"DECIMAL(10,2)\"]\nreturns = \"DATE\"\n")
	f("tsdec", "command = [\"sh\", \"-c\", \"cut -f2,3 | sed 's/\\t.*//'\"]\nargs = [\"DATE\", \"TIMESTAMP\", \"DECIMAL(10,2)\"]\nreturns = \"TIMESTAMP\"\n")
	f("dec", "command = [\"sh\", \"-c\", \"cut -f3\"]\nargs = [\"DATE\", \"TIMESTAMP\", \"DECIMAL(10,2)\"]\nreturns = \"DECIMAL(10,2)\"\n")
	f("jsoncomplex", "command = [\"python3\", \"-c\", \"import sys, json\\nfor l in sys.stdin:\\n d, xs, s = json.loads(l)\\n print(json.dumps({'day': d, 'n': len(xs), 'tags': [s['a'], str(s['b'])]}))\"]\n"+
		"args = [\"DATE\", \"BIGINT[]\", \"STRUCT(a VARCHAR, b DECIMAL(5,1))\"]\nreturns = \"STRUCT(day DATE, n BIGINT, tags VARCHAR[])\"\nformat = \"json\"\n")
	f("jsonlist", "command = [\"sh\", \"-c\", \"sed 's/.*/[1.5, null, 2.25]/'\"]\nargs = [\"BIGINT\"]\nreturns = \"DECIMAL(10,2)[]\"\nformat = \"json\"\n")
	eng, _ := attach(t, root, Options{})
	ctx := context.Background()
	day := int32(20484) // 2026-01-31
	us := int64(day)*86400*1_000_000 + 12*3600*1_000_000 + 30*60*1_000_000 + 123456
	args := []arrow.Array{dates(day), timestamps(us), decimals(2, -12345)}

	res, err := eng.funcs["tsvtypes"].Call(ctx, args)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.(*array.String).Value(0); got != "2026-01-31\t2026-01-31 12:30:00.123456\t-123.45!" {
		t.Fatalf("tsv rendering = %q", got)
	}
	if res, err = eng.funcs["nextday"].Call(ctx, args); err != nil || res.(*array.Date32).Value(0).FormattedString() != "2026-02-01" {
		t.Fatalf("date result: %v %v", res, err)
	}
	if res, err = eng.funcs["tsdec"].Call(ctx, args); err != nil || res.(*array.Timestamp).Value(0) != arrow.Timestamp(us) {
		t.Fatalf("timestamp result: %v %v", res, err)
	}
	if res, err = eng.funcs["dec"].Call(ctx, args); err != nil || res.(*array.Decimal128).Value(0).ToString(2) != "-123.45" {
		t.Fatalf("decimal result: %v %v", res, err)
	}

	// json: list and struct arguments and results.
	lb := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	lb.Append(true)
	lb.ValueBuilder().(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	xs := lb.NewArray()
	st := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "b", Type: &arrow.Decimal128Type{Precision: 5, Scale: 1}, Nullable: true})
	sb := array.NewStructBuilder(memory.DefaultAllocator, st)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("x")
	sb.FieldBuilder(1).(*array.Decimal128Builder).Append(decimal128.FromI64(25))
	res, err = eng.funcs["jsoncomplex"].Call(ctx, []arrow.Array{dates(day), xs, sb.NewArray()})
	if err != nil {
		t.Fatal(err)
	}
	row := res.(*array.Struct)
	if row.Field(0).(*array.Date32).Value(0).FormattedString() != "2026-01-31" || row.Field(1).(*array.Int64).Value(0) != 3 ||
		row.Field(2).(*array.List).ListValues().(*array.String).Value(1) != "2.5" {
		t.Fatalf("struct result: %v", row)
	}
	res, err = eng.funcs["jsonlist"].Call(ctx, []arrow.Array{ints(1, 2)})
	if err != nil {
		t.Fatal(err)
	}
	l := res.(*array.List)
	vals := l.ListValues().(*array.Decimal128)
	if l.Len() != 2 || vals.Len() != 6 || !vals.IsNull(1) || vals.Value(2).ToString(2) != "2.25" {
		t.Fatalf("list result: %v", l)
	}

	// Complex types need json.
	bad := t.TempDir()
	write(t, filepath.Join(bad, "functions", "x.toml"), "command = [\"cat\"]\nargs = [\"BIGINT[]\"]\nreturns = \"BIGINT\"\n")
	if _, err := Discover(ctx, Options{Root: bad}); err == nil || !strings.Contains(err.Error(), "format = \"json\"") {
		t.Fatalf("tsv list: %v", err)
	}
}

const pyTypesSrc = `
import datetime
from decimal import Decimal
import pyarrow as pa
import duct

@duct.function
def next_day(d: datetime.date) -> datetime.date:
    return d + datetime.timedelta(days=1)

@duct.function(types=["DECIMAL(10,2)"], returns="DECIMAL(12,2)")
def double_price(p):
    return p * 2

@duct.function
def doubled(xs: list[int]) -> list[int]:
    return [x * 2 for x in xs]

@duct.function(types=["STRUCT(a BIGINT, b VARCHAR)"], returns="STRUCT(n BIGINT, s VARCHAR[])")
def reshape(r):
    return {"n": r["a"] + 1, "s": [r["b"], r["b"].upper()]}

@duct.function(vectorized=True, types=["TIMESTAMP"], returns="TIMESTAMP")
def vplus(t):
    import pyarrow.compute as pc
    return pc.add(t, pa.scalar(datetime.timedelta(hours=1)))

@duct.table_function(columns={"n": "BIGINT", "sq": "BIGINT", "d": "DATE"})
def squares(upto: int):
    for lo in range(0, upto, 1000):
        hi = min(lo + 1000, upto)
        yield pa.record_batch({
            "n": list(range(lo, hi)),
            "sq": [i * i for i in range(lo, hi)],
            "d": [datetime.date(2026, 1, 1)] * (hi - lo),
        })

@duct.table_function(columns={"x": "BIGINT"})
def broken(k: int):
    raise RuntimeError("table kaput")
`

// TestPythonTypesAndTableFunctions covers the Arrow mapping of the new types, scalar and
// vectorized, and a table function that streams several batches.
func TestPythonTypesAndTableFunctions(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), pyTypesSrc)
	eng, names := attach(t, root, Options{Python: py, Workers: 1})
	if len(names) != 7 {
		t.Fatalf("names = %v", names)
	}
	defer func() { _ = eng.funcs["next_day"].Close() }()
	if got := eng.funcs["doubled"].ArgTypes[0] + " -> " + eng.funcs["doubled"].ReturnType; got != "BIGINT[] -> BIGINT[]" {
		t.Fatalf("doubled types = %s", got)
	}
	ctx := context.Background()
	res, err := eng.funcs["next_day"].Call(ctx, []arrow.Array{dates(20484, 20515)})
	if err != nil || res.(*array.Date32).Value(1).FormattedString() != "2026-03-03" && res.(*array.Date32).Value(1).FormattedString() != "2026-03-04" {
		t.Fatalf("next_day: %v %v", res, err)
	}
	if res.(*array.Date32).Value(0).FormattedString() != "2026-02-01" {
		t.Fatalf("next_day[0] = %v", res)
	}
	if res, err = eng.funcs["double_price"].Call(ctx, []arrow.Array{decimals(2, 1234)}); err != nil || res.(*array.Decimal128).Value(0).ToString(2) != "24.68" {
		t.Fatalf("double_price: %v %v", res, err)
	}
	if res, err = eng.funcs["vplus"].Call(ctx, []arrow.Array{timestamps(0)}); err != nil || res.(*array.Timestamp).Value(0) != arrow.Timestamp(3600*1_000_000) {
		t.Fatalf("vplus: %v %v", res, err)
	}

	tf := eng.tabs["squares"]
	if tf.Name == "" || len(tf.Columns) != 3 || tf.ArgTypes[0] != "BIGINT" || tf.Columns[2].Type != "DATE" {
		t.Fatalf("squares registered as %+v", tf)
	}
	batches, err := tf.Call(ctx, []arrow.Array{ints(2500)})
	if err != nil {
		t.Fatal(err)
	}
	rows := int64(0)
	for _, b := range batches {
		rows += b.NumRows()
	}
	if len(batches) < 1 || rows != 2500 {
		t.Fatalf("batches=%d rows=%d", len(batches), rows)
	}
	if last := batches[len(batches)-1]; last.Column(1).(*array.Int64).Value(int(last.NumRows())-1) != 2499*2499 {
		t.Fatalf("last square wrong")
	}
	if _, err := eng.tabs["broken"].Call(ctx, []arrow.Array{ints(1)}); err == nil || !strings.Contains(err.Error(), "table kaput") {
		t.Fatalf("broken: %v", err)
	}
}

func listFuncs(t *testing.T, root string, py []string) (*fakeEngine, *Manager) {
	t.Helper()
	eng := &fakeEngine{}
	m, err := AttachManaged(context.Background(), eng, Options{Root: root, Python: py, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return eng, m
}

func callInt(t *testing.T, eng *fakeEngine, name string, v int64) (int64, error) {
	t.Helper()
	res, err := eng.funcs[name].Call(context.Background(), []arrow.Array{ints(v)})
	if err != nil {
		return 0, err
	}
	return res.(*array.Int64).Value(0), nil
}

// TestManagerReload: new names, new bodies, removals, signature changes and
// broken sources, with calls in flight during the swap.
func TestManagerReload(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	fpy := filepath.Join(root, "functions", "f.py")
	write(t, fpy, "import duct\n\n@duct.function\ndef inc(x: int) -> int:\n    return x + 1\n\n@duct.function\ndef gone(x: int) -> int:\n    return x\n")
	write(t, filepath.Join(root, "functions", "e.toml"), "command = [\"cat\"]\nargs = [\"BIGINT\"]\nreturns = \"BIGINT\"\n")
	eng, m := listFuncs(t, root, py)
	if got := strings.Join(m.Names(), ","); got != "e,inc,gone" {
		t.Fatalf("names = %s", got)
	}
	ctx := context.Background()
	if v, err := callInt(t, eng, "inc", 1); err != nil || v != 2 {
		t.Fatalf("inc = %d %v", v, err)
	}
	oldInc := eng.funcs["inc"].Call // what DuckDB holds: it must follow the reloads

	// A new body, a new function, a removed one; the executable is untouched.
	write(t, fpy, "import duct\n\n@duct.function\ndef inc(x: int) -> int:\n    return x + 100\n\n@duct.function\ndef dbl(x: int) -> int:\n    return 2 * x\n")
	ch, err := m.Reload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ch.Added, ",") != "dbl" || strings.Join(ch.Removed, ",") != "gone" || ch.Empty() {
		t.Fatalf("change = %+v", ch)
	}
	if v, err := callInt(t, eng, "inc", 1); err != nil || v != 101 {
		t.Fatalf("inc after reload = %d %v", v, err)
	}
	if res, err := oldInc(ctx, []arrow.Array{ints(1)}); err != nil || res.(*array.Int64).Value(0) != 101 {
		t.Fatalf("the registered closure did not follow the reload: %v %v", res, err)
	}
	if v, err := callInt(t, eng, "dbl", 4); err != nil || v != 8 {
		t.Fatalf("dbl = %d %v", v, err)
	}
	if _, err := callInt(t, eng, "gone", 1); err == nil || !strings.Contains(err.Error(), "was removed") {
		t.Fatalf("gone: %v", err)
	}
	if v, err := callInt(t, eng, "e", 7); err != nil || v != 7 {
		t.Fatalf("exec after reload = %d %v", v, err)
	}
	if got := strings.Join(m.Names(), ","); got != "e,inc,gone,dbl" { // removed names stay guarded
		t.Fatalf("names = %s", got)
	}
	// Removed twice: not reported again; coming back works.
	if ch, err = m.Reload(ctx); err != nil || len(ch.Removed) != 0 || len(ch.Added) != 0 {
		t.Fatalf("second reload: %+v %v", ch, err)
	}
	write(t, fpy, "import duct\n\n@duct.function\ndef inc(x: int) -> int:\n    return x + 100\n\n@duct.function\ndef gone(x: int) -> int:\n    return -x\n")
	if _, err := m.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := callInt(t, eng, "gone", 5); err != nil || v != -5 {
		t.Fatalf("gone again = %d %v", v, err)
	}

	// A broken source keeps everything as it was.
	write(t, fpy, "import duct\n\n@duct.function\ndef inc(x: int) -> int:\n    return x +\n")
	if _, err := m.Reload(ctx); err == nil {
		t.Fatal("want an error for a syntax error")
	}
	if v, err := callInt(t, eng, "inc", 1); err != nil || v != 101 {
		t.Fatalf("inc after a failed reload = %d %v", v, err)
	}
	// A signature change cannot be applied.
	write(t, fpy, "import duct\n\n@duct.function\ndef inc(x: float) -> float:\n    return x\n")
	if _, err := m.Reload(ctx); err == nil || !strings.Contains(err.Error(), "restart") {
		t.Fatalf("signature change: %v", err)
	}
	if v, err := callInt(t, eng, "inc", 1); err != nil || v != 101 {
		t.Fatalf("inc after a refused reload = %d %v", v, err)
	}
}

// TestManagerReloadDrainsInFlight: a batch running during the reload finishes on
// the old code; calls that start after it use the new one, and the old workers stop.
func TestManagerReloadDrainsInFlight(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	fpy := filepath.Join(root, "functions", "f.py")
	src := func(ret string) string {
		return "import time, duct\n\n@duct.function\ndef slow(x: int) -> int:\n    time.sleep(x)\n    return " + ret + "\n"
	}
	write(t, fpy, src("1"))
	eng, m := listFuncs(t, root, py)
	var wg sync.WaitGroup
	wg.Add(1)
	var inflight int64
	var ierr error
	go func() {
		defer wg.Done()
		inflight, ierr = callInt(t, eng, "slow", 3) // sleeps 3 s on the old worker
	}()
	time.Sleep(1500 * time.Millisecond) // the worker is up and busy
	write(t, fpy, src("2"))
	start := time.Now()
	if _, err := m.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("Reload blocked on the in-flight call (%v)", time.Since(start))
	}
	if v, err := callInt(t, eng, "slow", 0); err != nil || v != 2 {
		t.Fatalf("new call = %d %v, want the new code (2)", v, err)
	}
	wg.Wait()
	if ierr != nil || inflight != 1 {
		t.Fatalf("in-flight call = %d %v, want it to finish on the old code (1)", inflight, ierr)
	}
}
