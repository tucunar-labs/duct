package udf

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// fakeEngine records the registered functions; it satisfies contract.Engine
// only as far as Attach needs (the ScalarRegistrar part).
type fakeEngine struct {
	contract.Engine
	mu    sync.Mutex
	funcs map[string]contract.ScalarFunction
	tabs  map[string]contract.TableFunction
}

func (f *fakeEngine) RegisterTable(_ context.Context, fn contract.TableFunction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tabs == nil {
		f.tabs = map[string]contract.TableFunction{}
	}
	f.tabs[fn.Name] = fn
	return nil
}

func (f *fakeEngine) RegisterScalar(_ context.Context, fn contract.ScalarFunction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.funcs == nil {
		f.funcs = map[string]contract.ScalarFunction{}
	}
	f.funcs[fn.Name] = fn
	return nil
}

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func strs(vals ...string) arrow.Array {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vals, nil)
	return b.NewArray()
}

func ints(vals ...int64) arrow.Array {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vals, nil)
	return b.NewArray()
}

// pythonCmd finds an interpreter that can import duct and pyarrow: the
// workspace venv of python/ (created by `uv sync`), or DUCT_TEST_PYTHON.
func pythonCmd(t *testing.T) []string {
	t.Helper()
	if v := os.Getenv("DUCT_TEST_PYTHON"); v != "" {
		return strings.Fields(v)
	}
	_, file, _, _ := runtime.Caller(0)
	py := filepath.Join(filepath.Dir(file), "..", "..", "python", ".venv", "bin", "python")
	if _, err := os.Stat(py); err != nil {
		t.Skip("python/.venv not found: run `cd python && uv sync` (or set DUCT_TEST_PYTHON)")
	}
	return []string{py}
}

// testSandbox, when set, is the Options.Sandbox of every attach (the sandboxed variants of the Python tests).
var testSandbox string

func attach(t *testing.T, root string, opt Options) (*fakeEngine, []string) {
	t.Helper()
	opt.Root = root
	if opt.Sandbox == "" {
		opt.Sandbox = testSandbox
	}
	eng := &fakeEngine{}
	names, err := Attach(context.Background(), eng, opt)
	if err != nil {
		t.Fatal(err)
	}
	return eng, names
}

func TestExecFunctionsTSVAndJSON(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "upper.toml"), "command = [\"tr\", \"a-z\", \"A-Z\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\n")
	write(t, filepath.Join(root, "functions", "echo.toml"), "command = [\"cat\"]\nargs = [\"VARCHAR\", \"BIGINT\"]\nreturns = \"VARCHAR\"\n")
	write(t, filepath.Join(root, "functions", "sum2.toml"), "command = [\"python3\", \"-c\", \"import sys, json\\nfor l in sys.stdin:\\n a = json.loads(l)\\n print(a[0] + a[1])\"]\nargs = [\"BIGINT\", \"BIGINT\"]\nreturns = \"BIGINT\"\nformat = \"json\"\n")
	eng, names := attach(t, root, Options{})
	if strings.Join(names, ",") != "echo,sum2,upper" {
		t.Fatalf("names = %v", names)
	}
	ctx := context.Background()
	res, err := eng.funcs["upper"].Call(ctx, []arrow.Array{strs("ab", "cd")})
	if err != nil {
		t.Fatal(err)
	}
	if a := res.(*array.String); a.Value(0) != "AB" || a.Value(1) != "CD" {
		t.Fatalf("upper = %v", a)
	}
	// Tabs, newlines and backslashes survive the TSV round trip (cat echoes the escaped row,
	// "value<TAB>number", so the answer is the row with the tab escaped).
	res, err = eng.funcs["echo"].Call(ctx, []arrow.Array{strs("a\tb", "x\ny", `back\slash`), ints(1, 2, 3)})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"a\tb\t1", "x\ny\t2", `back\slash` + "\t3"} {
		if got := res.(*array.String).Value(i); got != want {
			t.Errorf("echo row %d = %q, want %q", i, got, want)
		}
	}
	res, err = eng.funcs["sum2"].Call(ctx, []arrow.Array{ints(1, 20), ints(2, 30)})
	if err != nil {
		t.Fatal(err)
	}
	if a := res.(*array.Int64); a.Value(0) != 3 || a.Value(1) != 50 {
		t.Fatalf("sum2 = %v", a)
	}
}

func TestExecErrors(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "short.toml"), "command = [\"head\", \"-n1\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\n")
	write(t, filepath.Join(root, "functions", "fail.toml"), "command = [\"sh\", \"-c\", \"echo boom >&2; exit 3\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\n")
	write(t, filepath.Join(root, "functions", "slow.toml"), "command = [\"sleep\", \"5\"]\nargs = [\"VARCHAR\"]\nreturns = \"VARCHAR\"\ntimeout = \"200ms\"\n")
	write(t, filepath.Join(root, "functions", "num.toml"), "command = [\"cat\"]\nargs = [\"VARCHAR\"]\nreturns = \"DOUBLE\"\n")
	eng, _ := attach(t, root, Options{})
	in := []arrow.Array{strs("a", "b")}
	for name, want := range map[string]string{
		"short": "wrote 1 lines for 2 rows", "fail": "boom", "slow": "killed", "num": "not a number",
	} {
		_, err := eng.funcs[name].Call(context.Background(), in)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", name, err, want)
		}
	}
}

func TestExecDefinitionErrors(t *testing.T) {
	for name, body := range map[string]string{
		"nocmd":   "returns = \"BIGINT\"\n",
		"noret":   "command = [\"cat\"]\n",
		"badfmt":  "command = [\"cat\"]\nreturns = \"BIGINT\"\nformat = \"xml\"\n",
		"unknown": "command = [\"cat\"]\nreturns = \"BIGINT\"\nbogus = 1\n",
	} {
		root := t.TempDir()
		write(t, filepath.Join(root, "functions", name+".toml"), body)
		if _, err := Discover(context.Background(), Options{Root: root}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

const pySrc = `
import os
import duct

@duct.function
def inc(x: int) -> int:
    return x + 1

@duct.function
def noisy(x: int) -> int:
    print("stdout noise must not break the protocol")
    return x

@duct.function(vectorized=True, types=["BIGINT"], returns="BIGINT")
def vdouble(a):
    import pyarrow.compute as pc
    return pc.multiply(a, 2)

@duct.function
def boom(x: int) -> int:
    raise ValueError("kapow")

@duct.function(returns="VARCHAR")
def leak(x: int):
    return ",".join(sorted(k for k in os.environ if k.startswith(("DUCT_", "AWS_"))))
`

func TestPythonFunctions(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), pySrc)
	t.Setenv("DUCT_S3_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "super-secret")
	t.Setenv("DUCT_CATALOG_PASSWORD", "super-secret")
	eng, names := attach(t, root, Options{Python: py, Workers: 2})
	if len(names) != 5 {
		t.Fatalf("names = %v", names)
	}
	f := eng.funcs["inc"]
	defer func() {
		if f.Close != nil {
			_ = f.Close()
		}
	}()
	ctx := context.Background()
	res, err := f.Call(ctx, []arrow.Array{ints(1, 2, 3)})
	if err != nil {
		t.Fatal(err)
	}
	if a := res.(*array.Int64); a.Value(0) != 2 || a.Value(2) != 4 {
		t.Fatalf("inc = %v", a)
	}
	if res, err := eng.funcs["noisy"].Call(ctx, []arrow.Array{ints(7)}); err != nil || res.(*array.Int64).Value(0) != 7 {
		t.Fatalf("noisy: %v %v", res, err)
	}
	res, err = eng.funcs["vdouble"].Call(ctx, []arrow.Array{ints(5, 6)})
	if err != nil || res.(*array.Int64).Value(1) != 12 {
		t.Fatalf("vdouble: %v %v", res, err)
	}
	if _, err := eng.funcs["boom"].Call(ctx, []arrow.Array{ints(1)}); err == nil || !strings.Contains(err.Error(), "ValueError: kapow") {
		t.Fatalf("boom: %v", err)
	}
	// The worker survives a function error.
	if _, err := f.Call(ctx, []arrow.Array{ints(1)}); err != nil {
		t.Fatalf("after error: %v", err)
	}
	res, err = eng.funcs["leak"].Call(ctx, []arrow.Array{ints(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.(*array.String).Value(0); got != "" {
		t.Fatalf("worker sees credential variables: %q", got)
	}
}

func TestPythonUnresolvedTypesAreRejected(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), "import duct\n\n@duct.function\ndef f(x: list) -> int:\n    return 1\n")
	_, err := Discover(context.Background(), Options{Root: root, Python: py})
	if err == nil || !strings.Contains(err.Error(), "no supported SQL type") {
		t.Fatalf("err = %v", err)
	}
}

func TestPythonTimeoutKillsWorkerAndRecovers(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), "import time, duct\n\n@duct.function\ndef nap(x: int) -> int:\n    time.sleep(x)\n    return x\n")
	eng, _ := attach(t, root, Options{Python: py, Workers: 1, Timeout: 3 * time.Second})
	f := eng.funcs["nap"]
	defer func() { _ = f.Close() }()
	start := time.Now()
	if _, err := f.Call(context.Background(), []arrow.Array{ints(60)}); err == nil {
		t.Fatal("want a timeout")
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("timeout took %v", time.Since(start))
	}
	res, err := f.Call(context.Background(), []arrow.Array{ints(0)})
	if err != nil || res.(*array.Int64).Value(0) != 0 {
		t.Fatalf("after timeout: %v %v", res, err)
	}
}

// A slow cold start (interpreter + imports) must not count against the
// per-batch timeout.
func TestPythonSlowStartDoesNotCountAgainstBatchTimeout(t *testing.T) {
	py := pythonCmd(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), "import duct\n\n@duct.function\ndef one(x: int) -> int:\n    return x\n")
	slow := append([]string{"sh", "-c", `case "$*" in *duct.udf.worker*) sleep 4;; esac; exec "$@"`, "sh"}, py...)
	eng, _ := attach(t, root, Options{Python: slow, Workers: 1, Timeout: 2 * time.Second})
	f := eng.funcs["one"]
	defer func() { _ = f.Close() }()
	res, err := f.Call(context.Background(), []arrow.Array{ints(7)})
	if err != nil || res.(*array.Int64).Value(0) != 7 {
		t.Fatalf("cold start: %v %v", res, err)
	}
}

func TestPythonMissingSDKExplains(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), "x = 1\n")
	_, err := Discover(context.Background(), Options{Root: root, Python: []string{"python3", "-S", "-c", "import sys; sys.exit(1)", "--"}})
	if err == nil || !strings.Contains(err.Error(), "duct-data") {
		t.Fatalf("err = %v", err)
	}
}

// BenchmarkPythonBatch measures the per-batch cost of the worker round trip
// (Arrow IPC both ways) for the vectorized and the scalar path.
func BenchmarkPythonBatch(b *testing.B) {
	py := os.Getenv("DUCT_TEST_PYTHON")
	var cmd []string
	if py != "" {
		cmd = strings.Fields(py)
	} else {
		_, file, _, _ := runtime.Caller(0)
		p := filepath.Join(filepath.Dir(file), "..", "..", "python", ".venv", "bin", "python")
		if _, err := os.Stat(p); err != nil {
			b.Skip("no python/.venv")
		}
		cmd = []string{p}
	}
	root := b.TempDir()
	_ = os.MkdirAll(filepath.Join(root, "functions"), 0o755)
	_ = os.WriteFile(filepath.Join(root, "functions", "f.py"), []byte(pySrc), 0o644)
	eng := &fakeEngine{}
	if _, err := Attach(context.Background(), eng, Options{Root: root, Python: cmd, Workers: 1}); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = eng.funcs["inc"].Close() }()
	for _, n := range []int{1, 2048, 65536} {
		vals := make([]int64, n)
		for i := range vals {
			vals[i] = int64(i)
		}
		in := ints(vals...)
		for _, name := range []string{"vdouble", "inc"} {
			b.Run(fmt.Sprintf("%s/%d", name, n), func(b *testing.B) {
				for range b.N {
					res, err := eng.funcs[name].Call(context.Background(), []arrow.Array{in})
					if err != nil {
						b.Fatal(err)
					}
					res.Release()
				}
			})
		}
	}
}
