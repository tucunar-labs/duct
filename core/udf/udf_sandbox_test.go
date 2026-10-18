package udf

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/alonsoburon/duct/core/contract/v1/sandbox"
)

func needBwrap(t *testing.T) {
	t.Helper()
	if _, err := sandbox.Available(); err != nil {
		t.Skip("bubblewrap not usable here: ", err)
	}
}

// The Python tests of the types and table functions, with the workers inside bubblewrap.
func TestPythonSandboxedTypesAndTableFunctions(t *testing.T) {
	needBwrap(t)
	testSandbox = sandbox.Bwrap
	t.Cleanup(func() { testSandbox = "" })
	TestPythonTypesAndTableFunctions(t)
	TestPythonFunctions(t)
}

const sandboxProbeSrc = `
import os
import duct

def probe(path):
    try:
        if os.path.isdir(path):
            return "dir:" + ",".join(sorted(os.listdir(path)))
        with open(path, "rb") as f:
            return "file:" + str(len(f.read()))
    except Exception as e:
        return "ERR:" + type(e).__name__

@duct.function(returns="VARCHAR")
def peek(x: int):
    return "|".join([probe(".duct"), probe("lake"), probe("duct.toml"), probe(".env"), probe("functions"), probe("data.txt")])

@duct.function(returns="VARCHAR")
def scribble(x: int):
    out = []
    for p in ("functions/evil.py", "new.txt", ".duct/x"):
        try:
            open(p, "w").write("x")
            out.append("wrote " + p)
        except Exception as e:
            out.append("ERR")
    return ",".join(out)
`

// A sandboxed worker cannot see .duct/, the local lake, duct.toml or .env, sees functions/ and the project's files
// read-only, and cannot write anywhere in the project.
func TestPythonSandboxedWorkerIsolation(t *testing.T) {
	needBwrap(t)
	py := pythonCmd(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), sandboxProbeSrc)
	for f, c := range map[string]string{".duct/state.db": "hashes", "lake/a.parquet": "data", "duct.toml": "[x]\n", ".env": "K=V", "data.txt": "hello"} {
		write(t, filepath.Join(root, f), c)
	}
	eng, _ := attach(t, root, Options{Python: py, Workers: 1, Sandbox: sandbox.Bwrap, SandboxHide: []string{filepath.Join(root, "lake")}})
	f := eng.funcs["peek"]
	defer func() { _ = f.Close() }()
	res, err := f.Call(context.Background(), []arrow.Array{ints(1)})
	if err != nil {
		t.Fatal(err)
	}
	got := res.(*array.String).Value(0)
	parts := strings.Split(got, "|")
	if len(parts) != 6 {
		t.Fatalf("probe = %q", got)
	}
	// hidden directories exist but are empty (tmpfs), hidden files are empty or unreadable, everything else is real
	if parts[0] != "dir:" && parts[0] != "dir:venv" || parts[1] != "dir:" || strings.Contains(got, "hashes") || strings.Contains(got, "a.parquet") || (parts[2] != "file:0" && parts[2] != "ERR:PermissionError") || (parts[3] != "file:0" && parts[3] != "ERR:PermissionError") {
		t.Errorf("the sandbox exposes the project's secrets: %q", got)
	}
	if !strings.Contains(parts[4], "f.py") || parts[5] != "file:5" {
		t.Errorf("functions/ or the project's files are not visible: %q", got)
	}
	res, err = eng.funcs["scribble"].Call(context.Background(), []arrow.Array{ints(1)})
	if err != nil {
		t.Fatal(err)
	}
	if out := res.(*array.String).Value(0); strings.Contains(out, "wrote") {
		t.Errorf("a sandboxed worker wrote into the project: %q", out)
	}
	if _, err := os.Stat(filepath.Join(root, "functions", "evil.py")); err == nil {
		t.Error("evil.py exists on the host")
	}
}

// With "bwrap" a worker that cannot be wrapped fails closed; with auto it starts unisolated.
func TestPythonSandboxUvFailsClosed(t *testing.T) {
	needBwrap(t)
	root := t.TempDir()
	write(t, filepath.Join(root, "functions", "f.py"), "import duct\n")
	_, err := Discover(context.Background(), Options{Root: root, Python: []string{"uv", "run", "python"}, Sandbox: sandbox.Bwrap})
	if err == nil || !strings.Contains(err.Error(), "uv") {
		t.Fatalf("err = %v", err)
	}
}
