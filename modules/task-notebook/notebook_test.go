package notebook

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/contracttest"
)

func write(t *testing.T, root, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestConformance(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	root := t.TempDir()
	write(t, root, "ok.py", "print('ok')\n")
	write(t, root, "fail.py", "raise SystemExit(2)\n")
	write(t, root, "block.py", "import time\ntime.sleep(60)\n")
	env := map[string]any{"PATH": os.Getenv("PATH")}
	contracttest.TestTaskType(t, func(*testing.T) contracttest.TaskFixture {
		p := func(s string) contract.Config { return contract.Config{"notebook": s, "root": root, "env": env} }
		return contracttest.TaskFixture{Task: &Task{}, OK: p("ok.py"), Fail: p("fail.py"), Block: p("block.py")}
	})
}

func TestArgsBecomeFlags(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	root := t.TempDir()
	write(t, root, "nb.py", "import sys\nprint(sys.argv[1:])\n")
	m := &Task{}
	_ = m.Init(context.Background(), nil)
	res, err := m.Run(context.Background(), contract.TaskSpec{Params: contract.Config{
		"notebook": "nb.py", "root": root, "env": map[string]any{"PATH": os.Getenv("PATH")},
		"args": map[string]any{"year": 2026, "region": "cl"},
	}})
	if err != nil || !strings.Contains(res.Output, "['--region', 'cl', '--year', '2026']") {
		t.Fatalf("got %q, %v", res.Output, err)
	}
}
