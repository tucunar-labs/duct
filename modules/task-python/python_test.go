package python

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

func script(t *testing.T, root, name, body string) {
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
	script(t, root, "ok.py", "print('ok')\n")
	script(t, root, "fail.py", "raise SystemExit(2)\n")
	script(t, root, "block.py", "import time\ntime.sleep(60)\n")
	env := map[string]any{"PATH": os.Getenv("PATH")}
	contracttest.TestTaskType(t, func(*testing.T) contracttest.TaskFixture {
		p := func(s string) contract.Config {
			return contract.Config{"script": s, "root": root, "env": env}
		}
		return contracttest.TaskFixture{Task: &Task{}, OK: p("ok.py"), Fail: p("fail.py"), Block: p("block.py")}
	})
}

func TestArgsAndDuctEnv(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	root := t.TempDir()
	script(t, root, "e.py", "import os,sys\nprint(sys.argv[1:], os.environ.get('DUCT_URL'), os.environ.get('DUCT_S3_ACCESS_KEY_ID'))\n")
	t.Setenv("DUCT_S3_ACCESS_KEY_ID", "secret")
	m := &Task{}
	_ = m.Init(context.Background(), nil)
	res, err := m.Run(context.Background(), contract.TaskSpec{Params: contract.Config{
		"script": "e.py", "args": []any{"a", "b"}, "root": root,
		"env": map[string]any{"PATH": os.Getenv("PATH"), "DUCT_URL": "http://127.0.0.1:1"},
	}})
	if err != nil || !strings.Contains(res.Output, "['a', 'b'] http://127.0.0.1:1 None") {
		t.Fatalf("got %q, %v", res.Output, err)
	}
}
