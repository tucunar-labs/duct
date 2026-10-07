package shell

import (
	"context"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/contracttest"
)

func params(env map[string]any, argv ...string) contract.Config {
	l := make([]any, len(argv))
	for i, a := range argv {
		l[i] = a
	}
	return contract.Config{"command": l, "root": "/", "env": map[string]any{"PATH": "/usr/bin:/bin"}, "x": env}
}

func TestConformance(t *testing.T) {
	contracttest.TestTaskType(t, func(*testing.T) contracttest.TaskFixture {
		return contracttest.TaskFixture{
			Task:  &Task{},
			OK:    params(nil, "true"),
			Fail:  params(nil, "sh", "-c", "echo boom >&2; exit 3"),
			Block: params(nil, "sleep", "60"),
		}
	})
}

func TestNoShellInterpretation(t *testing.T) {
	m := &Task{}
	_ = m.Init(context.Background(), nil)
	res, err := m.Run(context.Background(), contract.TaskSpec{Params: params(nil, "echo", "$HOME; touch /tmp/pwned `id`")})
	if err != nil || strings.TrimSpace(res.Output) != "$HOME; touch /tmp/pwned `id`" {
		t.Fatalf("got %q, %v", res.Output, err)
	}
}

func TestEnvIsExactlyWhatWasGiven(t *testing.T) {
	t.Setenv("DUCT_S3_SECRET_ACCESS_KEY", "leak")
	m := &Task{}
	_ = m.Init(context.Background(), nil)
	p := params(nil, "env")
	p["env"] = map[string]any{"PATH": "/usr/bin:/bin", "DUCT_URL": "http://x"}
	res, err := m.Run(context.Background(), contract.TaskSpec{Params: p})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(res.Output, "leak") || !strings.Contains(res.Output, "DUCT_URL=http://x") {
		t.Fatalf("env = %q", res.Output)
	}
}

func TestCommandMustBeAList(t *testing.T) {
	m := &Task{}
	_ = m.Init(context.Background(), nil)
	if _, err := m.Run(context.Background(), contract.TaskSpec{Params: contract.Config{"command": "echo hi"}}); err == nil {
		t.Fatal("a string command must be rejected")
	}
}
