// Package python is a TaskType that runs a Python script in the project
// (python3 or `uv run`). User code reaches the lake only through
// DUCT_URL/DUCT_TOKEN (duct serve), never with lake credentials.
package python

import (
	"context"
	"errors"
	"fmt"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/taskrun"
	"github.com/alonsoburon/duct/core/registry"
)

const name = "python"

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindTaskType,
		Name: name,
		New:  func() contract.Module { return &Task{} },
	})
}

// Task implements contract.TaskType.
//
// Params: "script" (required, path relative to the project), "args" (list of
// strings), "uv" (bool: run as `uv run python script`), "python" (interpreter,
// default python3), "cwd", "env". The orchestrator adds "root" and "env".
type Task struct{ ready bool }

var _ contract.TaskType = (*Task)(nil)

// Name implements contract.Module.
func (*Task) Name() string { return name }

// Init implements contract.Module.
func (t *Task) Init(context.Context, contract.Config) error { t.ready = true; return nil }

// Close implements contract.Module. It is idempotent.
func (*Task) Close() error { return nil }

// Run implements contract.TaskType.
func (t *Task) Run(ctx context.Context, spec contract.TaskSpec) (contract.TaskResult, error) {
	if !t.ready {
		return contract.TaskResult{}, errors.New("python: not initialised")
	}
	script, err := taskrun.Script(spec.Params, "script")
	if err != nil {
		return contract.TaskResult{}, fmt.Errorf("python: %w", err)
	}
	args, err := taskrun.StringList(spec.Params, "args")
	if err != nil {
		return contract.TaskResult{}, fmt.Errorf("python: %w", err)
	}
	py, _ := spec.Params["python"].(string)
	if py == "" {
		py = "python3"
	}
	argv := []string{py, script}
	if uv, _ := spec.Params["uv"].(bool); uv {
		argv = []string{"uv", "run", "python", script}
	}
	argv = append(argv, args...)
	return taskrun.Run(ctx, taskrun.Cmd{Argv: argv, Dir: taskrun.Dir(spec.Params), Env: taskrun.Env(spec.Params), Sandbox: taskrun.SandboxOf(spec.Params, true)})
}
