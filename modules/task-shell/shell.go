// Package shell is a TaskType that runs a program with an argument vector
// (never through a shell), a working directory and an explicit environment.
package shell

import (
	"context"
	"errors"
	"fmt"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/taskrun"
	"github.com/alonsoburon/duct/core/registry"
)

const name = "shell"

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindTaskType,
		Name: name,
		New:  func() contract.Module { return &Task{} },
	})
}

// Task implements contract.TaskType.
//
// Params: "command" (required, list of strings: program and arguments, no
// shell), "cwd" (relative to the project root), "env" (map). The orchestrator
// adds "root" and the final "env" (see taskrun).
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
		return contract.TaskResult{}, errors.New("shell: not initialised")
	}
	argv, err := taskrun.StringList(spec.Params, "command")
	if err != nil {
		return contract.TaskResult{}, fmt.Errorf("shell: %w", err)
	}
	if len(argv) == 0 {
		return contract.TaskResult{}, errors.New("shell: param \"command\" is required (a list: [program, arg...])")
	}
	return taskrun.Run(ctx, taskrun.Cmd{Argv: argv, Dir: taskrun.Dir(spec.Params), Env: taskrun.Env(spec.Params), Sandbox: taskrun.SandboxOf(spec.Params, false)})
}
