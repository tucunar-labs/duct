// Package notebook is a TaskType that runs a marimo notebook as a script
// (`python nb.py --key value`, ADR 0006); the notebook reads its arguments
// with mo.cli_args().
package notebook

import (
	"context"
	"errors"
	"fmt"
	"sort"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/taskrun"
	"github.com/alonsoburon/duct/core/registry"
)

const name = "notebook"

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindTaskType,
		Name: name,
		New:  func() contract.Module { return &Task{} },
	})
}

// Task implements contract.TaskType.
//
// Params: "notebook" (required, path relative to the project), "args" (map:
// each key/value becomes `--key value`, sorted by key), "uv" (bool), "python",
// "cwd", "env". The orchestrator adds "root" and "env".
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
		return contract.TaskResult{}, errors.New("notebook: not initialised")
	}
	nb, err := taskrun.Script(spec.Params, "notebook")
	if err != nil {
		return contract.TaskResult{}, fmt.Errorf("notebook: %w", err)
	}
	py, _ := spec.Params["python"].(string)
	if py == "" {
		py = "python3"
	}
	argv := []string{py, nb}
	if uv, _ := spec.Params["uv"].(bool); uv {
		argv = []string{"uv", "run", "python", nb}
	}
	var kv map[string]any
	switch m := spec.Params["args"].(type) {
	case nil:
	case map[string]any:
		kv = m
	default:
		return contract.TaskResult{}, fmt.Errorf("notebook: args must be a map, not %T", m)
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		argv = append(argv, "--"+k, fmt.Sprint(kv[k]))
	}
	return taskrun.Run(ctx, taskrun.Cmd{Argv: argv, Dir: taskrun.Dir(spec.Params), Env: taskrun.Env(spec.Params)})
}
