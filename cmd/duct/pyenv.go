package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/udf"
)

// pyNeed is a Python module a command needs and the pip requirements that provide it.
type pyNeed struct {
	module       string
	requirements []string
}

// preparePython returns the argv prefix of an interpreter that can import every
// needed module.
//
// Environment choice (documented in ADR 0006): if the project brings its own
// Python (a pyproject.toml with uv installed, [python] command in duct.toml or
// DUCT_PYTHON), that environment is used as is and a missing module is an error
// that says what to add; otherwise Duct manages a venv in .duct/venv, created
// with uv on first use, and installs what is missing into it.
func preparePython(ctx context.Context, p *project.Project, need []pyNeed, log io.Writer) ([]string, error) {
	py := p.PythonCommand()
	managed := p.PythonManaged()
	if managed {
		venv := p.ManagedVenv()
		venvPy := filepath.Join(venv, "bin", "python")
		if _, err := os.Stat(venvPy); err != nil {
			uv, err := exec.LookPath("uv")
			if err != nil {
				return nil, project.ErrNoUV
			}
			_, _ = fmt.Fprintf(log, "creating %s with uv...\n", venv)
			if err := runLogged(ctx, log, nil, uv, "venv", venv); err != nil {
				return nil, err
			}
			if err := runLogged(ctx, log, nil, uv, "pip", "install", "--python", venvPy, p.PythonSDK()); err != nil {
				return nil, fmt.Errorf("install the SDK (set DUCT_SDK or [python] sdk to a path or index spec of duct-data): %w", err)
			}
		}
		py = []string{venvPy}
	}
	for _, n := range need {
		if canImport(ctx, py, n.module) {
			continue
		}
		if !managed {
			return nil, fmt.Errorf("the Python environment %q cannot import %s: add %s to your project's dependencies",
				strings.Join(py, " "), n.module, strings.Join(n.requirements, " "))
		}
		uv, err := exec.LookPath("uv")
		if err != nil {
			return nil, project.ErrNoUV
		}
		_, _ = fmt.Fprintf(log, "installing %s into %s...\n", strings.Join(n.requirements, " "), p.ManagedVenv())
		args := append([]string{"pip", "install", "--python", py[0]}, n.requirements...)
		if err := runLogged(ctx, log, nil, uv, args...); err != nil {
			return nil, err
		}
	}
	return py, nil
}

func canImport(ctx context.Context, py []string, module string) bool {
	argv := append(append([]string{}, py[1:]...), "-c", "import "+module)
	cmd := exec.CommandContext(ctx, py[0], argv...) //nolint:gosec // interpreter from duct.toml / the managed venv
	cmd.Env = udf.SafeEnv(nil)
	return cmd.Run() == nil
}

func runLogged(ctx context.Context, log io.Writer, env []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // fixed tools (uv)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if env != nil {
		cmd.Env = env
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	_ = log
	return nil
}
