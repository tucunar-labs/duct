package project

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// EnvPython overrides [python].command: a command line split on spaces, e.g.
// "uv run --project /srv/proj python".
const EnvPython = "DUCT_PYTHON"

// PythonSettings is the [python] table of duct.toml: which interpreter runs user
// code (UDF workers, `duct notebook`, `duct mlflow`).
//
//	[python]
//	command = ["uv", "run", "--project", ".", "python"]   # argv prefix; default below
//	sdk     = "duct-data[marimo,arrow,pg]"                # what the managed venv installs
//	workers = 2                                           # UDF worker processes
//
// Default command: with a pyproject.toml in the project and `uv` installed,
// `uv run --project <root> python` (the project's own environment); otherwise the
// managed venv `.duct/venv` if it exists, else `python3`.
type PythonSettings struct {
	Command []string `toml:"command"`
	SDK     string   `toml:"sdk"`
	Workers int      `toml:"workers"`
}

// DefaultSDK is what the managed venv installs when [python].sdk is empty.
const DefaultSDK = "duct-data[marimo,arrow,pg]"

// ManagedVenv is the folder of the managed virtual environment.
func (p *Project) ManagedVenv() string { return filepath.Join(p.Root, StateDir, "venv") }

// PythonCommand is the argv prefix that starts a Python able to import duct.
func (p *Project) PythonCommand() []string {
	if v := os.Getenv(EnvPython); v != "" {
		return strings.Fields(v)
	}
	if len(p.Config.Python.Command) > 0 {
		return append([]string(nil), p.Config.Python.Command...)
	}
	if _, err := os.Stat(filepath.Join(p.Root, "pyproject.toml")); err == nil {
		if uv, err := exec.LookPath("uv"); err == nil {
			return []string{uv, "run", "--project", p.Root, "python"}
		}
	}
	if py := filepath.Join(p.ManagedVenv(), "bin", "python"); fileExists(py) {
		return []string{py}
	}
	return []string{"python3"}
}

// PythonSDK is the pip requirement of the SDK for the managed venv.
func (p *Project) PythonSDK() string {
	if v := os.Getenv("DUCT_SDK"); v != "" {
		return v
	}
	if p.Config.Python.SDK != "" {
		return p.Config.Python.SDK
	}
	return DefaultSDK
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// ErrNoUV is returned when the managed venv is needed but uv is not installed.
var ErrNoUV = errors.New("uv is not installed (https://docs.astral.sh/uv/): install it, or set [python] command in duct.toml")

// PythonManaged reports whether Duct itself owns the Python environment (the
// venv in .duct/venv, created on demand with uv): no DUCT_PYTHON, no [python]
// command and no pyproject.toml in the project.
func (p *Project) PythonManaged() bool {
	if os.Getenv(EnvPython) != "" || len(p.Config.Python.Command) > 0 {
		return false
	}
	if _, err := os.Stat(filepath.Join(p.Root, "pyproject.toml")); err == nil {
		if _, err := exec.LookPath("uv"); err == nil {
			return false
		}
	}
	return true
}
