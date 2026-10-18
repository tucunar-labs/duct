package project

import "fmt"

// SandboxSettings is the [sandbox] table of duct.toml (ADR 0006, "Sandbox"): bubblewrap isolation of the user code
// Duct runs. Mode is the global setting; the other keys override it for one feature.
//
//	[sandbox]
//	mode      = "auto"   # "auto" (default: bwrap when it works) | "bwrap" (startup error without it) | "none"
//	notebooks = ""       # marimo processes of the console (also [notebooks] sandbox, which wins)
//	jobs      = ""       # python and notebook tasks of jobs/*.yaml and the notebook protocol
//	udf       = ""       # Python UDF workers (functions/*.py)
//	shell     = "none"   # shell tasks: sandboxed only when asked ("auto"/"bwrap" here, or `sandbox: true` in the task)
//
// Unlike the others, shell defaults to none: those tasks are deployment glue that writes into the project and uses
// host tools. Any task can opt out with `sandbox: false` (python, notebook, shell) or in with `sandbox: true`.
type SandboxSettings struct {
	Mode      string `toml:"mode"`
	Notebooks string `toml:"notebooks"`
	Jobs      string `toml:"jobs"`
	UDF       string `toml:"udf"`
	Shell     string `toml:"shell"`
}

// Sandbox features for SandboxSettings.For.
const (
	SandboxNotebooks = "notebooks"
	SandboxJobs      = "jobs"
	SandboxUDF       = "udf"
	SandboxShell     = "shell"
)

// For is the setting of one feature: its own key, else the global mode, else "auto" ("none" for shell). The
// [notebooks] sandbox key, when set, overrides the notebooks' one.
func (s SandboxSettings) For(feature, legacyNotebooks string) string {
	var own string
	switch feature {
	case SandboxNotebooks:
		own = firstSet(legacyNotebooks, s.Notebooks)
	case SandboxJobs:
		own = s.Jobs
	case SandboxUDF:
		own = s.UDF
	case SandboxShell:
		if s.Shell == "" {
			return "none"
		}
		own = s.Shell
	}
	return firstSet(own, s.Mode, "auto")
}

func firstSet(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// Check validates every key.
func (s SandboxSettings) Check() error {
	for k, v := range map[string]string{"mode": s.Mode, "notebooks": s.Notebooks, "jobs": s.Jobs, "udf": s.UDF, "shell": s.Shell} {
		switch v {
		case "", "auto", "bwrap", "none":
		default:
			return fmt.Errorf("[sandbox] %s: %q is not \"auto\", \"bwrap\" or \"none\"", k, v)
		}
	}
	return nil
}

// SandboxFor is the setting of a feature (SandboxNotebooks, SandboxJobs, SandboxUDF, SandboxShell) of this project.
func (p *Project) SandboxFor(feature string) string {
	return p.Config.Sandbox.For(feature, p.Config.Notebooks.Sandbox)
}

// SandboxHide lists the paths under the project root a sandboxed process must not see besides the fixed ones
// (.duct/, duct.toml, .git, .env*): the local lake directory.
func (p *Project) SandboxHide() []string { return []string{p.DataPath()} }
