package notebooks

import "github.com/alonsoburon/duct/core/contract/v1/sandbox"

// Sandbox modes of the notebooks' setting; the implementation is core/contract/v1/sandbox.
const (
	SandboxAuto  = sandbox.Auto  // bubblewrap when it works; otherwise only duct_admin edits
	SandboxBwrap = sandbox.Bwrap // bubblewrap or a startup error
	SandboxNone  = sandbox.None  // no isolation: editors run Python as the server's OS user
)

// ResolveSandbox applies the setting to this host. It returns the mode in force (SandboxBwrap or SandboxNone),
// whether editing must be restricted to duct_admin (the sandbox was not asked to be off and is not available)
// and a message for the log; an error means "bwrap" was demanded and is missing.
func ResolveSandbox(setting string) (mode string, adminOnly bool, msg string, err error) {
	mode, fallback, why, err := sandbox.Resolve(setting)
	switch {
	case err != nil:
		return "", false, "", err
	case fallback:
		return SandboxNone, true, "notebooks: no sandbox (" + why + "): only duct_admin can edit notebooks; install bubblewrap or set sandbox = \"none\" to accept the risk", nil
	case mode == SandboxNone:
		return SandboxNone, false, "notebooks: sandbox = none: editors run Python as the server's OS user and can read the lake and .duct/", nil
	}
	return mode, false, "", nil
}
