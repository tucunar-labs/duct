package notebooks

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Sandbox modes of `[notebooks] sandbox` (ADR 0006, "Sandbox").
const (
	SandboxAuto  = "auto"  // bubblewrap when it works; otherwise only duct_admin edits
	SandboxBwrap = "bwrap" // bubblewrap or a startup error
	SandboxNone  = "none"  // no isolation: editors run Python as the server's OS user
)

var (
	bwrapOnce sync.Once
	bwrapPath string
	bwrapErr  error
)

// BwrapAvailable finds bubblewrap and checks that it can create the namespaces
// the sandbox needs (some hosts disable unprivileged user namespaces). The
// answer is cached for the life of the process.
func BwrapAvailable() (string, error) {
	bwrapOnce.Do(func() {
		p, err := exec.LookPath("bwrap")
		if err != nil {
			bwrapErr = errors.New("bubblewrap (bwrap) is not installed")
			return
		}
		tru, err := exec.LookPath("true")
		if err != nil {
			bwrapErr = errors.New("cannot probe bubblewrap: no `true` in PATH")
			return
		}
		out, err := exec.Command(p, "--unshare-all", "--share-net", "--die-with-parent", "--cap-drop", "ALL", //nolint:gosec // probe of a fixed tool
			"--ro-bind", "/usr", "/usr", "--ro-bind-try", "/lib", "/lib", "--ro-bind-try", "/lib64", "/lib64", "--ro-bind-try", "/bin", "/bin",
			"--proc", "/proc", "--dev", "/dev", "--", tru).CombinedOutput()
		if err != nil {
			bwrapErr = fmt.Errorf("bubblewrap cannot create its namespaces here (user namespaces disabled?): %s", strings.TrimSpace(string(out)))
			return
		}
		bwrapPath = p
	})
	return bwrapPath, bwrapErr
}

// ResolveSandbox applies the setting to this host. It returns the mode in
// force (SandboxBwrap or SandboxNone), whether editing must be restricted to
// duct_admin (the sandbox was not asked to be off and is not available) and a
// message for the log; an error means "bwrap" was demanded and is missing.
func ResolveSandbox(setting string) (mode string, adminOnly bool, msg string, err error) {
	switch setting {
	case SandboxNone:
		return SandboxNone, false, "notebooks: sandbox = none: editors run Python as the server's OS user and can read the lake and .duct/", nil
	case SandboxBwrap:
		if _, e := BwrapAvailable(); e != nil {
			return "", false, "", fmt.Errorf("[notebooks] sandbox = \"bwrap\": %w", e)
		}
		return SandboxBwrap, false, "", nil
	}
	if _, e := BwrapAvailable(); e != nil {
		return SandboxNone, true, "notebooks: no sandbox (" + e.Error() + "): only duct_admin can edit notebooks; install bubblewrap or set sandbox = \"none\" to accept the risk", nil
	}
	return SandboxBwrap, false, "", nil
}

// sandboxSpec is what bwrap needs to know about one marimo process.
type sandboxSpec struct {
	Bwrap        string
	Python       []string // interpreter argv (the venv is bound read-only)
	ProjectRoot  string   // never exposed: no bind may contain it
	NotebooksDir string
	Writable     bool // notebooks dir read-write (edit mode); read-only otherwise
}

const (
	sandboxHome = "/home/sandbox"
	sandboxTmp  = 1 << 30 // bytes of the private /tmp and home (tmpfs, counted as memory)
)

// sandboxEnv replaces the allow-listed variables that point to host
// paths the sandbox does not have.
func sandboxEnv(env []string) []string {
	drop := map[string]bool{"HOME": true, "TMPDIR": true, "XDG_CACHE_HOME": true, "XDG_DATA_HOME": true, "XDG_CONFIG_HOME": true,
		"UV_CACHE_DIR": true, "UV_PROJECT_ENVIRONMENT": true}
	var out []string
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		if !drop[k] {
			out = append(out, kv)
		}
	}
	return append(out, "HOME="+sandboxHome, "TMPDIR=/tmp", "XDG_CACHE_HOME="+sandboxHome+"/.cache",
		"XDG_DATA_HOME="+sandboxHome+"/.local/share", "XDG_CONFIG_HOME="+sandboxHome+"/.config")
}

// bwrapArgv wraps argv in a bubblewrap command line.
//
// What the process sees: the system directories (/usr, /bin, /lib*, a few
// /etc files) and the Python environment read-only, a private tmpfs for /tmp
// and the home, its own /proc and /dev, and the notebooks directory (writable
// only in edit mode). Nothing else of the host exists for it: not the lake, not
// .duct/ (state DB, keys, catalog), not duct.toml. Every namespace is new except
// the network, which it needs to reach DUCT_URL on loopback (so it can also
// reach other loopback services of the host).
func bwrapArgv(sp sandboxSpec, argv []string) ([]string, error) {
	binds, err := pythonBinds(sp.Python[0], sp.ProjectRoot)
	if err != nil {
		return nil, err
	}
	a := []string{sp.Bwrap, "--unshare-all", "--share-net", "--die-with-parent", "--new-session", "--cap-drop", "ALL",
		"--ro-bind", "/usr", "/usr"}
	for _, d := range []string{"/bin", "/sbin", "/lib", "/lib64"} {
		if t, err := os.Readlink(d); err == nil { // merged /usr: a symlink
			a = append(a, "--symlink", t, d)
		} else {
			a = append(a, "--ro-bind-try", d, d)
		}
	}
	for _, f := range []string{"/etc/ssl", "/etc/ca-certificates", "/etc/pki", "/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf",
		"/etc/host.conf", "/etc/ld.so.cache", "/etc/ld.so.conf", "/etc/ld.so.conf.d", "/etc/localtime", "/etc/passwd", "/etc/group", "/etc/alternatives"} {
		a = append(a, "--ro-bind-try", f, f)
	}
	a = append(a, "--proc", "/proc", "--dev", "/dev",
		"--size", fmt.Sprint(sandboxTmp), "--tmpfs", "/tmp",
		"--size", fmt.Sprint(sandboxTmp), "--tmpfs", sandboxHome)
	for _, b := range binds {
		a = append(a, "--ro-bind", b.src, b.dst)
	}
	nb := sp.NotebooksDir
	if isUnder(sp.ProjectRoot, nb) {
		return nil, fmt.Errorf("notebooks: the notebooks directory %s is or contains the project root", nb)
	}
	if sp.Writable {
		a = append(a, "--bind", nb, nb)
	} else {
		a = append(a, "--ro-bind", nb, nb)
	}
	a = append(a, "--chdir", nb, "--")
	return append(a, argv...), nil
}

func isUnder(p, dir string) bool {
	rel, err := filepath.Rel(dir, p)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// pythonBinds lists the host directories of the interpreter that must be
// visible read-only: the venv (and the paths its .pth files point to, like an
// editable install of the SDK) and the base installation the venv links to.
// A directory that contains the project root is refused: it would hand the
// sandbox the lake and .duct/.
func pythonBinds(py, root string) ([]bind, error) {
	if !filepath.IsAbs(py) {
		p, err := exec.LookPath(py)
		if err != nil {
			return nil, fmt.Errorf("notebooks: the sandbox needs a direct Python interpreter, got %q: %w", py, err)
		}
		py = p
	}
	py, _ = filepath.Abs(py)
	set := map[string]string{} // destination -> source
	add := func(d string) { addAs(set, d, d) }
	venv := filepath.Dir(filepath.Dir(py))
	if f, err := os.Open(filepath.Join(venv, "pyvenv.cfg")); err == nil {
		add(venv)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if k, v, ok := strings.Cut(sc.Text(), "="); ok && strings.TrimSpace(k) == "home" {
				// .../bin -> the installation prefix, visible under the name the venv's symlink uses
				// (that name may itself be a symlink to the real directory)
				home := filepath.Dir(filepath.Clean(strings.TrimSpace(v)))
				if real, err := filepath.EvalSymlinks(home); err == nil {
					addAs(set, home, real)
				}
			}
		}
		_ = f.Close()
		pths, _ := filepath.Glob(filepath.Join(venv, "lib", "python*", "site-packages", "*.pth"))
		for _, pth := range pths {
			b, err := os.ReadFile(pth)
			if err != nil {
				continue
			}
			for _, line := range strings.Split(string(b), "\n") {
				line = strings.TrimSpace(line)
				if filepath.IsAbs(line) {
					if st, err := os.Stat(line); err == nil && st.IsDir() {
						add(line)
					}
				}
			}
		}
	}
	if real, err := filepath.EvalSymlinks(py); err == nil {
		add(filepath.Dir(filepath.Dir(real)))
	}
	var out []bind
	for d, src := range set {
		if isUnder(root, d) || isUnder(root, src) { // the root or one of its parents
			return nil, fmt.Errorf("notebooks: the Python environment at %s contains the project (%s) and would expose the lake and .duct/ to the sandbox: use a venv outside the project root's parents", d, root)
		}
		out = append(out, bind{src, d})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dst < out[j].dst })
	return out, nil
}

// bind is a read-only mount: host path src appears at dst.
type bind struct{ src, dst string }

func addAs(set map[string]string, dst, src string) {
	if dst != "" && dst != "/" && src != "/" && dst != "/usr" && !isUnder(dst, "/usr") {
		set[dst] = src
	}
}
