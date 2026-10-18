// Package sandbox runs user code (marimo notebooks, job tasks, Python UDF workers) inside bubblewrap
// (ADR 0006, "Sandbox"): the system and the Python environment read-only, a private /tmp and home, the
// project either not at all (notebooks: only their folder) or read-only without its secrets (.duct/, the local
// lake, duct.toml, .git, .env*), and an optional writable work directory. It is a helper shared by core and the
// process-running modules, like taskrun.
package sandbox

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Sandbox modes of the `[sandbox]` settings (ADR 0006, "Sandbox").
const (
	Auto  = "auto"  // bubblewrap when it works; otherwise the caller falls back (see Resolve)
	Bwrap = "bwrap" // bubblewrap or a startup error
	None  = "none"  // no isolation: the code runs as the server's OS user
)

var (
	bwrapOnce sync.Once
	bwrapPath string
	bwrapErr  error
)

// Available finds bubblewrap and checks that it can create the namespaces
// the sandbox needs (some hosts disable unprivileged user namespaces). The
// answer is cached for the life of the process.
func Available() (string, error) {
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

// Resolve applies a setting ("auto", "bwrap", "none"; "" is auto) to this host. It returns the mode in force
// (Bwrap or None), whether it is a fallback (auto asked for isolation and the host cannot give it) and the
// reason; an error means "bwrap" was demanded and is not available. What a fallback means is up to the caller
// (notebooks: only duct_admin edits; tasks and UDF workers: they run as the server's user with a warning).
func Resolve(setting string) (mode string, fallback bool, msg string, err error) {
	switch setting {
	case None:
		return None, false, "", nil
	case Bwrap:
		if _, e := Available(); e != nil {
			return "", false, "", fmt.Errorf("sandbox = \"bwrap\": %w", e)
		}
		return Bwrap, false, "", nil
	case "", Auto:
	default:
		return "", false, "", fmt.Errorf("sandbox %q is not \"auto\", \"bwrap\" or \"none\"", setting)
	}
	if _, e := Available(); e != nil {
		return None, true, e.Error(), nil
	}
	return Bwrap, false, "", nil
}

// Spec is what bwrap needs to know about one process.
type Spec struct {
	Bwrap string
	// Python is the interpreter (argv[0] of the command); its environment is bound read-only. "" for a command
	// that is not Python (a shell task): then only the system is visible.
	Python string
	// Root is the project directory. With ProjectRO it is visible read-only except for Hide; without it nothing of
	// it is visible besides ReadOnly and Writable, and no bind may contain it.
	Root      string
	ProjectRO bool
	// Hide are paths under Root that do not exist for the process (ProjectHide builds the usual list).
	Hide []string
	// ReadOnly and Writable are directories (or files) bound at their own path.
	ReadOnly []string
	Writable []string
	// Chdir is the working directory inside (default: Root with ProjectRO, else the first ReadOnly/Writable path).
	Chdir string
}

const (
	sandboxHome = "/home/sandbox"
	tmpSize     = 1 << 30 // bytes of the private /tmp and home (tmpfs, counted as memory)
)

// ProjectHide lists, among the places a task must not see, those that exist under root: the state directory
// (.duct/: state DB with password hashes, session and sharing keys, the catalog), duct.toml, .git, .env* files and
// any extra path (the local lake) that is under root.
func ProjectHide(root string, extra ...string) []string {
	var out []string
	add := func(p string) {
		if p = filepath.Clean(p); isUnder(p, root) && p != root {
			if _, err := os.Lstat(p); err == nil {
				out = append(out, p)
			}
		}
	}
	for _, n := range []string{".duct", "duct.toml", ".git"} {
		add(filepath.Join(root, n))
	}
	if m, _ := filepath.Glob(filepath.Join(root, ".env*")); len(m) > 0 {
		for _, p := range m {
			add(p)
		}
	}
	for _, p := range extra {
		if strings.Contains(p, "://") {
			continue
		}
		add(p)
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// Env replaces the allow-listed variables that point to host paths the sandbox does not have.
func Env(env []string) []string {
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

// Argv wraps argv in a bubblewrap command line.
//
// What the process sees: the system directories (/usr, /bin, /lib*, a few /etc files) and its Python environment
// read-only, a private tmpfs for /tmp and the home, its own /proc and /dev, and the project as Spec says. Every
// namespace is new except the network, which it needs to reach DUCT_URL on loopback (so it can also reach other
// loopback services of the host).
func Argv(sp Spec, argv []string) ([]string, error) {
	var binds []bind
	if sp.Python != "" {
		var err error
		if binds, err = pythonBinds(sp.Python, sp.Root); err != nil {
			return nil, err
		}
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
		"--size", fmt.Sprint(tmpSize), "--tmpfs", "/tmp",
		"--size", fmt.Sprint(tmpSize), "--tmpfs", sandboxHome)
	chdir := sp.Chdir
	if sp.ProjectRO {
		if chdir == "" {
			chdir = sp.Root
		}
		a = append(a, "--ro-bind", sp.Root, sp.Root)
	}
	for _, d := range sp.ReadOnly {
		if err := checkMount(sp, d); err != nil {
			return nil, err
		}
		a = append(a, "--ro-bind", d, d)
		if chdir == "" {
			chdir = d
		}
	}
	for _, d := range sp.Writable {
		if err := checkMount(sp, d); err != nil {
			return nil, err
		}
		for _, h := range sp.Hide {
			if isUnder(h, d) {
				return nil, fmt.Errorf("sandbox: the writable path %s contains %s, which must stay hidden", d, h)
			}
		}
		a = append(a, "--bind", d, d)
		if chdir == "" {
			chdir = d
		}
	}
	for _, h := range sp.Hide { // after the binds: they cover what the project mount exposed
		st, err := os.Lstat(h)
		if err != nil {
			continue
		}
		if st.IsDir() {
			a = append(a, "--tmpfs", h, "--remount-ro", h)
		} else {
			a = append(a, "--ro-bind", "/dev/null", h)
		}
	}
	for _, b := range binds { // the interpreter's environment may live in a hidden directory (.duct/venv)
		a = append(a, "--ro-bind", b.src, b.dst)
	}
	if chdir != "" {
		a = append(a, "--chdir", chdir)
	}
	a = append(a, "--")
	return append(a, argv...), nil
}

// checkMount refuses a bind that would hand the whole project to the sandbox, or a path that is hidden.
func checkMount(sp Spec, d string) error {
	if sp.ProjectRO {
		if !isUnder(d, sp.Root) {
			return fmt.Errorf("sandbox: %s is outside the project", d)
		}
		if d == sp.Root {
			return fmt.Errorf("sandbox: the project root itself cannot be a writable path")
		}
		for _, h := range sp.Hide {
			if isUnder(d, h) {
				return fmt.Errorf("sandbox: %s is inside %s, which is hidden", d, h)
			}
		}
		return nil
	}
	if isUnder(sp.Root, d) {
		return fmt.Errorf("sandbox: %s is or contains the project root", d)
	}
	return nil
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
			return nil, fmt.Errorf("sandbox: the sandbox needs a direct Python interpreter, got %q: %w", py, err)
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
			return nil, fmt.Errorf("sandbox: the Python environment at %s contains the project (%s) and would expose the lake and .duct/ to the sandbox: use a venv outside the project root's parents", d, root)
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
