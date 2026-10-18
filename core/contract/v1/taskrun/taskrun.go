// Package taskrun is the helper shared by the process-running TaskType modules
// (shell, python, notebook): it parses the common task parameters and runs a
// child process with a hard timeout contract (ctx cancellation kills the whole
// process group), an explicit environment and a bounded output tail.
//
// Reserved parameters the orchestrator adds to every TaskSpec.Params:
//   - "root": absolute project directory (default working directory).
//   - "env":  the COMPLETE environment of the child, as a map. The
//     orchestrator builds it from a small allow-list plus DUCT_URL/DUCT_TOKEN
//     and the task's own env, so lake and catalog credentials never reach
//     user code. A module must not add anything from its own environment.
//   - "sandbox": present when the task must run inside bubblewrap (ADR 0006, "Sandbox"): a map with
//     "hide" (paths under the root that do not exist for the child) and "writable" (the one project
//     directory the child may write). A module passes SandboxOf(params, isPython) to Cmd.Sandbox.
package taskrun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/sandbox"
)

// TailBytes is how much of the combined stdout/stderr is kept as the task output.
const TailBytes = 4096

// Cmd is one process to run. Argv is an argument vector: there is no shell,
// so nothing in it is ever interpreted.
type Cmd struct {
	Argv []string
	Dir  string
	Env  map[string]string // the whole environment of the child
	// Sandbox, when set, runs the process inside bubblewrap: the project read-only without its secrets, a private
	// /tmp, and Sandbox.Writable as the only writable project path.
	Sandbox *Sandbox
}

// Sandbox is the isolation of one task (see the package comment, parameter "sandbox").
type Sandbox struct {
	Root     string   // project directory
	Hide     []string // absolute paths under Root that the child does not see
	Writable string   // absolute directory under Root the child may write ("" = none)
	Python   bool     // argv[0] is a Python interpreter: its environment is made visible read-only
}

// SandboxOf reads the "sandbox" parameter the orchestrator adds (nil when the task is not sandboxed).
func SandboxOf(p contract.Config, python bool) *Sandbox {
	m, ok := p["sandbox"].(map[string]any)
	if !ok {
		return nil
	}
	sb := &Sandbox{Root: Root(p), Python: python}
	switch h := m["hide"].(type) {
	case []string:
		sb.Hide = h
	case []any:
		for _, x := range h {
			sb.Hide = append(sb.Hide, fmt.Sprint(x))
		}
	}
	if w, _ := m["writable"].(string); w != "" {
		if !filepath.IsAbs(w) {
			w = filepath.Join(sb.Root, w)
		}
		sb.Writable = filepath.Clean(w)
	}
	return sb
}

// wrap turns the command into its bubblewrap form.
func (sb *Sandbox) wrap(c Cmd) (argv, env []string, err error) {
	bw, err := sandbox.Available()
	if err != nil {
		return nil, nil, fmt.Errorf("sandbox: %w (install bubblewrap, or set sandbox: false on the task / [sandbox] in duct.toml)", err)
	}
	if rel, err := filepath.Rel(sb.Root, c.Dir); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, nil, fmt.Errorf("sandbox: the working directory %s is outside the project", c.Dir)
	}
	argv = append([]string(nil), c.Argv...)
	sp := sandbox.Spec{Bwrap: bw, Root: sb.Root, ProjectRO: true, Hide: sb.Hide, Chdir: c.Dir}
	if sb.Writable != "" {
		if err := os.MkdirAll(sb.Writable, 0o755); err != nil { //nolint:gosec // the task's work directory in the project
			return nil, nil, err
		}
		rw, err1 := filepath.EvalSymlinks(sb.Writable)
		rr, err2 := filepath.EvalSymlinks(sb.Root)
		if rel, err := filepath.Rel(rr, rw); err1 != nil || err2 != nil || err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil, nil, fmt.Errorf("sandbox: the writable directory %s must be a directory inside the project (symlinks are followed)", sb.Writable)
		}
		sp.Writable = []string{rw}
	}
	if sb.Python {
		if filepath.Base(argv[0]) == "uv" {
			return nil, nil, errors.New("sandbox: `uv run` cannot run inside the sandbox (it needs its cache and the network): give params.python a direct interpreter, or set sandbox: false on the task")
		}
		if strings.Contains(argv[0], "/") && !filepath.IsAbs(argv[0]) {
			argv[0] = filepath.Join(c.Dir, argv[0])
		}
		sp.Python = argv[0]
	}
	if argv, err = sandbox.Argv(sp, argv); err != nil {
		return nil, nil, err
	}
	return argv, sandbox.Env(envList(c.Env)), nil
}

// Run starts the process and waits for it. On ctx cancellation it kills the
// process group, waits until it is gone and returns ctx.Err(). A non-zero
// exit is an error that carries the output tail.
func Run(ctx context.Context, c Cmd) (contract.TaskResult, error) {
	if len(c.Argv) == 0 || c.Argv[0] == "" {
		return contract.TaskResult{}, errors.New("empty command")
	}
	if err := ctx.Err(); err != nil {
		return contract.TaskResult{}, err
	}
	argv, env := c.Argv, envList(c.Env)
	if c.Sandbox != nil {
		var err error
		if argv, env, err = c.Sandbox.wrap(c); err != nil {
			return contract.TaskResult{}, err
		}
	}
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // argv comes from the project's own jobs/*.yaml, no shell
	cmd.Dir = c.Dir
	cmd.Env = env
	tail := &tailBuffer{max: TailBytes}
	cmd.Stdout, cmd.Stderr = tail, tail
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return contract.TaskResult{}, fmt.Errorf("start %s: %w", c.Argv[0], err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		out := tail.String()
		if err != nil {
			return contract.TaskResult{Output: out}, fmt.Errorf("%s: %w\n%s", filepath.Base(c.Argv[0]), err, out)
		}
		return contract.TaskResult{Output: out}, nil
	case <-ctx.Done():
		termGroup(cmd) // polite first, then hard
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			killGroup(cmd)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
			}
		}
		return contract.TaskResult{Output: tail.String()}, ctx.Err()
	}
}

func envList(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+m[k])
	}
	return out
}

// tailBuffer keeps the last max bytes written; safe for the two copy goroutines of exec.
type tailBuffer struct {
	mu  sync.Mutex
	max int
	buf bytes.Buffer
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf.Write(p)
	if over := t.buf.Len() - t.max; over > 0 {
		t.buf.Next(over)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimRight(t.buf.String(), "\n")
}

// Root returns the "root" parameter ("" if absent).
func Root(p contract.Config) string {
	s, _ := p["root"].(string)
	return s
}

// Env returns the "env" parameter as the child's environment.
func Env(p contract.Config) map[string]string {
	out := map[string]string{}
	switch m := p["env"].(type) {
	case map[string]string:
		for k, v := range m {
			out[k] = v
		}
	case map[string]any:
		for k, v := range m {
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}

// Dir resolves the optional "cwd" parameter against the project root.
func Dir(p contract.Config) string {
	root := Root(p)
	cwd, _ := p["cwd"].(string)
	switch {
	case cwd == "":
		return root
	case filepath.IsAbs(cwd):
		return cwd
	default:
		return filepath.Join(root, cwd)
	}
}

// StringList reads a list of strings parameter. A single string is rejected
// on purpose: it would invite shell-style splitting.
func StringList(p contract.Config, key string) ([]string, error) {
	switch v := p[key].(type) {
	case nil:
		return nil, nil
	case []string:
		return v, nil
	case []any:
		out := make([]string, 0, len(v))
		for _, x := range v {
			out = append(out, fmt.Sprint(x))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%s must be a list of strings, not %T", key, v)
	}
}

// Script resolves a file parameter against the project root and checks it exists.
func Script(p contract.Config, key string) (string, error) {
	s, _ := p[key].(string)
	if s == "" {
		return "", fmt.Errorf("param %q is required", key)
	}
	if !filepath.IsAbs(s) {
		s = filepath.Join(Root(p), s)
	}
	if st, err := os.Stat(s); err != nil || st.IsDir() {
		return "", fmt.Errorf("%s: not a file", s)
	}
	return s, nil
}
