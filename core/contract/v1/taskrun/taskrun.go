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
)

// TailBytes is how much of the combined stdout/stderr is kept as the task output.
const TailBytes = 4096

// Cmd is one process to run. Argv is an argument vector: there is no shell,
// so nothing in it is ever interpreted.
type Cmd struct {
	Argv []string
	Dir  string
	Env  map[string]string // the whole environment of the child
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
	cmd := exec.Command(c.Argv[0], c.Argv[1:]...) //nolint:gosec // argv comes from the project's own jobs/*.yaml, no shell
	cmd.Dir = c.Dir
	cmd.Env = envList(c.Env)
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
