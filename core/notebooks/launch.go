package notebooks

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/udf"
)

// execLauncher runs `python -m marimo edit|run` on 127.0.0.1 with a random
// port and a token password (marimo's own authentication, which the proxy of
// the console answers; nothing else on the machine knows it).
type execLauncher struct {
	Python  func(ctx context.Context) ([]string, error)
	Dir     string // project root
	Sandbox string // SandboxBwrap wraps the process in bubblewrap (sandbox.go); anything else runs it as is
}

const startTimeout = 2 * time.Minute

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Launch implements Launcher.
func (l *execLauncher) Launch(ctx context.Context, spec LaunchSpec) (Process, string, error) {
	if l.Python == nil {
		return nil, "", errors.New("notebooks: no Python interpreter configured")
	}
	py, err := l.Python(ctx)
	if err != nil {
		return nil, "", err
	}
	port, err := freePort()
	if err != nil {
		return nil, "", err
	}
	args := append(append([]string{}, py[1:]...), "-m", "marimo", spec.Mode,
		"--headless", "--host", "127.0.0.1", "--port", strconv.Itoa(port),
		"--base-url", spec.BasePath, "--token-password-file", "-")
	if spec.Mode == contract.NotebookEdit {
		args = append(args, "--skip-update-check", "--no-sandbox")
	}
	args = append(args, spec.File)
	env := append(udf.SafeEnv(spec.Env), "MARIMO_SKIP_UPDATE_CHECK=1")
	argv := append([]string{py[0]}, args...)
	dir := l.Dir
	if l.Sandbox == SandboxBwrap {
		bw, err := BwrapAvailable()
		if err != nil {
			return nil, "", err
		}
		nbDir := filepath.Dir(spec.File)
		if argv, err = bwrapArgv(sandboxSpec{Bwrap: bw, Python: py, ProjectRoot: l.Dir, NotebooksDir: nbDir, Writable: spec.Mode == contract.NotebookEdit}, argv); err != nil {
			return nil, "", err
		}
		env, dir = sandboxEnv(env), "" // inside, the working directory is the notebooks dir (bwrap --chdir)
	}
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // interpreter from duct.toml / the managed venv, behind bwrap when sandboxed
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdin = strings.NewReader(spec.Password + "\n")
	tail := &tailBuffer{max: 4096}
	cmd.Stdout, cmd.Stderr = tail, tail // marimo prints a URL with the access token: never forwarded
	cmd.WaitDelay = 3 * time.Second     // kernel children may keep the output pipes open
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return nil, "", fmt.Errorf("start %s: %w", py[0], err)
	}
	p := &execProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	addr := "127.0.0.1:" + strconv.Itoa(port)
	if err := waitReady(ctx, addr, spec, p.done); err != nil {
		p.Stop()
		msg := strings.TrimSpace(strings.ReplaceAll(tail.String(), spec.Password, "***"))
		if msg != "" {
			err = fmt.Errorf("%w\n%s", err, lastLines(msg, 6))
		}
		return nil, "", fmt.Errorf("%w (is marimo installed? add duct-data[marimo] to the Python environment)", err)
	}
	return p, addr, nil
}

func lastLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// waitReady polls the notebook URL until marimo answers with something other than a refused connection.
func waitReady(ctx context.Context, addr string, spec LaunchSpec, exited <-chan struct{}) error {
	deadline := time.After(startTimeout)
	cl := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+spec.BasePath+"/", http.NoBody)
		req.SetBasicAuth("duct", spec.Password)
		if resp, err := cl.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return nil
		}
		select {
		case <-exited:
			return errors.New("marimo exited while starting")
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return errors.New("marimo did not start in time")
		case <-time.After(150 * time.Millisecond):
		}
	}
}

type execProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	once sync.Once
}

func (p *execProcess) Done() <-chan struct{} { return p.done }

// Stop asks the process group to end (SIGTERM: marimo shuts its kernels down)
// and returns at once; a watcher kills what is left after a grace period.
func (p *execProcess) Stop() {
	p.once.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		termGroup(p.cmd)
		go func() {
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
			}
			killGroup(p.cmd) // also what outlived marimo in its group
		}()
	})
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	max int
	b   bytes.Buffer
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.b.Write(p)
	if over := t.b.Len() - t.max; over > 0 {
		t.b.Next(over)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	sc := bufio.NewScanner(bytes.NewReader(t.b.Bytes()))
	var out []string
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return strings.Join(out, "\n")
}
