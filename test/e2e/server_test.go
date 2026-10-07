//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Readiness budgets are generous on purpose: the e2e suite runs on loaded
// machines (CI, `just check` in parallel), where starting a server (DuckDB
// extensions, catalog attach) can take far longer than on an idle laptop.
const (
	serveStartBudget = 3 * time.Minute // "serving ..." lines
	serveReadyBudget = 2 * time.Minute // /readyz = 200
	retryBudget      = 30 * time.Second
)

// logTail keeps the last lines a process wrote (stdout and stderr), safe for
// concurrent use, so a failure can print what the server said.
type logTail struct {
	mu    sync.Mutex
	lines []string
	max   int
}

func (l *logTail) add(line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, line)
	if len(l.lines) > l.max {
		l.lines = l.lines[len(l.lines)-l.max:]
	}
}

func (l *logTail) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// server is a running `duct serve` child process.
type server struct {
	t     *testing.T
	step  string // test name, for failure messages
	cmd   *exec.Cmd
	log   *logTail
	addrs map[string]string
	exit  chan error
}

// failf fails the test with the step, what went wrong and the last 50 lines of
// server output.
func (s *server) failf(format string, a ...any) { s.failTf(s.t, format, a...) }

// failTf is failf on a (sub)test t: FailNow must run on the goroutine of the
// test that calls it.
func (s *server) failTf(t *testing.T, format string, a ...any) {
	t.Helper()
	t.Fatalf("step %q: %s\n--- last 50 lines of `duct serve` output ---\n%s\n--- end ---",
		s.step, fmt.Sprintf(format, a...), s.log)
}

// startServer starts `duct serve` with the given --listen/--protocols flags
// (want = protocols that must report an address) and returns once every
// protocol is bound AND /readyz answers 200. On test failure the last 50 lines
// of server output are logged.
func startServer(t *testing.T, bin, root string, want int, extra ...string) *server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	args := append([]string{"serve", "--project", root}, extra...)
	cmd := exec.CommandContext(ctx, bin, args...) //nolint:gosec // test binary
	cmd.Dir = root
	s := &server{t: t, step: t.Name(), cmd: cmd, log: &logTail{max: 50}, addrs: map[string]string{}, exit: make(chan error, 1)}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("step %q: start duct serve: %v", s.step, err)
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-s.exit:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
		}
		if t.Failed() {
			t.Logf("step %q: last 50 lines of `duct serve` output:\n%s", s.step, s.log)
		}
	})

	var mu sync.Mutex
	bound := make(chan struct{})
	var once sync.Once
	var readers sync.WaitGroup
	scan := func(r io.Reader, parse bool) {
		defer readers.Done()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			s.log.add(line)
			if !parse {
				continue
			}
			if f := strings.Fields(line); len(f) == 4 && f[0] == "serving" {
				mu.Lock()
				s.addrs[f[1]] = f[3]
				if len(s.addrs) == want {
					once.Do(func() { close(bound) })
				}
				mu.Unlock()
			}
		}
	}
	readers.Add(2)
	go scan(stdout, true)
	go scan(stderr, false)
	go func() { readers.Wait(); s.exit <- cmd.Wait() }()

	select {
	case <-bound:
	case err := <-s.exit:
		s.exit <- err // keep it for the cleanup
		s.failf("duct serve exited before binding %d protocols: %v", want, err)
	case <-time.After(serveStartBudget):
		mu.Lock()
		got := fmt.Sprint(s.addrs)
		mu.Unlock()
		s.failf("duct serve did not report %d protocols within %s (got %s)", want, serveStartBudget, got)
	}
	mu.Lock()
	defer mu.Unlock()
	s.waitReady(s.addrs["http"])
	t.Logf("serving: %v", s.addrs)
	return s
}

// waitReady polls GET /readyz until 200, tolerating connection refused and 503
// while the server finishes starting.
func (s *server) waitReady(httpAddr string) {
	s.t.Helper()
	if httpAddr == "" {
		return
	}
	deadline := time.Now().Add(serveReadyBudget)
	cl := &http.Client{Timeout: 10 * time.Second}
	var last string
	for time.Now().Before(deadline) {
		select {
		case err := <-s.exit:
			s.exit <- err
			s.failf("duct serve exited while waiting for /readyz: %v", err)
		default:
		}
		resp, err := cl.Get("http://" + httpAddr + "/readyz")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			last = fmt.Sprintf("status %d: %s", resp.StatusCode, b)
		} else {
			last = err.Error()
		}
		time.Sleep(250 * time.Millisecond)
	}
	s.failf("/readyz not 200 within %s (last: %s)", serveReadyBudget, last)
}

// transient reports errors worth retrying: the listener is not accepting yet
// or the connection dropped.
func transient(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		(errors.As(err, &ne) && ne.Timeout()) || strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "cannot open a session")
}

// retry runs fn until it succeeds, fails with a non-transient error, or the
// budget runs out; it returns the last error.
func retry(fn func() error) error {
	deadline := time.Now().Add(retryBudget)
	for delay := 100 * time.Millisecond; ; delay = min(delay*2, 2*time.Second) {
		err := fn()
		if err == nil || !transient(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(delay)
	}
}
