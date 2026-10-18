// Package notebook is the notebook Protocol of `duct serve` (ADR 0006): it
// supervises a Python process that serves the project's marimo notebooks as an
// ASGI app (duct.notebook.asgi) behind an authentication middleware that covers
// http AND websocket. The credentials are checked here, in Go, through the
// AuthProvider: the Python side only forwards what the browser sent to a
// loopback endpoint guarded by a per-process secret, and never sees the lake or
// the credential store.
package notebook

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/sandbox"
	"github.com/alonsoburon/duct/core/registry"
)

const (
	name          = "notebook"
	defaultListen = "127.0.0.1:8124"
	defaultRole   = "duct_admin"
	// AuthHeader carries the per-process secret on the loopback auth endpoint.
	AuthHeader = "X-Duct-Auth"
)

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindProtocol,
		Name: name,
		New:  func() contract.Module { return &Server{} },
	})
}

// Server implements contract.Protocol and contract.Addresser.
//
// Config keys (`[serve.notebook]`; `duct serve` fills dir, duct_url and python):
//
//	listen     loopback address of the notebook web app (default 127.0.0.1:8124)
//	dir        folder with the marimo notebooks (default <project>/notebooks)
//	role       the only role allowed in, and the role of the identity the notebooks
//	           query as (default duct_admin)
//	token_env  environment variable that holds a token of a user of that role; the
//	           notebooks get it as DUCT_TOKEN (default DUCT_NOTEBOOK_TOKEN)
//	python     argv prefix of the interpreter (default python3)
//	duct_url   base URL of the HTTP protocol, given to the notebooks as DUCT_URL
//
// It is not started by default: name it in [serve].protocols or --protocols.
type Server struct {
	listen, dir, role, tokenEnv, ductURL string
	python                               []string
	// sandbox is sandbox.Bwrap to run the Python app inside bubblewrap (read-only notebooks folder, private
	// /tmp, no lake, no .duct/, loopback network); sandboxAuto lets an interpreter that cannot be wrapped
	// (`uv run`) start unisolated with a warning. Filled by `duct serve` from [sandbox] jobs.
	sandbox     string
	sandboxAuto bool

	app     net.Listener // reserved port of the Python app
	appAddr string
	authLn  net.Listener
	secret  string
	lim     limiter
}

var (
	_ contract.Protocol  = (*Server)(nil)
	_ contract.Addresser = (*Server)(nil)
)

// Name implements contract.Module.
func (*Server) Name() string { return name }

// Addr implements contract.Addresser: where the notebook web app listens.
func (s *Server) Addr() string { return s.appAddr }

// Init implements contract.Module.
func (s *Server) Init(_ context.Context, cfg contract.Config) error {
	str := func(k, def string) string {
		if v, ok := cfg[k].(string); ok && v != "" {
			return v
		}
		return def
	}
	s.listen = str("listen", defaultListen)
	s.role = str("role", defaultRole)
	s.tokenEnv = str("token_env", "DUCT_NOTEBOOK_TOKEN")
	s.ductURL = str("duct_url", "")
	s.dir = str("dir", "")
	s.sandbox = str("sandbox", sandbox.None)
	s.sandboxAuto, _ = cfg["sandbox_auto"].(bool)
	if s.dir == "" {
		return errors.New("notebook: dir is required (`duct serve` sets <project>/notebooks)")
	}
	if st, err := os.Stat(s.dir); err != nil || !st.IsDir() {
		return fmt.Errorf("notebook: %s is not a directory (create it and put marimo notebooks there)", s.dir)
	}
	switch v := cfg["python"].(type) {
	case []string:
		s.python = v
	case []any:
		for _, x := range v {
			if t, ok := x.(string); ok {
				s.python = append(s.python, t)
			}
		}
	case string:
		s.python = strings.Fields(v)
	}
	if len(s.python) == 0 {
		s.python = []string{"python3"}
	}
	host, _, err := net.SplitHostPort(s.listen)
	if err != nil {
		return fmt.Errorf("notebook: listen: %w", err)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("notebook: listen %q: notebooks run code, they may only listen on loopback (put a TLS reverse proxy in front)", s.listen)
	}
	// Reserve the port now so Addr is known before Serve; the child takes it over.
	if s.app, err = net.Listen("tcp", s.listen); err != nil {
		return fmt.Errorf("notebook: %w", err)
	}
	s.appAddr = s.app.Addr().String()
	if s.authLn, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
		_ = s.app.Close()
		return fmt.Errorf("notebook: %w", err)
	}
	var b [32]byte
	_, _ = rand.Read(b[:])
	s.secret = hex.EncodeToString(b[:])
	return nil
}

// Close implements contract.Module.
func (s *Server) Close() error {
	for _, l := range []net.Listener{s.app, s.authLn} {
		if l != nil {
			_ = l.Close()
		}
	}
	return nil
}

// Serve implements contract.Protocol.
func (s *Server) Serve(ctx context.Context, _ contract.QueryHandler, auth contract.AuthProvider) error {
	if s.authLn == nil {
		return errors.New("notebook: not initialised")
	}
	env := map[string]string{}
	if tok := os.Getenv(s.tokenEnv); tok != "" {
		id, err := auth.Authenticate(ctx, contract.Credentials{Scheme: "bearer", Secret: tok})
		if err != nil {
			return fmt.Errorf("notebook: %s is not a valid token: %w", s.tokenEnv, err)
		}
		if id.Role != s.role {
			return fmt.Errorf("notebook: %s belongs to role %q, notebooks run as role %q", s.tokenEnv, id.Role, s.role)
		}
		env["DUCT_TOKEN"] = tok
	}
	if s.ductURL != "" {
		env["DUCT_URL"] = s.ductURL
	}
	env["DUCT_NOTEBOOK_AUTH_SECRET"] = s.secret

	mux := http.NewServeMux()
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) { s.handleAuth(w, r, auth) })
	authSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	authErr := make(chan error, 1)
	go func() { authErr <- authSrv.Serve(s.authLn) }()
	defer func() { _ = authSrv.Close() }()

	host, port, _ := net.SplitHostPort(s.appAddr)
	_ = s.app.Close() // hand the port to the child
	argv := append(append([]string{}, s.python...), "-m", "duct.notebook.asgi",
		"--dir", s.dir, "--host", host, "--port", port,
		"--auth-url", "http://"+s.authLn.Addr().String()+"/auth", "--allow-role", s.role)
	envList, dir := childEnv(env), filepath.Dir(s.dir)
	if s.sandbox == sandbox.Bwrap {
		wrapped, err := s.wrap(argv)
		switch {
		case err == nil:
			argv, envList, dir = wrapped, sandbox.Env(envList), ""
		case s.sandboxAuto:
			slog.Warn("notebook: the Python app runs without the sandbox (sandbox = auto): " + err.Error())
		default:
			return fmt.Errorf("notebook: %w", err)
		}
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // interpreter from the project's own duct.toml
	cmd.Dir = dir
	cmd.Env = envList
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("notebook: start %s: %w", argv[0], err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("notebook: the Python app exited: %v (is duct-data[marimo] installed in %q?)", err, strings.Join(s.python, " "))
	case err := <-authErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		<-done
		return nil
	}
}

// wrap puts the Python app in bubblewrap: only the notebooks folder (read-only: this protocol only runs them) is
// visible of the project.
func (s *Server) wrap(argv []string) ([]string, error) {
	bw, err := sandbox.Available()
	if err != nil {
		return nil, err
	}
	if b := filepath.Base(argv[0]); b == "uv" || b == "uvx" {
		return nil, errors.New("`uv run` cannot run inside the sandbox: set [python] command to a direct interpreter, or [sandbox] notebooks = \"none\"")
	}
	py := argv[0]
	if strings.Contains(py, "/") && !filepath.IsAbs(py) {
		py = filepath.Join(filepath.Dir(s.dir), py)
		argv = append([]string{py}, argv[1:]...)
	}
	return sandbox.Argv(sandbox.Spec{Bwrap: bw, Python: py, Root: filepath.Dir(s.dir), ReadOnly: []string{s.dir}, Chdir: s.dir}, argv)
}

// childEnv is a small allow-list of the server environment plus extra: the
// notebook process never inherits lake or catalog credentials.
func childEnv(extra map[string]string) []string {
	var env []string
	for _, k := range []string{"PATH", "HOME", "USER", "LANG", "LC_ALL", "TMPDIR", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR",
		"VIRTUAL_ENV", "PYTHONPATH", "PYTHONHOME", "UV_CACHE_DIR", "UV_PROJECT_ENVIRONMENT", "XDG_CACHE_HOME", "XDG_DATA_HOME", "XDG_CONFIG_HOME"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// handleAuth answers the Python middleware: 200 {"user","role"} for valid
// credentials of the notebook role, 401 for bad ones, 403 for a valid user of
// another role, 429 after repeated password failures.
func (s *Server) handleAuth(w http.ResponseWriter, r *http.Request, auth contract.AuthProvider) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get(AuthHeader)), []byte(s.secret)) != 1 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	cred, ok := credentials(r.Header.Get("Authorization"))
	if !ok {
		http.Error(w, "unauthenticated", http.StatusUnauthorized)
		return
	}
	// Tokens carry enough entropy not to need throttling (and a flood of bad ones must
	// not lock everybody out); passwords are throttled per user.
	key := cred.User
	if key != "" && s.lim.blocked(key) {
		http.Error(w, "too many failures", http.StatusTooManyRequests)
		return
	}
	id, err := auth.Authenticate(r.Context(), cred)
	if err != nil {
		if errors.Is(err, contract.ErrUnauthenticated) {
			if key != "" {
				s.lim.fail(key)
			}
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		slog.Error("notebook: authentication provider failed", "error", err)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if id.Role != s.role {
		http.Error(w, "role not allowed", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"user": id.User, "role": id.Role})
}

func credentials(h string) (contract.Credentials, bool) {
	scheme, rest, ok := strings.Cut(h, " ")
	if !ok || rest == "" {
		return contract.Credentials{}, false
	}
	switch strings.ToLower(scheme) {
	case "bearer":
		return contract.Credentials{Scheme: "bearer", Secret: strings.TrimSpace(rest)}, true
	case "basic":
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rest))
		if err != nil {
			return contract.Credentials{}, false
		}
		u, p, ok := strings.Cut(string(raw), ":")
		if !ok || u == "" {
			return contract.Credentials{}, false
		}
		return contract.Credentials{Scheme: "password", User: u, Secret: p}, true
	}
	return contract.Credentials{}, false
}

// limiter blocks a key for a minute after five failures in a minute.
type limiter struct {
	mu    sync.Mutex
	fails map[string][]time.Time
}

const (
	maxFails = 5
	window   = time.Minute
)

func (l *limiter) prune(key string, now time.Time) []time.Time {
	var keep []time.Time
	for _, t := range l.fails[key] {
		if now.Sub(t) < window {
			keep = append(keep, t)
		}
	}
	if len(keep) == 0 {
		delete(l.fails, key)
	} else {
		l.fails[key] = keep
	}
	return keep
}

func (l *limiter) blocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.prune(key, time.Now())) >= maxFails
}

func (l *limiter) fail(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fails == nil {
		l.fails = map[string][]time.Time{}
	}
	if len(l.fails) > 10000 { // bound memory under a flood of distinct keys
		l.fails = map[string][]time.Time{}
	}
	l.fails[key] = append(l.prune(key, time.Now()), time.Now())
}
