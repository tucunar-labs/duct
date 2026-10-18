// Package web is the Duct workspace UI (ADR 0012) as a Protocol module: login,
// catalog browser and SQL editor, served by `duct serve` next to the other
// protocols. Everything the browser needs is embedded in the binary.
//
// The module never touches data except through the QueryHandler, always as
// the identity of the logged-in session.
package web

//go:generate templ generate
//go:generate tailwindcss -i ui/input.css -o static/app.css --minify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

const name = "web"

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindProtocol,
		Name: name,
		New:  func() contract.Module { return &Web{} },
	})
}

// Web implements contract.Protocol.
//
// Config keys (all optional): "notebooks_listen" (address of the separate origin that serves the
// notebooks, default: the console's host with port+1, i.e. 127.0.0.1:8485; "off" disables notebooks),
// "notebooks_url" (public origin of that listener, like "https://nb.example.com", for use behind a
// proxy; give it a host name of its own, a sibling subdomain of the console's),
// "listen" (address, default "127.0.0.1:8484";
// "addr" is accepted as an alias; use port 0 for a random one, see Addr), "secure_cookies" (bool; default: set the Secure flag
// when the request is HTTPS or X-Forwarded-Proto says so), "session_ttl"
// (absolute session timeout, Go duration, default 12h), "session_idle_timeout"
// (default 2h, capped to session_ttl), "session_key" (base64 of 32 bytes; or env
// DUCT_WEB_SESSION_KEY), "session_key_file" (path where a generated key is
// persisted, mode 0600; `duct serve` passes .duct/web_session.key), "session_key_previous"
// (old key still accepted for decryption, env DUCT_WEB_SESSION_KEY_PREVIOUS;
// key rotation), "trust_proxy" (bool: client IP from the right-most
// X-Forwarded-For, default false), "login_max_attempts" (per username, default
// 5), "login_ip_max_attempts" (per IP, default 20), "login_lockout" (default 15m,
// doubles on repeated lockouts, max 24h), "row_limit" (rows shown per query, default
// 1000), "query_timeout" (Go duration, default 60s), "dashboards_dir" (folder with
// the dashboards/*.yaml files; `duct serve` fills it with <project>/dashboards;
// unset: no dashboards).
type Web struct {
	ln           net.Listener
	addr         string
	nbLn         net.Listener // the notebook origin (nbauth.go); nil: notebooks are disabled
	nbURL        string
	secure       *bool
	sessionTTL   time.Duration
	idleTimeout  time.Duration
	keys         [][]byte
	trustProxy   bool
	userMax      int
	ipMax        int
	lockout      time.Duration
	rowLimit     int
	queryTimeout time.Duration
	dashDir      string
}

var (
	_ contract.Protocol  = (*Web)(nil)
	_ contract.Addresser = (*Web)(nil)
)

// Name implements contract.Module.
func (*Web) Name() string { return name }

// Init implements contract.Module.
func (w *Web) Init(_ context.Context, cfg contract.Config) error {
	w.addr = "127.0.0.1:8484"
	w.sessionTTL = 12 * time.Hour
	w.rowLimit = 1000
	w.queryTimeout = 60 * time.Second
	for _, k := range []string{"addr", "listen"} {
		if s, ok := cfg[k].(string); ok && s != "" {
			w.addr = s
		}
	}
	if s, ok := cfg["dashboards_dir"].(string); ok {
		w.dashDir = s
	}
	if b, ok := cfg["secure_cookies"].(bool); ok {
		w.secure = &b
	}
	var err error
	if w.sessionTTL, err = durationCfg(cfg, "session_ttl", w.sessionTTL); err != nil {
		return err
	}
	if w.queryTimeout, err = durationCfg(cfg, "query_timeout", w.queryTimeout); err != nil {
		return err
	}
	if w.idleTimeout, err = durationCfg(cfg, "session_idle_timeout", 0); err != nil {
		return err
	}
	if w.lockout, err = durationCfg(cfg, "login_lockout", 0); err != nil {
		return err
	}
	if b, ok := cfg["trust_proxy"].(bool); ok {
		w.trustProxy = b
	}
	if w.userMax, err = intCfg(cfg, "login_max_attempts"); err != nil {
		return err
	}
	if w.ipMax, err = intCfg(cfg, "login_ip_max_attempts"); err != nil {
		return err
	}
	if w.keys, err = resolveKeys(cfg, os.Getenv); err != nil {
		return err
	}
	switch n := cfg["row_limit"].(type) {
	case int:
		w.rowLimit = n
	case float64:
		w.rowLimit = int(n)
	case nil:
	default:
		return fmt.Errorf("web: row_limit must be a number, got %T", n)
	}
	if w.rowLimit <= 0 {
		return errors.New("web: row_limit must be positive")
	}
	// Bind now so Addr() is known to the caller before Serve (contract.Addresser).
	ln, err := net.Listen("tcp", w.addr)
	if err != nil {
		return fmt.Errorf("web: %w", err)
	}
	w.ln = ln
	return w.initNotebooks(cfg, ln)
}

// initNotebooks binds the notebook origin. An explicit notebooks_listen that cannot be bound is an
// error; the derived default (the console's port + 1) only disables notebooks with a warning.
func (w *Web) initNotebooks(cfg contract.Config, console net.Listener) error {
	if s, ok := cfg["notebooks_url"].(string); ok && s != "" {
		u, err := url.Parse(s)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
			return fmt.Errorf("web: notebooks_url %q must look like https://nb.example.com", s)
		}
		if safeOrigin(u.Scheme+"://"+u.Host) == "" {
			return fmt.Errorf("web: notebooks_url %q has characters not allowed in an origin", s)
		}
		w.nbURL = u.Scheme + "://" + u.Host
	}
	addr, explicit := "", false
	if s, ok := cfg["notebooks_listen"].(string); ok && s != "" {
		if s == "off" {
			return nil
		}
		addr, explicit = s, true
	} else {
		host, port, _ := net.SplitHostPort(console.Addr().String())
		if _, reqPort, err := net.SplitHostPort(w.addr); err == nil && reqPort != "0" {
			p, _ := strconv.Atoi(port)
			port = strconv.Itoa(p + 1)
		} else {
			port = "0"
		}
		addr = net.JoinHostPort(host, port)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		if explicit {
			return fmt.Errorf("web: notebooks_listen: %w", err)
		}
		slog.Warn("web: the notebook origin could not listen, notebooks are disabled (set notebooks_listen)", "addr", addr, "err", err)
		return nil
	}
	w.nbLn = ln
	return nil
}

// NotebooksAddr is the bound address of the notebook origin ("" if disabled).
func (w *Web) NotebooksAddr() string {
	if w.nbLn == nil {
		return ""
	}
	return w.nbLn.Addr().String()
}

// Addr implements contract.Addresser: the bound "host:port" after Init.
func (w *Web) Addr() string {
	if w.ln == nil {
		return w.addr
	}
	return w.ln.Addr().String()
}

func intCfg(cfg contract.Config, key string) (int, error) {
	switch n := cfg[key].(type) {
	case nil:
		return 0, nil
	case int:
		if n > 0 {
			return n, nil
		}
	case float64:
		if n >= 1 {
			return int(n), nil
		}
	}
	return 0, fmt.Errorf("web: %s must be a positive number", key)
}

func durationCfg(cfg contract.Config, key string, def time.Duration) (time.Duration, error) {
	s, ok := cfg[key].(string)
	if !ok || s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("web: bad %s %q", key, s)
	}
	return d, nil
}

// Handler returns the HTTP handler of the UI. Exposed so `duct serve` (or a
// test) can mount it without opening a listener.
func (w *Web) Handler(h contract.QueryHandler, auth contract.AuthProvider) http.Handler {
	console, _ := w.Handlers(h, auth)
	return console
}

// Handlers returns the handler of the console and the one of the notebook origin; they share the
// sessions (the notebook origin only honours tickets minted by the console).
func (w *Web) Handlers(h contract.QueryHandler, auth contract.AuthProvider) (console, notebooks http.Handler) {
	limit, timeout, ttl := w.rowLimit, w.queryTimeout, w.sessionTTL
	if limit == 0 {
		limit = 1000
	}
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	if ttl == 0 {
		ttl = 12 * time.Hour
	}
	cfg := appConfig{
		rowLimit: limit, queryTimeout: timeout, sessionTTL: ttl, secure: w.secure,
		idleTimeout: w.idleTimeout, keys: w.keys, trustProxy: w.trustProxy,
		userMax: w.userMax, ipMax: w.ipMax, lockout: w.lockout, minLoginTime: 200 * time.Millisecond, dashboardsDir: w.dashDir,
		nbURL: w.nbURL,
	}
	if w.nbLn != nil {
		_, cfg.nbPort, _ = net.SplitHostPort(w.nbLn.Addr().String())
	}
	a := newApp(h, auth, cfg)
	return a.routes(), a.nbRoutes()
}

// Serve implements contract.Protocol: it blocks until ctx is cancelled.
func (w *Web) Serve(ctx context.Context, h contract.QueryHandler, auth contract.AuthProvider) error {
	ln := w.ln
	if ln == nil {
		return errors.New("web: Serve before Init")
	}
	console, nbHandler := w.Handlers(h, auth)
	newSrv := func(h http.Handler) *http.Server {
		return &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second, BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) }}
	}
	srvs := map[*http.Server]net.Listener{newSrv(console): ln}
	if w.nbLn != nil {
		if w.nbURL == "" {
			slog.Warn("web: the notebook origin shares the console's host name (cookies ignore ports): fine to try out, but set notebooks_url to a host name of its own in production")
		}
		srvs[newSrv(nbHandler)] = w.nbLn
	}
	done := make(chan error, len(srvs))
	for srv, l := range srvs {
		go func() { done <- srv.Serve(l) }()
	}
	var first error
	select {
	case err := <-done:
		first = fmt.Errorf("web: %w", err)
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for srv := range srvs {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
		}
	}
	for range srvs {
		select {
		case <-done:
		case <-time.After(6 * time.Second):
		}
	}
	return first
}

// Close implements contract.Module: it releases the listener if Serve never
// ran (or already stopped); closing twice is fine.
func (w *Web) Close() error {
	if w.nbLn != nil {
		_ = w.nbLn.Close()
	}
	if w.ln == nil {
		return nil
	}
	if err := w.ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}
