// Package servepg is the Postgres wire Protocol of `duct serve`, built on the
// duckgres fork (ADR 0008): psql, JDBC, ODBC, Power BI and Excel connect to it
// as a Duct user and see only the views of their role.
//
// Per-session recipe: own accept loop + Duct auth (cleartext password message,
// ideally over TLS) -> a private in-process DuckDB with the lake attached
// READ ONLY and locked down -> duckgres' QueryAccessPolicy (read-only, role
// schema only, bare names resolved into it) -> duckgres' message loop.
//
// This module cannot use the shared contract.QueryHandler for execution:
// duckgres needs its own DuckDB connection (pg_catalog emulation installs
// session views and macros). The security does not rest on the handler but on
// the same three layers as the rest of the server: read-only attach, the
// DuckDB lockdown of ADR 0007, and the statement authorizer. See AGENTS.md.
package servepg

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/posthog/duckgres/server"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

const (
	name            = "pg"
	defaultListen   = "127.0.0.1:5433"
	startupTimeout  = 30 * time.Second
	defaultMaxConns = 32
	defaultIdle     = 15 * time.Minute
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
// Config keys: listen (default 127.0.0.1:5433; :0 = random port), tls_cert +
// tls_key (both or none; without them the password travels in clear text, so a
// non-loopback listen address needs allow_insecure = true), max_connections
// (default 32), idle_timeout (Go duration, default 15m), memory_limit (DuckDB
// per session, default 256MB), threads (per session, default 2). `catalog` and
// `data_path` are set by `duct serve` (the lake to attach read-only).
type Server struct {
	ln       net.Listener
	tlsCfg   *tls.Config
	lake     lake
	maxConns int
	idle     time.Duration
	sessCfg  sessionConfig

	srv      *server.Server
	conns    chan struct{}
	pids     atomic.Int32
	roles    sync.Map // user -> role, for the audit hook
	auditor  atomic.Pointer[contract.Auditor]
	failures failureTracker
	wg       sync.WaitGroup
}

var (
	_ contract.Protocol  = (*Server)(nil)
	_ contract.Addresser = (*Server)(nil)
)

// Name implements contract.Module.
func (*Server) Name() string { return name }

// Addr implements contract.Addresser.
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func intKey(cfg contract.Config, key string, def int) (int, error) {
	switch v := cfg[key].(type) {
	case nil:
		return def, nil
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	}
	return 0, fmt.Errorf("serve-pg: %s must be a number", key)
}

// Init implements contract.Module: it validates the lake, checks that a
// session can be opened (extensions available, catalog readable) and binds
// the listener.
func (s *Server) Init(ctx context.Context, cfg contract.Config) error {
	listen := defaultListen
	if v, ok := cfg["listen"].(string); ok && v != "" {
		listen = v
	}
	var err error
	if s.lake, err = parseLake(cfg); err != nil {
		return err
	}
	if s.maxConns, err = intKey(cfg, "max_connections", defaultMaxConns); err != nil {
		return err
	}
	if s.maxConns < 1 {
		return errors.New("serve-pg: max_connections must be >= 1")
	}
	s.idle = defaultIdle
	if v, ok := cfg["idle_timeout"].(string); ok && v != "" {
		if s.idle, err = time.ParseDuration(v); err != nil {
			return fmt.Errorf("serve-pg: idle_timeout: %w", err)
		}
	}
	s.sessCfg = sessionConfig{memoryLimit: "256MB", threads: 2}
	if v, ok := cfg["memory_limit"].(string); ok && v != "" {
		s.sessCfg.memoryLimit = v
	}
	if s.sessCfg.threads, err = intKey(cfg, "threads", 2); err != nil || s.sessCfg.threads < 1 {
		return errors.New("serve-pg: threads must be a number >= 1")
	}

	cert, _ := cfg["tls_cert"].(string)
	key, _ := cfg["tls_key"].(string)
	if (cert == "") != (key == "") {
		return errors.New("serve-pg: tls_cert and tls_key must be set together")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return fmt.Errorf("serve-pg: load TLS pair: %w", err)
		}
		s.tlsCfg = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	} else if insecure, _ := cfg["allow_insecure"].(bool); !insecure && !isLoopback(listen) {
		return fmt.Errorf("serve-pg: %s is not a loopback address and TLS is not configured: set tls_cert and tls_key (or allow_insecure = true)", listen)
	}

	// Fail early (and warm the extension cache) instead of at the first login.
	probe, err := openSession(ctx, s.lake, s.sessCfg)
	if err != nil {
		return fmt.Errorf("serve-pg: cannot attach the lake read-only: %w", err)
	}
	_ = probe.Close()

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("serve-pg: %w", err)
	}
	s.ln = ln
	s.conns = make(chan struct{}, s.maxConns)
	s.pids.Store(1000)
	s.srv = &server.Server{}
	server.InitMinimalServer(s.srv, server.Config{QueryAuditHook: s.auditHook}, nil)
	return nil
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Close implements contract.Module.
func (s *Server) Close() error {
	if s.ln != nil {
		_ = s.ln.Close()
	}
	return nil
}

// Serve implements contract.Protocol.
func (s *Server) Serve(ctx context.Context, h contract.QueryHandler, auth contract.AuthProvider) error {
	if s.ln == nil {
		return errors.New("serve-pg: not initialised")
	}
	if a, ok := h.(contract.Auditor); ok {
		s.auditor.Store(&a)
	} else {
		slog.Warn("serve-pg: the query handler does not audit; Postgres statements will not be recorded")
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-sctx.Done(); _ = s.ln.Close() }()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if sctx.Err() != nil {
				break
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			s.wg.Wait()
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(sctx, conn, auth)
		}()
	}
	// Graceful: sessions end when their client does or when ctx is cancelled
	// (conn closed below); give them a moment to finish the current statement.
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
	}
	return nil
}

// auditHook is duckgres' Config.QueryAuditHook: one call per statement.
func (s *Server) auditHook(user, sql string, d time.Duration, rows int64, err error) {
	ap := s.auditor.Load()
	if ap == nil {
		return
	}
	role := ""
	if r, ok := s.roles.Load(user); ok {
		role, _ = r.(string)
	}
	rec := contract.AuditRecord{At: time.Now().Add(-d).UTC(), User: user, Role: role, Protocol: name, SQL: sql, Duration: d, Rows: rows}
	if err != nil {
		var ae *server.QueryAuditError
		switch {
		case errors.As(err, &ae) && ae.Code == "42501":
			rec.Err = "permission denied: " + ae.Message
		case errors.As(err, &ae):
			rec.Err = ae.Message
		default:
			rec.Err = err.Error()
		}
	}
	(*ap).Audit(context.Background(), rec)
}

func (s *Server) handle(ctx context.Context, conn net.Conn, auth contract.AuthProvider) {
	defer func() { _ = conn.Close() }()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("serve-pg: recovered from panic in a session", "panic", r)
		}
	}()
	select {
	case s.conns <- struct{}{}:
		defer func() { <-s.conns }()
	default:
		_ = server.WriteErrorResponse(conn, "FATAL", "53300", "too many connections")
		return
	}
	remote := ipOf(conn.RemoteAddr())
	if s.failures.banned(remote) {
		_ = server.WriteErrorResponse(conn, "FATAL", "28P01", "too many failed logins, try later")
		return
	}
	// Closing the connection on shutdown unblocks the message loop.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	_ = conn.SetReadDeadline(time.Now().Add(startupTimeout))
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var params map[string]string
	tlsUp := false
	for params == nil {
		startup, err := server.ReadStartupMessage(reader)
		if err != nil {
			return
		}
		switch {
		case startup.GSSENCRequest:
			if _, err := conn.Write([]byte("N")); err != nil {
				return
			}
		case startup.SSLRequest:
			if s.tlsCfg == nil {
				if _, err := conn.Write([]byte("N")); err != nil {
					return
				}
				continue
			}
			if _, err := conn.Write([]byte("S")); err != nil {
				return
			}
			tc := tls.Server(conn, s.tlsCfg)
			if err := tc.Handshake(); err != nil {
				return
			}
			conn = tc
			reader, writer = bufio.NewReader(tc), bufio.NewWriter(tc)
			tlsUp = true
		case startup.CancelRequest:
			if startup.CancelCredentialsPresent {
				s.srv.CancelQuery(server.BackendKey{Pid: startup.CancelPID, SecretKey: startup.CancelSecretKey})
			}
			return
		default:
			params = startup.Params
		}
	}
	if s.tlsCfg != nil && !tlsUp {
		_ = server.WriteErrorResponse(writer, "FATAL", "28000", "TLS is required: connect with sslmode=require")
		_ = writer.Flush()
		return
	}
	user := params["user"]
	id, ok := s.login(ctx, reader, writer, auth, user, remote)
	if !ok {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	s.roles.Store(id.User, id.Role)
	s.session(ctx, conn, reader, writer, id, params["application_name"])
}

// login runs the cleartext-password exchange against the AuthProvider.
func (s *Server) login(ctx context.Context, r *bufio.Reader, w *bufio.Writer, auth contract.AuthProvider, user, remote string) (contract.Identity, bool) {
	fail := func(code, msg string) (contract.Identity, bool) {
		_ = server.WriteErrorResponse(w, "FATAL", code, msg)
		_ = w.Flush()
		return contract.Identity{}, false
	}
	if user == "" {
		return fail("28000", "no user given")
	}
	if err := server.WriteAuthCleartextPassword(w); err != nil || w.Flush() != nil {
		return contract.Identity{}, false
	}
	typ, body, err := server.ReadMessage(r)
	if err != nil || typ != 'p' {
		return contract.Identity{}, false
	}
	password := strings.TrimRight(string(body), "\x00")
	id, err := auth.Authenticate(ctx, contract.Credentials{Scheme: "password", User: user, Secret: password})
	if err != nil {
		if errors.Is(err, contract.ErrUnauthenticated) {
			s.failures.fail(remote)
			time.Sleep(300 * time.Millisecond) // slow down guessing
			return fail("28P01", fmt.Sprintf("password authentication failed for user %q", user))
		}
		slog.Error("serve-pg: authentication provider failed", "error", err)
		return fail("XX000", "authentication unavailable")
	}
	if id.Role == "" {
		return fail("28000", "user has no role")
	}
	return id, true
}

func ipOf(a net.Addr) string {
	if t, ok := a.(*net.TCPAddr); ok {
		return t.IP.String()
	}
	return a.String()
}

// failureTracker bans an address after too many bad logins in a window.
type failureTracker struct {
	mu sync.Mutex
	by map[string][]time.Time
}

const (
	maxFailures = 10
	failWindow  = 5 * time.Minute
)

func (f *failureTracker) fail(ip string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.by == nil {
		f.by = map[string][]time.Time{}
	}
	f.by[ip] = append(f.prune(ip), time.Now())
}

func (f *failureTracker) banned(ip string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.prune(ip)) >= maxFailures
}

func (f *failureTracker) prune(ip string) []time.Time {
	cut := time.Now().Add(-failWindow)
	var keep []time.Time
	for _, t := range f.by[ip] {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	if len(keep) == 0 {
		delete(f.by, ip)
	} else {
		f.by[ip] = keep
	}
	return keep
}
