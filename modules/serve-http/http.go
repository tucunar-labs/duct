// Package servehttp is the HTTP Protocol of `duct serve`: a ClickHouse-like
// query API (POST SQL, FORMAT JSON/CSV/ArrowStream/Parquet) with bearer-token
// auth. It never touches data except through the QueryHandler, which runs the
// statement as the authenticated identity over its role views (ADR 0007, 0008).
package servehttp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	ductmetrics "github.com/alonsoburon/duct/core/metrics"
	"github.com/alonsoburon/duct/core/registry"
)

const (
	name           = "http"
	defaultListen  = "127.0.0.1:8123"
	maxQueryBytes  = 1 << 20
	defaultMaxConc = 32
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
// Config keys: listen (default 127.0.0.1:8123; use :0 for a random port),
// tls_cert and tls_key (both or none), max_concurrent (default 32).
type Server struct {
	ln      net.Listener
	tlsCfg  *tls.Config
	sem     chan struct{}
	metrics metrics

	mu     sync.Mutex
	mounts []mount
}

type mount struct {
	pattern string
	h       http.Handler
}

// Mount adds a handler to the HTTP listener under pattern (http.ServeMux
// syntax, e.g. "/admin/v1/"). It must be called before Serve; the paths of the
// query API ("/", "/query", "/healthz", "/metrics") are taken. `duct serve`
// uses it for the admin API and /readyz, which need the project and not only
// the query handler.
func (s *Server) Mount(pattern string, h http.Handler) {
	s.mu.Lock()
	s.mounts = append(s.mounts, mount{pattern, h})
	s.mu.Unlock()
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

// Init implements contract.Module: it binds the listener.
func (s *Server) Init(_ context.Context, cfg contract.Config) error {
	listen := defaultListen
	if v, ok := cfg["listen"].(string); ok && v != "" {
		listen = v
	}
	conc := defaultMaxConc
	switch v := cfg["max_concurrent"].(type) {
	case int:
		conc = v
	case int64:
		conc = int(v)
	case float64:
		conc = int(v)
	}
	if conc < 1 {
		return errors.New("serve-http: max_concurrent must be >= 1")
	}
	s.sem = make(chan struct{}, conc)
	cert, _ := cfg["tls_cert"].(string)
	key, _ := cfg["tls_key"].(string)
	if (cert == "") != (key == "") {
		return errors.New("serve-http: tls_cert and tls_key must be set together")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return fmt.Errorf("serve-http: load TLS pair: %w", err)
		}
		s.tlsCfg = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("serve-http: %w", err)
	}
	s.ln = ln
	return nil
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
		return errors.New("serve-http: not initialised")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/metrics", s.metrics.serve)
	s.mu.Lock()
	for _, m := range s.mounts {
		mux.Handle(m.pattern, m.h)
	}
	s.mu.Unlock()
	q := &queryAPI{h: h, auth: auth, sem: s.sem, m: &s.metrics}
	mux.Handle("/query", q)
	mux.Handle("/", q) // ClickHouse clients post to the root

	srv := &http.Server{
		Handler:           s.metrics.wrap(mux),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		TLSConfig:         s.tlsCfg,
	}
	errc := make(chan error, 1)
	go func() {
		if s.tlsCfg != nil {
			errc <- srv.ServeTLS(s.ln, "", "")
		} else {
			errc <- srv.Serve(s.ln)
		}
	}()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		_ = srv.Close()
	}
	<-errc
	return nil
}

type queryAPI struct {
	h    contract.QueryHandler
	auth contract.AuthProvider
	sem  chan struct{}
	m    *metrics
}

var formatSuffix = regexp.MustCompile(`(?is)\s+FORMAT\s+([A-Za-z]+)\s*;?\s*$`)

// splitFormat separates a trailing "FORMAT x" clause from the SQL.
func splitFormat(sql string) (string, string) {
	if m := formatSuffix.FindStringSubmatchIndex(sql); m != nil {
		return strings.TrimSpace(sql[:m[0]]), sql[m[2]:m[3]]
	}
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";")), ""
}

func (a *queryAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/query" {
		http.NotFound(w, r)
		return
	}
	id, err := a.authenticate(r)
	if err != nil {
		if errors.Is(err, contract.ErrUnauthenticated) {
			a.m.authFail.Add(1)
			w.Header().Set("WWW-Authenticate", `Bearer realm="duct"`)
			httpError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		slog.Error("serve-http: authentication provider failed", "error", err)
		httpError(w, http.StatusInternalServerError, "authentication unavailable")
		return
	}
	sql, ok := readSQL(w, r)
	if !ok {
		return
	}
	sql, format := splitFormat(sql)
	if format == "" {
		format = r.URL.Query().Get("default_format")
	}
	if format == "" {
		format = "JSON"
	}
	enc, ok := lookupFormat(format)
	if !ok {
		httpError(w, http.StatusBadRequest, fmt.Sprintf("unknown format %q (JSON, JSONEachRow, CSV, CSVWithNames, ArrowStream, Parquet)", format))
		return
	}
	select {
	case a.sem <- struct{}{}:
		defer func() { <-a.sem }()
	default:
		httpError(w, http.StatusTooManyRequests, "too many concurrent queries")
		return
	}
	a.m.inflight.Add(1)
	defer a.m.inflight.Add(-1)

	st, err := a.h.Query(r.Context(), id, sql)
	if err != nil {
		a.m.queryErr.Add(1)
		code := http.StatusBadRequest
		switch {
		case strings.HasPrefix(err.Error(), "permission denied"):
			a.m.denied.Add(1)
			code = http.StatusForbidden
		case strings.HasPrefix(err.Error(), "quota exceeded"):
			code = http.StatusTooManyRequests
		}
		httpError(w, code, err.Error())
		return
	}
	defer st.Release()
	rows, err := writeResult(w, st, enc)
	a.m.rows.Add(rows)
	if err != nil {
		a.m.queryErr.Add(1)
		var late *lateError
		if errors.As(err, &late) {
			// Bytes already left: the only honest signal is to cut the connection.
			panic(http.ErrAbortHandler)
		}
		code := http.StatusInternalServerError
		if strings.HasPrefix(err.Error(), "quota exceeded") {
			code = http.StatusTooManyRequests
		}
		httpError(w, code, err.Error())
	}
}

func (a *queryAPI) authenticate(r *http.Request) (contract.Identity, error) {
	h := r.Header.Get("Authorization")
	switch {
	case strings.HasPrefix(h, "Bearer "):
		return a.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "bearer", Secret: strings.TrimSpace(h[7:])})
	case strings.HasPrefix(h, "Basic "):
		user, pass, ok := r.BasicAuth()
		if ok {
			return a.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "password", User: user, Secret: pass})
		}
	case r.Header.Get("X-ClickHouse-User") != "":
		return a.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "password", User: r.Header.Get("X-ClickHouse-User"), Secret: r.Header.Get("X-ClickHouse-Key")})
	}
	return contract.Identity{}, contract.ErrUnauthenticated
}

// readSQL returns the statement: the body of a POST, or ?query= (GET/POST).
func readSQL(w http.ResponseWriter, r *http.Request) (string, bool) {
	var parts []string
	if q := r.URL.Query().Get("query"); q != "" {
		parts = append(parts, q)
	}
	if r.Method == http.MethodPost {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxQueryBytes))
		if err != nil {
			httpError(w, http.StatusRequestEntityTooLarge, "query too large (max 1 MiB)")
			return "", false
		}
		if s := strings.TrimSpace(string(b)); s != "" {
			parts = append(parts, s)
		}
	} else if r.Method != http.MethodGet {
		httpError(w, http.StatusMethodNotAllowed, "use POST (SQL in the body) or GET ?query=")
		return "", false
	}
	if len(parts) == 0 {
		httpError(w, http.StatusBadRequest, "empty query")
		return "", false
	}
	return strings.Join(parts, " "), true
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, msg+"\n")
}

// metrics are minimal Prometheus counters.
type metrics struct {
	byCode   [6]atomic.Int64 // 1xx..5xx at [1..5]
	authFail atomic.Int64
	denied   atomic.Int64
	queryErr atomic.Int64
	rows     atomic.Int64
	inflight atomic.Int64
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) {
	s.code = c
	s.ResponseWriter.WriteHeader(c)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (m *metrics) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: 200}
		defer func() {
			if c := sw.code / 100; c >= 1 && c <= 5 {
				m.byCode[c].Add(1)
			}
		}()
		next.ServeHTTP(sw, r)
	})
}

func (m *metrics) serve(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder
	b.WriteString("# HELP duct_http_requests_total HTTP requests by status class.\n# TYPE duct_http_requests_total counter\n")
	for c := 1; c <= 5; c++ {
		fmt.Fprintf(&b, "duct_http_requests_total{code=\"%dxx\"} %d\n", c, m.byCode[c].Load())
	}
	counter := func(n, help string, v int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", n, help, n, n, v)
	}
	counter("duct_http_auth_failures_total", "Requests with missing or bad credentials.", m.authFail.Load())
	counter("duct_http_denied_total", "Statements refused by governance.", m.denied.Load())
	counter("duct_http_query_errors_total", "Statements that failed or were refused.", m.queryErr.Load())
	counter("duct_http_rows_returned_total", "Rows sent to clients.", m.rows.Load())
	fmt.Fprintf(&b, "# HELP duct_http_inflight_queries Statements running now.\n# TYPE duct_http_inflight_queries gauge\nduct_http_inflight_queries %d\n", m.inflight.Load())
	_, _ = io.WriteString(w, b.String())
	ductmetrics.Default().WriteText(w)
}
