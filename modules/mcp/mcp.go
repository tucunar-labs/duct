// Package mcp is the MCP (Model Context Protocol) Protocol of `duct serve`
// (ADR 0010, 0011): Genie v1. It exposes the semantic layer to an LLM client
// as tools over the Streamable HTTP transport; every call runs as the
// authenticated identity through the QueryHandler, so role views, row filters,
// masks and the audit log apply exactly as for any other protocol.
package mcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
	"github.com/alonsoburon/duct/core/semantic"
)

const (
	name          = "mcp"
	defaultListen = "127.0.0.1:8765"
	endpoint      = "/mcp"
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
// Config keys: listen (default 127.0.0.1:8765; :0 = random port; a non-loopback
// address needs TLS), tls_cert and tls_key (both or none), semantic_dir (where
// the *.ossie.yaml models are; `duct serve` fills it with <project>/semantic),
// sql (bool, default false: also expose a read-only `sql` tool).
type Server struct {
	ln     net.Listener
	tlsCfg *tls.Config
	model  *semantic.Model // nil: no semantic model, only the sql tool can work
	sql    bool
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

// Init implements contract.Module: it loads the model and binds the listener.
func (s *Server) Init(_ context.Context, cfg contract.Config) error {
	listen := defaultListen
	if v, ok := cfg["listen"].(string); ok && v != "" {
		listen = v
	}
	cert, _ := cfg["tls_cert"].(string)
	key, _ := cfg["tls_key"].(string)
	if (cert == "") != (key == "") {
		return errors.New("mcp: tls_cert and tls_key must be set together")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return fmt.Errorf("mcp: load TLS pair: %w", err)
		}
		s.tlsCfg = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("mcp: listen %q: %w", listen, err)
	}
	if s.tlsCfg == nil && !isLoopback(host) {
		return fmt.Errorf("mcp: %q is not a loopback address: bearer tokens would travel in clear text; set tls_cert and tls_key, or put it behind a TLS proxy and keep 127.0.0.1", listen)
	}
	if dir, _ := cfg["semantic_dir"].(string); dir != "" {
		m, err := semantic.LoadDir(dir)
		switch {
		case err == nil:
			s.model = m
		case errors.Is(err, semantic.ErrNoModel):
		default:
			return fmt.Errorf("mcp: semantic model: %w", err)
		}
	}
	s.sql, _ = cfg["sql"].(bool)
	if s.model == nil && !s.sql {
		return errors.New("mcp: no semantic model (semantic/*.ossie.yaml) and the sql tool is disabled: nothing to serve")
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("mcp: %w", err)
	}
	s.ln = ln
	return nil
}

func isLoopback(host string) bool {
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
func (s *Server) Serve(ctx context.Context, h contract.QueryHandler, authp contract.AuthProvider) error {
	if s.ln == nil {
		return errors.New("mcp: not initialised")
	}
	srv := newTools(s, h)
	mcpHandler := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return srv }, &sdk.StreamableHTTPOptions{
		SessionTimeout: 30 * time.Minute,
		Logger:         slog.Default(),
	})
	verify := func(ctx context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		id, err := authp.Authenticate(ctx, contract.Credentials{Scheme: "bearer", Secret: token})
		if err != nil {
			if errors.Is(err, contract.ErrUnauthenticated) {
				return nil, fmt.Errorf("%w: bad or revoked token", auth.ErrInvalidToken)
			}
			slog.Error("mcp: authentication provider failed", "error", err)
			return nil, errors.New("authentication unavailable")
		}
		return &auth.TokenInfo{UserID: id.User, Extra: map[string]any{extraRole: id.Role}}, nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle(endpoint, auth.RequireBearerToken(verify, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})(mcpHandler))

	hs := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		TLSConfig:         s.tlsCfg,
	}
	errc := make(chan error, 1)
	go func() {
		if s.tlsCfg != nil {
			errc <- hs.ServeTLS(s.ln, "", "")
		} else {
			errc <- hs.Serve(s.ln)
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
	if err := hs.Shutdown(sctx); err != nil {
		_ = hs.Close()
	}
	<-errc
	return nil
}
