// Package sharingserver is the OpenSharing (Delta Sharing) Protocol of `duct
// serve` (ADR 0014, phase 2): a REST server that lists the shares granted to
// the bearer token's recipient and hands out short-lived URLs of a Parquet
// copy that is already filtered and masked for that recipient. It never gives
// out a URL of a base data file.
package sharingserver

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/registry"
	"github.com/alonsoburon/duct/core/s3conn"
	"github.com/alonsoburon/duct/core/sharing"
)

const (
	name          = "sharing"
	defaultListen = "127.0.0.1:8124"
	// RESTPrefix is where the protocol lives; the profile's endpoint ends with it.
	RESTPrefix = "/delta-sharing"
	// queryTimeout bounds one materialization query (large tables take a while).
	queryTimeout = 30 * time.Minute
)

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindProtocol,
		Name: name,
		New:  func() contract.Module { return &Server{} },
	})
}

// Server implements contract.Protocol, contract.Addresser and
// sharing.EngineUser.
//
// Config keys: listen (default 127.0.0.1:8124; :0 = random port; a non-loopback
// address needs TLS), tls_cert and tls_key (both or none), public_url (what
// recipients use to reach this server, default http(s)://<listen>; the profile
// endpoint is public_url + /delta-sharing), storage (folder or s3://bucket/prefix
// of the copies; `duct serve` fills <project>/shares or <lake prefix>.shares and
// it must lie outside the lake's data path), s3 and data_path (filled by `duct
// serve`), s3_public_endpoint (host[:port] recipients use for presigned S3
// URLs), signing_key_file (HMAC key of local file URLs; `duct serve` fills
// .duct/sharing.key), url_ttl (default "15m"), grace (default "1h"),
// rows_per_file (default 2000000), refresh_interval (e.g. "5m"; default off:
// copies refresh when a client asks), tmp_dir.
type Server struct {
	ln      net.Listener
	tlsCfg  *tls.Config
	baseURL string
	files   sharing.Storage
	opt     sharing.Options
	refresh time.Duration
	eng     contract.Engine
}

var (
	_ contract.Protocol  = (*Server)(nil)
	_ contract.Addresser = (*Server)(nil)
	_ sharing.EngineUser = (*Server)(nil)
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

// UseEngine implements sharing.EngineUser.
func (s *Server) UseEngine(eng contract.Engine) { s.eng = eng }

// Endpoint is the URL clients put in their profile (valid after Init).
func (s *Server) Endpoint() string { return s.baseURL }

func durationCfg(cfg contract.Config, key string, def time.Duration) (time.Duration, error) {
	switch v := cfg[key].(type) {
	case nil:
		return def, nil
	case string:
		if v == "" {
			return def, nil
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("sharing: %s %q is not a positive duration (e.g. 15m)", key, v)
		}
		return d, nil
	}
	return 0, fmt.Errorf("sharing: %s must be a duration string", key)
}

// Init implements contract.Module: it binds the listener and opens the storage.
func (s *Server) Init(_ context.Context, cfg contract.Config) error {
	listen := defaultListen
	if v, ok := cfg["listen"].(string); ok && v != "" {
		listen = v
	}
	cert, _ := cfg["tls_cert"].(string)
	key, _ := cfg["tls_key"].(string)
	if (cert == "") != (key == "") {
		return errors.New("sharing: tls_cert and tls_key must be set together")
	}
	if cert != "" {
		pair, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return fmt.Errorf("sharing: load TLS pair: %w", err)
		}
		s.tlsCfg = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("sharing: listen %q: %w", listen, err)
	}
	if s.tlsCfg == nil && !isLoopback(host) {
		return fmt.Errorf("sharing: %q is not a loopback address: bearer tokens would travel in clear text; set tls_cert and tls_key, or put it behind a TLS proxy and keep 127.0.0.1", listen)
	}
	var d time.Duration
	if d, err = durationCfg(cfg, "url_ttl", sharing.DefaultURLTTL); err != nil {
		return err
	}
	s.opt.URLTTL = d
	if d, err = durationCfg(cfg, "grace", sharing.DefaultGrace); err != nil {
		return err
	}
	s.opt.Grace = d
	if s.refresh, err = durationCfg(cfg, "refresh_interval", 0); err != nil {
		return err
	}
	switch v := cfg["rows_per_file"].(type) {
	case int:
		s.opt.RowsPerFile = int64(v)
	case int64:
		s.opt.RowsPerFile = v
	case float64:
		s.opt.RowsPerFile = int64(v)
	}
	s.opt.TempDir, _ = cfg["tmp_dir"].(string)

	location, _ := cfg["storage"].(string)
	if location == "" {
		return errors.New("sharing: storage is not set (a folder or s3://bucket/prefix outside the lake's data path)")
	}
	if dp, _ := cfg["data_path"].(string); dp != "" && sharing.Overlaps(location, dp) {
		return fmt.Errorf("sharing: storage %q overlaps the lake data path %q: `duct gc` would delete the copies; use a sibling location", location, dp)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("sharing: %w", err)
	}
	s.ln = ln
	scheme := "http"
	if s.tlsCfg != nil {
		scheme = "https"
	}
	public, _ := cfg["public_url"].(string)
	if public == "" {
		public = scheme + "://" + ln.Addr().String()
	}
	s.baseURL = strings.TrimSuffix(public, "/") + RESTPrefix
	spec := sharing.StorageSpec{Location: location, BaseURL: s.baseURL}
	spec.S3PublicEndpoint, _ = cfg["s3_public_endpoint"].(string)
	if c, ok := cfg["s3"].(s3conn.Config); ok {
		spec.S3 = &c
	}
	if msg, _ := cfg["s3_error"].(string); msg != "" {
		_ = ln.Close()
		return fmt.Errorf("sharing: %s", msg)
	}
	if kf, _ := cfg["signing_key_file"].(string); kf != "" && s3conn.Scheme(location) != "s3" {
		if spec.SigningKey, err = sharing.LoadOrCreateKey(kf); err != nil {
			_ = ln.Close()
			return err
		}
	}
	if spec.SigningKey == nil && s3conn.Scheme(location) != "s3" { // no key file: URLs die with the process
		spec.SigningKey = make([]byte, 32)
		if _, err := rand.Read(spec.SigningKey); err != nil {
			_ = ln.Close()
			return err
		}
	}
	if s.files, err = sharing.OpenStorage(spec); err != nil {
		_ = ln.Close()
		return err
	}
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

// Serve implements contract.Protocol. It reads data through its own governed
// handler (materializing a big table outlasts the 60 s of the shared one);
// h is not used.
func (s *Server) Serve(ctx context.Context, _ contract.QueryHandler, auth contract.AuthProvider) error {
	if s.ln == nil {
		return errors.New("sharing: not initialised")
	}
	if s.eng == nil {
		return errors.New("sharing: no engine attached (sharing.EngineUser)")
	}
	own := governance.NewHandler(s.eng, governance.HandlerOptions{Timeout: queryTimeout})
	defer func() { _ = own.Close() }()
	q := own.ForProtocol(name)
	svc := sharing.NewService(s.eng, q, s.files, s.opt)
	a := &api{svc: svc, auth: auth, audit: q.(contract.Auditor), now: time.Now}
	a.local, _ = s.files.(*sharing.LocalStorage)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle(RESTPrefix+"/", a)
	srv := &http.Server{
		Handler:           mux,
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
	if s.refresh > 0 {
		go func() {
			t := time.NewTicker(s.refresh)
			defer t.Stop()
			for {
				if st, err := svc.Refresh(ctx, ""); err != nil {
					slog.Warn("sharing: refresh failed", "error", err)
				} else if st.Rewritten > 0 {
					slog.Info("sharing: copies refreshed", "checked", st.Checked, "rewritten", st.Rewritten)
				}
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
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
