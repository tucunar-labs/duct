package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
)

// serveOptions override duct.toml's [serve] table.
type serveOptions struct {
	Protocols []string          // empty: [serve].protocols, else every linked protocol
	Listen    map[string]string // protocol -> address
	// Ready is called once every protocol is bound, with protocol -> address.
	Ready func(addrs map[string]string)
}

type listenFlag map[string]string

func (l listenFlag) String() string { return fmt.Sprint(map[string]string(l)) }

func (l listenFlag) Set(v string) error {
	name, addr, ok := strings.Cut(v, "=")
	if !ok || name == "" || addr == "" {
		return errors.New("use --listen <protocol>=<host:port>")
	}
	l[name] = addr
	return nil
}

// cmdServe is `duct serve`: every protocol module over one governed handler.
func cmdServe(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs, dir := newFlags("serve", stderr)
	protos := fs.String("protocols", "", "comma-separated protocols to start (default: [serve].protocols or all linked)")
	listen := listenFlag{}
	fs.Var(listen, "listen", "listen address override, <protocol>=<host:port> (repeatable)")
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	opt := serveOptions{Listen: listen, Ready: func(addrs map[string]string) {
		names := make([]string, 0, len(addrs))
		for n := range addrs {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			_, _ = fmt.Fprintf(stdout, "serving %-6s on %s\n", n, addrs[n])
		}
	}}
	if *protos != "" {
		opt.Protocols = strings.Split(*protos, ",")
	}
	return serve(ctx, p, opt)
}

// protocolNames resolves which protocol modules to start.
func protocolNames(p *project.Project, opt serveOptions) ([]string, error) {
	names := opt.Protocols
	if len(names) == 0 {
		names = p.Config.Serve.Protocols
	}
	if len(names) == 0 {
		for _, m := range registry.List(contract.KindProtocol) {
			names = append(names, m.Name)
		}
	}
	if len(names) == 0 {
		return nil, errors.New("no protocol module is linked into this binary")
	}
	return names, nil
}

// serve runs the server until ctx is cancelled or a protocol fails.
//
//  1. An unlocked engine migrates the Duct tables and regenerates the role
//     views (the only moment the server writes DDL), then closes.
//  2. A privileged engine attaches the lake and the Duct state and applies the
//     lockdown of ADR 0007 (allowed_directories, enable_external_access=false,
//     allow_persistent_secrets=false, no autoinstall/autoload,
//     lock_configuration=true). It is the only handle on the data the shared
//     governance.Handler has.
//  3. Each protocol module is Init-ed and served with a handler that runs
//     statements as the authenticated identity, plus the local auth provider.
func serve(ctx context.Context, p *project.Project, opt serveOptions) error {
	// duckgres logs every statement at INFO; the audit table is the record.
	slog.SetLogLoggerLevel(slog.LevelWarn)
	names, err := protocolNames(p, opt)
	if err != nil {
		return err
	}
	protocols := make([]contract.Protocol, 0, len(names))
	for _, n := range names {
		info, ok := registry.Lookup(contract.KindProtocol, n)
		if !ok {
			return fmt.Errorf("protocol %q is not linked into this binary (see `duct modules`)", n)
		}
		proto, ok := info.New().(contract.Protocol)
		if !ok {
			return fmt.Errorf("module protocol/%s does not implement contract.Protocol", n)
		}
		protocols = append(protocols, proto)
	}

	setup, err := openEngine(ctx, p, false)
	if err != nil {
		return err
	}
	if _, err := project.Migrate(ctx, setup); err != nil {
		_ = setup.Close()
		return err
	}
	if _, err := governance.NewStore(setup).Regenerate(ctx); err != nil {
		_ = setup.Close()
		return fmt.Errorf("regenerate role views: %w", err)
	}
	if err := setup.Close(); err != nil {
		return err
	}

	eng, err := openEngine(ctx, p, true)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	handler := governance.NewHandler(eng, governance.HandlerOptions{})
	defer func() { _ = handler.Close() }()
	auth := governance.NewStore(eng)

	modCfg := func(name string) contract.Config {
		cfg := contract.Config{}
		maps.Copy(cfg, p.Config.Serve.Modules[name])
		if a, ok := opt.Listen[name]; ok {
			cfg["listen"] = a
		}
		// Reserved keys: where the lake is, for protocols that run their own
		// read-only DuckDB session (serve-pg).
		p.ModuleConfig(cfg)
		if name == "web" { // persistent web sessions survive restarts
			if _, ok := cfg["session_key_file"]; !ok {
				cfg["session_key_file"] = filepath.Join(filepath.Dir(p.StatePath()), "web_session.key")
			}
		}
		return cfg
	}
	for i, proto := range protocols {
		if err := proto.Init(ctx, modCfg(names[i])); err != nil {
			for _, q := range protocols {
				_ = q.Close()
			}
			return fmt.Errorf("protocol %s: %w", names[i], err)
		}
	}
	defer func() {
		for _, q := range protocols {
			_ = q.Close()
		}
	}()
	if opt.Ready != nil {
		addrs := map[string]string{}
		for i, proto := range protocols {
			if a, ok := proto.(contract.Addresser); ok {
				addrs[names[i]] = a.Addr()
			}
		}
		opt.Ready(addrs)
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i, proto := range protocols {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := proto.Serve(sctx, handler.ForProtocol(names[i]), auth)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("protocol %s: %w", names[i], err)
				}
				mu.Unlock()
				cancel()
			}
		}()
	}
	wg.Wait()
	// Flush the audit log before the engine goes away (deferred closes).
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer fcancel()
	_ = handler.FlushAudit(fctx)
	return firstErr
}
