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
	"github.com/alonsoburon/duct/core/dashboards"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/registry"
	"github.com/alonsoburon/duct/core/semantic"
	"github.com/alonsoburon/duct/core/sharing"
)

// serveOptions override duct.toml's [serve] table.
type serveOptions struct {
	Protocols []string          // empty: [serve].protocols, else every linked protocol
	Listen    map[string]string // protocol -> address
	NoJobs    bool              // do not run the jobs/ orchestrator in this process
	NoStream  bool              // do not run [stream] / [alerts] in this process (embedded server of `duct notebook`)
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
	noJobs := fs.Bool("no-jobs", false, "do not run the scheduler and workers of jobs/*.yaml in this process")
	if err := parse(fs, args); err != nil {
		return err
	}
	p, err := findProject(*dir)
	if err != nil {
		return err
	}
	opt := serveOptions{Listen: listen, NoJobs: *noJobs, Ready: func(addrs map[string]string) {
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
			// mcp serves the semantic layer: only on by default when there is a model.
			if m.Name == "mcp" && !hasSemanticModel(p) {
				continue
			}
			// notebook runs user code: only when [serve.notebook] exists (or it is named).
			if m.Name == "notebook" {
				if _, ok := p.Config.Serve.Modules["notebook"]; !ok {
					continue
				}
			}
			// sharing only starts when [serve.sharing] exists (or --protocols names it).
			if m.Name == "sharing" {
				if _, ok := p.Config.Serve.Modules["sharing"]; !ok {
					continue
				}
			}
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

	eng, udfs, err := openEngineUDFManager(ctx, p, true)
	if err != nil {
		return err
	}
	defer func() { _ = eng.Close() }()
	var udfNames []string
	if udfs != nil {
		udfNames = udfs.Names()
	}
	handler := governance.NewHandler(eng, governance.HandlerOptions{AdminOnlyFunctions: udfNames})
	defer func() { _ = handler.Close() }()
	auth := governance.NewStore(eng)
	// The semantic model also serves dashboards (web) through the handler.
	live, err := semantic.NewLive(filepath.Join(p.Root, semanticDirName))
	if err != nil {
		return fmt.Errorf("semantic model: %w", err)
	}
	if m := live.Model(); m != nil {
		handler.SetSemanticModel(m)
	}

	modCfg := func(name string) contract.Config {
		cfg := contract.Config{}
		maps.Copy(cfg, p.Config.Serve.Modules[name])
		if a, ok := opt.Listen[name]; ok {
			cfg["listen"] = a
		}
		// Reserved keys: where the lake is, for protocols that run their own
		// read-only DuckDB session (serve-pg).
		p.ModuleConfig(cfg)
		if name == "mcp" { // the semantic models live in the project
			if _, ok := cfg["semantic_dir"]; !ok {
				cfg["semantic_dir"] = filepath.Join(p.Root, semanticDirName)
			}
			cfg["semantic_live"] = live // reloaded in place by startSemantic
		}
		if name == "web" { // dashboards/*.yaml live in the project
			if _, ok := cfg["dashboards_dir"]; !ok {
				cfg["dashboards_dir"] = filepath.Join(p.Root, dashboards.Dir)
			}
		}
		if name == "notebook" { // see notebookConfig (notebook.go)
			notebookConfig(p, cfg)
		}
		if name == "sharing" { // copies live outside the lake; file URLs are signed with a persistent key
			sharingConfig(p, cfg)
		}
		if name == "web" { // persistent web sessions survive restarts
			if _, ok := cfg["session_key_file"]; !ok {
				cfg["session_key_file"] = filepath.Join(filepath.Dir(p.StatePath()), "web_session.key")
			}
		}
		return cfg
	}
	for _, i := range initOrder(names) {
		proto := protocols[i]
		if eu, ok := proto.(sharing.EngineUser); ok {
			eu.UseEngine(eng)
		}
		cfg := modCfg(names[i])
		if names[i] == "notebook" { // the notebooks reach the lake through this server's HTTP protocol
			if _, ok := cfg["duct_url"]; !ok {
				cfg["duct_url"] = httpURLOf(p, names, protocols)
			}
		}
		if err := proto.Init(ctx, cfg); err != nil {
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
	byName := map[string]contract.Protocol{}
	for i, proto := range protocols {
		byName[names[i]] = proto
	}
	mountOperability(p, eng, handler, auth, byName)  // /admin/v1, /readyz, DB-backed metrics
	jobsHolder := wireConsole(p, eng, handler, auth) // web console: admin API and jobs (console.go)
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
	jobsDone, err := startJobs(sctx, p, httpURLOf(p, names, protocols), opt.NoJobs, jobsHolder)
	if err != nil {
		return fmt.Errorf("jobs: %w", err)
	}
	semanticDone := startSemantic(sctx, p, live, handler)
	functionsDone := startFunctionsReload(sctx, p, udfs, handler)
	streamingDone := func() error { return nil }
	if !opt.NoStream {
		streamingDone = startStreaming(sctx, cancel, p) // [stream] / [alerts] in duct.toml
	}
	// Audit retention and export outlive the protocols: they stop after the
	// last audit record is flushed (below), so the final export is complete.
	auditStop, auditDone := startAuditMaintenance(ctx, p, eng)
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
	cancel()
	semanticDone()
	functionsDone()
	<-jobsDone // running tasks are cancelled and recorded before the engine closes
	if err := streamingDone(); err != nil && firstErr == nil {
		firstErr = err
	}
	// Flush the audit log before the engine goes away (deferred closes).
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer fcancel()
	_ = handler.FlushAudit(fctx)
	auditStop()
	<-auditDone
	return firstErr
}

// startAuditMaintenance runs [audit] retention (hourly) and export (every
// minute) inside `duct serve`. The returned stop ends it after one last export.
func startAuditMaintenance(ctx context.Context, p *project.Project, eng contract.Engine) (stop func(), done <-chan struct{}) {
	ch := make(chan struct{})
	ret, err := p.Config.Audit.RetentionDuration()
	if err != nil || (ret <= 0 && !p.Config.Audit.Export) {
		close(ch)
		return func() {}, ch
	}
	mctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	go func() {
		defer close(ch)
		governance.RunAuditMaintenance(mctx, eng, governance.MaintenanceOptions{Retention: ret, Export: p.Config.Audit.Export})
	}()
	return cancel, ch
}
