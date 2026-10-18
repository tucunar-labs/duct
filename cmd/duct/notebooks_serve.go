package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/notebooks"
	"github.com/alonsoburon/duct/core/project"
)

// wireNotebooks plugs the notebook service of the web console (ADR 0006) into
// the governance handler (contract.NotebooksConsole). One marimo process per
// user is started on demand, with DUCT_URL (this server's HTTP protocol) and a
// token of that user, which is revoked when the process ends. It does nothing
// when the web protocol is not running. The returned stop ends every process
// and must run before the engine closes.
func wireNotebooks(p *project.Project, h *governance.Handler, tokens notebooks.TokenIssuer, eng contract.Engine, ductURL string, withWeb bool, launcher notebooks.Launcher) (stop func(), err error) {
	if !withWeb {
		return func() {}, nil
	}
	// Sandbox (ADR 0006): every marimo process runs inside bubblewrap. Without it, only duct_admin may
	// edit (the others get run mode, which only executes code that an admin wrote). A test launcher is its own thing.
	sandbox, editors := notebooks.SandboxNone, p.Config.Notebooks.EditorRoles()
	if launcher == nil {
		mode, adminOnly, msg, err := notebooks.ResolveSandbox(p.SandboxFor(project.SandboxNotebooks))
		if err != nil {
			return nil, err
		}
		if msg != "" {
			slog.Warn(msg)
		}
		sandbox = mode
		if adminOnly {
			editors = []string{notebooks.AdminRole}
		}
	}
	idle, maxAge, _ := p.Config.Notebooks.Durations() // validated by project.Load
	var (
		mu sync.Mutex
		py []string
	)
	svc := notebooks.New(notebooks.Options{
		Dir:         filepath.Join(p.Root, notebooksDir),
		Root:        p.Root,
		Engine:      eng,
		Tokens:      tokens,
		Launcher:    launcher,
		Editors:     editors,
		Sandbox:     sandbox,
		IdleTimeout: idle, MaxAge: maxAge, MaxKernels: p.Config.Notebooks.MaxKernels,
		DuctURL: func() string { return ductURL },
		Python: func(ctx context.Context) ([]string, error) { // the venv is created once, on the first process
			mu.Lock()
			defer mu.Unlock()
			if py != nil {
				return py, nil
			}
			got, err := preparePython(ctx, p, []pyNeed{{"marimo", []string{"marimo"}}, {"duct", []string{p.PythonSDK()}}}, os.Stderr)
			if err == nil {
				py = got
			}
			return got, err
		},
	})
	h.SetNotebooksConsole(svc)
	return svc.Close, nil
}
