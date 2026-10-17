package main

import (
	"context"
	"sync/atomic"

	"github.com/alonsoburon/duct/core/admin"
	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/orchestrator"
	"github.com/alonsoburon/duct/core/project"
)

// orchHolder shares the orchestrator of this process (created when the first
// job exists, see jobsServer) with the jobs service of the web console.
type orchHolder struct {
	o atomic.Pointer[orchestrator.Orchestrator]
}

func (h *orchHolder) get() *orchestrator.Orchestrator { return h.o.Load() }

// wireConsole plugs what the web console needs beyond SQL into the governance
// handler: the admin API (same rules, same audit as /admin/v1; see
// admin.API.Invoke) and the jobs service. The protocols reach them through the
// optional contract interfaces AdminCaller, AuditReader and JobsConsole.
func wireConsole(p *project.Project, eng contract.Engine, h *governance.Handler, auth contract.AuthProvider) *orchHolder {
	api := admin.New(admin.Config{
		Auth: auth, Handler: h, Engine: eng, Project: p, Version: version,
		Components: func(ctx context.Context) map[string]any { return componentsStatus(ctx, p, eng) },
	})
	h.SetAdminInvoker(api.Invoke)
	holder := &orchHolder{}
	h.SetJobsConsole(&orchestrator.Console{Eng: eng, Root: p.Root, Orch: holder.get})
	return holder
}
