package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/sandbox"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/notebooks"
	"github.com/alonsoburon/duct/core/orchestrator"
	"github.com/alonsoburon/duct/core/project"
)

var sandboxLogged sync.Map

// sandboxMode resolves the [sandbox] setting of a feature on this host to sandbox.Bwrap or sandbox.None. An
// explicit "bwrap" without a working bubblewrap is an error. With "auto" and no bubblewrap the answer is None,
// logged once, and auto is true (the caller may then let a process that cannot be wrapped run unisolated).
func sandboxMode(p *project.Project, feature string) (mode string, auto bool, err error) {
	setting := p.SandboxFor(feature)
	mode, fallback, why, err := sandbox.Resolve(setting)
	if err != nil {
		return "", false, fmt.Errorf("%s: %w", feature, err)
	}
	if fallback {
		if _, dup := sandboxLogged.LoadOrStore(feature, true); !dup {
			slog.Warn("sandbox: " + feature + " run without isolation (" + why + "): install bubblewrap or set [sandbox] " + feature + " = \"none\" to silence this")
		}
	}
	return mode, setting == sandbox.Auto || setting == "", nil
}

// taskSandbox is what the orchestrator needs to isolate python, notebook and shell tasks.
func taskSandbox(p *project.Project) (orchestrator.TaskSandbox, error) {
	jobs, _, err := sandboxMode(p, project.SandboxJobs)
	if err != nil {
		return orchestrator.TaskSandbox{}, err
	}
	shell, _, err := sandboxMode(p, project.SandboxShell)
	if err != nil {
		return orchestrator.TaskSandbox{}, err
	}
	return orchestrator.TaskSandbox{
		Modes: map[string]string{orchestrator.TypePython: jobs, orchestrator.TypeNotebook: jobs, orchestrator.TypeShell: shell},
		Hide:  sandbox.ProjectHide(p.Root, p.SandboxHide()...),
	}, nil
}

// notebookGuard is the orchestrator's NotebookGuard (ADR 0006, "Menor privilegio de los autores"): a notebook
// task runs unattended with the identity of the jobs token, so it only starts when no author with another role
// changed the notebook after that identity last reviewed it (see notebooks.Policy.CheckForJob).
func notebookGuard(eng contract.Engine, token string) func(ctx context.Context, job, task, path string) error {
	return func(ctx context.Context, job, task, path string) error {
		if token == "" {
			return nil // no token, no rights to escalate
		}
		id, err := governance.NewStore(eng).Authenticate(ctx, contract.Credentials{Scheme: "bearer", Secret: token})
		if err != nil {
			return fmt.Errorf("notebook task %s/%s: the jobs token is not valid, so the notebook's authorship cannot be checked: %w", job, task, err)
		}
		return notebooks.Policy{Engine: eng}.CheckForJob(ctx, id, path)
	}
}
