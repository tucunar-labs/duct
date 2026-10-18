package governance

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
)

// What the web console needs beyond SQL is exposed through optional contract
// interfaces (contract.AdminCaller, AuditReader, JobsConsole) that the handler
// returned by ForProtocol implements. The admin API and the jobs service live
// in packages that import governance, so they are plugged in by `duct serve`.

// AdminInvoker runs one admin API call for an identity (core/admin.API.Invoke).
type AdminInvoker func(ctx context.Context, protocol string, id contract.Identity, method, path string, body []byte) (status int, resp []byte)

type consoleHooks struct {
	admin atomic.Pointer[AdminInvoker]
	jobs  atomic.Pointer[contract.JobsConsole]
	// notebooks is the notebook service (notebooks.go in this package audits it).
	notebooks atomic.Pointer[contract.NotebooksConsole]
}

// SetAdminInvoker plugs the admin API into the handler (contract.AdminCaller).
func (h *Handler) SetAdminInvoker(fn AdminInvoker) { h.console.admin.Store(&fn) }

// SetJobsConsole plugs the jobs service into the handler (contract.JobsConsole).
func (h *Handler) SetJobsConsole(c contract.JobsConsole) { h.console.jobs.Store(&c) }

var errAdminOnly = errors.New("permission denied: this action requires the " + AdminRole + " role")

var (
	_ contract.AdminCaller = (*protocolHandler)(nil)
	_ contract.AuditReader = (*protocolHandler)(nil)
	_ contract.JobsConsole = (*protocolHandler)(nil)
)

// AdminCall implements contract.AdminCaller.
func (p *protocolHandler) AdminCall(ctx context.Context, id contract.Identity, method, path string, body []byte) (int, []byte, error) {
	fn := p.h.console.admin.Load()
	if fn == nil {
		return 0, nil, contract.ErrConsoleUnavailable
	}
	status, resp := (*fn)(ctx, p.name, id, method, path, body)
	return status, resp, nil
}

// AuditPage implements contract.AuditReader.
func (p *protocolHandler) AuditPage(ctx context.Context, id contract.Identity, q contract.AuditQuery) ([]contract.AuditEntry, int64, error) {
	if !IsAdmin(id) {
		return nil, 0, errAdminOnly
	}
	_ = p.h.audit.flush(ctx) // what was just done shows up
	f := AuditFilter{User: q.User, Status: q.Status, Protocol: q.Protocol, From: q.From, To: q.To, Limit: q.Limit, Offset: q.Offset}
	total, err := CountAudit(ctx, p.h.eng, f)
	if err != nil {
		return nil, 0, err
	}
	rows, err := QueryAudit(ctx, p.h.eng, f)
	if err != nil {
		return nil, 0, err
	}
	out := make([]contract.AuditEntry, len(rows))
	for i, r := range rows {
		e := contract.AuditEntry{User: r.User, Role: r.Role, Protocol: r.Protocol, Status: r.Status,
			Duration: time.Duration(r.DurationMS) * time.Millisecond, Rows: r.Rows, Cached: r.Cached, Hits: r.Hits, SQL: r.SQL, Error: r.Error}
		e.At, _ = time.Parse(project.AuditTimeLayout, r.At)
		out[i] = e
	}
	return out, total, nil
}

func (p *protocolHandler) jobs() (contract.JobsConsole, error) {
	c := p.h.console.jobs.Load()
	if c == nil {
		return nil, contract.ErrConsoleUnavailable
	}
	return *c, nil
}

// Jobs implements contract.JobsConsole.
func (p *protocolHandler) Jobs(ctx context.Context) ([]contract.JobInfo, error) {
	c, err := p.jobs()
	if err != nil {
		return nil, err
	}
	return c.Jobs(ctx)
}

// JobRuns implements contract.JobsConsole.
func (p *protocolHandler) JobRuns(ctx context.Context, job string, limit int) ([]contract.JobRunInfo, error) {
	c, err := p.jobs()
	if err != nil {
		return nil, err
	}
	return c.JobRuns(ctx, job, limit)
}

// JobRun implements contract.JobsConsole.
func (p *protocolHandler) JobRun(ctx context.Context, runID int64) (contract.JobRunInfo, []contract.JobTaskRun, bool, error) {
	c, err := p.jobs()
	if err != nil {
		return contract.JobRunInfo{}, nil, false, err
	}
	return c.JobRun(ctx, runID)
}

// jobAction is the shared path of the three actions: admin only, audited.
func (p *protocolHandler) jobAction(ctx context.Context, id contract.Identity, what string, fn func(contract.JobsConsole) error) error {
	start := time.Now()
	rec := contract.AuditRecord{At: start.UTC(), User: id.User, Role: id.Role, Protocol: p.name, SQL: "JOBS " + what}
	err := func() error {
		if !IsAdmin(id) {
			return errAdminOnly
		}
		c, err := p.jobs()
		if err != nil {
			return err
		}
		return fn(c)
	}()
	rec.Duration = time.Since(start)
	if err != nil {
		rec.Err = err.Error()
	}
	p.h.Audit(ctx, rec)
	return err
}

// RunJob implements contract.JobsConsole (admin only, audited).
func (p *protocolHandler) RunJob(ctx context.Context, id contract.Identity, job string) (runID int64, err error) {
	err = p.jobAction(ctx, id, "run job="+job, func(c contract.JobsConsole) (e error) {
		runID, e = c.RunJob(ctx, id, job)
		return e
	})
	return runID, err
}

// CancelJobRun implements contract.JobsConsole (admin only, audited).
func (p *protocolHandler) CancelJobRun(ctx context.Context, id contract.Identity, runID int64) error {
	return p.jobAction(ctx, id, fmt.Sprintf("cancel run=%d", runID), func(c contract.JobsConsole) error {
		return c.CancelJobRun(ctx, id, runID)
	})
}

// SetJobEnabled implements contract.JobsConsole (admin only, audited).
func (p *protocolHandler) SetJobEnabled(ctx context.Context, id contract.Identity, job string, enabled bool) error {
	verb := "disable"
	if enabled {
		verb = "enable"
	}
	return p.jobAction(ctx, id, verb+" job="+job, func(c contract.JobsConsole) error {
		return c.SetJobEnabled(ctx, id, job, enabled)
	})
}
