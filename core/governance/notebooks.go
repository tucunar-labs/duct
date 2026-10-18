package governance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// SetNotebooksConsole plugs the notebook service into the handler
// (contract.NotebooksConsole). The service does the authorization; this layer
// puts every action that opens, changes or reveals something into the audit
// log ("NOTEBOOK ..." statements; permission errors show as denied).
func (h *Handler) SetNotebooksConsole(c contract.NotebooksConsole) { h.console.notebooks.Store(&c) }

var _ contract.NotebooksConsole = (*protocolHandler)(nil)

func (p *protocolHandler) nb() (contract.NotebooksConsole, error) {
	c := p.h.console.notebooks.Load()
	if c == nil {
		return nil, contract.ErrConsoleUnavailable
	}
	return *c, nil
}

// nbAudited runs fn and records "NOTEBOOK <what>" for id.
func (p *protocolHandler) nbAudited(ctx context.Context, id contract.Identity, what string, fn func(contract.NotebooksConsole) error) error {
	return p.nbAuditedMore(ctx, id, what, nil, fn)
}

// nbAuditedMore is nbAudited with a suffix computed after fn ran (what the action turned out to do).
func (p *protocolHandler) nbAuditedMore(ctx context.Context, id contract.Identity, what string, more func() string, fn func(contract.NotebooksConsole) error) error {
	start := time.Now()
	rec := contract.AuditRecord{At: start.UTC(), User: id.User, Role: id.Role, Protocol: p.name, SQL: "NOTEBOOK " + what}
	err := func() error {
		c, err := p.nb()
		if err != nil {
			return err
		}
		return fn(c)
	}()
	rec.Duration = time.Since(start)
	if more != nil {
		rec.SQL += more()
	}
	if err != nil {
		rec.Err = err.Error()
	}
	p.h.Audit(ctx, rec)
	return err
}

// Notebooks implements contract.NotebooksConsole.
func (p *protocolHandler) Notebooks(ctx context.Context, id contract.Identity) ([]contract.NotebookInfo, error) {
	c, err := p.nb()
	if err != nil {
		return nil, err
	}
	return c.Notebooks(ctx, id)
}

// CanEditNotebooks implements contract.NotebooksConsole.
func (p *protocolHandler) CanEditNotebooks(id contract.Identity) bool {
	c, err := p.nb()
	return err == nil && c.CanEditNotebooks(id)
}

// Notebook implements contract.NotebooksConsole.
func (p *protocolHandler) Notebook(ctx context.Context, id contract.Identity, name string) (contract.NotebookInfo, error) {
	c, err := p.nb()
	if err != nil {
		return contract.NotebookInfo{}, err
	}
	return c.Notebook(ctx, id, name)
}

// OpenNotebook implements contract.NotebooksConsole (audited).
func (p *protocolHandler) OpenNotebook(ctx context.Context, id contract.Identity, name string, edit bool) (ses contract.NotebookSession, err error) {
	mode := contract.NotebookRun
	if edit {
		mode = contract.NotebookEdit
	}
	more := func() string { // the code runs with another user's rights (least privilege of the authors)
		if ses.RunAs != "" {
			return " runas=" + ses.RunAs + " (role " + ses.RunAsRole + ") authors=" + strings.Join(ses.Authors, ",")
		}
		return ""
	}
	err = p.nbAuditedMore(ctx, id, fmt.Sprintf("open mode=%s name=%s", mode, name), more, func(c contract.NotebooksConsole) (e error) {
		ses, e = c.OpenNotebook(ctx, id, name, edit)
		return e
	})
	return ses, err
}

// NotebookTarget implements contract.NotebooksConsole (not audited: it runs on every proxied request).
func (p *protocolHandler) NotebookTarget(ctx context.Context, id contract.Identity, name, mode string) (contract.NotebookSession, error) {
	c, err := p.nb()
	if err != nil {
		return contract.NotebookSession{}, err
	}
	return c.NotebookTarget(ctx, id, name, mode)
}

// StopNotebooks implements contract.NotebooksConsole.
func (p *protocolHandler) StopNotebooks(ctx context.Context, user string) {
	if c, err := p.nb(); err == nil {
		c.StopNotebooks(ctx, user)
	}
}

// CreateNotebook implements contract.NotebooksConsole (audited).
func (p *protocolHandler) CreateNotebook(ctx context.Context, id contract.Identity, name string) error {
	return p.nbAudited(ctx, id, "create name="+name, func(c contract.NotebooksConsole) error { return c.CreateNotebook(ctx, id, name) })
}

// RenameNotebook implements contract.NotebooksConsole (audited).
func (p *protocolHandler) RenameNotebook(ctx context.Context, id contract.Identity, from, to string) error {
	return p.nbAudited(ctx, id, "rename name="+from+" to="+to, func(c contract.NotebooksConsole) error { return c.RenameNotebook(ctx, id, from, to) })
}

// DeleteNotebook implements contract.NotebooksConsole (audited).
func (p *protocolHandler) DeleteNotebook(ctx context.Context, id contract.Identity, name string) error {
	return p.nbAudited(ctx, id, "delete name="+name, func(c contract.NotebooksConsole) error { return c.DeleteNotebook(ctx, id, name) })
}

// NotebookVersions implements contract.NotebooksConsole.
func (p *protocolHandler) NotebookVersions(ctx context.Context, id contract.Identity, name string) ([]contract.NotebookVersion, error) {
	c, err := p.nb()
	if err != nil {
		return nil, err
	}
	return c.NotebookVersions(ctx, id, name)
}

// NotebookDiff implements contract.NotebooksConsole (audited: it shows code).
func (p *protocolHandler) NotebookDiff(ctx context.Context, id contract.Identity, name string, from, to int64) (out []contract.DiffLine, err error) {
	err = p.nbAudited(ctx, id, fmt.Sprintf("diff name=%s from=%d to=%d", name, from, to), func(c contract.NotebooksConsole) (e error) {
		out, e = c.NotebookDiff(ctx, id, name, from, to)
		return e
	})
	return out, err
}

// RestoreNotebook implements contract.NotebooksConsole (audited).
func (p *protocolHandler) RestoreNotebook(ctx context.Context, id contract.Identity, name string, version int64) error {
	return p.nbAudited(ctx, id, "restore name="+name+" version="+strconv.FormatInt(version, 10), func(c contract.NotebooksConsole) error {
		return c.RestoreNotebook(ctx, id, name, version)
	})
}

// NotebookComments implements contract.NotebooksConsole.
func (p *protocolHandler) NotebookComments(ctx context.Context, id contract.Identity, name string) ([]contract.NotebookComment, error) {
	c, err := p.nb()
	if err != nil {
		return nil, err
	}
	return c.NotebookComments(ctx, id, name)
}

// AddNotebookComment implements contract.NotebooksConsole (audited; the text is not logged).
func (p *protocolHandler) AddNotebookComment(ctx context.Context, id contract.Identity, name, ref, body string) error {
	return p.nbAudited(ctx, id, "comment name="+name, func(c contract.NotebooksConsole) error { return c.AddNotebookComment(ctx, id, name, ref, body) })
}

// ResolveNotebookComment implements contract.NotebooksConsole (audited).
func (p *protocolHandler) ResolveNotebookComment(ctx context.Context, id contract.Identity, name string, comment int64, resolved bool) error {
	verb := "reopen"
	if resolved {
		verb = "resolve"
	}
	return p.nbAudited(ctx, id, fmt.Sprintf("%s comment name=%s id=%d", verb, name, comment), func(c contract.NotebooksConsole) error {
		return c.ResolveNotebookComment(ctx, id, name, comment, resolved)
	})
}

// TrustNotebook implements contract.NotebooksConsole (audited: it lets another author's code run with the user's role).
func (p *protocolHandler) TrustNotebook(ctx context.Context, id contract.Identity, name, hash string) error {
	short := hash
	if len(short) > 12 {
		short = short[:12]
	}
	return p.nbAudited(ctx, id, "trust name="+name+" hash="+short, func(c contract.NotebooksConsole) error { return c.TrustNotebook(ctx, id, name, hash) })
}
