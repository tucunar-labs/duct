package contract

import (
	"context"
	"errors"
	"time"
)

// Notebook errors the console maps to pages and status codes.
var (
	// ErrNotebookNotFound: there is no such notebook (or version, or comment).
	ErrNotebookNotFound = errors.New("notebook not found")
	// ErrNotebookForbidden: the identity's role may not do that (for example a
	// viewer asking for edit mode).
	ErrNotebookForbidden = errors.New("permission denied")
	// ErrNotebookLocked: another user is editing the notebook.
	ErrNotebookLocked = errors.New("notebook is being edited by someone else")
	// ErrNotebookInvalid: a bad name, an existing name or an empty comment.
	ErrNotebookInvalid = errors.New("invalid notebook request")
	// ErrNotebookUntrusted: the content was changed by authors whose privileges the reader cannot be given
	// safely and needs an explicit TrustNotebook first; the error is a *NotebookUntrustedError.
	ErrNotebookUntrusted = errors.New("notebook needs to be trusted before it runs")
)

// NotebookUntrustedError says why a notebook does not run for a reader: who changed it since the reader (or
// someone of their role) last saw it, and the hash to pass to TrustNotebook.
type NotebookUntrustedError struct {
	Notebook string
	Hash     string   // SHA-256 of the content that would run
	Authors  []string // who wrote it since the last review
	Reason   string
}

// Error implements error.
func (e *NotebookUntrustedError) Error() string {
	return ErrNotebookUntrusted.Error() + ": " + e.Reason
}

// Is makes errors.Is(err, ErrNotebookUntrusted) true.
func (e *NotebookUntrustedError) Is(target error) bool { return target == ErrNotebookUntrusted }

// Modes of a notebook session.
const (
	NotebookEdit = "edit" // marimo edit: the user changes the code
	NotebookRun  = "run"  // marimo run: the user runs the notebook, code hidden
)

// NotebookInfo is a row of the notebook list.
type NotebookInfo struct {
	Name       string
	Modified   time.Time
	Size       int64
	Creator    string // who created it in Duct; "" for files that were already there
	LastEditor string // author of the latest recorded version
	Versions   int
	Comments   int // unresolved comments
	LockedBy   string
	CanEdit    bool // the asking identity's role is an editor role
}

// NotebookSession tells the console where a user's marimo process listens and
// how the proxy must authenticate to it. The process is private to the user
// (one per user, notebook and mode) and bound to 127.0.0.1; the browser never
// sees Target or Auth.
type NotebookSession struct {
	Mode     string // NotebookEdit or NotebookRun (can be run when edit was asked and the file is locked)
	Target   string // host:port
	Auth     string // Authorization header the proxy sends to marimo
	BasePath string // URL prefix marimo serves under: /nb/<mode>/<name>
	LockedBy string // when edit was asked but this user is editing the file: Mode is then run
	// RunAs is set when the code runs with another user's rights than the reader's (least privilege of the
	// authors, ADR 0006): that user, their role and who wrote the code. The console shows it as a banner with
	// the option to trust the version. Hash is the content that was started.
	RunAs     string
	RunAsRole string
	Authors   []string
	Hash      string
	Trusted   bool // the reader trusted this content: it runs with their own role
}

// NotebookVersion is one recorded save of a notebook.
type NotebookVersion struct {
	ID           int64
	Notebook     string
	Hash         string
	Author       string
	Action       string // baseline, save, create, restore, rename, delete or external
	At           time.Time
	Size         int64
	Added        int // lines added with respect to the previous version
	Removed      int
	RestoredFrom int64 // version restored by an Action == "restore"
}

// DiffLine is one line of a unified diff: Op is ' ', '+' or '-'.
type DiffLine struct {
	Op   byte
	Text string
}

// NotebookComment is a comment on a notebook. Ref is an optional free-text
// pointer ("celda 3"): marimo does not keep cell ids in the file, so there is
// nothing stable to anchor a comment to.
type NotebookComment struct {
	ID         int64
	Notebook   string
	Ref        string
	Author     string
	Body       string
	At         time.Time
	Resolved   bool
	ResolvedBy string
	ResolvedAt time.Time
}

// NotebooksConsole is an OPTIONAL interface of a QueryHandler for the web
// console (ADR 0006): list, open, version history, comments and lifecycle of
// the project's marimo notebooks, as the already-authenticated identity.
//
// Roles: every authenticated identity lists notebooks, opens them in run
// mode and reads and writes comments; the roles named in [notebooks] editors
// (and only those) open edit mode, see the history and restore versions, and
// create or rename; delete needs the admin role. Editing is exclusive per
// file: while one user edits, the others get run mode and ErrNotebookLocked
// semantics (OpenNotebook reports LockedBy). Every action is audited by the
// handler that implements this interface.
type NotebooksConsole interface {
	Notebooks(ctx context.Context, id Identity) ([]NotebookInfo, error)
	// CanEditNotebooks reports whether the identity's role is an editor role.
	CanEditNotebooks(id Identity) bool
	Notebook(ctx context.Context, id Identity, name string) (NotebookInfo, error)
	// OpenNotebook starts (or reuses) the user's process for the notebook. With
	// edit=true and an editor role it opens edit mode unless another user holds
	// the file (then run mode, LockedBy set); edit=true with a viewer role is
	// ErrNotebookForbidden.
	OpenNotebook(ctx context.Context, id Identity, name string, edit bool) (NotebookSession, error)
	// NotebookTarget resolves the running process for the proxy and marks the
	// session active (idle timeout). It never starts anything.
	NotebookTarget(ctx context.Context, id Identity, name, mode string) (NotebookSession, error)
	// StopNotebooks ends every process of the user (logout).
	StopNotebooks(ctx context.Context, user string)

	CreateNotebook(ctx context.Context, id Identity, name string) error
	RenameNotebook(ctx context.Context, id Identity, from, to string) error
	DeleteNotebook(ctx context.Context, id Identity, name string) error

	NotebookVersions(ctx context.Context, id Identity, name string) ([]NotebookVersion, error)
	// NotebookDiff compares two versions; to == 0 means the file as it is now,
	// from == 0 means the version before to.
	NotebookDiff(ctx context.Context, id Identity, name string, from, to int64) ([]DiffLine, error)
	RestoreNotebook(ctx context.Context, id Identity, name string, version int64) error

	NotebookComments(ctx context.Context, id Identity, name string) ([]NotebookComment, error)
	AddNotebookComment(ctx context.Context, id Identity, name, ref, body string) error
	ResolveNotebookComment(ctx context.Context, id Identity, name string, comment int64, resolved bool) error
	// TrustNotebook records that the identity trusts the content with this hash (the one the console showed), so
	// the notebook runs with their own role from now on; a later edit needs trusting again. A hash that is not
	// the current content is ErrNotebookInvalid.
	TrustNotebook(ctx context.Context, id Identity, name, hash string) error
}
