// Package notebooks is the notebook service of the web console (ADR 0006): the
// marimo files of <project>/notebooks, one marimo process per user (edit or
// run mode) on 127.0.0.1 that the console proxies behind its own session, an
// exclusive lock per file while someone edits, a version history with
// snapshots and diffs, comments, and create / rename / delete / restore.
//
// It implements contract.NotebooksConsole. Authorization lives here (editor
// roles, admin-only delete, the lock); the caller audits (core/governance).
// State goes to the Duct tables duct_notebook_versions and
// duct_notebook_comments; the processes get DUCT_URL and a short-lived token
// of their user, never lake credentials.
package notebooks

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/txretry"
)

// AdminRole may delete notebooks (same name as governance.AdminRole).
const AdminRole = "duct_admin"

const (
	maxCommentBytes = 4000
	maxRefBytes     = 80
	maxFileBytes    = 4 << 20 // bigger files are neither snapshotted nor restorable
	diffContext     = 3
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// ValidName reports whether name is a usable notebook name (file name without .py).
func ValidName(name string) bool { return nameRE.MatchString(name) }

// TokenIssuer mints and revokes the per-process tokens (governance.Store).
type TokenIssuer interface {
	CreateToken(ctx context.Context, user, label string, ttl time.Duration) (token, id string, err error)
	RevokeToken(ctx context.Context, id string) error
}

// Options configure New. Dir and Engine are required.
type Options struct {
	Dir     string // <project>/notebooks
	Root    string // working directory of the processes (default: parent of Dir)
	Engine  contract.Engine
	Tokens  TokenIssuer
	Editors []string // roles that edit; default AdminRole

	IdleTimeout  time.Duration // default 30m
	MaxAge       time.Duration // default 12h
	MaxKernels   int           // default 8
	CaptureEvery time.Duration // how often the files being edited are checked for saves (default 2s)

	// DuctURL is the URL of duct serve's HTTP protocol the notebooks query (may be "").
	DuctURL func() string
	// Python resolves the interpreter argv (creating the managed venv on first use).
	Python func(ctx context.Context) ([]string, error)
	// Sandbox is SandboxBwrap or SandboxNone (the resolved setting, see ResolveSandbox): whether the
	// default launcher isolates marimo with bubblewrap. Ignored when Launcher is set.
	Sandbox string
	// Launcher starts marimo; default: the real thing. Tests plug a fake.
	Launcher Launcher
	Now      func() time.Time
}

// Service implements contract.NotebooksConsole.
type Service struct {
	o       Options
	editors map[string]bool
	now     func() time.Time

	histMu sync.Mutex // serializes recording versions

	mu      sync.Mutex
	kernels map[kkey]*kernel
	closed  bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

var _ contract.NotebooksConsole = (*Service)(nil)

// New builds the service and starts its janitor (idle timeouts, save capture).
func New(o Options) *Service {
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 30 * time.Minute
	}
	if o.MaxAge <= 0 {
		o.MaxAge = 12 * time.Hour
	}
	if o.MaxKernels <= 0 {
		o.MaxKernels = 8
	}
	if o.CaptureEvery <= 0 { // also how often idleness is checked: finer when the timeout is short
		o.CaptureEvery = max(100*time.Millisecond, min(2*time.Second, o.IdleTimeout/3))
	}
	if o.Root == "" {
		o.Root = filepath.Dir(o.Dir)
	}
	if len(o.Editors) == 0 {
		o.Editors = []string{AdminRole}
	}
	if o.Launcher == nil {
		o.Launcher = &execLauncher{Python: o.Python, Dir: o.Root, Sandbox: o.Sandbox}
	}
	s := &Service{o: o, editors: map[string]bool{}, now: o.Now, kernels: map[kkey]*kernel{}}
	if s.now == nil {
		s.now = time.Now
	}
	for _, r := range o.Editors {
		s.editors[r] = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.wg.Add(1)
	go s.janitor(ctx)
	return s
}

// Close stops every process and the janitor.
func (s *Service) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
	s.mu.Lock()
	var ks []*kernel
	for _, k := range s.kernels {
		ks = append(ks, k)
	}
	s.mu.Unlock()
	for _, k := range ks {
		s.stop(k, "shutdown")
	}
	for _, k := range ks { // do not leave the process behind when duct exits
		if k.proc != nil {
			select {
			case <-k.proc.Done():
			case <-time.After(8 * time.Second):
			}
		}
	}
}

// CanEditNotebooks implements contract.NotebooksConsole.
func (s *Service) CanEditNotebooks(id contract.Identity) bool { return s.CanEdit(id.Role) }

// CanEdit reports whether a role may open edit mode.
func (s *Service) CanEdit(role string) bool { return s.editors[role] }

func (s *Service) path(name string) string { return filepath.Join(s.o.Dir, name+".py") }

func (s *Service) checkName(name string) error {
	if !ValidName(name) {
		return fmt.Errorf("%w: the name must be letters, digits, - or _ (up to 64)", contract.ErrNotebookInvalid)
	}
	return nil
}

func (s *Service) requireEditor(id contract.Identity) error {
	if !s.CanEdit(id.Role) {
		return fmt.Errorf("%w: role %q may not edit notebooks (see [notebooks] editors)", contract.ErrNotebookForbidden, id.Role)
	}
	return nil
}

func (s *Service) exists(name string) (os.FileInfo, error) {
	if err := s.checkName(name); err != nil {
		return nil, contract.ErrNotebookNotFound
	}
	st, err := os.Stat(s.path(name))
	if err != nil || !st.Mode().IsRegular() {
		return nil, contract.ErrNotebookNotFound
	}
	return st, nil
}

// ---- state helpers ----

func newID() int64 {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return time.Now().UnixMicro()*1000 + int64(uint16(b[0])|uint16(b[1])<<8)%1000
}

func (s *Service) rows(ctx context.Context, q string, args ...any) ([][]string, error) {
	st, err := s.o.Engine.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				if col := rec.Column(c); !col.IsNull(i) {
					row[c] = strings.Clone(col.ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	return out, st.Err()
}

func (s *Service) exec(ctx context.Context, sql string, args ...any) error {
	return txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return s.o.Engine.Exec(ctx, sql, args...)
	})
}

func atoi(v string) int64 { n, _ := strconv.ParseInt(v, 10, 64); return n }

func micro(v string) time.Time {
	n := atoi(v)
	if n == 0 {
		return time.Time{}
	}
	return time.UnixMicro(n).UTC()
}

func hashOf(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:])
}

// ---- list ----

// Notebooks implements contract.NotebooksConsole.
func (s *Service) Notebooks(ctx context.Context, id contract.Identity) ([]contract.NotebookInfo, error) {
	ents, err := os.ReadDir(s.o.Dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	counts, last, creators, open, err := s.summary(ctx)
	if err != nil {
		return nil, err
	}
	locks := s.locks()
	var out []contract.NotebookInfo
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), ".py")
		if !ok || !ValidName(name) || e.IsDir() {
			continue
		}
		st, err := e.Info()
		if err != nil || !st.Mode().IsRegular() {
			continue
		}
		out = append(out, contract.NotebookInfo{
			Name: name, Modified: st.ModTime().UTC(), Size: st.Size(),
			Creator: creators[name], LastEditor: last[name], Versions: counts[name], Comments: open[name],
			LockedBy: locks[name], CanEdit: s.CanEdit(id.Role),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Service) summary(ctx context.Context) (counts map[string]int, last, creators map[string]string, open map[string]int, err error) {
	counts, last, creators, open = map[string]int{}, map[string]string{}, map[string]string{}, map[string]int{}
	rs, err := s.rows(ctx, `SELECT notebook, CAST(count(*) AS BIGINT), CAST(max(id) AS BIGINT) FROM duct.duct_notebook_versions GROUP BY notebook`)
	if err != nil {
		return
	}
	lastIDs := map[int64]string{}
	for _, r := range rs {
		counts[r[0]] = int(atoi(r[1]))
		lastIDs[atoi(r[2])] = r[0]
	}
	if rs, err = s.rows(ctx, `SELECT CAST(id AS BIGINT), notebook, author FROM duct.duct_notebook_versions
		WHERE id IN (SELECT max(id) FROM duct.duct_notebook_versions GROUP BY notebook)`); err != nil {
		return
	}
	for _, r := range rs {
		last[r[1]] = r[2]
	}
	if rs, err = s.rows(ctx, `SELECT notebook, author FROM duct.duct_notebook_versions WHERE action = 'create' ORDER BY id`); err != nil {
		return
	}
	for _, r := range rs {
		creators[r[0]] = r[1]
	}
	if rs, err = s.rows(ctx, `SELECT notebook, CAST(count(*) AS BIGINT) FROM duct.duct_notebook_comments WHERE NOT resolved GROUP BY notebook`); err != nil {
		return
	}
	for _, r := range rs {
		open[r[0]] = int(atoi(r[1]))
	}
	return
}

// Notebook implements contract.NotebooksConsole.
func (s *Service) Notebook(ctx context.Context, id contract.Identity, name string) (contract.NotebookInfo, error) {
	if _, err := s.exists(name); err != nil {
		return contract.NotebookInfo{}, err
	}
	all, err := s.Notebooks(ctx, id)
	if err != nil {
		return contract.NotebookInfo{}, err
	}
	for _, n := range all {
		if n.Name == name {
			return n, nil
		}
	}
	return contract.NotebookInfo{}, contract.ErrNotebookNotFound
}

// ---- lifecycle ----

// Template is the content of a new notebook: it connects with duct.connect(),
// which reads DUCT_URL and DUCT_TOKEN of the process.
func Template() string {
	return `import marimo

app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    import duct

    mo.md("# Nuevo notebook")
    return duct, mo


@app.cell
def _(duct):
    # DUCT_URL y DUCT_TOKEN los pone Duct: las consultas corren con tu rol.
    with duct.connect() as con:
        res = con.sql("SELECT 'hola' AS saludo, 42 AS respuesta")
    res.rows
    return


if __name__ == "__main__":
    app.run()
`
}

// CreateNotebook implements contract.NotebooksConsole.
func (s *Service) CreateNotebook(ctx context.Context, id contract.Identity, name string) error {
	if err := s.requireEditor(id); err != nil {
		return err
	}
	if err := s.checkName(name); err != nil {
		return err
	}
	if err := os.MkdirAll(s.o.Dir, 0o755); err != nil { //nolint:gosec // project folder, readable like the rest
		return err
	}
	f, err := os.OpenFile(s.path(name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // idem
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: %q already exists", contract.ErrNotebookInvalid, name)
	}
	if err != nil {
		return err
	}
	content := Template()
	_, werr := f.WriteString(content)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(s.path(name))
		return werr
	}
	_, err = s.record(ctx, name, id.User, "create", content, 0, true)
	return err
}

// RenameNotebook implements contract.NotebooksConsole.
func (s *Service) RenameNotebook(ctx context.Context, id contract.Identity, from, to string) error {
	if err := s.requireEditor(id); err != nil {
		return err
	}
	if _, err := s.exists(from); err != nil {
		return err
	}
	if err := s.checkName(to); err != nil {
		return err
	}
	if from == to {
		return fmt.Errorf("%w: the new name is the same", contract.ErrNotebookInvalid)
	}
	if err := s.takeFile(id, from); err != nil {
		return err
	}
	if _, err := os.Stat(s.path(to)); err == nil {
		return fmt.Errorf("%w: %q already exists", contract.ErrNotebookInvalid, to)
	}
	if _, err := s.sync(ctx, from, id.User, "external"); err != nil {
		return err
	}
	if err := os.Rename(s.path(from), s.path(to)); err != nil {
		return err
	}
	for _, t := range []string{"duct_notebook_versions", "duct_notebook_comments"} {
		if err := s.exec(ctx, "UPDATE duct."+t+" SET notebook = ? WHERE notebook = ?", to, from); err != nil {
			return err
		}
	}
	b, err := os.ReadFile(s.path(to))
	if err != nil {
		return err
	}
	_, err = s.record(ctx, to, id.User, "rename", string(b), 0, true)
	return err
}

// DeleteNotebook implements contract.NotebooksConsole: admin only. The history
// and comments stay (a restore recreates the file).
func (s *Service) DeleteNotebook(ctx context.Context, id contract.Identity, name string) error {
	if id.Role != AdminRole {
		return fmt.Errorf("%w: deleting a notebook needs the %s role", contract.ErrNotebookForbidden, AdminRole)
	}
	if _, err := s.exists(name); err != nil {
		return err
	}
	if err := s.takeFile(id, name); err != nil {
		return err
	}
	if _, err := s.sync(ctx, name, id.User, "external"); err != nil {
		return err
	}
	b, err := os.ReadFile(s.path(name))
	if err != nil {
		return err
	}
	if err := os.Remove(s.path(name)); err != nil {
		return err
	}
	_, err = s.record(ctx, name, id.User, "delete", string(b), 0, true)
	return err
}

// takeFile makes the file free for a change by id: another user's editing
// session is ErrNotebookLocked; the user's own edit session is saved and
// stopped, and every run-mode process on the file ends (it would show stale code).
func (s *Service) takeFile(id contract.Identity, name string) error {
	s.mu.Lock()
	for _, k := range s.kernels {
		if k.name == name && k.mode == contract.NotebookEdit && k.user != id.User {
			s.mu.Unlock()
			return fmt.Errorf("%w (%s)", contract.ErrNotebookLocked, k.user)
		}
	}
	s.mu.Unlock()
	s.stopWhere(func(k *kernel) bool { return k.name == name }, "file changed")
	return nil
}

// ---- history ----

// record writes a version row for content (content is what the file holds
// now). With force false an unchanged hash is not recorded. It returns whether
// a row was written.
func (s *Service) record(ctx context.Context, name, author, action, content string, restoredFrom int64, force bool) (bool, error) {
	if len(content) > maxFileBytes {
		return false, fmt.Errorf("%w: the file is larger than %d MB", contract.ErrNotebookInvalid, maxFileBytes>>20)
	}
	s.histMu.Lock()
	defer s.histMu.Unlock()
	prevHash, prev, err := s.latestContent(ctx, name)
	if err != nil {
		return false, err
	}
	h := hashOf(content)
	if !force && h == prevHash {
		return false, nil
	}
	added, removed := countChanges(diffLines(splitLines(prev), splitLines(content)))
	var rf any
	if restoredFrom > 0 {
		rf = restoredFrom
	}
	err = s.exec(ctx, `INSERT INTO duct.duct_notebook_versions
		(id, notebook, content_hash, author, action, created_at_us, size_bytes, lines_added, lines_removed, restored_from, content)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		newID(), name, h, author, action, s.now().UnixMicro(), int64(len(content)), int32(added), int32(removed), rf, content)
	return err == nil, err
}

// latestContent is the hash and content of the newest version ("" if none).
func (s *Service) latestContent(ctx context.Context, name string) (hash, content string, err error) {
	rs, err := s.rows(ctx, `SELECT content_hash, content FROM duct.duct_notebook_versions WHERE notebook = ? ORDER BY id DESC LIMIT 1`, name)
	if err != nil || len(rs) == 0 {
		return "", "", err
	}
	return rs[0][0], rs[0][1], nil
}

// sync records the file as it is on disk when it differs from the newest
// version (a save by marimo, or a change made outside Duct). It returns whether
// a version was written. Too-large files are skipped silently.
func (s *Service) sync(ctx context.Context, name, author, action string) (bool, error) {
	b, err := os.ReadFile(s.path(name))
	if err != nil || len(b) > maxFileBytes {
		return false, nil //nolint:nilerr // a vanished or huge file has nothing to snapshot
	}
	return s.record(ctx, name, author, action, string(b), 0, false)
}

// editorOf is the user holding the edit lock on name ("" if nobody).
func (s *Service) editorOf(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range s.kernels {
		if k.name == name && k.mode == contract.NotebookEdit {
			return k.user
		}
	}
	return ""
}

func (s *Service) locks() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for _, k := range s.kernels {
		if k.mode == contract.NotebookEdit {
			out[k.name] = k.user
		}
	}
	return out
}

// ensureBaseline records the file as the first version if it has none, so the
// first save is a diff against what was there.
func (s *Service) ensureBaseline(ctx context.Context, name string) error {
	rs, err := s.rows(ctx, `SELECT CAST(count(*) AS BIGINT) FROM duct.duct_notebook_versions WHERE notebook = ?`, name)
	if err != nil || (len(rs) > 0 && atoi(rs[0][0]) > 0) {
		return err
	}
	_, err = s.sync(ctx, name, "sistema", "baseline")
	return err
}

// NotebookVersions implements contract.NotebooksConsole (editors only).
func (s *Service) NotebookVersions(ctx context.Context, id contract.Identity, name string) ([]contract.NotebookVersion, error) {
	if err := s.requireEditor(id); err != nil {
		return nil, err
	}
	if err := s.checkName(name); err != nil {
		return nil, contract.ErrNotebookNotFound
	}
	if _, err := os.Stat(s.path(name)); err == nil { // saves made outside the editor show up too
		author, action := s.editorOf(name), "save"
		if author == "" {
			author, action = "externo", "external"
		}
		if _, err := s.sync(ctx, name, author, action); err != nil {
			return nil, err
		}
	}
	rs, err := s.rows(ctx, `SELECT CAST(id AS BIGINT), content_hash, author, action, CAST(created_at_us AS BIGINT), CAST(size_bytes AS BIGINT),
		CAST(lines_added AS BIGINT), CAST(lines_removed AS BIGINT), CAST(COALESCE(restored_from, 0) AS BIGINT)
		FROM duct.duct_notebook_versions WHERE notebook = ? ORDER BY id DESC`, name)
	if err != nil {
		return nil, err
	}
	out := make([]contract.NotebookVersion, 0, len(rs))
	for _, r := range rs {
		out = append(out, contract.NotebookVersion{ID: atoi(r[0]), Notebook: name, Hash: r[1], Author: r[2], Action: r[3],
			At: micro(r[4]), Size: atoi(r[5]), Added: int(atoi(r[6])), Removed: int(atoi(r[7])), RestoredFrom: atoi(r[8])})
	}
	return out, nil
}

func (s *Service) versionContent(ctx context.Context, name string, version int64) (string, int64, error) {
	rs, err := s.rows(ctx, `SELECT content, CAST(id AS BIGINT) FROM duct.duct_notebook_versions WHERE notebook = ? AND id = ?`, name, version)
	if err != nil {
		return "", 0, err
	}
	if len(rs) == 0 {
		return "", 0, contract.ErrNotebookNotFound
	}
	return rs[0][0], atoi(rs[0][1]), nil
}

// NotebookDiff implements contract.NotebooksConsole (editors only).
func (s *Service) NotebookDiff(ctx context.Context, id contract.Identity, name string, from, to int64) ([]contract.DiffLine, error) {
	if err := s.requireEditor(id); err != nil {
		return nil, err
	}
	if err := s.checkName(name); err != nil {
		return nil, contract.ErrNotebookNotFound
	}
	var newer string
	if to == 0 {
		b, err := os.ReadFile(s.path(name))
		if err != nil {
			return nil, contract.ErrNotebookNotFound
		}
		newer = string(b)
	} else {
		var err error
		if newer, _, err = s.versionContent(ctx, name, to); err != nil {
			return nil, err
		}
	}
	var older string
	if from == 0 {
		if to == 0 {
			return nil, contract.ErrNotebookNotFound
		}
		rs, err := s.rows(ctx, `SELECT content FROM duct.duct_notebook_versions WHERE notebook = ? AND id < ? ORDER BY id DESC LIMIT 1`, name, to)
		if err != nil {
			return nil, err
		}
		if len(rs) > 0 {
			older = rs[0][0]
		}
	} else {
		var err error
		if older, _, err = s.versionContent(ctx, name, from); err != nil {
			return nil, err
		}
	}
	return withContext(diffLines(splitLines(older), splitLines(newer)), diffContext), nil
}

// RestoreNotebook implements contract.NotebooksConsole: the file gets the
// content of the version (it is created if it was deleted), after saving what
// is there now as a version, and the restore is itself a new version.
func (s *Service) RestoreNotebook(ctx context.Context, id contract.Identity, name string, version int64) error {
	if err := s.requireEditor(id); err != nil {
		return err
	}
	if err := s.checkName(name); err != nil {
		return contract.ErrNotebookNotFound
	}
	content, vid, err := s.versionContent(ctx, name, version)
	if err != nil {
		return err
	}
	if err := s.takeFile(id, name); err != nil {
		return err
	}
	if _, err := os.Stat(s.path(name)); err == nil {
		if _, err := s.sync(ctx, name, id.User, "external"); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(s.o.Dir, 0o755); err != nil { //nolint:gosec // project folder
		return err
	}
	tmp := s.path(name) + ".restore"
	if err := os.WriteFile(tmp, []byte(content), 0o644); err != nil { //nolint:gosec // like the other notebooks
		return err
	}
	if err := os.Rename(tmp, s.path(name)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	_, err = s.record(ctx, name, id.User, "restore", content, vid, true)
	return err
}

// ---- comments ----

// NotebookComments implements contract.NotebooksConsole.
func (s *Service) NotebookComments(ctx context.Context, _ contract.Identity, name string) ([]contract.NotebookComment, error) {
	if err := s.checkName(name); err != nil {
		return nil, contract.ErrNotebookNotFound
	}
	rs, err := s.rows(ctx, `SELECT CAST(id AS BIGINT), COALESCE(ref, ''), author, body, CAST(created_at_us AS BIGINT), CAST(resolved AS VARCHAR),
		COALESCE(resolved_by, ''), CAST(COALESCE(resolved_at_us, 0) AS BIGINT)
		FROM duct.duct_notebook_comments WHERE notebook = ? ORDER BY id`, name)
	if err != nil {
		return nil, err
	}
	out := make([]contract.NotebookComment, 0, len(rs))
	for _, r := range rs {
		out = append(out, contract.NotebookComment{ID: atoi(r[0]), Notebook: name, Ref: r[1], Author: r[2], Body: r[3], At: micro(r[4]),
			Resolved: r[5] == "true" || r[5] == "1", ResolvedBy: r[6], ResolvedAt: micro(r[7])})
	}
	return out, nil
}

// AddNotebookComment implements contract.NotebooksConsole.
func (s *Service) AddNotebookComment(ctx context.Context, id contract.Identity, name, ref, body string) error {
	if _, err := s.exists(name); err != nil {
		return err
	}
	body, ref = strings.TrimSpace(body), strings.TrimSpace(ref)
	if body == "" {
		return fmt.Errorf("%w: the comment is empty", contract.ErrNotebookInvalid)
	}
	if len(body) > maxCommentBytes || len(ref) > maxRefBytes {
		return fmt.Errorf("%w: the comment is too long (%d characters at most, reference %d)", contract.ErrNotebookInvalid, maxCommentBytes, maxRefBytes)
	}
	var r any
	if ref != "" {
		r = ref
	}
	return s.exec(ctx, `INSERT INTO duct.duct_notebook_comments (id, notebook, ref, author, body, created_at_us, resolved) VALUES (?, ?, ?, ?, ?, ?, false)`,
		newID(), name, r, id.User, body, s.now().UnixMicro())
}

// ResolveNotebookComment implements contract.NotebooksConsole: the author or
// an editor role (re)opens or resolves a comment.
func (s *Service) ResolveNotebookComment(ctx context.Context, id contract.Identity, name string, comment int64, resolved bool) error {
	if err := s.checkName(name); err != nil {
		return contract.ErrNotebookNotFound
	}
	rs, err := s.rows(ctx, `SELECT author FROM duct.duct_notebook_comments WHERE notebook = ? AND id = ?`, name, comment)
	if err != nil {
		return err
	}
	if len(rs) == 0 {
		return contract.ErrNotebookNotFound
	}
	if rs[0][0] != id.User && !s.CanEdit(id.Role) {
		return fmt.Errorf("%w: only the author or an editor resolves a comment", contract.ErrNotebookForbidden)
	}
	if resolved {
		return s.exec(ctx, `UPDATE duct.duct_notebook_comments SET resolved = true, resolved_by = ?, resolved_at_us = ? WHERE id = ?`, id.User, s.now().UnixMicro(), comment)
	}
	return s.exec(ctx, `UPDATE duct.duct_notebook_comments SET resolved = false, resolved_by = NULL, resolved_at_us = NULL WHERE id = ?`, comment)
}
