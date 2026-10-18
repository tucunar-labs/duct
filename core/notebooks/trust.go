package notebooks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Least privilege of the authors (ADR 0006): the code of a notebook runs with the token of whoever opens it, so
// whoever can edit it could make a more privileged reader run their code with that reader's rights. The rule that
// closes it, evaluated on every open and run:
//
//   - The reader's own saves, and those of any user of the reader's role (same privileges: nothing to escalate),
//     count as review. So does an explicit "Confiar en esta versión" of the reader (user, notebook, content
//     hash; audited) for the content it was given on.
//   - The authors of the versions after the newest review are the unreviewed authors. With none, the code runs
//     as the reader.
//   - Reader duct_admin, unreviewed authors of a single non-admin role: the code runs with the token of the latest
//     such author (their code, their data access), with a banner. Several non-admin roles: needs trust.
//   - Reader of another role: admin authors are ignored (running their code with less rights escalates
//     nothing); an author of a third role is not comparable and needs trust (running it as the reader would
//     expose the reader's data to the author, running it as the author would expose the author's to the reader).
//   - Authors Duct cannot attribute need trust: a user that no longer exists or is disabled, and the marker
//     unknownAuthor (several editors were live when a file changed behind the editor's back). Changes outside
//     Duct ("externo": git, a shell) and the "sistema" baseline are repository code, outside this boundary.
//
// A new edit changes the content hash, so trust does not carry over.

const (
	externalAuthor = "externo"
	systemAuthor   = "sistema"
	// unknownAuthor marks a change that could be any of several editors (see attribute).
	unknownAuthor = "?editores"
)

// Verdict is what the rule says for one reader and one content.
type Verdict struct {
	Hash      string
	RunAs     string // user whose token runs the code (the reader unless Limited)
	RunAsRole string
	Limited   bool // RunAs is not the reader
	Trusted   bool // the reader trusted this very content
	NeedTrust bool
	Authors   []string // unreviewed authors, newest first
	Reason    string
}

// Policy evaluates the rule against the version history.
type Policy struct{ Engine contract.Engine }

// HashOf is the content hash of a notebook file as the history stores it.
func HashOf(content []byte) string {
	h := sha256.Sum256(content)
	return hex.EncodeToString(h[:])
}

func (pl Policy) rows(ctx context.Context, q string, args ...any) ([][]string, error) {
	st, err := pl.Engine.Query(ctx, q, args...)
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

// Evaluate applies the rule for reader and the content with this hash of the notebook.
func (pl Policy) Evaluate(ctx context.Context, reader contract.Identity, name, hash string) (Verdict, error) {
	v := Verdict{Hash: hash, RunAs: reader.User, RunAsRole: reader.Role}
	trusted := map[string]bool{}
	rs, err := pl.rows(ctx, `SELECT content_hash FROM duct.duct_notebook_trust WHERE user_name = ? AND notebook = ?`, reader.User, name)
	if err != nil {
		return v, err
	}
	for _, r := range rs {
		trusted[r[0]] = true
	}
	if trusted[hash] {
		v.Trusted = true
		return v, nil
	}
	vs, err := pl.rows(ctx, `SELECT author, content_hash FROM duct.duct_notebook_versions WHERE notebook = ? ORDER BY id DESC`, name)
	if err != nil {
		return v, err
	}
	if len(vs) == 0 {
		return v, nil // never edited through Duct: repository code
	}
	users := map[string]userRow{}
	us, err := pl.rows(ctx, `SELECT name, role, CAST(disabled AS VARCHAR) FROM duct.duct_users`)
	if err != nil {
		return v, err
	}
	for _, u := range us {
		users[u[0]] = userRow{role: u[1], disabled: u[2] == "1" || strings.EqualFold(u[2], "true")}
	}
	var unreviewed []string // distinct authors, newest first
	seen := map[string]bool{}
	for _, row := range vs {
		author, h := row[0], row[1]
		if trusted[h] || author == reader.User {
			break
		}
		if u, ok := users[author]; ok && u.role == reader.Role && !u.disabled {
			break // same privileges: nothing to escalate, and their save is a review
		}
		if !seen[author] {
			seen[author] = true
			unreviewed = append(unreviewed, author)
		}
	}
	v.Authors = unreviewed
	var (
		lower     []string // unreviewed authors with a role that is not the reader's and not admin
		lowerRole = map[string]bool{}
	)
	for _, a := range unreviewed {
		switch a {
		case externalAuthor, systemAuthor:
			continue
		case unknownAuthor:
			v.NeedTrust, v.Reason = true, "the file changed while several editors had it open and the author cannot be told"
			return v, nil
		}
		u, ok := users[a]
		switch {
		case !ok || u.disabled:
			v.NeedTrust, v.Reason = true, "its author "+a+" no longer exists or is disabled"
			return v, nil
		case u.role == AdminRole && reader.Role != AdminRole:
			continue // their rights are higher: running their code with the reader's lowers it
		}
		lower = append(lower, a)
		lowerRole[u.role] = true
	}
	switch {
	case len(lower) == 0:
		return v, nil
	case reader.Role == AdminRole && len(lowerRole) == 1:
		v.RunAs, v.RunAsRole, v.Limited = lower[0], users[lower[0]].role, true
		return v, nil
	}
	v.NeedTrust = true
	v.Reason = "it was changed by " + strings.Join(lower, ", ") + " (role " + joinKeys(lowerRole) + "), whose privileges are not comparable with " + reader.Role
	return v, nil
}

type userRow struct {
	role     string
	disabled bool
}

func joinKeys(m map[string]bool) string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return strings.Join(ks, ", ")
}

// Trust records that user trusts the content with this hash of the notebook.
func (pl Policy) Trust(ctx context.Context, user, name, hash string, now time.Time) error {
	rs, err := pl.rows(ctx, `SELECT CAST(count(*) AS BIGINT) FROM duct.duct_notebook_trust WHERE user_name = ? AND notebook = ? AND content_hash = ?`, user, name, hash)
	if err != nil {
		return err
	}
	if len(rs) > 0 && rs[0][0] != "0" {
		return nil
	}
	return pl.Engine.Exec(ctx, `INSERT INTO duct.duct_notebook_trust (user_name, notebook, content_hash, created_at_us) VALUES (?, ?, ?, ?)`,
		user, name, hash, now.UnixMicro())
}

// CheckForJob decides whether a scheduled notebook task may run unattended as the job's identity: only when the
// rule says the code runs with that identity's own rights (no unreviewed author with another role). Otherwise
// it fails closed: an admin approves the current content with `duct notebook approve <name>`.
func (pl Policy) CheckForJob(ctx context.Context, job contract.Identity, path string) error {
	name, ok := strings.CutSuffix(filepath.Base(path), ".py")
	if !ok || filepath.Base(filepath.Dir(path)) != "notebooks" || !ValidName(name) {
		return nil // not a console notebook: repository code
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil //nolint:nilerr // the task reports the missing file itself
	}
	v, err := pl.Evaluate(ctx, job, name, HashOf(b))
	if err != nil {
		return fmt.Errorf("notebook %s: checking its authorship: %w", name, err)
	}
	if v.NeedTrust || v.Limited {
		by := strings.Join(v.Authors, ", ")
		if v.NeedTrust && v.Reason != "" {
			by += " (" + v.Reason + ")"
		}
		return fmt.Errorf("notebook %s was changed by %s after the last review by %s (%s): a scheduled run would give that code the job's rights; "+
			"review it and approve it with `duct notebook approve %s --user %s`", name, by, job.User, job.Role, name, job.User)
	}
	return nil
}

// ---- Service integration ----

func (s *Service) policy() Policy { return Policy{Engine: s.o.Engine} }

// fileHash reads the notebook and returns its content hash.
func (s *Service) fileHash(name string) (string, error) {
	b, err := os.ReadFile(s.path(name))
	if err != nil {
		return "", contract.ErrNotebookNotFound
	}
	return HashOf(b), nil
}

// TrustNotebook implements contract.NotebooksConsole.
func (s *Service) TrustNotebook(ctx context.Context, id contract.Identity, name, hash string) error {
	if _, err := s.exists(name); err != nil {
		return err
	}
	s.attribute(ctx)
	cur, err := s.fileHash(name)
	if err != nil {
		return err
	}
	if hash != cur {
		return fmt.Errorf("%w: the notebook changed since you looked at it; open it again", contract.ErrNotebookInvalid)
	}
	if err := s.policy().Trust(ctx, id.User, name, hash, s.now()); err != nil {
		return err
	}
	// Processes started under the old verdict end: the next open runs with the reader's own role.
	s.stopWhere(func(k *kernel) bool { return k.name == name && k.user == id.User }, "trusted")
	return nil
}

// attribute records the notebook files that changed since the last look (a save by marimo, or a change made
// behind its back by code in an edit sandbox, which can write every notebook, or outside Duct) as versions, with
// the author the circumstances allow: the lock holder of the file; else the only editor with a live edit process
// (it is the only one who could write); else, with several, unknownAuthor; with none, "externo".
func (s *Service) attribute(ctx context.Context, gone ...*kernel) {
	ents, err := os.ReadDir(s.o.Dir)
	if err != nil {
		return
	}
	s.mu.Lock()
	editors := map[string]bool{}
	holder := map[string]string{}
	for _, k := range s.kernels {
		if k.mode == contract.NotebookEdit && !k.stopped.Load() {
			editors[k.user] = true
			holder[k.name] = k.user
		}
	}
	s.mu.Unlock()
	for _, k := range gone { // an edit process that just ended: it could write too
		if k.mode == contract.NotebookEdit {
			editors[k.user] = true
			holder[k.name] = k.user
		}
	}
	s.stampMu.Lock()
	defer s.stampMu.Unlock()
	if s.stamps == nil {
		s.stamps = map[string]string{}
	}
	for _, e := range ents {
		name, ok := strings.CutSuffix(e.Name(), ".py")
		if !ok || !ValidName(name) || e.IsDir() {
			continue
		}
		st, err := e.Info()
		if err != nil || !st.Mode().IsRegular() {
			continue
		}
		stamp := fmt.Sprintf("%d/%d", st.ModTime().UnixNano(), st.Size())
		old, seen := s.stamps[name]
		if seen && old == stamp {
			continue
		}
		s.stamps[name] = stamp
		author := externalAuthor
		switch {
		case holder[name] != "":
			author = holder[name]
		case len(editors) == 1:
			for u := range editors {
				author = u
			}
		case len(editors) > 1:
			author = unknownAuthor
		}
		if !seen && author == externalAuthor {
			continue // first look at a file nobody is known to have touched: nothing to attribute
		}
		action := "save"
		if author == externalAuthor {
			action = "external"
		}
		if _, err := s.syncTracked(ctx, name, author, action); err != nil {
			s.stamps[name] = "" // try again on the next look
		}
	}
}

// syncTracked is sync, but a file that has no history yet is recorded only when the author is known to be a
// live editor (a file written by an editor's code), so repository notebooks keep no rows until opened for editing.
func (s *Service) syncTracked(ctx context.Context, name, author, action string) (bool, error) {
	if author == externalAuthor {
		rs, err := s.rows(ctx, `SELECT CAST(count(*) AS BIGINT) FROM duct.duct_notebook_versions WHERE notebook = ?`, name)
		if err != nil || len(rs) == 0 || atoi(rs[0][0]) == 0 {
			return false, err
		}
	}
	return s.sync(ctx, name, author, action)
}

// verdict is Evaluate for the current content of the file, after attributing pending changes.
func (s *Service) verdict(ctx context.Context, id contract.Identity, name string) (Verdict, error) {
	s.attribute(ctx)
	h, err := s.fileHash(name)
	if err != nil {
		return Verdict{}, err
	}
	return s.policy().Evaluate(ctx, id, name, h)
}
