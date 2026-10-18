package notebooks

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// kkey identifies a process: one per (mode, user, notebook).
type kkey struct{ mode, user, name string }

// kernel is one marimo process of one user. While starting, ready is open and
// the entry already holds the edit lock.
type kernel struct {
	key     kkey
	user    string
	name    string
	mode    string
	ready   chan struct{}
	err     error // set before ready closes
	addr    string
	pass    string
	tokenID string
	proc    Process
	started time.Time
	last    atomic.Int64 // unix nanos of the latest request through the proxy
	stopped atomic.Bool
	stamp   string // mtime and size at the last capture (janitor only)
}

func (k *kernel) session() contract.NotebookSession {
	return contract.NotebookSession{
		Mode: k.mode, Target: k.addr, BasePath: "/nb/" + k.mode + "/" + k.name,
		Auth: "Basic " + base64.StdEncoding.EncodeToString([]byte("duct:"+k.pass)),
	}
}

// Launcher starts a marimo process.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Process, string, error)
}

// LaunchSpec is what a marimo process needs. Env has DUCT_URL / DUCT_TOKEN
// (the Launcher adds only a small allow-list of the server's own environment).
type LaunchSpec struct {
	Mode     string // contract.NotebookEdit or NotebookRun
	Name     string
	File     string
	BasePath string
	Password string // marimo's --token-password: the proxy sends it, nobody else knows it
	Env      map[string]string
}

// Process is a running marimo process (and its children).
type Process interface {
	Stop() // terminates the process group; safe to call twice
	Done() <-chan struct{}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// OpenNotebook implements contract.NotebooksConsole.
func (s *Service) OpenNotebook(ctx context.Context, id contract.Identity, name string, edit bool) (contract.NotebookSession, error) {
	if _, err := s.exists(name); err != nil {
		return contract.NotebookSession{}, err
	}
	if edit {
		if err := s.requireEditor(id); err != nil {
			return contract.NotebookSession{}, err
		}
		if err := s.ensureBaseline(ctx, name); err != nil {
			return contract.NotebookSession{}, err
		}
	}
	mode, lockedBy := contract.NotebookRun, ""
	if edit {
		mode = contract.NotebookEdit
	}
	for {
		k, holder, err := s.acquire(ctx, id, name, mode)
		if err != nil {
			return contract.NotebookSession{}, err
		}
		if k == nil { // someone else edits: fall back to run mode
			mode, lockedBy = contract.NotebookRun, holder
			continue
		}
		select {
		case <-k.ready:
		case <-ctx.Done():
			return contract.NotebookSession{}, ctx.Err()
		}
		if k.err != nil {
			return contract.NotebookSession{}, k.err
		}
		if k.stopped.Load() { // it died between ready and now: try again
			continue
		}
		k.last.Store(s.now().UnixNano())
		ses := k.session()
		ses.LockedBy = lockedBy
		return ses, nil
	}
}

// acquire returns the user's kernel for (mode, name), starting it if needed.
// For edit mode held by another user it returns (nil, holder, nil).
func (s *Service) acquire(ctx context.Context, id contract.Identity, name, mode string) (*kernel, string, error) {
	key := kkey{mode, id.User, name}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, "", errors.New("notebooks: shutting down")
	}
	if mode == contract.NotebookEdit {
		for _, o := range s.kernels {
			if o.name == name && o.mode == contract.NotebookEdit && o.user != id.User {
				s.mu.Unlock()
				return nil, o.user, nil
			}
		}
	}
	if k, ok := s.kernels[key]; ok {
		s.mu.Unlock()
		return k, "", nil
	}
	if len(s.kernels) >= s.o.MaxKernels {
		s.mu.Unlock()
		return nil, "", fmt.Errorf("%w: %d notebook processes are running (max_kernels); try again when someone closes theirs", contract.ErrNotebookInvalid, s.o.MaxKernels)
	}
	k := &kernel{key: key, user: id.User, name: name, mode: mode, ready: make(chan struct{}), started: s.now(), pass: randHex(24)}
	k.last.Store(s.now().UnixNano())
	s.kernels[key] = k
	s.mu.Unlock()

	// The start outlives the request that asked for it only as long as it takes: a
	// browser that leaves does not leave a half-started process behind.
	go s.start(k, id)
	return k, "", nil
}

func (s *Service) start(k *kernel, id contract.Identity) {
	fail := func(err error) {
		k.err = err
		s.mu.Lock()
		delete(s.kernels, k.key)
		s.mu.Unlock()
		if k.tokenID != "" {
			s.revoke(k.tokenID)
		}
		close(k.ready)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	env := map[string]string{}
	if u := s.ductURL(); u != "" {
		env["DUCT_URL"] = u
	}
	if s.o.Tokens != nil {
		tok, tid, err := s.o.Tokens.CreateToken(ctx, id.User, "notebook "+k.name+" ("+k.mode+")", s.o.MaxAge+time.Minute)
		if err != nil {
			fail(fmt.Errorf("token for the notebook: %w", err))
			return
		}
		k.tokenID, env["DUCT_TOKEN"] = tid, tok
	}
	proc, addr, err := s.o.Launcher.Launch(ctx, LaunchSpec{
		Mode: k.mode, Name: k.name, File: s.path(k.name), BasePath: "/nb/" + k.mode + "/" + k.name, Password: k.pass, Env: env,
	})
	if err != nil {
		fail(err)
		return
	}
	k.proc, k.addr = proc, addr
	close(k.ready)
	go func() { // the process ending by itself frees the lock
		<-proc.Done()
		s.stop(k, "process exited")
	}()
}

func (s *Service) ductURL() string {
	if s.o.DuctURL == nil {
		return ""
	}
	return s.o.DuctURL()
}

func (s *Service) revoke(tokenID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.o.Tokens.RevokeToken(ctx, tokenID); err != nil {
		slog.Warn("notebooks: revoking the process token failed", "token", tokenID, "err", err)
	}
}

// stop ends a kernel: the process dies, its token is revoked, the lock is
// released and, for edit mode, the last save is recorded. Idempotent.
func (s *Service) stop(k *kernel, why string) {
	if !k.stopped.CompareAndSwap(false, true) {
		return
	}
	<-k.ready
	s.mu.Lock()
	if s.kernels[k.key] == k {
		delete(s.kernels, k.key)
	}
	s.mu.Unlock()
	if k.proc != nil {
		k.proc.Stop()
	}
	if k.mode == contract.NotebookEdit {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if _, err := s.sync(ctx, k.name, k.user, "save"); err != nil {
			slog.Warn("notebooks: recording the final save failed", "notebook", k.name, "err", err)
		}
		cancel()
	}
	if k.tokenID != "" && s.o.Tokens != nil {
		s.revoke(k.tokenID)
	}
	slog.Info("notebooks: process stopped", "user", k.user, "notebook", k.name, "mode", k.mode, "why", why)
}

func (s *Service) stopWhere(pred func(*kernel) bool, why string) {
	s.mu.Lock()
	var ks []*kernel
	for _, k := range s.kernels {
		if pred(k) {
			ks = append(ks, k)
		}
	}
	s.mu.Unlock()
	for _, k := range ks {
		s.stop(k, why)
	}
}

// StopNotebooks implements contract.NotebooksConsole (logout).
func (s *Service) StopNotebooks(_ context.Context, user string) {
	s.stopWhere(func(k *kernel) bool { return k.user == user }, "logout")
}

// NotebookTarget implements contract.NotebooksConsole.
func (s *Service) NotebookTarget(_ context.Context, id contract.Identity, name, mode string) (contract.NotebookSession, error) {
	if mode != contract.NotebookEdit && mode != contract.NotebookRun {
		return contract.NotebookSession{}, contract.ErrNotebookNotFound
	}
	if mode == contract.NotebookEdit {
		if err := s.requireEditor(id); err != nil {
			return contract.NotebookSession{}, err
		}
	}
	s.mu.Lock()
	k := s.kernels[kkey{mode, id.User, name}]
	s.mu.Unlock()
	if k == nil {
		return contract.NotebookSession{}, contract.ErrNotebookNotFound
	}
	select {
	case <-k.ready:
	default:
		return contract.NotebookSession{}, contract.ErrNotebookNotFound
	}
	if k.err != nil || k.stopped.Load() {
		return contract.NotebookSession{}, contract.ErrNotebookNotFound
	}
	k.last.Store(s.now().UnixNano())
	return k.session(), nil
}

// janitor ends processes that are idle or too old and records the saves of
// the files being edited.
func (s *Service) janitor(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(s.o.CaptureEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := s.now()
		s.mu.Lock()
		var ks []*kernel
		for _, k := range s.kernels {
			ks = append(ks, k)
		}
		s.mu.Unlock()
		for _, k := range ks {
			select {
			case <-k.ready:
			default:
				continue
			}
			if k.err != nil || k.stopped.Load() {
				continue
			}
			switch {
			case now.Sub(time.Unix(0, k.last.Load())) > s.o.IdleTimeout:
				s.stop(k, "idle")
			case now.Sub(k.started) > s.o.MaxAge:
				s.stop(k, "max age")
			case k.mode == contract.NotebookEdit:
				st, err := os.Stat(s.path(k.name))
				if err != nil {
					continue
				}
				stamp := fmt.Sprintf("%d/%d", st.ModTime().UnixNano(), st.Size())
				if stamp == k.stamp {
					continue
				}
				k.stamp = stamp
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if _, err := s.sync(cctx, k.name, k.user, "save"); err != nil {
					slog.Warn("notebooks: recording a save failed", "notebook", k.name, "err", err)
				}
				cancel()
			}
		}
	}
}

// Running lists the live processes (for tests and status): "mode:user:name".
func (s *Service) Running() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.kernels {
		out = append(out, k.mode+":"+k.user+":"+k.name)
	}
	return out
}
