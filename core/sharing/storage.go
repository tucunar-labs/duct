package sharing

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Object is one stored file.
type Object struct {
	Key      string // relative to the storage root, slash separated
	Size     int64
	Modified time.Time
}

// Storage is where the materialized copies live. It is never the lake's own
// data path (ducklake_delete_orphaned_files would delete them) and holds only
// copies made for recipients, never base data files.
type Storage interface {
	// Put stores size bytes from src under key, atomically.
	Put(ctx context.Context, key string, src io.Reader, size int64) error
	// List returns every object whose key starts with prefix.
	List(ctx context.Context, prefix string) ([]Object, error)
	// Delete removes the given keys (missing ones are fine).
	Delete(ctx context.Context, keys []string) error
	// URL returns a URL that downloads key and stops working after ttl.
	URL(ctx context.Context, key string, ttl time.Duration) (string, error)
}

// validKey rejects keys that could leave the storage root.
func validKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.ContainsAny(key, "\\\x00") {
		return fmt.Errorf("sharing: invalid storage key %q", key)
	}
	for _, p := range strings.Split(key, "/") {
		if p == "" || p == "." || p == ".." {
			return fmt.Errorf("sharing: invalid storage key %q", key)
		}
	}
	return nil
}

// DeletePrefix removes every object under prefix.
func DeletePrefix(ctx context.Context, st Storage, prefix string) error {
	objs, err := st.List(ctx, strings.TrimSuffix(prefix, "/")+"/")
	if err != nil {
		return err
	}
	keys := make([]string, len(objs))
	for i, o := range objs {
		keys[i] = o.Key
	}
	return st.Delete(ctx, keys)
}

// ---- signed URLs for local storage ----

// Signer signs and checks the expiring URLs of a LocalStorage.
type Signer struct{ key []byte }

// NewSigner returns a Signer with the given secret (at least 16 bytes).
func NewSigner(key []byte) (*Signer, error) {
	if len(key) < 16 {
		return nil, errors.New("sharing: signing key too short")
	}
	return &Signer{key: key}, nil
}

// LoadOrCreateKey reads a signing key file, creating it (mode 600) with
// random bytes if it does not exist.
func LoadOrCreateKey(file string) ([]byte, error) {
	if b, err := os.ReadFile(file); err == nil {
		if k, err := hex.DecodeString(strings.TrimSpace(string(b))); err == nil && len(k) >= 16 {
			return k, nil
		}
		return nil, fmt.Errorf("sharing: %s is not a valid signing key", file)
	}
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(file, []byte(hex.EncodeToString(k)+"\n"), 0o600); err != nil {
		return nil, err
	}
	return k, nil
}

func (s *Signer) mac(key string, exp int64) string {
	m := hmac.New(sha256.New, s.key)
	_, _ = io.WriteString(m, key+"\n"+strconv.FormatInt(exp, 10))
	return hex.EncodeToString(m.Sum(nil))
}

// Sign returns the exp and sig query values for key.
func (s *Signer) Sign(key string, exp time.Time) (expUnix, sig string) {
	e := exp.Unix()
	return strconv.FormatInt(e, 10), s.mac(key, e)
}

// Verify checks a signature and the expiry.
func (s *Signer) Verify(key, expUnix, sig string, now time.Time) error {
	e, err := strconv.ParseInt(expUnix, 10, 64)
	if err != nil {
		return errors.New("bad expiry")
	}
	if !hmac.Equal([]byte(s.mac(key, e)), []byte(sig)) {
		return errors.New("bad signature")
	}
	if now.Unix() > e {
		return errors.New("url expired")
	}
	return nil
}

// ---- local folder ----

// FilesPath is the URL path (under the REST prefix) where LocalStorage files are served.
const FilesPath = "/_files/"

// LocalStorage keeps the copies in a folder and serves them through the
// sharing server, with HMAC-signed URLs that expire.
type LocalStorage struct {
	Root string
	// BaseURL is the public URL of the REST prefix, e.g.
	// https://host:8124/delta-sharing (no trailing slash).
	BaseURL string
	Signer  *Signer
}

var _ Storage = (*LocalStorage)(nil)

func (l *LocalStorage) path(key string) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	return filepath.Join(l.Root, filepath.FromSlash(key)), nil
}

// Put implements Storage.
func (l *LocalStorage) Put(ctx context.Context, key string, src io.Reader, _ int64) error {
	dst, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".part-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := io.Copy(tmp, &ctxReader{ctx, src}); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o640); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), dst)
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// Open opens a stored file for serving.
func (l *LocalStorage) Open(key string) (*os.File, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
}

// List implements Storage.
func (l *LocalStorage) List(_ context.Context, prefix string) ([]Object, error) {
	var out []Object
	root := filepath.Clean(l.Root)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil //nolint:nilerr // vanished while listing
		}
		out = append(out, Object{Key: key, Size: info.Size(), Modified: info.ModTime()})
		return nil
	})
	return out, err
}

// Delete implements Storage, and removes folders that become empty.
func (l *LocalStorage) Delete(_ context.Context, keys []string) error {
	root := filepath.Clean(l.Root)
	for _, k := range keys {
		p, err := l.path(k)
		if err != nil {
			return err
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		for d := filepath.Dir(p); d != root && strings.HasPrefix(d, root); d = filepath.Dir(d) {
			if os.Remove(d) != nil { // not empty
				break
			}
		}
	}
	return nil
}

// URL implements Storage.
func (l *LocalStorage) URL(_ context.Context, key string, ttl time.Duration) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	if l.Signer == nil || l.BaseURL == "" {
		return "", errors.New("sharing: local storage has no signer or public URL")
	}
	exp, sig := l.Signer.Sign(key, time.Now().Add(ttl))
	u := strings.TrimSuffix(l.BaseURL, "/") + FilesPath + (&url.URL{Path: key}).EscapedPath()
	return u + "?exp=" + exp + "&sig=" + sig, nil
}

// KeyFromPath validates the URL path of a file request (the part after
// FilesPath) as a storage key. Non-canonical paths (.., //, .) are refused,
// never normalised.
func KeyFromPath(p string) (string, bool) {
	if p == "" || validKey(p) != nil || path.Clean(p) != p {
		return "", false
	}
	return p, true
}
