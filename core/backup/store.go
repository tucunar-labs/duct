package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/alonsoburon/duct/core/s3conn"
)

// Object is one stored object; Key is relative to the store root and uses "/".
type Object struct {
	Key  string
	Size int64
}

// Store is where backups live and where lake files are read and written: a
// directory or an S3 prefix. Keys are slash separated and relative.
type Store interface {
	// Describe is a log-safe description (never credentials).
	Describe() string
	// Put streams r into key and returns its size and hex SHA-256.
	Put(ctx context.Context, key string, r io.Reader) (size int64, sha string, err error)
	// Get opens key; the caller closes it.
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	// List returns every object under prefix, sorted by key.
	List(ctx context.Context, prefix string) ([]Object, error)
	// Delete removes one object (no error if it is missing).
	Delete(ctx context.Context, key string) error
}

// safeKey rejects keys that could leave the store (a manifest is input).
func safeKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\\") || strings.ContainsRune(key, 0) {
		return fmt.Errorf("unsafe object path %q", key)
	}
	for _, part := range strings.Split(key, "/") {
		if part == ".." || part == "" || part == "." {
			return fmt.Errorf("unsafe object path %q", key)
		}
	}
	return nil
}

// ---- directory ----

type dirStore struct{ root string }

// NewDirStore is a Store over a local directory.
func NewDirStore(root string) Store { return &dirStore{root: root} }

func (d *dirStore) Describe() string { return d.root }

func (d *dirStore) path(key string) (string, error) {
	if err := safeKey(key); err != nil {
		return "", err
	}
	return filepath.Join(d.root, filepath.FromSlash(key)), nil
}

func (d *dirStore) Put(_ context.Context, key string, r io.Reader) (int64, string, error) {
	p, err := d.path(key)
	if err != nil {
		return 0, "", err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return 0, "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".duct-tmp-*")
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op once renamed
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		_ = tmp.Close()
		return 0, "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return 0, "", err
	}
	if err := tmp.Close(); err != nil {
		return 0, "", err
	}
	if err := os.Chmod(tmp.Name(), 0o640); err != nil {
		return 0, "", err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

func (d *dirStore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(p) //nolint:gosec // key validated by safeKey
	if err != nil {
		return nil, 0, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, st.Size(), nil
}

func (d *dirStore) List(_ context.Context, prefix string) ([]Object, error) {
	var out []Object
	err := filepath.WalkDir(d.root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if e.IsDir() || !e.Type().IsRegular() || strings.HasPrefix(e.Name(), ".duct-tmp-") {
			return nil
		}
		rel, err := filepath.Rel(d.root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			info, err := e.Info()
			if err != nil {
				return err
			}
			out = append(out, Object{Key: key, Size: info.Size()})
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, err
}

func (d *dirStore) Delete(_ context.Context, key string) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	// Drop directories left empty, up to the root.
	for dir := filepath.Dir(p); dir != d.root && strings.HasPrefix(dir, d.root); dir = filepath.Dir(dir) {
		if os.Remove(dir) != nil {
			break
		}
	}
	return nil
}

// ---- S3 ----

type s3Store struct {
	c      *s3conn.Client
	bucket string
	prefix string // no leading or trailing slash
}

// NewS3Store is a Store over s3://bucket/prefix reached with cfg.
func NewS3Store(cfg s3conn.Config, url string) (Store, error) {
	bucket, prefix, err := s3conn.ParseStorage(url)
	if err != nil {
		return nil, err
	}
	c, err := s3conn.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &s3Store{c: c, bucket: bucket, prefix: prefix}, nil
}

func (s *s3Store) Describe() string {
	if s.prefix == "" {
		return "s3://" + s.bucket
	}
	return "s3://" + s.bucket + "/" + s.prefix
}

func (s *s3Store) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return s.prefix + "/" + k
}

// Put spools the stream to a temporary file (the payload hash is signed), then
// uploads it.
func (s *s3Store) Put(ctx context.Context, key string, r io.Reader) (int64, string, error) {
	if err := safeKey(key); err != nil {
		return 0, "", err
	}
	tmp, err := os.CreateTemp("", "duct-s3-put-*")
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		return 0, "", err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if err := s.c.Put(ctx, s.bucket, s.key(key), tmp, n, sum); err != nil {
		return 0, "", err
	}
	return n, sum, nil
}

func (s *s3Store) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if err := safeKey(key); err != nil {
		return nil, 0, err
	}
	return s.c.Get(ctx, s.bucket, s.key(key))
}

func (s *s3Store) List(ctx context.Context, prefix string) ([]Object, error) {
	full := s.key(prefix)
	objs, err := s.c.List(ctx, s.bucket, full)
	if err != nil {
		return nil, err
	}
	strip := ""
	if s.prefix != "" {
		strip = s.prefix + "/"
	}
	out := make([]Object, 0, len(objs))
	for _, o := range objs {
		k := strings.TrimPrefix(o.Key, strip)
		if k == o.Key && strip != "" {
			continue
		}
		out = append(out, Object{Key: k, Size: o.Size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *s3Store) Delete(ctx context.Context, key string) error {
	if err := safeKey(key); err != nil {
		return err
	}
	return s.c.Delete(ctx, s.bucket, s.key(key))
}

// ---- copying between stores ----

// copyObject copies src/srcKey to dst/dstKey. Inside one S3 server it is a
// server-side copy (no data through this process; the checksum is then unknown
// and returned empty). Otherwise the bytes stream through and the SHA-256 is
// computed on the way. sizeHint (the object's length, 0 if unknown) picks a
// multipart copy above 5 GiB.
func copyObject(ctx context.Context, src Store, srcKey string, dst Store, dstKey string, sizeHint int64) (size int64, sha string, err error) {
	if a, ok := src.(*s3Store); ok {
		if b, ok := dst.(*s3Store); ok && a.c.SameServer(b.c) {
			if err := safeKey(srcKey); err != nil {
				return 0, "", err
			}
			if err := safeKey(dstKey); err != nil {
				return 0, "", err
			}
			if err := a.c.CopySized(ctx, a.bucket, a.key(srcKey), b.bucket, b.key(dstKey), sizeHint); err != nil {
				return 0, "", err
			}
			return -1, "", nil
		}
	}
	rc, _, err := src.Get(ctx, srcKey)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = rc.Close() }()
	return dst.Put(ctx, dstKey, rc)
}

// putFile uploads a local file.
func putFile(ctx context.Context, dst Store, key, local string) (int64, string, error) {
	f, err := os.Open(local) //nolint:gosec // path built by this package
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = f.Close() }()
	return dst.Put(ctx, key, f)
}

// getFile downloads key to a local file and returns its SHA-256.
func getFile(ctx context.Context, src Store, key, local string) (string, error) {
	rc, _, err := src.Get(ctx, key)
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	h := sha256.New()
	if err := writeLocal(local, io.TeeReader(rc, h)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// writeLocal writes r to a local file, creating its folders.
func writeLocal(local string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(local), 0o750); err != nil {
		return err
	}
	f, err := os.Create(local) //nolint:gosec // path built by this package
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// readAll reads a small object.
func readAll(ctx context.Context, s Store, key string) ([]byte, error) {
	rc, _, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(io.LimitReader(rc, 64<<20))
}

// joinKey joins store keys.
func joinKey(parts ...string) string { return path.Join(parts...) }
