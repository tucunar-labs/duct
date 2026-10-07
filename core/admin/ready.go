package admin

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/s3conn"
)

func randomSecret() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// Ready is the /readyz handler: the catalog answers and the storage accepts a
// write. /healthz (liveness) stays in the HTTP module and checks nothing. The
// result is cached for a few seconds so probes cannot load the backends.
type Ready struct {
	Engine  contract.Engine
	Project *project.Project
	TTL     time.Duration // default 5 s

	mu     sync.Mutex
	at     time.Time
	result map[string]string
	ok     bool
}

// Checks returns the checks and whether all passed.
func (rd *Ready) Checks(ctx context.Context) (map[string]string, bool) {
	ttl := rd.TTL
	if ttl == 0 {
		ttl = 5 * time.Second
	}
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if rd.result != nil && time.Since(rd.at) < ttl {
		return rd.result, rd.ok
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res := map[string]string{"catalog": "ok", "storage": "ok"}
	ok := true
	if err := rd.checkCatalog(ctx); err != nil {
		res["catalog"], ok = "error: "+rd.redact(err.Error()), false
	}
	if err := rd.checkStorage(ctx); err != nil {
		res["storage"], ok = "error: "+rd.redact(err.Error()), false
	}
	rd.result, rd.ok, rd.at = res, ok, time.Now()
	return res, ok
}

func (rd *Ready) redact(msg string) string {
	if rd.Project.CatalogIsPostgres() {
		if dsn, err := rd.Project.CatalogDSN(); err == nil {
			if params, err := pgconn.Parse(dsn); err == nil {
				msg = params.Redact(msg)
			}
		}
	}
	if cfg, ok, _ := rd.Project.S3(); ok {
		msg = cfg.Redact(msg)
	}
	return msg
}

func (rd *Ready) checkCatalog(ctx context.Context) error {
	if _, err := queryRows(ctx, rd.Engine, "SELECT count(*) FROM duct.duct_migrations"); err != nil {
		return err
	}
	_, err := queryRows(ctx, rd.Engine, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")
	return err
}

func (rd *Ready) checkStorage(ctx context.Context) error {
	if !rd.Project.StorageIsS3() {
		f, err := os.CreateTemp(rd.Project.DataPath(), ".duct-ready-*")
		if err != nil {
			return fmt.Errorf("lake folder not writable: %w", err)
		}
		name := f.Name()
		_ = f.Close()
		return os.Remove(name)
	}
	cfg, _, err := rd.Project.S3()
	if err != nil {
		return err
	}
	c, err := s3conn.NewClient(cfg)
	if err != nil {
		return err
	}
	bucket, prefix, err := s3conn.ParseStorage(rd.Project.Config.Storage)
	if err != nil {
		return err
	}
	raw := make([]byte, 6)
	_, _ = rand.Read(raw)
	key := ".duct-ready-" + hex.EncodeToString(raw)
	if prefix != "" {
		key = prefix + "/" + key
	}
	if err := c.PutBytes(ctx, bucket, key, []byte("ok")); err != nil {
		return fmt.Errorf("object store not writable: %w", err)
	}
	return c.Delete(ctx, bucket, key)
}

// ServeHTTP implements http.Handler: 200 {"status":"ready"} or 503.
func (rd *Ready) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	checks, ok := rd.Checks(r.Context())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	status, text := http.StatusOK, "ready"
	if !ok {
		status, text = http.StatusServiceUnavailable, "not ready"
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": text, "checks": checks})
}
