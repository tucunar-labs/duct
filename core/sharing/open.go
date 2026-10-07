package sharing

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/s3conn"
)

// StorageSpec says where the copies live.
type StorageSpec struct {
	// Location is a local folder or s3://bucket/prefix. It must NOT be inside
	// the lake's data path (DuckLake's orphan cleanup would delete the copies).
	Location string
	// S3 is the object-store connection for s3:// locations.
	S3 *s3conn.Config
	// S3PublicEndpoint (host[:port]) is how recipients reach the object store
	// when that differs from S3.Endpoint.
	S3PublicEndpoint string
	// BaseURL is the public URL of the REST prefix, for local folders.
	BaseURL string
	// SigningKey signs local file URLs (not needed to write or delete).
	SigningKey []byte
}

// OpenStorage opens the storage a spec describes.
func OpenStorage(spec StorageSpec) (Storage, error) {
	if s3conn.Scheme(spec.Location) == "s3" {
		if spec.S3 == nil {
			return nil, errors.New("sharing: s3:// storage needs the S3 connection")
		}
		prov, err := s3conn.NewProvider(*spec.S3, spec.Location, false)
		if err != nil {
			return nil, err
		}
		return NewS3Storage(spec.Location, prov, S3Options{PublicEndpoint: spec.S3PublicEndpoint})
	}
	if strings.Contains(spec.Location, "://") {
		return nil, fmt.Errorf("sharing: unsupported storage %q (use a folder or s3://bucket/prefix)", spec.Location)
	}
	root, err := filepath.Abs(spec.Location)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	l := &LocalStorage{Root: root, BaseURL: spec.BaseURL}
	if len(spec.SigningKey) > 0 {
		if l.Signer, err = NewSigner(spec.SigningKey); err != nil {
			return nil, err
		}
	}
	return l, nil
}

// Overlaps reports whether the sharing location is inside (or contains) the
// lake data path; both being local folders or s3:// URLs.
func Overlaps(location, dataPath string) bool {
	norm := func(s string) string {
		if !strings.Contains(s, "://") {
			if a, err := filepath.Abs(s); err == nil {
				s = a
			}
		}
		return strings.TrimSuffix(s, "/") + "/"
	}
	a, b := norm(location), norm(dataPath)
	return strings.HasPrefix(a, b) || strings.HasPrefix(b, a)
}

// EngineUser is an OPTIONAL interface of a Protocol that needs the engine with
// the Duct tables besides the QueryHandler (the sharing server keeps its model
// and the state of its copies there). `duct serve` calls UseEngine before Serve.
type EngineUser interface {
	UseEngine(eng contract.Engine)
}
