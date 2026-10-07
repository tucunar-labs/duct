// Package backup implements `duct backup` and `duct restore` (ADR 0013):
// a consistent copy of the DuckLake catalog, the Duct-owned tables, the project
// files and the lake data files the catalog references, with a manifest that
// carries versions, the snapshot id, row counts and checksums.
//
// Layout of one backup, under <to>/<id>/ (id is a UTC timestamp):
//
//	manifest.json           written last: a backup without it is incomplete
//	catalog/<table>.parquet every DuckLake catalog table (ducklake_*, inlined data too)
//	state/<table>.parquet   every Duct-owned table (duct_*)
//	project/<path>          duct.toml and the project files (never .env*, .duct/, lake/)
//	data/<path>             lake data and delete files, relative to the lake path
//
// Catalog and state are copied through DuckDB inside one read transaction per
// database (REPEATABLE READ on Postgres, a read snapshot on SQLite), so the
// copy is consistent with the snapshot id in the manifest, and Parquet makes it
// independent of the catalog engine. Nothing in a backup is a credential;
// password hashes and token hashes are in the state tables, so treat a backup
// as sensitive as the catalog itself.
package backup

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

// FormatVersion is the manifest format this package reads and writes.
const FormatVersion = 1

// ManifestFile is the name of the manifest inside a backup.
const ManifestFile = "manifest.json"

// Count is a table with its row count.
type Count struct {
	Schema string `json:"schema,omitempty"`
	Name   string `json:"name"`
	Rows   int64  `json:"rows"`
}

// File is one object stored in the backup.
type File struct {
	Path string `json:"path"` // relative to the backup root
	Kind string `json:"kind"` // catalog, state, project, data
	Size int64  `json:"size"`
	// SHA256 is empty for data files copied server-side inside one S3 server.
	SHA256 string `json:"sha256,omitempty"`
}

// LakeFile is a data or delete file the catalog references.
type LakeFile struct {
	Path string `json:"path"` // relative to the lake path
	Size int64  `json:"size"`
}

// Manifest describes a backup.
type Manifest struct {
	Format        int       `json:"format"`
	ID            string    `json:"id"`
	CreatedAt     time.Time `json:"created_at"`
	DuctVersion   string    `json:"duct_version"`
	DuckDBVersion string    `json:"duckdb_version"`
	DuckLake      string    `json:"ducklake_version"`
	CatalogType   string    `json:"catalog_type"` // sqlite or postgres
	Storage       string    `json:"storage"`      // lake location as configured (no secrets)
	StorageKind   string    `json:"storage_kind"` // local or s3
	CatalogOnly   bool      `json:"catalog_only"`
	SnapshotID    int64     `json:"snapshot_id"`
	SnapshotTime  string    `json:"snapshot_time"`
	Migrations    []int     `json:"migrations"`
	// Tables are the lake tables at SnapshotID with their row counts.
	Tables []Count `json:"tables"`
	// CatalogTables and StateTables are the copied metadata tables.
	CatalogTables []Count `json:"catalog_tables"`
	StateTables   []Count `json:"state_tables"`
	Files         []File  `json:"files"`
	// LakeFiles are every data and delete file referenced by the catalog,
	// copied or not (catalog-only backups rely on them being on durable storage).
	LakeFiles []LakeFile `json:"lake_files"`
	// Skipped lists project files left out (too large).
	Skipped []string `json:"skipped,omitempty"`
}

// DataBytes is the total size of the referenced lake files.
func (m *Manifest) DataBytes() int64 {
	var n int64
	for _, f := range m.LakeFiles {
		n += f.Size
	}
	return n
}

var idRe = regexp.MustCompile(`^\d{8}T\d{6}Z(-\d+)?$`)

func newID(t time.Time) string { return t.UTC().Format("20060102T150405Z") }

func parseManifest(b []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest.json: %w", err)
	}
	if m.Format != FormatVersion {
		return nil, fmt.Errorf("manifest.json: format %d is not supported by this duct (it reads format %d)", m.Format, FormatVersion)
	}
	if !idRe.MatchString(m.ID) {
		return nil, fmt.Errorf("manifest.json: bad id %q", m.ID)
	}
	for _, f := range m.Files {
		if err := safeKey(f.Path); err != nil {
			return nil, fmt.Errorf("manifest.json: %w", err)
		}
	}
	for _, f := range m.LakeFiles {
		if err := safeKey(f.Path); err != nil {
			return nil, fmt.Errorf("manifest.json: %w", err)
		}
	}
	return &m, nil
}
