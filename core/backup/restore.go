package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/s3conn"
)

// Source is an opened backup.
type Source struct {
	Store    Store // rooted at the backup folder
	Manifest *Manifest
}

// OpenSource opens the backup at from (a directory or s3://bucket/prefix). It
// may be the backup folder itself (it holds manifest.json) or a root of
// backups, in which case id picks one (default: the newest complete one). An
// s3:// source is reached with the DUCT_S3_* / AWS_* environment: a project to
// read an [s3] table from does not exist yet.
func OpenSource(ctx context.Context, from, id string) (*Source, error) {
	open := func(s string) (Store, error) {
		if s3conn.Scheme(s) != "" {
			cfg, err := (&project.Project{}).S3Connection()
			if err != nil {
				return nil, err
			}
			return NewS3Store(cfg, s)
		}
		abs, err := filepath.Abs(s)
		if err != nil {
			return nil, err
		}
		return NewDirStore(abs), nil
	}
	root, err := open(from)
	if err != nil {
		return nil, err
	}
	store := root
	if raw, err := readAll(ctx, root, ManifestFile); err == nil && id == "" {
		m, err := parseManifest(raw)
		if err != nil {
			return nil, err
		}
		return &Source{Store: store, Manifest: m}, nil
	}
	list, err := List(ctx, root)
	if err != nil {
		return nil, err
	}
	pick := ""
	for _, e := range list {
		if !e.Complete {
			continue
		}
		if id == "" || e.ID == id {
			pick = e.ID // list is oldest first: the last match is the newest
		}
	}
	if pick == "" {
		if id != "" {
			return nil, fmt.Errorf("restore: no complete backup %q under %s", id, root.Describe())
		}
		return nil, fmt.Errorf("restore: no complete backup under %s", root.Describe())
	}
	sub := strings.TrimSuffix(from, "/") + "/" + pick
	if store, err = open(sub); err != nil {
		return nil, err
	}
	raw, err := readAll(ctx, store, ManifestFile)
	if err != nil {
		return nil, fmt.Errorf("restore: read manifest of %s: %w", pick, err)
	}
	m, err := parseManifest(raw)
	if err != nil {
		return nil, err
	}
	return &Source{Store: store, Manifest: m}, nil
}

// ConfigFile returns the duct.toml stored in the backup (decrypted with c if
// the backup is encrypted).
func (s *Source) ConfigFile(ctx context.Context, c *Crypto) ([]byte, error) {
	name := "project/" + project.ConfigFile
	for _, f := range s.Manifest.Files {
		if f.Path != name {
			continue
		}
		if !f.Encrypted {
			break
		}
		tmp, err := os.MkdirTemp("", "duct-cfg-*")
		if err != nil {
			return nil, err
		}
		defer func() { _ = os.RemoveAll(tmp) }()
		if err := c.fetchFile(ctx, s.Store, f, filepath.Join(tmp, "duct.toml")); err != nil {
			return nil, err
		}
		return os.ReadFile(filepath.Join(tmp, "duct.toml")) //nolint:gosec // our own temp file
	}
	return readAll(ctx, s.Store, name)
}

// RestoreOptions configure Restore.
type RestoreOptions struct {
	Log func(format string, args ...any)
	// Crypto carries the identities that open an encrypted backup (see
	// Crypto.Identities); nil for plain backups.
	Crypto *Crypto
}

func (o *RestoreOptions) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

// fixed keys of ducklake_metadata that belong to the target, not the backup.
// ('encrypted' comes from the backup: the file keys in the catalog only make
// sense in a lake that is marked encrypted.)
const keepMetadata = "('data_path', 'version', 'created_by')"

// Restore writes the backup into the project p, which must be freshly
// initialised (an empty lake, the Duct tables migrated) and opened through eng
// (unlocked). It copies the data files first, then replaces the catalog and the
// Duct tables, then the project files. Close eng afterwards and call Verify
// with a new engine: DuckLake caches the catalog.
func Restore(ctx context.Context, p *project.Project, eng contract.Engine, src *Source, opt RestoreOptions) error {
	m := src.Manifest
	if m.Encrypted && (opt.Crypto == nil || len(opt.Crypto.Identities) == 0) {
		return errors.New("restore: this backup is encrypted: pass --identity <age identity file> (or set " + project.EnvBackupIdentity + ")")
	}
	for _, v := range m.Migrations {
		if v > maxMigration() {
			return fmt.Errorf("restore: the backup has Duct migration %d, newer than this duct knows (%d); use a newer duct", v, maxMigration())
		}
	}
	if m.CatalogOnly && !p.StorageIsS3() {
		return errors.New("restore: this is a catalog-only backup: its lake data must already be on s3:// storage (pass --storage s3://bucket/prefix)")
	}
	r := &restore{p: p, src: src, opt: &opt, s: session{eng: eng}}
	if p.CatalogIsPostgres() {
		dsn, err := p.CatalogDSN()
		if err != nil {
			return err
		}
		if r.pg, err = pgconn.Parse(dsn); err != nil {
			return err
		}
		r.s.redact = r.pg.Redact
	}
	stage, err := os.MkdirTemp("", "duct-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stage) }()
	r.stage = stage
	defer r.cleanup()
	return r.run(ctx)
}

func maxMigration() int {
	n := 0
	for _, m := range project.Migrations {
		n = max(n, m.Version)
	}
	return n
}

type restore struct {
	p     *project.Project
	src   *Source
	opt   *RestoreOptions
	s     session
	pg    pgconn.Params
	stage string
	// scratch, if set, is a SQLite catalog file that restoreCatalog fills
	// instead of the project's catalog (partial restores).
	scratch string

	attached bool
}

func (r *restore) cleanup() {
	ctx := context.Background()
	if r.attached {
		_ = r.s.eng.Exec(ctx, "DETACH "+catAlias)
	}
	if r.pg != nil {
		_ = r.s.eng.Exec(ctx, "DROP SECRET IF EXISTS "+catSecret)
	}
}

func (r *restore) run(ctx context.Context) error {
	m := r.src.Manifest
	if err := r.checkTargetEmpty(ctx); err != nil {
		return err
	}
	if err := r.stageParquet(ctx); err != nil {
		return err
	}
	if err := r.restoreData(ctx); err != nil {
		return err
	}
	if err := r.restoreCatalog(ctx); err != nil {
		return err
	}
	if err := r.restoreState(ctx); err != nil {
		return err
	}
	if err := r.restoreProjectFiles(ctx); err != nil {
		return err
	}
	r.opt.logf("restored snapshot %d (%d tables, %d lake files) from backup %s", m.SnapshotID, len(m.Tables), len(m.LakeFiles), m.ID)
	return nil
}

// checkTargetEmpty refuses a lake that already has tables or history.
func (r *restore) checkTargetEmpty(ctx context.Context) error {
	n, err := r.s.int(ctx, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake'")
	if err != nil {
		return err
	}
	snaps, err := r.s.count(ctx, "ducklake_snapshots('lake')")
	if err != nil {
		return err
	}
	if n > 0 || snaps > 1 {
		return fmt.Errorf("restore: the target lake is not empty (%d tables, %d snapshots); restore into a new project or an empty catalog database", n, snaps)
	}
	return nil
}

func (r *restore) stageParquet(ctx context.Context) error {
	for _, f := range r.src.Manifest.Files {
		if f.Kind != "catalog" && f.Kind != "state" {
			continue
		}
		if err := r.opt.Crypto.fetchFile(ctx, r.src.Store, f, filepath.Join(r.stage, filepath.FromSlash(f.Path))); err != nil {
			return fmt.Errorf("restore: read %s: %w", f.Path, err)
		}
	}
	return nil
}

// lakeTarget is the Store over the target lake.
func (r *restore) lakeTarget() (Store, error) { return lakeStore(r.p) }

func (r *restore) restoreData(ctx context.Context) error {
	m := r.src.Manifest
	dst, err := r.lakeTarget()
	if err != nil {
		return err
	}
	have := map[string]int64{}
	if objs, err := dst.List(ctx, ""); err == nil {
		for _, o := range objs {
			have[o.Key] = o.Size
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("restore: list the target lake: %w", err)
	}
	if m.CatalogOnly {
		var missing []string
		for _, f := range m.LakeFiles {
			if sz, ok := have[f.Path]; !ok || (f.Size > 0 && sz != f.Size) {
				missing = append(missing, f.Path)
			}
		}
		if len(missing) > 0 {
			if len(missing) > 5 {
				missing = append(missing[:5], fmt.Sprintf("... and %d more", len(missing)-5))
			}
			return fmt.Errorf("restore: the lake data of this catalog-only backup is not at %s: %s", r.p.Config.Storage, strings.Join(missing, ", "))
		}
		r.opt.logf("catalog-only backup: %d lake files found in place", len(m.LakeFiles))
		return nil
	}
	var data []File
	for _, f := range m.Files {
		if f.Kind == "data" {
			data = append(data, f)
		}
	}
	workers := 4
	if _, ok := dst.(*s3Store); ok {
		workers = 8
	}
	return parallel(ctx, workers, len(data), func(ctx context.Context, i int) error {
		f := data[i]
		rel := strings.TrimPrefix(f.Path, "data/")
		if sz, ok := have[rel]; ok && sz == f.Size {
			return nil // already there (restoring onto the lake that wrote it)
		}
		if f.Encrypted {
			if err := r.opt.Crypto.decryptPut(ctx, r.src.Store, f, dst, rel); err != nil {
				return fmt.Errorf("restore: copy %s: %w", rel, err)
			}
			return nil
		}
		size, sha, err := copyObject(ctx, r.src.Store, f.Path, dst, rel, f.Size)
		if err != nil {
			return fmt.Errorf("restore: copy %s: %w", rel, err)
		}
		if size >= 0 && (size != f.Size || (f.SHA256 != "" && sha != f.SHA256)) {
			return fmt.Errorf("restore: %s does not match the manifest (size %d, want %d)", rel, size, f.Size)
		}
		return nil
	})
}

func (r *restore) attachCatalog(ctx context.Context) error {
	if r.scratch != "" {
		if err := r.s.exec(ctx, fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, BUSY_TIMEOUT 5000)", ql(r.scratch), catAlias)); err != nil {
			return err
		}
	} else if r.pg != nil {
		if err := r.s.exec(ctx, r.pg.SecretSQL(catSecret)); err != nil {
			return err
		}
		if err := r.s.exec(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s)", catAlias, catSecret)); err != nil {
			return err
		}
	} else if err := r.s.exec(ctx, fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, BUSY_TIMEOUT 5000)", ql(r.p.CatalogPath()), catAlias)); err != nil {
		return err
	}
	r.attached = true
	return nil
}

// qualified is the name of a table of the attached catalog.
func (r *restore) qualified(t string) string {
	if r.pg != nil && r.scratch == "" {
		return catAlias + ".public." + qi(t)
	}
	return catAlias + "." + qi(t)
}

func (r *restore) restoreCatalog(ctx context.Context) error {
	if err := r.attachCatalog(ctx); err != nil {
		return err
	}
	schema := "main"
	if r.pg != nil && r.scratch == "" {
		schema = "public"
	}
	var stmts []string
	for _, t := range r.src.Manifest.CatalogTables {
		file := filepath.Join(r.stage, "catalog", t.Name+".parquet")
		s, err := r.tableStmts(ctx, catAlias, schema, r.qualified(t.Name), t.Name, file, t.Name == "ducklake_metadata")
		if err != nil {
			return err
		}
		stmts = append(stmts, s...)
	}
	if err := r.s.tx(ctx, stmts); err != nil {
		return fmt.Errorf("restore catalog: %w", err)
	}
	return nil
}

func (r *restore) restoreState(ctx context.Context) error {
	var stmts []string
	for _, t := range r.src.Manifest.StateTables {
		file := filepath.Join(r.stage, "state", t.Name+".parquet")
		s, err := r.tableStmts(ctx, "duct", "", "duct."+qi(t.Name), t.Name, file, false)
		if err != nil {
			return err
		}
		stmts = append(stmts, s...)
	}
	if err := r.s.tx(ctx, stmts); err != nil {
		return fmt.Errorf("restore Duct tables: %w", err)
	}
	// Bring the schema up to this binary's migrations (a backup from an older duct).
	if _, err := project.Migrate(ctx, r.s.eng); err != nil {
		return fmt.Errorf("restore: migrate: %w", err)
	}
	return nil
}

// tableStmts replaces the content of dst with the Parquet file: the table's own
// column types are the target of every cast, columns the file lacks become
// NULL. A table the target does not have yet (a DuckLake inlined-data table)
// is created from the file. keepTarget keeps the target's own ducklake_metadata
// keys (data path, version).
func (r *restore) tableStmts(ctx context.Context, db, schema, dst, table, file string, metadata bool) ([]string, error) {
	q := fmt.Sprintf("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = %s AND table_name = %s", ql(db), ql(table))
	if schema != "" {
		q += " AND schema_name = " + ql(schema)
	}
	cols, err := r.s.rows(ctx, q+" ORDER BY column_index")
	if err != nil {
		return nil, err
	}
	pqCols, err := r.s.names(ctx, "SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet("+ql(file)+"))")
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return []string{fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet(%s)", dst, ql(file))}, nil
	}
	have := map[string]bool{}
	for _, c := range pqCols {
		have[c] = true
	}
	names := make([]string, len(cols))
	sel := make([]string, len(cols))
	for i, c := range cols {
		names[i] = qi(c[0])
		if have[c[0]] {
			sel[i] = fmt.Sprintf("CAST(%s AS %s)", qi(c[0]), c[1])
		} else {
			sel[i] = fmt.Sprintf("CAST(NULL AS %s)", c[1])
		}
	}
	del, where := "DELETE FROM "+dst, ""
	if metadata {
		del += ` WHERE "key" NOT IN ` + keepMetadata
		where = ` WHERE "key" NOT IN ` + keepMetadata
	}
	return []string{
		del,
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet(%s)%s", dst, strings.Join(names, ", "), strings.Join(sel, ", "), ql(file), where),
	}, nil
}

func (r *restore) restoreProjectFiles(ctx context.Context) error {
	for _, f := range r.src.Manifest.Files {
		if f.Kind != "project" {
			continue
		}
		rel := strings.TrimPrefix(f.Path, "project/")
		if rel == project.ConfigFile {
			continue // written by the caller, with the target's own catalog and storage
		}
		dst := filepath.Join(r.p.Root, filepath.FromSlash(rel))
		if err := r.opt.Crypto.fetchFile(ctx, r.src.Store, f, dst); err != nil {
			return fmt.Errorf("restore: %s: %w", rel, err)
		}
	}
	return nil
}
