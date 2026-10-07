package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
	"github.com/alonsoburon/duct/core/s3conn"
)

// Aliases of the temporary attaches of a backup or restore.
const (
	catAlias  = "duct_bk_cat"
	catSecret = "duct_bk_secret"
)

// maxProjectFile is the largest project file a backup carries; bigger ones are
// listed in the manifest as skipped (a project folder is not a place for data).
const maxProjectFile = 64 << 20

// Options configure Run.
type Options struct {
	// To is the backup root: a directory or s3://bucket/prefix. Each backup is
	// a subfolder named after its timestamp.
	To string
	// CatalogOnly leaves the lake data files out; only valid when they live on
	// durable object storage (s3://).
	CatalogOnly bool
	// Keep, if > 0, deletes the oldest complete backups under To beyond the
	// newest Keep (this one included) once the new one is complete.
	Keep int
	// Version is the duct version recorded in the manifest.
	Version string
	// Log receives progress lines (may be nil). Never receives secrets.
	Log func(format string, args ...any)
	// Now is the clock (tests).
	Now func() time.Time
}

func (o *Options) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

// OpenDestination returns the Store for a backup root (a directory or an
// s3://bucket/prefix reached with the project's object store connection).
func OpenDestination(p *project.Project, to string) (Store, error) {
	if s3conn.Scheme(to) != "" {
		cfg, err := p.S3Connection()
		if err != nil {
			return nil, err
		}
		return NewS3Store(cfg, to)
	}
	abs, err := filepath.Abs(to)
	if err != nil {
		return nil, err
	}
	return NewDirStore(abs), nil
}

// lakeStore is the Store over the lake data path.
func lakeStore(p *project.Project) (Store, error) {
	if p.StorageIsS3() {
		cfg, _, err := p.S3()
		if err != nil {
			return nil, err
		}
		return NewS3Store(cfg, p.Config.Storage)
	}
	return NewDirStore(p.DataPath()), nil
}

// Run takes a backup of the project through eng (an unlocked engine opened on
// it) and returns the manifest. Writers may keep running: the catalog is read
// in one transaction, so the copy is consistent with SnapshotID, and the data
// files it lists are immutable. Do not run `duct gc` meanwhile (it can delete
// files the snapshot still lists; the backup then fails instead of being wrong).
func Run(ctx context.Context, p *project.Project, eng contract.Engine, opt Options) (*Manifest, error) {
	if opt.To == "" {
		return nil, errors.New("backup: no destination (--to)")
	}
	if opt.CatalogOnly && !p.StorageIsS3() {
		return nil, errors.New("backup: --catalog-only needs the lake on s3:// storage (local files are not durable by themselves)")
	}
	dst, err := OpenDestination(p, opt.To)
	if err != nil {
		return nil, err
	}
	src, err := lakeStore(p)
	if err != nil {
		return nil, err
	}
	now := time.Now
	if opt.Now != nil {
		now = opt.Now
	}

	// A destination inside the project would be walked as project files.
	if abs, err := filepath.Abs(opt.To); err == nil && s3conn.Scheme(opt.To) == "" {
		if rel, err := filepath.Rel(p.Root, abs); err == nil && !strings.HasPrefix(rel, "..") {
			return nil, errors.New("backup: the destination is inside the project folder; choose another place")
		}
	}

	b := &backup{p: p, opt: &opt, dst: dst, src: src, s: session{eng: eng}}
	if p.CatalogIsPostgres() {
		dsn, err := p.CatalogDSN()
		if err != nil {
			return nil, err
		}
		params, err := pgconn.Parse(dsn)
		if err != nil {
			return nil, err
		}
		b.pg = params
		b.s.redact = params.Redact
	}
	stage, err := os.MkdirTemp("", "duct-backup-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(stage) }()
	b.stage = stage

	id := newID(now())
	for n := 2; ; n++ { // two backups in the same second
		rc, _, err := dst.Get(ctx, id+"/"+ManifestFile)
		if err != nil {
			break
		}
		_ = rc.Close()
		if n > 99 {
			return nil, errors.New("backup: cannot pick an id")
		}
		id = newID(now()) + "-" + strconv.Itoa(n)
	}
	b.id = id
	b.m = &Manifest{
		Format: FormatVersion, ID: id, CreatedAt: now().UTC(), DuctVersion: opt.Version,
		CatalogType: "sqlite", Storage: p.Config.Storage, StorageKind: "local", CatalogOnly: opt.CatalogOnly,
	}
	if p.CatalogIsPostgres() {
		b.m.CatalogType = "postgres"
	}
	if p.StorageIsS3() {
		b.m.StorageKind = "s3"
	}
	defer b.cleanup()
	if err := b.run(ctx); err != nil {
		return nil, err
	}
	if opt.Keep > 0 {
		if err := prune(ctx, dst, opt.Keep, now(), opt.logf); err != nil {
			opt.logf("retention: %v", err)
		}
	}
	return b.m, nil
}

type backup struct {
	p     *project.Project
	opt   *Options
	dst   Store
	src   Store
	s     session
	pg    pgconn.Params
	stage string
	id    string
	m     *Manifest

	mu       sync.Mutex
	attached bool
}

func (b *backup) cleanup() {
	ctx := context.Background()
	if b.attached {
		_ = b.s.eng.Exec(ctx, "DETACH "+catAlias)
	}
	if b.pg != nil {
		_ = b.s.eng.Exec(ctx, "DROP SECRET IF EXISTS "+catSecret)
	}
}

func (b *backup) key(parts ...string) string { return joinKey(append([]string{b.id}, parts...)...) }

func (b *backup) run(ctx context.Context) error {
	if err := b.attachCatalog(ctx); err != nil {
		return err
	}
	meta, err := b.copyMetadata(ctx)
	if err != nil {
		return err
	}
	if err := b.snapshotInfo(ctx, meta); err != nil {
		return err
	}
	if err := b.referencedFiles(ctx); err != nil {
		return err
	}
	if err := b.countTables(ctx); err != nil {
		return err
	}
	b.opt.logf("snapshot %d, %d tables, %d lake files (%s)", b.m.SnapshotID, len(b.m.Tables), len(b.m.LakeFiles), humanBytes(b.m.DataBytes()))

	// Upload the staged parquet, then project files, then the data files; the
	// manifest goes last so an interrupted backup is never mistaken for a good one.
	for _, g := range []struct{ kind, dir string }{{"catalog", "catalog"}, {"state", "state"}} {
		if err := b.uploadStaged(ctx, g.kind, g.dir); err != nil {
			return err
		}
	}
	if err := b.uploadProject(ctx); err != nil {
		return err
	}
	if !b.opt.CatalogOnly {
		if err := b.copyData(ctx); err != nil {
			return err
		}
	}
	sort.Slice(b.m.Files, func(i, j int) bool { return b.m.Files[i].Path < b.m.Files[j].Path })
	raw, err := json.MarshalIndent(b.m, "", "  ")
	if err != nil {
		return err
	}
	if _, _, err := b.dst.Put(ctx, b.key(ManifestFile), strings.NewReader(string(raw)+"\n")); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// attachCatalog attaches the catalog database plainly (the metadata catalog
// DuckLake opens is hidden from table listings).
func (b *backup) attachCatalog(ctx context.Context) error {
	if b.pg != nil {
		if err := b.s.exec(ctx, b.pg.SecretSQL(catSecret)); err != nil {
			return err
		}
		if err := b.s.exec(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s, READ_ONLY)", catAlias, catSecret)); err != nil {
			return err
		}
	} else if err := b.s.exec(ctx, fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, READ_ONLY, BUSY_TIMEOUT 5000)", ql(b.p.CatalogPath()), catAlias)); err != nil {
		return err
	}
	b.attached = true
	return nil
}

func (b *backup) catalogSchema() string {
	if b.pg != nil {
		return "public"
	}
	return "main"
}

// copyMetadata writes every ducklake_* table and every duct_* table to Parquet
// in the staging folder, one read transaction per database.
func (b *backup) copyMetadata(ctx context.Context) (metaTables, error) {
	var out metaTables
	cat, err := b.s.names(ctx, fmt.Sprintf(`SELECT table_name FROM duckdb_tables() WHERE database_name = %s AND schema_name = %s AND table_name LIKE 'ducklake\_%%' ESCAPE '\' ORDER BY 1`, ql(catAlias), ql(b.catalogSchema())))
	if err != nil {
		return out, err
	}
	if len(cat) == 0 {
		return out, errors.New("backup: the catalog has no DuckLake tables (is this project initialised?)")
	}
	state, err := b.s.names(ctx, `SELECT table_name FROM duckdb_tables() WHERE database_name = 'duct' AND table_name LIKE 'duct\_%' ESCAPE '\' ORDER BY 1`)
	if err != nil {
		return out, err
	}
	if len(state) == 0 {
		return out, errors.New("backup: the project has no Duct tables (run `duct migrate`)")
	}
	out.catalog, out.state = cat, state
	for _, g := range []struct {
		dir, alias string
		tables     []string
		counts     *[]Count
	}{{"catalog", catAlias, cat, &b.m.CatalogTables}, {"state", "duct", state, &b.m.StateTables}} {
		stmts := make([]string, 0, len(g.tables))
		for _, t := range g.tables {
			stmts = append(stmts, fmt.Sprintf("COPY (SELECT * FROM %s.%s) TO %s (FORMAT parquet, COMPRESSION zstd)",
				g.alias, qi(t), ql(filepath.Join(b.stage, g.dir, t+".parquet"))))
		}
		if err := os.MkdirAll(filepath.Join(b.stage, g.dir), 0o750); err != nil {
			return out, err
		}
		if err := b.s.tx(ctx, stmts); err != nil {
			return out, fmt.Errorf("copy %s tables: %w", g.dir, err)
		}
		for _, t := range g.tables {
			n, err := b.s.count(ctx, "read_parquet("+ql(filepath.Join(b.stage, g.dir, t+".parquet"))+")")
			if err != nil {
				return out, err
			}
			*g.counts = append(*g.counts, Count{Name: t, Rows: n})
		}
	}
	return out, nil
}

type metaTables struct{ catalog, state []string }

func (b *backup) pq(table string) string {
	return "read_parquet(" + ql(filepath.Join(b.stage, "catalog", table+".parquet")) + ")"
}

// snapshotInfo fills versions, the snapshot id (from the copy itself, so it is
// the one the metadata describes) and the applied migrations.
func (b *backup) snapshotInfo(ctx context.Context, _ metaTables) error {
	r, err := b.s.rows(ctx, "SELECT snapshot_id, CAST(snapshot_time AS VARCHAR) FROM "+b.pq("ducklake_snapshot")+" ORDER BY snapshot_id DESC LIMIT 1")
	if err != nil {
		return err
	}
	if len(r) == 0 {
		return errors.New("backup: the catalog has no snapshots")
	}
	if b.m.SnapshotID, err = strconv.ParseInt(r[0][0], 10, 64); err != nil {
		return err
	}
	b.m.SnapshotTime = r[0][1]
	enc, err := b.s.names(ctx, "SELECT value FROM "+b.pq("ducklake_metadata")+" WHERE key = 'encrypted'")
	if err != nil {
		return err
	}
	if len(enc) > 0 && strings.EqualFold(enc[0], "true") {
		return errors.New("backup: encrypted lakes are not supported yet")
	}
	if v, err := b.s.names(ctx, "SELECT version()"); err == nil && len(v) > 0 {
		b.m.DuckDBVersion = v[0]
	}
	if v, err := b.s.names(ctx, "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'"); err == nil && len(v) > 0 {
		b.m.DuckLake = v[0]
	}
	mig, err := b.s.names(ctx, "SELECT CAST(version AS VARCHAR) FROM read_parquet("+ql(filepath.Join(b.stage, "state", "duct_migrations.parquet"))+") ORDER BY version")
	if err != nil {
		return err
	}
	for _, v := range mig {
		n, _ := strconv.Atoi(v)
		b.m.Migrations = append(b.m.Migrations, n)
	}
	return nil
}

// referencedFiles lists every data and delete file in the catalog copy, as a
// path relative to the lake path (data_path + schema path + table path + file).
func (b *backup) referencedFiles(ctx context.Context) error {
	q := fmt.Sprintf(`
SELECT f.path, CAST(f.path_is_relative AS BIGINT), t.path, CAST(t.path_is_relative AS BIGINT),
       s.path, CAST(s.path_is_relative AS BIGINT), COALESCE(f.file_size_bytes, 0)
FROM (SELECT table_id, path, path_is_relative, file_size_bytes FROM %s
      UNION ALL SELECT table_id, path, path_is_relative, file_size_bytes FROM %s) f
JOIN (SELECT table_id, any_value(schema_id) AS schema_id, any_value(path) AS path, any_value(path_is_relative) AS path_is_relative FROM %s GROUP BY table_id) t USING (table_id)
JOIN (SELECT schema_id, any_value(path) AS path, any_value(path_is_relative) AS path_is_relative FROM %s GROUP BY schema_id) s USING (schema_id)
ORDER BY 1`, b.pq("ducklake_data_file"), b.pq("ducklake_delete_file"), b.pq("ducklake_table"), b.pq("ducklake_schema"))
	rows, err := b.s.rows(ctx, q)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if r[1] != "1" || r[3] != "1" || r[5] != "1" {
			return fmt.Errorf("backup: the catalog references a file outside the lake path (%s); not supported", r[0])
		}
		rel := path.Join(r[4], r[2], r[0])
		if err := safeKey(rel); err != nil {
			return fmt.Errorf("backup: catalog file path: %w", err)
		}
		if seen[rel] {
			continue
		}
		seen[rel] = true
		size, _ := strconv.ParseInt(r[6], 10, 64)
		b.m.LakeFiles = append(b.m.LakeFiles, LakeFile{Path: rel, Size: size})
	}
	return nil
}

// countTables counts the rows of every lake table AT the backed-up snapshot.
func (b *backup) countTables(ctx context.Context) error {
	n := b.m.SnapshotID
	q := fmt.Sprintf(`SELECT s.schema_name, t.table_name FROM %s t JOIN %s s USING (schema_id)
WHERE t.begin_snapshot <= %d AND (t.end_snapshot IS NULL OR t.end_snapshot > %d)
  AND s.begin_snapshot <= %d AND (s.end_snapshot IS NULL OR s.end_snapshot > %d) ORDER BY 1, 2`,
		b.pq("ducklake_table"), b.pq("ducklake_schema"), n, n, n, n)
	rows, err := b.s.rows(ctx, q)
	if err != nil {
		return err
	}
	for _, r := range rows {
		c, err := b.s.count(ctx, fmt.Sprintf("lake.%s.%s AT (VERSION => %d)", qi(r[0]), qi(r[1]), n))
		if err != nil {
			return fmt.Errorf("count %s.%s at snapshot %d: %w", r[0], r[1], n, err)
		}
		b.m.Tables = append(b.m.Tables, Count{Schema: r[0], Name: r[1], Rows: c})
	}
	return nil
}

func (b *backup) addFile(f File) {
	b.mu.Lock()
	b.m.Files = append(b.m.Files, f)
	b.mu.Unlock()
}

func (b *backup) uploadStaged(ctx context.Context, kind, dir string) error {
	ents, err := os.ReadDir(filepath.Join(b.stage, dir))
	if err != nil {
		return err
	}
	for _, e := range ents {
		key := dir + "/" + e.Name()
		size, sha, err := putFile(ctx, b.dst, b.key(key), filepath.Join(b.stage, dir, e.Name()))
		if err != nil {
			return fmt.Errorf("upload %s: %w", key, err)
		}
		b.addFile(File{Path: key, Kind: kind, Size: size, SHA256: sha})
	}
	return nil
}

// skipDirs are never part of the project files.
var skipDirs = map[string]bool{"node_modules": true, "__pycache__": true, "venv": true}

func (b *backup) uploadProject(ctx context.Context) error {
	data := b.p.DataPath()
	err := filepath.WalkDir(b.p.Root, func(full string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := e.Name()
		if e.IsDir() {
			if full == b.p.Root {
				return nil
			}
			if strings.HasPrefix(name, ".") || skipDirs[name] || full == data || full == filepath.Join(b.p.Root, project.LakeDir) {
				return filepath.SkipDir
			}
			return nil
		}
		if !e.Type().IsRegular() || strings.HasPrefix(name, ".env") {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(b.p.Root, full)
		if err != nil {
			return err
		}
		key := "project/" + filepath.ToSlash(rel)
		if info.Size() > maxProjectFile {
			b.m.Skipped = append(b.m.Skipped, filepath.ToSlash(rel))
			return nil
		}
		size, sha, err := putFile(ctx, b.dst, b.key(key), full)
		if err != nil {
			return fmt.Errorf("upload %s: %w", key, err)
		}
		b.addFile(File{Path: key, Kind: "project", Size: size, SHA256: sha})
		return nil
	})
	return err
}

// copyData copies the referenced lake files, a few at a time.
func (b *backup) copyData(ctx context.Context) error {
	workers := 4
	if _, ok := b.src.(*s3Store); ok {
		workers = 8
	}
	var done, doneBytes int64
	var mu sync.Mutex
	err := parallel(ctx, workers, len(b.m.LakeFiles), func(ctx context.Context, i int) error {
		lf := b.m.LakeFiles[i]
		size, sha, err := copyObject(ctx, b.src, lf.Path, b.dst, b.key("data", lf.Path))
		if err != nil {
			return fmt.Errorf("copy lake file %s: %w", lf.Path, err)
		}
		if size < 0 { // server-side copy: trust the catalog's size
			size = lf.Size
		} else if lf.Size > 0 && size != lf.Size {
			return fmt.Errorf("lake file %s has %d bytes, the catalog says %d", lf.Path, size, lf.Size)
		}
		b.addFile(File{Path: "data/" + lf.Path, Kind: "data", Size: size, SHA256: sha})
		mu.Lock()
		done++
		doneBytes += size
		if done%100 == 0 {
			b.opt.logf("copied %d/%d lake files (%s)", done, len(b.m.LakeFiles), humanBytes(doneBytes))
		}
		mu.Unlock()
		return nil
	})
	return err
}

// parallel runs fn for 0..n-1 with at most workers at a time; the first error
// stops the rest.
func parallel(ctx context.Context, workers, n int, fn func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	idx := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				if ctx.Err() != nil {
					continue
				}
				if err := fn(ctx, i); err != nil {
					once.Do(func() { first = err; cancel() })
				}
			}
		}()
	}
	for i := range n {
		idx <- i
	}
	close(idx)
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// Entry is a backup found under a root.
type Entry struct {
	ID       string
	Complete bool
}

// List returns the backups under root, oldest first. Folders without a
// manifest are reported as incomplete.
func List(ctx context.Context, root Store) ([]Entry, error) {
	objs, err := root.List(ctx, "")
	if err != nil {
		return nil, err
	}
	byID := map[string]bool{}
	for _, o := range objs {
		id, rest, ok := strings.Cut(o.Key, "/")
		if !ok || !idRe.MatchString(id) {
			continue
		}
		if rest == ManifestFile {
			byID[id] = true
		} else if _, seen := byID[id]; !seen {
			byID[id] = false
		}
	}
	out := make([]Entry, 0, len(byID))
	for id, ok := range byID {
		out = append(out, Entry{ID: id, Complete: ok})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// prune keeps the newest keep complete backups and removes the older ones, and
// incomplete folders older than a day.
func prune(ctx context.Context, root Store, keep int, now time.Time, logf func(string, ...any)) error {
	list, err := List(ctx, root)
	if err != nil {
		return err
	}
	var complete []string
	for _, e := range list {
		if e.Complete {
			complete = append(complete, e.ID)
			continue
		}
		if t, err := time.Parse("20060102T150405Z", strings.SplitN(e.ID, "-", 2)[0]); err == nil && now.Sub(t) > 24*time.Hour {
			if err := removeBackup(ctx, root, e.ID); err != nil {
				return err
			}
			logf("removed incomplete backup %s", e.ID)
		}
	}
	for len(complete) > keep {
		if err := removeBackup(ctx, root, complete[0]); err != nil {
			return err
		}
		logf("retention: removed backup %s", complete[0])
		complete = complete[1:]
	}
	return nil
}

// removeBackup deletes a backup; the manifest goes first so a half-deleted
// backup is never taken for a good one.
func removeBackup(ctx context.Context, root Store, id string) error {
	if !idRe.MatchString(id) {
		return fmt.Errorf("bad backup id %q", id)
	}
	if err := root.Delete(ctx, id+"/"+ManifestFile); err != nil {
		return err
	}
	objs, err := root.List(ctx, id+"/")
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := root.Delete(ctx, o.Key); err != nil {
			return err
		}
	}
	return nil
}
