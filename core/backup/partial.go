package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/project"
)

const scratchAlias = "duct_bk_scratch"

// TableOptions configure RestoreTables.
type TableOptions struct {
	// Tables are "schema.table" names (a bare name means schema main).
	Tables []string
	// Overwrite drops a table that already exists in the target before
	// restoring it; without it an existing table is an error.
	Overwrite bool
	// NewProject also restores the Duct state tables (users, roles, policies,
	// jobs, audit...) and the project files of the backup; leave it false when
	// restoring into a project that already lives.
	NewProject bool
	Log        func(format string, args ...any)
	Crypto     *Crypto
}

// TableResult is one restored table.
type TableResult struct {
	Schema, Name string
	Rows         int64 // rows in the target after the restore
	Want         int64 // rows in the backup at its snapshot
}

// RestoreTables restores only the named lake tables of a backup into the
// project p (opened through eng, unlocked), with their rows as of the backup's
// snapshot.
//
// How: the backup's catalog is loaded into a scratch DuckLake (a temporary
// SQLite catalog whose data path is the backup's data files; encrypted or
// S3-hosted backups are staged to a temporary folder first, only the files of
// the selected tables) and each table is recreated in the target with
// CREATE TABLE + INSERT ... SELECT through DuckLake itself. Merging catalog rows
// directly is not possible in a catalog that already has its own ids and
// snapshots, so the data is re-written as new Parquet files in one new snapshot.
//
// Limits: columns (names, types, NOT NULL) and rows are restored; partitioning,
// comments, views, macros, semantic models, grants and the table's snapshot
// history are not. Tables that depend on each other (views, policies) must be
// restored together by the caller; a restored table starts a fresh history in
// the target.
func RestoreTables(ctx context.Context, p *project.Project, eng contract.Engine, src *Source, opt TableOptions) ([]TableResult, error) {
	m := src.Manifest
	if m.Encrypted && (opt.Crypto == nil || len(opt.Crypto.Identities) == 0) {
		return nil, errors.New("restore: this backup is encrypted: pass --identity <age identity file> (or set " + project.EnvBackupIdentity + ")")
	}
	if m.CatalogOnly {
		return nil, errors.New("restore: a catalog-only backup has no data files: a partial restore needs a full one")
	}
	for _, v := range m.Migrations {
		if v > maxMigration() {
			return nil, fmt.Errorf("restore: the backup has Duct migration %d, newer than this duct knows (%d)", v, maxMigration())
		}
	}
	want, err := selectTables(m, opt.Tables)
	if err != nil {
		return nil, err
	}
	stage, err := os.MkdirTemp("", "duct-restore-tables-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(stage) }()

	ro := &RestoreOptions{Log: opt.Log, Crypto: opt.Crypto}
	r := &restore{p: p, src: src, opt: ro, s: session{eng: eng}, stage: stage}
	if p.CatalogIsPostgres() {
		dsn, err := p.CatalogDSN()
		if err != nil {
			return nil, err
		}
		if r.pg, err = pgconn.Parse(dsn); err != nil {
			return nil, err
		}
		r.s.redact = r.pg.Redact
	}
	defer r.cleanup()
	if err := r.stageParquet(ctx); err != nil {
		return nil, err
	}

	// Existing tables in the target.
	exists := map[string]bool{}
	rows, err := r.s.rows(ctx, "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' AND NOT internal")
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		exists[row[0]+"."+row[1]] = true
	}
	for _, t := range want {
		if exists[t.Schema+"."+t.Name] && !opt.Overwrite {
			return nil, fmt.Errorf("restore: table %s.%s already exists in the target (use --overwrite to replace it)", t.Schema, t.Name)
		}
	}

	dataPath, err := r.stageTableData(ctx, want)
	if err != nil {
		return nil, err
	}
	if err := r.openScratch(ctx, dataPath); err != nil {
		return nil, err
	}
	defer func() { _ = r.s.eng.Exec(context.Background(), "DETACH "+scratchAlias) }()

	var out []TableResult
	for _, t := range want {
		res, err := r.copyTable(ctx, t, exists[t.Schema+"."+t.Name])
		if err != nil {
			return out, err
		}
		out = append(out, res)
		opt.logf("table %s.%s: %d rows", t.Schema, t.Name, res.Rows)
	}
	if opt.NewProject {
		if err := r.restoreState(ctx); err != nil {
			return out, err
		}
		if err := r.restoreProjectFiles(ctx); err != nil {
			return out, err
		}
	}
	return out, nil
}

func (o *TableOptions) logf(format string, args ...any) {
	if o.Log != nil {
		o.Log(format, args...)
	}
}

// selectTables resolves "schema.table" names against the manifest.
func selectTables(m *Manifest, names []string) ([]Count, error) {
	if len(names) == 0 {
		return nil, errors.New("restore: no tables selected")
	}
	have := map[string]Count{}
	var avail []string
	for _, t := range m.Tables {
		have[t.Schema+"."+t.Name] = t
		avail = append(avail, t.Schema+"."+t.Name)
	}
	seen := map[string]bool{}
	var out []Count
	for _, n := range names {
		if !strings.Contains(n, ".") {
			n = "main." + n
		}
		t, ok := have[n]
		if !ok {
			return nil, fmt.Errorf("restore: the backup has no table %q (it has: %s)", n, strings.Join(avail, ", "))
		}
		if !seen[n] {
			seen[n] = true
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Schema+"."+out[i].Name < out[j].Schema+"."+out[j].Name })
	return out, nil
}

// stageTableData makes the data files of the selected tables readable by
// DuckDB and returns the data path to attach the scratch lake with: the backup's
// own data folder when it is a plain local backup, otherwise a staged copy.
func (r *restore) stageTableData(ctx context.Context, want []Count) (string, error) {
	m := r.src.Manifest
	q := fmt.Sprintf(`SELECT s.schema_name, t.table_name, s.path, CAST(s.path_is_relative AS BIGINT), t.path, CAST(t.path_is_relative AS BIGINT)
FROM %s t JOIN %s s USING (schema_id)
WHERE t.begin_snapshot <= %d AND (t.end_snapshot IS NULL OR t.end_snapshot > %d)
  AND s.begin_snapshot <= %d AND (s.end_snapshot IS NULL OR s.end_snapshot > %d)`,
		r.pqFile("ducklake_table"), r.pqFile("ducklake_schema"), m.SnapshotID, m.SnapshotID, m.SnapshotID, m.SnapshotID)
	rows, err := r.s.rows(ctx, q)
	if err != nil {
		return "", err
	}
	prefix := map[string]string{}
	for _, row := range rows {
		if row[3] != "1" || row[5] != "1" {
			return "", fmt.Errorf("restore: table %s.%s lives outside the lake path; not supported", row[0], row[1])
		}
		prefix[row[0]+"."+row[1]] = path.Join(row[2], row[4]) + "/"
	}
	if d, ok := r.src.Store.(*dirStore); ok && !m.Encrypted {
		return filepath.Join(d.root, "data") + string(filepath.Separator), nil
	}
	dst := NewDirStore(filepath.Join(r.stage, "lake"))
	var files []File
	for _, f := range m.Files {
		if f.Kind != "data" {
			continue
		}
		rel := strings.TrimPrefix(f.Path, "data/")
		for _, t := range want {
			if pf, ok := prefix[t.Schema+"."+t.Name]; ok && strings.HasPrefix(rel, pf) {
				files = append(files, f)
				break
			}
		}
	}
	err = parallel(ctx, 4, len(files), func(ctx context.Context, i int) error {
		f := files[i]
		rel := strings.TrimPrefix(f.Path, "data/")
		if f.Encrypted {
			return r.opt.Crypto.decryptPut(ctx, r.src.Store, f, dst, rel)
		}
		size, sha, err := copyObject(ctx, r.src.Store, f.Path, dst, rel, f.Size)
		if err != nil {
			return err
		}
		if size >= 0 && (size != f.Size || (f.SHA256 != "" && sha != f.SHA256)) {
			return fmt.Errorf("%s does not match the manifest", f.Path)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("restore: stage data files: %w", err)
	}
	return filepath.Join(r.stage, "lake") + string(filepath.Separator), nil
}

func (r *restore) pqFile(table string) string {
	return "read_parquet(" + ql(filepath.Join(r.stage, "catalog", table+".parquet")) + ")"
}

// openScratch builds the scratch DuckLake: an empty one, then its catalog
// tables replaced by the backup's, then attached read-only on dataPath.
func (r *restore) openScratch(ctx context.Context, dataPath string) error {
	r.scratch = filepath.Join(r.stage, "scratch.db")
	empty := filepath.Join(r.stage, "scratch-empty") + string(filepath.Separator)
	if err := os.MkdirAll(empty, 0o750); err != nil {
		return err
	}
	if err := r.s.exec(ctx, fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s)", ql("ducklake:sqlite:"+r.scratch), scratchAlias, ql(empty))); err != nil {
		return err
	}
	if err := r.s.exec(ctx, "DETACH "+scratchAlias); err != nil {
		return err
	}
	if err := r.restoreCatalog(ctx); err != nil { // into the scratch catalog (r.scratch)
		return err
	}
	if err := r.s.exec(ctx, "DETACH "+catAlias); err != nil {
		return err
	}
	r.attached = false
	return r.s.exec(ctx, fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, OVERRIDE_DATA_PATH true, READ_ONLY)", ql("ducklake:sqlite:"+r.scratch), scratchAlias, ql(dataPath)))
}

// copyTable recreates one table in the target from the scratch lake.
func (r *restore) copyTable(ctx context.Context, t Count, replace bool) (TableResult, error) {
	res := TableResult{Schema: t.Schema, Name: t.Name, Want: t.Rows}
	src := scratchAlias + "." + qi(t.Schema) + "." + qi(t.Name)
	dst := "lake." + qi(t.Schema) + "." + qi(t.Name)
	cols, err := r.s.rows(ctx, `SELECT column_name, column_type, "null" FROM (DESCRIBE SELECT * FROM `+src+`)`)
	if err != nil {
		return res, fmt.Errorf("restore %s.%s: %w", t.Schema, t.Name, err)
	}
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = qi(c[0]) + " " + c[1]
		if c[2] == "NO" {
			defs[i] += " NOT NULL"
		}
	}
	var stmts []string
	if replace {
		stmts = append(stmts, "DROP TABLE "+dst)
	}
	stmts = append(stmts,
		"CREATE SCHEMA IF NOT EXISTS lake."+qi(t.Schema),
		fmt.Sprintf("CREATE TABLE %s (%s)", dst, strings.Join(defs, ", ")),
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", dst, src))
	if err := r.s.tx(ctx, stmts); err != nil {
		return res, fmt.Errorf("restore %s.%s: %w", t.Schema, t.Name, err)
	}
	if res.Rows, err = r.s.count(ctx, dst); err != nil {
		return res, err
	}
	if res.Rows != t.Rows {
		return res, fmt.Errorf("restore %s.%s: %d rows restored, the backup has %d", t.Schema, t.Name, res.Rows, t.Rows)
	}
	return res, nil
}
