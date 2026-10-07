package project

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
)

// CatalogMigrateOptions configures MigrateCatalogToPostgres.
type CatalogMigrateOptions struct {
	// DSN is the target connection string, password included (a secret).
	DSN string
	// DryRun reads the source and inspects the target but writes nothing.
	DryRun bool
}

// CatalogCheck is one verified count: the source value must equal the target's.
type CatalogCheck struct {
	Name           string
	Source, Target int64
}

// CatalogMigrateReport says what was (or, in a dry run, would be) copied.
type CatalogMigrateReport struct {
	Target      string // log-safe user@host:port/database
	DryRun      bool
	Flushed     bool           // inlined rows were flushed to Parquet in the source first
	Inlined     int64          // source rows still inlined (dry run: they would be flushed)
	Checks      []CatalogCheck // counts compared after the copy (dry run: source side only)
	TargetState string         // "empty" or "initialised, empty lake"
}

// Verified reports whether every check matched (always false in a dry run).
func (r *CatalogMigrateReport) Verified() bool {
	if r.DryRun {
		return false
	}
	for _, c := range r.Checks {
		if c.Source != c.Target {
			return false
		}
	}
	return len(r.Checks) > 0
}

// Aliases of the temporary attaches used by the migration.
const (
	migSrc       = "duct_mig_src"       // source SQLite catalog, plain attach (read only)
	migDst       = "duct_mig_dst"       // target Postgres, plain attach, schema public
	migDstState  = "duct_mig_dst_state" // target Postgres, schema duct
	migDstLake   = "duct_mig_dst_lake"  // target as a DuckLake catalog (init and verification)
	migSecret    = "duct_mig_secret"
	inlinedTable = "ducklake_inlined_" // ducklake_inlined_data_<id>_<v>, ducklake_inlined_delete_<id>
)

// isInlinedData is a per-table inlined data or delete table, or the registry
// ducklake_inlined_data_tables. A migrated lake carries none: they are flushed to Parquet first and DuckLake
// recreates them on demand, with its own column types for the target.
func isInlinedData(t string) bool { return strings.HasPrefix(t, inlinedTable) }

// MigrateCatalogToPostgres copies the project's SQLite DuckLake catalog and
// the Duct-owned tables to Postgres (ADR 0001) through eng, an unlocked engine
// opened on the SQLite project with the postgres extension loaded. It
//
//  1. applies pending Duct migrations on the source and flushes inlined data
//     to Parquet (the catalog then holds no row data, only metadata),
//  2. initialises an empty DuckLake in the target (refuses a target that
//     already holds tables or snapshots),
//  3. copies every ducklake_* table and every duct_* table in one go,
//  4. re-attaches the target fresh and compares counts: catalog tables, tables
//     and rows per lake table, snapshots, data files and Duct state.
//
// It never touches duct.toml; the caller switches it only when the returned
// error is nil and Verified is true. Writers on the source must be stopped.
func (p *Project) MigrateCatalogToPostgres(ctx context.Context, eng contract.Engine, opts CatalogMigrateOptions) (rep *CatalogMigrateReport, err error) {
	if p.CatalogIsPostgres() {
		return nil, errors.New("the catalog is already Postgres")
	}
	dst, err := pgconn.Parse(opts.DSN)
	if err != nil {
		return nil, err
	}
	// Errors from DuckDB can echo the connection string.
	defer func() {
		if err != nil {
			err = errors.New(dst.Redact(err.Error()))
		}
	}()
	m := &catMigration{p: p, eng: eng, dst: dst, rep: &CatalogMigrateReport{Target: dst.Describe(), DryRun: opts.DryRun}}
	defer m.cleanup()
	if err := m.run(ctx, opts); err != nil {
		return m.rep, err
	}
	return m.rep, nil
}

type catMigration struct {
	p   *Project
	eng contract.Engine
	dst pgconn.Params
	rep *CatalogMigrateReport

	attached []string
}

func ql(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func qi(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func (m *catMigration) exec(ctx context.Context, q string) error {
	if err := m.eng.Exec(ctx, q); err != nil {
		return fmt.Errorf("%s: %w", firstTwo(q), err)
	}
	return nil
}

func firstTwo(q string) string {
	if f := strings.Fields(q); len(f) > 2 {
		return f[0] + " " + f[1] + " " + f[2]
	}
	return q
}

func (m *catMigration) attach(ctx context.Context, q, alias string) error {
	if err := m.exec(ctx, q); err != nil {
		return err
	}
	m.attached = append(m.attached, alias)
	return nil
}

func (m *catMigration) detach(alias string) {
	_ = m.eng.Exec(context.Background(), "DETACH "+alias)
	for i, a := range m.attached {
		if a == alias {
			m.attached = append(m.attached[:i], m.attached[i+1:]...)
			return
		}
	}
}

func (m *catMigration) cleanup() {
	for len(m.attached) > 0 {
		m.detach(m.attached[len(m.attached)-1])
	}
	_ = m.eng.Exec(context.Background(), "DROP SECRET IF EXISTS "+migSecret)
}

func (m *catMigration) rows(ctx context.Context, q string) ([][]string, error) {
	st, err := m.eng.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", firstTwo(q), err)
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				if rec.Column(c).IsNull(i) {
					row[c] = "\x00NULL"
				} else {
					row[c] = strings.Clone(rec.Column(c).ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	return out, st.Err()
}

func (m *catMigration) count(ctx context.Context, from string) (int64, error) {
	r, err := m.rows(ctx, "SELECT count(*) FROM "+from)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(r[0][0], 10, 64)
}

func (m *catMigration) names(ctx context.Context, q string) ([]string, error) {
	r, err := m.rows(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(r))
	for i := range r {
		out[i] = r[i][0]
	}
	return out, nil
}

func (m *catMigration) lakeOptions() string {
	o := "DATA_PATH " + ql(m.p.DataPath()) + ", META_SECRET " + migSecret
	if !strings.Contains(m.p.Config.Storage, "://") {
		o += ", OVERRIDE_DATA_PATH true"
	}
	return o
}

func (m *catMigration) run(ctx context.Context, opts CatalogMigrateOptions) error {
	// 1. Source: state tables up to date, inlined data flushed.
	if _, err := Migrate(ctx, m.eng); err != nil {
		return fmt.Errorf("source migrations: %w", err)
	}
	if err := m.exec(ctx, m.dst.SecretSQL(migSecret)); err != nil {
		return err
	}
	if err := m.attach(ctx, fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, READ_ONLY, BUSY_TIMEOUT 5000)", ql(m.p.CatalogPath()), migSrc), migSrc); err != nil {
		return err
	}
	// Target first: connectivity and emptiness, before the source is touched.
	if err := m.attach(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s)", migDst, migSecret), migDst); err != nil {
		return err
	}
	if err := m.checkTargetEmpty(ctx); err != nil {
		return err
	}
	inl, err := m.inlinedRows(ctx)
	if err != nil {
		return err
	}
	m.rep.Inlined = inl
	if !opts.DryRun && inl > 0 {
		m.detach(migSrc) // flush writes to the catalog; re-attach to see the result
		if err := m.exec(ctx, "CALL ducklake_flush_inlined_data('lake')"); err != nil {
			return err
		}
		if err := m.attach(ctx, fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, READ_ONLY, BUSY_TIMEOUT 5000)", ql(m.p.CatalogPath()), migSrc), migSrc); err != nil {
			return err
		}
		m.rep.Flushed = true
		if left, err := m.inlinedRows(ctx); err != nil {
			return err
		} else if left > 0 {
			return fmt.Errorf("%d inlined rows remain in the source after flushing", left)
		}
		m.rep.Inlined = 0
	}

	srcCat, err := m.names(ctx, fmt.Sprintf("SELECT table_name FROM duckdb_tables() WHERE database_name = %s AND table_name LIKE 'ducklake\\_%%' ESCAPE '\\' ORDER BY table_name", ql(migSrc)))
	if err != nil {
		return err
	}
	if len(srcCat) == 0 {
		return errors.New("the source has no DuckLake catalog tables")
	}
	kept := srcCat[:0]
	for _, t := range srcCat {
		if !isInlinedData(t) {
			kept = append(kept, t)
		}
	}
	srcCat = kept
	userTables, err := m.userTables(ctx, "lake")
	if err != nil {
		return err
	}
	stateTables, err := m.names(ctx, "SELECT table_name FROM duckdb_tables() WHERE database_name = 'duct' AND table_name LIKE 'duct\\_%' ESCAPE '\\' ORDER BY table_name")
	if err != nil {
		return err
	}

	if opts.DryRun {
		for _, t := range srcCat {
			n, err := m.count(ctx, migSrc+"."+qi(t))
			if err != nil {
				return err
			}
			m.rep.Checks = append(m.rep.Checks, CatalogCheck{Name: "catalog " + t, Source: n})
		}
		return m.sourceChecks(ctx, userTables, stateTables, nil)
	}

	// Create the DuckLake tables in the target (ATTACH initialises an empty
	// lake), then the Duct tables, then re-attach the plain view of Postgres
	// so the new tables are visible.
	if err := m.attach(ctx, fmt.Sprintf("ATTACH 'ducklake:postgres:' AS %s (%s)", migDstLake, m.lakeOptions()), migDstLake); err != nil {
		return err
	}
	m.detach(migDstLake)
	if err := m.exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.duct", migDst)); err != nil {
		return err
	}
	m.detach(migDst)
	if err := m.attach(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s)", migDst, migSecret), migDst); err != nil {
		return err
	}
	if err := m.attach(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s, SCHEMA 'duct')", migDstState, migSecret), migDstState); err != nil {
		return err
	}
	if _, err := MigrateAt(ctx, m.eng, migDstState); err != nil {
		return fmt.Errorf("target Duct tables: %w", err)
	}
	// One attach (both schemas) for the copy: a DuckDB transaction can write
	// to a single attached database.
	m.detach(migDstState)
	m.detach(migDst)
	if err := m.attach(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s)", migDst, migSecret), migDst); err != nil {
		return err
	}

	// 3. Copy.
	var stmts []string
	for _, t := range srcCat {
		s, err := m.copyStmts(ctx, migSrc+"."+qi(t), fmt.Sprintf("%s.public.%s", migDst, qi(t)), migDst, "public", t)
		if err != nil {
			return err
		}
		stmts = append(stmts, s...)
	}
	for _, t := range stateTables {
		if t == "duct_migrations" {
			continue
		}
		s, err := m.copyStmts(ctx, "duct."+qi(t), migDst+".duct."+qi(t), migDst, "duct", t)
		if err != nil {
			return err
		}
		stmts = append(stmts, s...)
	}
	if err := m.eng.ExecTx(ctx, stmts...); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	// 4. Verify against a fresh attach of the target.
	m.detach(migDst)
	if err := m.attach(ctx, fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s)", migDst, migSecret), migDst); err != nil {
		return err
	}
	if err := m.attach(ctx, fmt.Sprintf("ATTACH 'ducklake:postgres:' AS %s (%s)", migDstLake, m.lakeOptions()), migDstLake); err != nil {
		return err
	}
	for _, t := range srcCat {
		s, err := m.count(ctx, migSrc+"."+qi(t))
		if err != nil {
			return err
		}
		d, err := m.count(ctx, fmt.Sprintf("%s.public.%s", migDst, qi(t)))
		if err != nil {
			return err
		}
		m.rep.Checks = append(m.rep.Checks, CatalogCheck{"catalog " + t, s, d})
	}
	if err := m.sourceChecks(ctx, userTables, stateTables, func(kind, schema, table string) (int64, error) {
		switch kind {
		case "table":
			return m.count(ctx, fmt.Sprintf("%s.%s.%s", migDstLake, qi(schema), qi(table)))
		default:
			return m.count(ctx, migDst+".duct."+qi(table))
		}
	}); err != nil {
		return err
	}
	for _, c := range m.rep.Checks {
		if c.Source != c.Target {
			return fmt.Errorf("verification failed: %s has %d rows in the source and %d in Postgres (duct.toml not changed)", c.Name, c.Source, c.Target)
		}
	}
	return nil
}

// sourceChecks adds snapshots, lake tables and Duct tables counts. dst is nil
// in a dry run (target side stays 0).
func (m *catMigration) sourceChecks(ctx context.Context, userTables [][2]string, stateTables []string, dst func(kind, schema, table string) (int64, error)) error {
	snapSrc, err := m.count(ctx, "ducklake_snapshots('lake')")
	if err != nil {
		return err
	}
	var snapDst int64
	if dst != nil {
		if snapDst, err = m.count(ctx, "ducklake_snapshots("+ql(migDstLake)+")"); err != nil {
			return err
		}
	}
	m.rep.Checks = append(m.rep.Checks, CatalogCheck{"snapshots", snapSrc, snapDst})
	m.rep.Checks = append(m.rep.Checks, CatalogCheck{"lake tables", int64(len(userTables)), int64(len(userTables))})
	if dst != nil {
		got, err := m.userTables(ctx, migDstLake)
		if err != nil {
			return err
		}
		m.rep.Checks[len(m.rep.Checks)-1].Target = int64(len(got))
	}
	for _, t := range userTables {
		n, err := m.count(ctx, fmt.Sprintf("lake.%s.%s", qi(t[0]), qi(t[1])))
		if err != nil {
			return err
		}
		var d int64
		if dst != nil {
			if d, err = dst("table", t[0], t[1]); err != nil {
				return err
			}
		}
		m.rep.Checks = append(m.rep.Checks, CatalogCheck{"rows " + t[0] + "." + t[1], n, d})
	}
	for _, t := range stateTables {
		n, err := m.count(ctx, "duct."+qi(t))
		if err != nil {
			return err
		}
		var d int64
		if dst != nil {
			if d, err = dst("state", "", t); err != nil {
				return err
			}
		}
		m.rep.Checks = append(m.rep.Checks, CatalogCheck{"state " + t, n, d})
	}
	return nil
}

func (m *catMigration) userTables(ctx context.Context, catalog string) ([][2]string, error) {
	r, err := m.rows(ctx, fmt.Sprintf("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = %s AND NOT internal ORDER BY 1, 2", ql(catalog)))
	if err != nil {
		return nil, err
	}
	out := make([][2]string, len(r))
	for i := range r {
		out[i] = [2]string{r[i][0], r[i][1]}
	}
	return out, nil
}

func (m *catMigration) inlinedRows(ctx context.Context) (int64, error) {
	names, err := m.names(ctx, fmt.Sprintf("SELECT table_name FROM duckdb_tables() WHERE database_name = %s AND table_name LIKE 'ducklake\\_inlined\\_%%' ESCAPE '\\' AND table_name <> 'ducklake_inlined_data_tables'", ql(migSrc)))
	if err != nil {
		return 0, err
	}
	var total int64
	for _, t := range names {
		n, err := m.count(ctx, migSrc+"."+qi(t))
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// checkTargetEmpty refuses a target that already holds a lake with content.
func (m *catMigration) checkTargetEmpty(ctx context.Context) error {
	have, err := m.names(ctx, fmt.Sprintf("SELECT table_name FROM duckdb_tables() WHERE database_name = %s AND schema_name = 'public' AND table_name LIKE 'ducklake\\_%%' ESCAPE '\\'", ql(migDst)))
	if err != nil {
		return err
	}
	if len(have) == 0 {
		m.rep.TargetState = "empty"
		return nil
	}
	set := map[string]bool{}
	for _, h := range have {
		set[h] = true
	}
	if !set["ducklake_table"] || !set["ducklake_snapshot"] {
		return errors.New("the target database has some ducklake_* tables but is not a DuckLake catalog; use an empty database")
	}
	tables, err := m.count(ctx, migDst+".public.ducklake_table")
	if err != nil {
		return err
	}
	snaps, err := m.count(ctx, migDst+".public.ducklake_snapshot")
	if err != nil {
		return err
	}
	if tables > 0 || snaps > 1 {
		return fmt.Errorf("the target already holds a DuckLake catalog with data (%d tables, %d snapshots); refusing to overwrite it", tables, snaps)
	}
	m.rep.TargetState = "initialised, empty lake"
	return nil
}

// copyStmts replaces the content of dst with src, column by column, casting to
// the target types (SQLite stores booleans and timestamps loosely).
func (m *catMigration) copyStmts(ctx context.Context, src, dst, dstCatalog, dstSchema, table string) ([]string, error) {
	cols, err := m.rows(ctx, fmt.Sprintf("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = %s AND schema_name = %s AND table_name = %s ORDER BY column_index",
		ql(dstCatalog), ql(dstSchema), ql(table)))
	if err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("target table %s does not exist (is the source catalog a different DuckLake version?)", table)
	}
	names := make([]string, len(cols))
	sel := make([]string, len(cols))
	for i, c := range cols {
		names[i] = qi(c[0])
		sel[i] = fmt.Sprintf("CAST(%s AS %s)", qi(c[0]), c[1])
	}
	return []string{
		"DELETE FROM " + dst,
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", dst, strings.Join(names, ", "), strings.Join(sel, ", "), src),
	}, nil
}
