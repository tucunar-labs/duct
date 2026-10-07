// Package engineduckdb is the DuckDB implementation of contract.Engine. It is
// the only package in Duct that links cgo (duckdb-go); build with
// -tags duckdb_arrow (the justfile and mise do it through GOFLAGS).
package engineduckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/registry"
	"github.com/alonsoburon/duct/core/s3conn"
)

// Catalog names fixed by the Engine contract.
const (
	lakeCatalog  = "lake"
	stateCatalog = "duct"
)

const defaultBusyTimeoutMS = 5000

// extensions loaded explicitly at Init. They are installed first (a no-op when
// already cached); in locked mode nothing can be installed or loaded later.
var extensions = []string{"ducklake", "sqlite_scanner"}

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindEngine,
		Name: "duckdb",
		New:  func() contract.Module { return &Engine{} },
	})
}

// Engine is a DuckDB database with a DuckLake catalog attached.
type Engine struct {
	mu     sync.Mutex
	db     *sql.DB
	closed bool
	stop   context.CancelFunc // stops the STS credential refresher
	done   chan struct{}
}

var _ contract.Engine = (*Engine)(nil)

// Name implements contract.Module.
func (*Engine) Name() string { return "duckdb" }

type config struct {
	catalog   string // DuckLake metadata: SQLite file (empty when pgCatalog is set)
	dataPath  string // local directory or URL for Parquet files
	state     string // SQLite file with Duct-owned tables (optional)
	locked    bool
	busyMS    int
	localData bool

	// s3:// data_path (ADR 0001): connection and credentials of the object store.
	s3       s3conn.Config
	s3Scope  string // allowed_directories entry: s3://bucket/prefix/
	provider *s3conn.Provider

	// Postgres catalog (ADR 0001): DuckLake metadata in the public schema and
	// the Duct-owned tables in stateSchema of the same (or another) database.
	pgCatalog   pgconn.Params
	pgState     pgconn.Params
	stateSchema string
	extra       []string // extra extensions to INSTALL + LOAD (e.g. postgres for catalog migration)
}

const defaultStateSchema = "duct"

func parseConfig(cfg contract.Config) (config, error) {
	c := config{busyMS: defaultBusyTimeoutMS}
	var err error
	if c.catalog, err = strKey(cfg, "catalog", true); err != nil {
		return c, err
	}
	if pgconn.IsDSN(c.catalog) {
		if c.pgCatalog, err = pgconn.Parse(c.catalog); err != nil {
			return c, fmt.Errorf("engine-duckdb: catalog: %w", err)
		}
		c.catalog = ""
	}
	if c.dataPath, err = strKey(cfg, "data_path", true); err != nil {
		return c, err
	}
	if c.state, err = strKey(cfg, "state", false); err != nil {
		return c, err
	}
	if pgconn.IsDSN(c.state) {
		if c.pgState, err = pgconn.Parse(c.state); err != nil {
			return c, fmt.Errorf("engine-duckdb: state: %w", err)
		}
		c.state = ""
		c.stateSchema = defaultStateSchema
		if v, err := strKey(cfg, "state_schema", false); err != nil {
			return c, err
		} else if v != "" {
			c.stateSchema = v
		}
	}
	if v, ok := cfg["extensions"]; ok {
		switch l := v.(type) {
		case []string:
			c.extra = l
		case []any:
			for _, x := range l {
				name, ok := x.(string)
				if !ok {
					return c, errors.New("engine-duckdb: extensions must be strings")
				}
				c.extra = append(c.extra, name)
			}
		default:
			return c, errors.New("engine-duckdb: extensions must be a list of strings")
		}
	}
	if v, ok := cfg["locked"]; ok {
		if c.locked, ok = v.(bool); !ok {
			return c, errors.New("engine-duckdb: locked must be a bool")
		}
	}
	if v, ok := cfg["busy_timeout_ms"]; ok {
		switch n := v.(type) {
		case int:
			c.busyMS = n
		case float64:
			c.busyMS = int(n)
		default:
			return c, errors.New("engine-duckdb: busy_timeout_ms must be a number")
		}
	}
	if (c.pgCatalog == nil && !filepath.IsAbs(c.catalog)) || (c.state != "" && !filepath.IsAbs(c.state)) {
		return c, errors.New("engine-duckdb: catalog and state must be absolute paths or postgres connection strings")
	}
	c.localData = !strings.Contains(c.dataPath, "://")
	if !c.localData {
		if err := c.parseS3(cfg); err != nil {
			return c, err
		}
	}
	if c.localData {
		if !filepath.IsAbs(c.dataPath) {
			return c, errors.New("engine-duckdb: a local data_path must be absolute")
		}
		c.dataPath = filepath.Clean(c.dataPath)
	}
	return c, nil
}

// parseS3 reads the "s3" key (an s3conn.Config built by core/project) for an
// s3:// data_path.
func (c *config) parseS3(cfg contract.Config) error {
	var err error
	if c.s3Scope, err = s3conn.Scope(c.dataPath); err != nil {
		return fmt.Errorf("engine-duckdb: %w (only s3:// is supported as remote data_path)", err)
	}
	if msg, _ := cfg["s3_error"].(string); msg != "" {
		return errors.New("engine-duckdb: s3 config: " + msg)
	}
	s3, ok := cfg["s3"].(s3conn.Config)
	if !ok {
		return errors.New("engine-duckdb: an s3:// data_path needs the s3 key (connection and credentials; `duct` builds it from duct.toml and the environment)")
	}
	c.s3 = s3
	c.provider, err = s3conn.NewProvider(s3, c.dataPath, false)
	return err
}

func strKey(cfg contract.Config, key string, required bool) (string, error) {
	v, ok := cfg[key]
	if !ok {
		if required {
			return "", fmt.Errorf("engine-duckdb: %s is required", key)
		}
		return "", nil
	}
	s, ok := v.(string)
	if !ok || (required && s == "") {
		return "", fmt.Errorf("engine-duckdb: %s must be a non-empty string", key)
	}
	return s, nil
}

// Init opens an in-memory DuckDB, installs and loads the extensions, attaches
// the DuckLake catalog as "lake" (and the Duct state database as "duct") and,
// if locked, closes the door: external access off, allowed_directories limited
// to the lake, configuration locked. The lock is applied after ATTACH.
//
// Config keys: catalog (abs path of the SQLite catalog, or a postgres
// connection string: postgres://user:pass@host/db or postgres:dbname=... host=...),
// data_path (abs local dir or URL), state (abs path of the SQLite state file,
// or a postgres connection string whose schema state_schema, default "duct",
// holds the Duct tables; optional), locked (bool), busy_timeout_ms (default
// 5000, SQLite only), extensions (extra extensions to load).
//
// Postgres is reached through temporary secrets (CREATE TEMPORARY SECRET), the
// only path that works with allow_persistent_secrets=false, and errors are
// scrubbed of the password.
func (e *Engine) Init(ctx context.Context, cfg contract.Config) error {
	if mem, _ := cfg["memory"].(bool); mem { // see memory.go
		return e.initMemory(ctx)
	}
	c, err := parseConfig(cfg)
	if err != nil {
		return err
	}

	var ready atomic.Bool
	connector, err := duckdb.NewConnector("", func(ex driver.ExecerContext) error {
		if !ready.Load() {
			return nil
		}
		// USE is per connection; every pooled connection defaults to the lake.
		_, err := ex.ExecContext(context.Background(), "USE "+lakeCatalog, nil)
		return err
	})
	if err != nil {
		return fmt.Errorf("engine-duckdb: open: %w", err)
	}
	db := sql.OpenDB(connector)
	// Settings and ATTACH are database-wide, but run setup on one connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("engine-duckdb: connect: %w", err)
	}
	defer func() { _ = conn.Close() }()

	fail := func(err error) error {
		_ = db.Close()
		return err
	}
	run := func(stmts []string) error {
		for _, stmt := range stmts {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("engine-duckdb: %s: %w", firstWords(stmt), err)
			}
		}
		return nil
	}
	if c.locked {
		// Must come before the secret manager is first used (ATTACH): DuckDB
		// refuses to change it afterwards. ADR 0007 lists it with the lock,
		// but the tested order only holds when it is set up front.
		if err := run([]string{"SET allow_persistent_secrets = false"}); err != nil {
			return fail(err)
		}
	}
	if err := run(loadStatements(c)); err != nil {
		return fail(c.scrub(err))
	}
	if !c.localData {
		cr, err := c.provider.Credentials(ctx)
		if err != nil {
			return fail(err)
		}
		if err := run([]string{c.s3.SecretSQL(cr)}); err != nil {
			return fail(c.scrub(err))
		}
	}
	if c.pgCatalog != nil {
		if err := attachPostgres(ctx, conn, c); err != nil {
			return fail(c.scrub(err))
		}
		if err := tuneRetries(ctx, conn); err != nil {
			return fail(err)
		}
	} else {
		if err := ensureWAL(ctx, conn, c); err != nil {
			return fail(err)
		}
		if err := run(attachStatements(c)); err != nil {
			return fail(err)
		}
	}
	ready.Store(true)
	if _, err := conn.ExecContext(ctx, "USE "+lakeCatalog); err != nil {
		return fail(fmt.Errorf("engine-duckdb: use lake: %w", err))
	}
	if c.locked {
		for _, stmt := range lockStatements(c) {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fail(fmt.Errorf("engine-duckdb: lock: %w", err))
			}
		}
	}
	e.mu.Lock()
	e.db = db
	if c.provider != nil && c.provider.Temporary() {
		rctx, cancel := context.WithCancel(context.Background())
		e.stop, e.done = cancel, make(chan struct{})
		go refreshS3(rctx, e.done, db, c)
	}
	e.mu.Unlock()
	return nil
}

// refreshS3 renews the STS credentials of the S3 secret before they expire.
// CREATE SECRET is allowed after the lock (ADR 0007), and the secret is
// database-wide, so one pooled connection is enough.
func refreshS3(ctx context.Context, done chan<- struct{}, db *sql.DB, c config) {
	defer close(done)
	tk := time.NewTicker(30 * time.Second)
	defer tk.Stop()
	last := ""
	if cr, err := c.provider.Credentials(ctx); err == nil {
		last = cr.SessionToken
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		cr, err := c.provider.Credentials(ctx)
		if err != nil {
			continue // keep the current secret; retried on the next tick
		}
		if cr.SessionToken == last {
			continue
		}
		if _, err := db.ExecContext(ctx, c.s3.SecretSQL(cr)); err == nil {
			last = cr.SessionToken
		}
	}
}

func loadStatements(c config) []string {
	exts := append([]string{}, extensions...)
	if c.pgCatalog != nil || c.pgState != nil {
		exts = append(exts, "postgres")
	}
	if !c.localData {
		exts = append(exts, "httpfs")
	}
	exts = append(exts, c.extra...)
	var s []string
	seen := map[string]bool{}
	for _, ext := range exts {
		if seen[ext] {
			continue
		}
		seen[ext] = true
		s = append(s, "INSTALL "+ext, "LOAD "+ext)
	}
	return s
}

// scrub removes the Postgres passwords from an error (DuckDB echoes the
// connection string in some failures).
func (c config) scrub(err error) error {
	if err == nil {
		return nil
	}
	msg := c.s3.Redact(err.Error())
	for _, p := range []pgconn.Params{c.pgCatalog, c.pgState} {
		msg = p.Redact(msg)
	}
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

const (
	catalogSecret = "duct_pg_catalog"
	stateSecret   = "duct_pg_state"
)

// retriable is a transient failure of concurrent first-time initialisation
// (two processes creating the same Postgres schema or tables).
func retriable(err error) bool {
	m := strings.ToLower(err.Error())
	for _, frag := range []string{"already exists", "duplicate key", "concurrent", "deadlock", "could not serialize", "conflict"} {
		if strings.Contains(m, frag) {
			return true
		}
	}
	return false
}

func withRetry(ctx context.Context, f func() error) error {
	wait := 100 * time.Millisecond
	var err error
	for range 6 {
		if err = f(); err == nil || !retriable(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait):
		}
		wait *= 2
	}
	return err
}

// attachPostgres attaches the DuckLake catalog stored in Postgres as "lake"
// and, if configured, the Duct state schema as "duct".
func attachPostgres(ctx context.Context, conn *sql.Conn, c config) error {
	ex := func(q string) error {
		_, err := conn.ExecContext(ctx, q)
		if err != nil {
			return fmt.Errorf("engine-duckdb: %s: %w", firstWords(q), err)
		}
		return nil
	}
	if err := ex(c.pgCatalog.SecretSQL(catalogSecret)); err != nil {
		return err
	}
	opts := fmt.Sprintf("DATA_PATH %s, META_SECRET %s", quote(c.dataPath), catalogSecret)
	opts += ", OVERRIDE_DATA_PATH true" // storage may have moved (rclone sync ./lake s3://...)
	if err := withRetry(ctx, func() error {
		return ex(fmt.Sprintf("ATTACH 'ducklake:postgres:' AS %s (%s)", lakeCatalog, opts))
	}); err != nil {
		return err
	}
	if c.pgState == nil {
		return nil
	}
	secret := catalogSecret
	if !paramsEqual(c.pgState, c.pgCatalog) {
		secret = stateSecret
		if err := ex(c.pgState.SecretSQL(stateSecret)); err != nil {
			return err
		}
	}
	// The schema must exist before ATTACH ... SCHEMA; create it through a
	// throwaway attach (idempotent, retried when two processes race).
	const boot = "duct_pg_boot"
	if err := withRetry(ctx, func() error {
		if err := ex(fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s)", boot, secret)); err != nil {
			return err
		}
		defer func() { _, _ = conn.ExecContext(ctx, "DETACH "+boot) }()
		return ex(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", boot, c.stateSchema))
	}); err != nil {
		return err
	}
	return ex(fmt.Sprintf("ATTACH '' AS %s (TYPE postgres, SECRET %s, SCHEMA %s)", stateCatalog, secret, quote(c.stateSchema)))
}

func paramsEqual(a, b pgconn.Params) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// tuneRetries raises DuckLake's snapshot-conflict retries: with a shared
// Postgres catalog several processes commit concurrently, and each lost race
// costs one retry (exponential backoff). Must run before the configuration lock.
func tuneRetries(ctx context.Context, conn *sql.Conn) error {
	for _, q := range []string{"SET ducklake_max_retry_count = 100"} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("engine-duckdb: %s: %w", firstWords(q), err)
		}
	}
	return nil
}

// ensureWAL puts the SQLite catalog in WAL mode. DuckLake's ATTACH rejects
// JOURNAL_MODE, so it is set through a plain sqlite attach first; the mode
// persists in the file header. busy_timeout is passed to DuckLake's ATTACH
// afterwards (it is per connection, not persistent).
func ensureWAL(ctx context.Context, conn *sql.Conn, c config) error {
	const alias = "duct_wal_init"
	attach := fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, JOURNAL_MODE 'WAL', BUSY_TIMEOUT %d)", quote(c.catalog), alias, c.busyMS)
	if _, err := conn.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("engine-duckdb: wal init: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(ctx, "DETACH "+alias) }()
	var mode string
	if err := conn.QueryRowContext(ctx, "SELECT * FROM sqlite_query('"+alias+"', 'PRAGMA journal_mode')").Scan(&mode); err != nil {
		return fmt.Errorf("engine-duckdb: wal check: %w", err)
	}
	if strings.EqualFold(mode, "wal") {
		return nil
	}
	// A fresh file only records the mode after its first write.
	for _, q := range []string{"CREATE TABLE " + alias + ".duct_wal_probe (i INTEGER)", "DROP TABLE " + alias + ".duct_wal_probe"} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("engine-duckdb: wal init: %w", err)
		}
	}
	return nil
}

func attachStatements(c config) []string {
	opts := fmt.Sprintf("DATA_PATH %s, BUSY_TIMEOUT %d", quote(c.dataPath), c.busyMS)
	// Relative layout (ADR 0001): the project folder may move, and the lake may
	// move to s3://, so the data path recorded in the catalog must not pin it.
	opts += ", OVERRIDE_DATA_PATH true"
	s := []string{fmt.Sprintf("ATTACH %s AS %s (%s)", quote("ducklake:sqlite:"+c.catalog), lakeCatalog, opts)}
	if c.state != "" {
		s = append(s, fmt.Sprintf("ATTACH %s AS %s (TYPE sqlite, JOURNAL_MODE 'WAL', BUSY_TIMEOUT %d)", quote(c.state), stateCatalog, c.busyMS))
	}
	return s
}

func lockStatements(c config) []string {
	dir := c.dataPath + "/"
	if !c.localData {
		dir = c.s3Scope
	}
	return []string{
		"SET allowed_directories = [" + quote(dir) + "]",
		"SET enable_external_access = false",
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET lock_configuration = true",
	}
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func firstWords(s string) string {
	if f := strings.Fields(s); len(f) > 1 {
		return f[0] + " " + f[1]
	}
	return s
}

func (e *Engine) handle() (*sql.DB, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.db == nil || e.closed {
		return nil, errors.New("engine-duckdb: not initialised or closed")
	}
	return e.db, nil
}

// Exec implements contract.Engine.
func (e *Engine) Exec(ctx context.Context, query string, args ...any) error {
	db, err := e.handle()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, query, args...)
	return err
}

// ExecTx implements contract.Engine.
func (e *Engine) ExecTx(ctx context.Context, stmts ...string) error {
	db, err := e.handle()
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, s := range stmts {
		if _, err := tx.ExecContext(ctx, s); err != nil {
			return errors.Join(err, tx.Rollback())
		}
	}
	return tx.Commit()
}

// Query implements contract.Engine. The returned Stream owns a connection
// until its last Release.
func (e *Engine) Query(ctx context.Context, query string, args ...any) (contract.Stream, error) {
	db, err := e.handle()
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	rr, err := arrowQuery(ctx, conn, query, args...)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	st := &stream{RecordReader: rr, conn: conn}
	st.refs.Store(1)
	return st, nil
}

func arrowQuery(ctx context.Context, conn *sql.Conn, query string, args ...any) (array.RecordReader, error) {
	var rr array.RecordReader
	err := conn.Raw(func(dc any) error {
		d, ok := dc.(driver.Conn)
		if !ok {
			return errors.New("engine-duckdb: unexpected driver connection")
		}
		ar, err := duckdb.NewArrowFromConn(d)
		if err != nil {
			return err
		}
		rr, err = ar.QueryContext(ctx, query, args...)
		return err
	})
	return rr, err
}

var _ contract.Transactor = (*Engine)(nil)

// InTx implements contract.Transactor: one connection, BEGIN ... COMMIT or
// ROLLBACK, with input (if any) registered as the view contract.InputView.
func (e *Engine) InTx(ctx context.Context, input contract.Stream, fn func(ctx context.Context, tx contract.Tx) error) error {
	db, err := e.handle()
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return err
	}
	rollback := func(err error) error {
		// ctx may be cancelled: roll back regardless.
		_, rerr := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		return errors.Join(err, rerr)
	}
	if input != nil {
		var release func()
		err := conn.Raw(func(dc any) error {
			d, ok := dc.(driver.Conn)
			if !ok {
				return errors.New("engine-duckdb: unexpected driver connection")
			}
			ar, err := duckdb.NewArrowFromConn(d)
			if err != nil {
				return err
			}
			release, err = ar.RegisterView(input, contract.InputView)
			return err
		})
		if err != nil {
			return rollback(err)
		}
		defer release()
	}
	if err := fn(ctx, connTx{conn}); err != nil {
		return rollback(err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return rollback(err)
	}
	return nil
}

type connTx struct{ conn *sql.Conn }

func (t connTx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := t.conn.ExecContext(ctx, query, args...)
	return err
}

func (t connTx) Query(ctx context.Context, query string, args ...any) (contract.Stream, error) {
	return arrowQuery(ctx, t.conn, query, args...)
}

// stream returns the connection to the pool when its last reference goes.
type stream struct {
	array.RecordReader
	conn *sql.Conn
	refs atomic.Int64
}

func (s *stream) Retain() {
	s.refs.Add(1)
	s.RecordReader.Retain()
}

func (s *stream) Release() {
	s.RecordReader.Release()
	if s.refs.Add(-1) == 0 {
		_ = s.conn.Close()
	}
}

// Close implements contract.Module. Idempotent; safe without Init.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.db == nil {
		e.closed = true
		return nil
	}
	e.closed = true
	if e.stop != nil {
		e.stop()
		<-e.done
	}
	return e.db.Close()
}
