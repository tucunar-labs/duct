package servepg

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/server/sessionmeta"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/s3conn"
)

// duckgresCatalog is the catalog name duckgres' policy and metadata layer
// hard-codes for the lake.
const duckgresCatalog = "ducklake"

// lake is where the data is: the same keys as the engine's config.
type lake struct {
	catalog  string // abs path of the DuckLake SQLite metadata, or a postgres connection string
	pg       pgconn.Params
	dataPath string // abs local directory, or s3://bucket/prefix
	busyMS   int

	// s3:// data_path: connection, the allowed_directories entry, and the
	// provider of (read-only, prefix-scoped when STS is on) credentials.
	s3       s3conn.Config
	s3Scope  string
	provider *s3conn.Provider
}

func (l lake) remote() bool { return l.provider != nil }

// scrub removes the Postgres password and the S3 credentials from text.
func (l lake) scrub(text string) string { return l.pg.Redact(l.s3.Redact(text)) }

func parseLake(cfg contract.Config) (lake, error) {
	l := lake{busyMS: 5000}
	l.catalog, _ = cfg["catalog"].(string)
	l.dataPath, _ = cfg["data_path"].(string)
	if l.catalog == "" || l.dataPath == "" {
		return l, errors.New("serve-pg: catalog and data_path are required (`duct serve` sets them)")
	}
	if strings.Contains(l.dataPath, "://") {
		var err error
		if l.s3Scope, err = s3conn.Scope(l.dataPath); err != nil {
			return l, fmt.Errorf("serve-pg: %w (only s3:// is supported as remote data_path)", err)
		}
		if msg, _ := cfg["s3_error"].(string); msg != "" {
			return l, errors.New("serve-pg: s3 config: " + msg)
		}
		s3, ok := cfg["s3"].(s3conn.Config)
		if !ok {
			return l, errors.New("serve-pg: an s3:// data_path needs the s3 key (`duct serve` sets it)")
		}
		l.s3 = s3
		if l.provider, err = s3conn.NewProvider(s3, l.dataPath, true); err != nil {
			return l, fmt.Errorf("serve-pg: %w", err)
		}
		if pgconn.IsDSN(l.catalog) {
			if l.pg, err = pgconn.Parse(l.catalog); err != nil {
				return l, fmt.Errorf("serve-pg: catalog: %w", err)
			}
		} else if !filepath.IsAbs(l.catalog) {
			return l, errors.New("serve-pg: catalog must be an absolute path or a postgres connection string")
		}
		return l, nil
	}
	if pgconn.IsDSN(l.catalog) {
		var err error
		if l.pg, err = pgconn.Parse(l.catalog); err != nil {
			return l, fmt.Errorf("serve-pg: catalog: %w", err)
		}
	} else if !filepath.IsAbs(l.catalog) {
		return l, errors.New("serve-pg: catalog must be an absolute path or a postgres connection string")
	}
	if !filepath.IsAbs(l.dataPath) {
		return l, errors.New("serve-pg: data_path must be an absolute path")
	}
	l.dataPath = filepath.Clean(l.dataPath)
	return l, nil
}

type sessionConfig struct {
	memoryLimit string
	threads     int
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// openSession opens a private in-process DuckDB for one login: the lake is
// attached READ ONLY as "ducklake" and becomes the default catalog. The Duct
// state database (password hashes, tokens, audit) is never attached. The
// lockdown is applied later (lockDown), after duckgres has installed its
// session metadata (that needs SET search_path).
//
// One connection only: duckgres' per-session TEMP macros and views live in it.
func openSession(ctx context.Context, l lake, c sessionConfig) (*sql.DB, error) {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	stmts := []string{
		// Before the first ATTACH: DuckDB refuses to change it once the secret
		// manager has been used.
		"SET allow_persistent_secrets = false",
		"SET memory_limit = " + quote(c.memoryLimit),
		fmt.Sprintf("SET threads = %d", c.threads),
		"INSTALL ducklake", "LOAD ducklake", "INSTALL sqlite_scanner", "LOAD sqlite_scanner",
	}
	const opts = "OVERRIDE_DATA_PATH true, " // the lake may have moved to s3://
	if l.remote() {
		cr, err := l.provider.Credentials(ctx)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		// Temporary secret (persistent ones are disabled); the data path is
		// remote, so the catalog keeps the URL as is.
		stmts = append(stmts, "INSTALL httpfs", "LOAD httpfs", l.s3.SecretSQL(cr))
	}
	if l.pg != nil {
		// Temporary secret: the only way to reach Postgres with persistent
		// secrets disabled. Errors are scrubbed of the password below.
		stmts = append(stmts, "INSTALL postgres", "LOAD postgres", l.pg.SecretSQL("duct_pg_catalog"),
			fmt.Sprintf("ATTACH 'ducklake:postgres:' AS %s (DATA_PATH %s, %sREAD_ONLY, META_SECRET duct_pg_catalog)",
				duckgresCatalog, quote(l.dataPath), opts))
	} else {
		stmts = append(stmts, fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, %sREAD_ONLY, BUSY_TIMEOUT %d)",
			quote("ducklake:sqlite:"+l.catalog), duckgresCatalog, quote(l.dataPath), opts, l.busyMS))
	}
	stmts = append(stmts, "USE "+duckgresCatalog)
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %s", strings.Fields(s)[0], l.scrub(err.Error()))
		}
	}
	return db, nil
}

// lockDown is ADR 0007's lockdown in its tested order: allowed_directories
// before enable_external_access=false, then secrets, extensions, and the
// configuration lock last.
func lockDown(ctx context.Context, db *sql.DB, l lake) error {
	dir := l.dataPath + "/"
	if l.remote() {
		dir = l.s3Scope
	}
	for _, s := range []string{
		"SET allowed_directories = [" + quote(dir) + "]",
		"SET enable_external_access = false",
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET lock_configuration = true",
	} {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("lock down: %s: %w", s, err)
		}
	}
	return nil
}

// keepS3Fresh renews the STS credentials of a long session's S3 secret until
// ctx ends (CREATE SECRET stays allowed after the lock, ADR 0007).
func keepS3Fresh(ctx context.Context, db *sql.DB, l lake) {
	if !l.remote() || !l.provider.Temporary() {
		return
	}
	last := ""
	if cr, err := l.provider.Credentials(ctx); err == nil {
		last = cr.SessionToken
	}
	tk := time.NewTicker(30 * time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		cr, err := l.provider.Credentials(ctx)
		if err != nil || cr.SessionToken == last {
			continue
		}
		if _, err := db.ExecContext(ctx, l.s3.SecretSQL(cr)); err == nil {
			last = cr.SessionToken
		}
	}
}

var pidSeq = func(s *Server) int32 { return s.pids.Add(1) }

// session runs one authenticated connection to its end.
func (s *Server) session(ctx context.Context, conn net.Conn, r *bufio.Reader, w *bufio.Writer, id contract.Identity, app string) {
	fatal := func(code, msg string) {
		_ = server.WriteErrorResponse(w, "FATAL", code, msg)
		_ = w.Flush()
	}
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := openSession(initCtx, s.lake, s.sessCfg)
	if err != nil {
		slog.Error("serve-pg: cannot open a session", "user", id.User, "error", err)
		fatal("58000", "cannot open a session")
		return
	}
	exec := server.NewLocalExecutor(db)
	defer func() { _ = exec.Close() }()
	sctx, stopFresh := context.WithCancel(ctx)
	defer stopFresh()
	go keepS3Fresh(sctx, db, s.lake)

	schema := contract.RoleSchema(id.Role)
	err = sessionmeta.InitSessionDatabaseMetadataWithAccess(initCtx, exec, duckgresCatalog,
		&sessionmeta.MetadataAccessPolicy{AllowedSchemas: []string{schema}})
	if err == nil {
		err = lockDown(initCtx, db, s.lake)
	}
	if err != nil {
		slog.Error("serve-pg: cannot prepare the session", "user", id.User, "error", err)
		fatal("58000", "cannot prepare the session")
		return
	}
	policy := &server.QueryAccessPolicy{
		ReadOnly:       true,
		AllowedSchemas: []string{schema},
		DefaultSchema:  schema,
		RelationExists: server.NewRelationLookup(exec, duckgresCatalog),
		Intercept:      s.intercept(ctx, id),
	}

	pid, key := pidSeq(s), server.GenerateSecretKey()
	var qexec server.QueryExecutor = exec
	if lp := s.limiter.Load(); lp != nil {
		qexec = newQuotaExecutor(exec, *lp, id)
		defer func() { _ = qexec.Close() }() // frees slots left open; also closes db (Close is idempotent for sql.DB)
	}
	cc := server.NewClientConn(s.srv, conn, r, w, id.User, "", duckgresCatalog, app, qexec, pid, key, -1, "")
	defer server.CancelClientConn(cc)
	server.SetConnectionIdleTimeout(cc, s.idle)
	server.SetConnectionPhysicalCatalog(cc, duckgresCatalog)
	server.SetCatalogUseRewrite(cc, true)
	server.SetQueryAccessPolicy(cc, policy)
	if err := server.WriteAuthOK(w); err != nil {
		return
	}
	server.SendInitialParams(cc)
	if err := server.WriteReadyForQuery(w, 'I'); err != nil || w.Flush() != nil {
		return
	}
	if err := server.RunMessageLoop(cc); err != nil && ctx.Err() == nil {
		slog.Debug("serve-pg: session ended", "user", id.User, "error", err)
	}
	_ = server.CloseConnectionMetrics(cc)
}

// adminMarker prefixes the constant SELECT that stands for an administrative
// statement's result, so the audit hook can tell it from a user statement.
const adminMarker = "/*duct:admin*/ "

// intercept hands governance statements (CREATE ROLE, GRANT, ALTER TABLE ...
// SET ROW FILTER, ...) to the handler's Administrator before duckgres' policy
// sees them: the handler runs them for an admin identity, refuses them for
// anyone else, and answers with a constant SELECT that is returned instead.
// The successful statement is audited here with its original text.
func (s *Server) intercept(ctx context.Context, id contract.Identity) func(string) (string, bool, error) {
	ap := s.admin.Load()
	if ap == nil {
		return nil
	}
	return func(query string) (string, bool, error) {
		start := time.Now()
		repl, handled, err := (*ap).Administer(ctx, id, query)
		if !handled || err != nil {
			return "", handled, err // a refusal or failure is audited by duckgres' hook
		}
		if au := s.auditor.Load(); au != nil {
			(*au).Audit(context.Background(), contract.AuditRecord{
				At: start.UTC(), User: id.User, Role: id.Role, Protocol: name, SQL: query, Duration: time.Since(start),
			})
		}
		return adminMarker + repl, true, nil
	}
}
