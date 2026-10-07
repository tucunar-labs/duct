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
)

// duckgresCatalog is the catalog name duckgres' policy and metadata layer
// hard-codes for the lake.
const duckgresCatalog = "ducklake"

// lake is where the data is: the same keys as the engine's config.
type lake struct {
	catalog  string // abs path of the DuckLake SQLite metadata, or a postgres connection string
	pg       pgconn.Params
	dataPath string // abs local directory
	busyMS   int
}

func parseLake(cfg contract.Config) (lake, error) {
	l := lake{busyMS: 5000}
	l.catalog, _ = cfg["catalog"].(string)
	l.dataPath, _ = cfg["data_path"].(string)
	if l.catalog == "" || l.dataPath == "" {
		return l, errors.New("serve-pg: catalog and data_path are required (`duct serve` sets them)")
	}
	if strings.Contains(l.dataPath, "://") {
		return l, errors.New("serve-pg: a remote data_path is not supported yet (locked mode needs httpfs + secrets)")
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
	if l.pg != nil {
		// Temporary secret: the only way to reach Postgres with persistent
		// secrets disabled. Errors are scrubbed of the password below.
		stmts = append(stmts, "INSTALL postgres", "LOAD postgres", l.pg.SecretSQL("duct_pg_catalog"),
			fmt.Sprintf("ATTACH 'ducklake:postgres:' AS %s (DATA_PATH %s, OVERRIDE_DATA_PATH true, READ_ONLY, META_SECRET duct_pg_catalog)",
				duckgresCatalog, quote(l.dataPath)))
	} else {
		stmts = append(stmts, fmt.Sprintf("ATTACH %s AS %s (DATA_PATH %s, OVERRIDE_DATA_PATH true, READ_ONLY, BUSY_TIMEOUT %d)",
			quote("ducklake:sqlite:"+l.catalog), duckgresCatalog, quote(l.dataPath), l.busyMS))
	}
	stmts = append(stmts, "USE "+duckgresCatalog)
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %s", strings.Fields(s)[0], l.pg.Redact(err.Error()))
		}
	}
	return db, nil
}

// lockDown is ADR 0007's lockdown in its tested order: allowed_directories
// before enable_external_access=false, then secrets, extensions, and the
// configuration lock last.
func lockDown(ctx context.Context, db *sql.DB, l lake) error {
	for _, s := range []string{
		"SET allowed_directories = [" + quote(l.dataPath+"/") + "]",
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
	}

	pid, key := pidSeq(s), server.GenerateSecretKey()
	cc := server.NewClientConn(s.srv, conn, r, w, id.User, "", duckgresCatalog, app, exec, pid, key, -1, "")
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
