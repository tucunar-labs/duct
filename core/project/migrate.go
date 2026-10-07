package project

import (
	"context"
	"fmt"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Migration is one versioned change to the Duct-owned tables. SQL must be
// portable between SQLite and Postgres: tables are referenced as duct.<name>,
// only types both understand (INTEGER, BIGINT, VARCHAR, TIMESTAMP, BOOLEAN)
// and no engine-specific syntax. Migrations are append-only: never edit one
// that shipped, add a new version.
type Migration struct {
	Version int
	Name    string
	SQL     []string
}

// Migrations is the ordered list of Duct-owned schema changes.
var Migrations = []Migration{
	{
		Version: 1,
		Name:    "runs",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_runs (
				id BIGINT PRIMARY KEY,
				kind VARCHAR NOT NULL,
				status VARCHAR NOT NULL,
				started_at TIMESTAMP NOT NULL,
				finished_at TIMESTAMP,
				detail VARCHAR
			)`,
		},
	},
	{
		Version: 2,
		Name:    "pipeline_runs",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_pipeline_runs (
				run_id BIGINT NOT NULL,
				target VARCHAR NOT NULL,
				status VARCHAR NOT NULL,
				rows_written BIGINT NOT NULL,
				rows_dropped BIGINT NOT NULL,
				expectations VARCHAR NOT NULL,
				error VARCHAR,
				started_at TIMESTAMP NOT NULL,
				finished_at TIMESTAMP NOT NULL,
				duration_ms BIGINT NOT NULL,
				PRIMARY KEY (run_id, target)
			)`,
		},
	},
	{
		Version: 3,
		Name:    "ingest_state",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_ingest_state (
				name VARCHAR PRIMARY KEY,
				watermark VARCHAR,
				rows_loaded BIGINT NOT NULL,
				updated_at TIMESTAMP NOT NULL
			)`,
		},
	},
	{
		// Governance (ADR 0007): roles, users, tokens, grants, row filters,
		// masks and the query audit log. Relations are "schema.table".
		Version: 4,
		Name:    "governance",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_roles (
				name VARCHAR PRIMARY KEY,
				created_at TIMESTAMP NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_users (
				name VARCHAR PRIMARY KEY,
				role VARCHAR NOT NULL,
				password_hash VARCHAR NOT NULL,
				disabled BOOLEAN NOT NULL,
				created_at TIMESTAMP NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_tokens (
				id VARCHAR PRIMARY KEY,
				user_name VARCHAR NOT NULL,
				token_hash VARCHAR NOT NULL,
				label VARCHAR,
				created_at TIMESTAMP NOT NULL,
				expires_at TIMESTAMP,
				revoked BOOLEAN NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_grants (
				role VARCHAR NOT NULL,
				relation VARCHAR NOT NULL,
				privilege VARCHAR NOT NULL,
				granted_at TIMESTAMP NOT NULL,
				PRIMARY KEY (role, relation, privilege)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_row_filters (
				role VARCHAR NOT NULL,
				relation VARCHAR NOT NULL,
				predicate VARCHAR NOT NULL,
				PRIMARY KEY (role, relation)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_masks (
				role VARCHAR NOT NULL,
				relation VARCHAR NOT NULL,
				column_name VARCHAR NOT NULL,
				expr VARCHAR NOT NULL,
				PRIMARY KEY (role, relation, column_name)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_audit (
				occurred_at TIMESTAMP NOT NULL,
				user_name VARCHAR NOT NULL,
				role VARCHAR NOT NULL,
				protocol VARCHAR NOT NULL,
				sql_text VARCHAR NOT NULL,
				duration_ms BIGINT NOT NULL,
				rows_returned BIGINT NOT NULL,
				status VARCHAR NOT NULL,
				error VARCHAR
			)`,
		},
	},
}

const createMigrations = `CREATE TABLE IF NOT EXISTS duct.duct_migrations (
	version INTEGER PRIMARY KEY,
	name VARCHAR NOT NULL,
	applied_at TIMESTAMP NOT NULL
)`

// Migrate applies every pending migration, each in its own transaction, and
// returns the versions applied. Running it again is a no-op.
//
// Several processes may run it at once on a shared (Postgres) state: a
// migration another process finished first counts as applied.
func Migrate(ctx context.Context, eng contract.Engine) ([]int, error) {
	return migrate(ctx, eng, Migrations, "duct")
}

// MigrateAt is Migrate against the attached catalog alias instead of "duct"
// (catalog migration creates the tables in the target before switching).
func MigrateAt(ctx context.Context, eng contract.Engine, alias string) ([]int, error) {
	return migrate(ctx, eng, Migrations, alias)
}

func migrate(ctx context.Context, eng contract.Engine, list []Migration, alias string) ([]int, error) {
	at := func(sql string) string { return strings.ReplaceAll(sql, "duct.duct_", alias+".duct_") }
	if err := eng.Exec(ctx, at(createMigrations)); err != nil {
		if done, err2 := appliedVersions(ctx, eng, alias); err2 != nil || done == nil {
			return nil, fmt.Errorf("create duct_migrations: %w", err)
		}
	}
	done, err := appliedVersions(ctx, eng, alias)
	if err != nil {
		return nil, err
	}
	var applied []int
	for _, m := range list {
		if done[m.Version] {
			continue
		}
		stmts := make([]string, 0, len(m.SQL)+1)
		for _, s := range m.SQL {
			stmts = append(stmts, at(s))
		}
		stmts = append(stmts, at(fmt.Sprintf(
			"INSERT INTO duct.duct_migrations (version, name, applied_at) VALUES (%d, '%s', now())",
			m.Version, strings.ReplaceAll(m.Name, "'", "''"))))
		if err := eng.ExecTx(ctx, stmts...); err != nil {
			// Lost a race with another process applying the same version.
			if now, err2 := appliedVersions(ctx, eng, alias); err2 == nil && now[m.Version] {
				continue
			}
			return applied, fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
		}
		applied = append(applied, m.Version)
	}
	return applied, nil
}

func appliedVersions(ctx context.Context, eng contract.Engine, alias string) (map[int]bool, error) {
	st, err := eng.Query(ctx, "SELECT CAST(version AS BIGINT) FROM "+alias+".duct_migrations")
	if err != nil {
		return nil, err
	}
	defer st.Release()
	done := map[int]bool{}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			var v int
			if _, err := fmt.Sscan(rec.Column(0).ValueStr(i), &v); err != nil {
				return nil, err
			}
			done[v] = true
		}
	}
	return done, st.Err()
}
