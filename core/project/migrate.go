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
	// Before, if set, runs before SQL (outside its transaction) for work that
	// is engine specific. It must be idempotent: when it fails the migration
	// is not recorded and the whole thing runs again.
	Before func(ctx context.Context, eng contract.Engine, alias string) error
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
	{
		// Alerts (ADR 0009): the alerts currently firing, so a restart of
		// `duct alerts` neither re-notifies them nor forgets to resolve them.
		Version: 5,
		Name:    "alert_state",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_alert_state (
				rule VARCHAR NOT NULL,
				alert_key VARCHAR NOT NULL,
				severity VARCHAR NOT NULL,
				message VARCHAR NOT NULL,
				labels VARCHAR NOT NULL,
				firing_since TIMESTAMP NOT NULL,
				PRIMARY KEY (rule, alert_key)
			)`,
		},
	},
	{
		// Orchestrator (ADR 0005): runs and task attempts of jobs/*.yaml, the
		// fire keys that make a cron tick / trigger start exactly one run across
		// machines, and trigger cursors (last snapshot, files seen). Params and
		// credentials are never stored: workers read them from their own jobs/.
		Version: 6,
		Name:    "jobs",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_jobs_runs (
				id BIGINT PRIMARY KEY,
				job VARCHAR NOT NULL,
				trigger_kind VARCHAR NOT NULL,
				detail VARCHAR,
				status VARCHAR NOT NULL,
				scheduled_for TIMESTAMP,
				started_at TIMESTAMP NOT NULL,
				finished_at TIMESTAMP,
				error VARCHAR,
				pinned_to VARCHAR,
				heartbeat_at TIMESTAMP,
				finalizer VARCHAR
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_task_runs (
				run_id BIGINT NOT NULL,
				task_id VARCHAR NOT NULL,
				job VARCHAR NOT NULL,
				task_type VARCHAR NOT NULL,
				depends_on VARCHAR NOT NULL,
				status VARCHAR NOT NULL,
				attempts INTEGER NOT NULL,
				max_attempts INTEGER NOT NULL,
				not_before TIMESTAMP,
				owner VARCHAR,
				heartbeat_at TIMESTAMP,
				started_at TIMESTAMP,
				finished_at TIMESTAMP,
				duration_ms BIGINT,
				error VARCHAR,
				output VARCHAR,
				PRIMARY KEY (run_id, task_id)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_jobs_fires (
				fire_key VARCHAR PRIMARY KEY,
				run_id BIGINT NOT NULL,
				fired_at TIMESTAMP NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_jobs_state (
				state_key VARCHAR PRIMARY KEY,
				state_value VARCHAR NOT NULL,
				updated_at TIMESTAMP NOT NULL
			)`,
		},
	},
	{
		// Data sharing (ADR 0014): shares, their tables, recipients (each one a
		// role share_<name> plus a user with the same name), grants of a share
		// to a recipient, and the materialized copy that serves each
		// (share, recipient, table). Policies (row filters, masks) live in the
		// governance tables of the recipient's role, not here.
		Version: 7,
		Name:    "sharing",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_shares (
				name VARCHAR PRIMARY KEY,
				created_at TIMESTAMP NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_share_tables (
				share VARCHAR NOT NULL,
				alias VARCHAR NOT NULL,
				relation VARCHAR NOT NULL,
				partition_filter VARCHAR,
				added_at TIMESTAMP NOT NULL,
				PRIMARY KEY (share, alias)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_recipients (
				name VARCHAR PRIMARY KEY,
				role VARCHAR NOT NULL,
				created_at TIMESTAMP NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_share_grants (
				share VARCHAR NOT NULL,
				recipient VARCHAR NOT NULL,
				granted_at TIMESTAMP NOT NULL,
				PRIMARY KEY (share, recipient)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_share_copies (
				share VARCHAR NOT NULL,
				recipient VARCHAR NOT NULL,
				alias VARCHAR NOT NULL,
				version BIGINT NOT NULL,
				source_snapshot BIGINT NOT NULL,
				fingerprint VARCHAR NOT NULL,
				prefix VARCHAR NOT NULL,
				schema_string VARCHAR NOT NULL,
				files VARCHAR NOT NULL,
				num_rows BIGINT NOT NULL,
				total_bytes BIGINT NOT NULL,
				table_id VARCHAR NOT NULL,
				created_at TIMESTAMP NOT NULL,
				checked_at TIMESTAMP NOT NULL,
				PRIMARY KEY (share, recipient, alias)
			)`,
		},
	},
	{
		// Quotas (ADR 0013, admin API): per role caps enforced by the governance
		// handler. NULL means no limit.
		Version: 8,
		Name:    "quotas",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_quotas (
				role VARCHAR PRIMARY KEY,
				max_concurrent INTEGER,
				max_rows BIGINT,
				updated_at TIMESTAMP NOT NULL
			)`,
		},
	},
	{
		// Audit log v2 (ADR 0007): audit_id (stable key for the export to the
		// lake), cached/hits (dashboard cache hits, coalesced), a state table
		// for the export watermark, and fixed-width occurred_at text on SQLite
		// plus indexes (see auditBefore). NULL cached/hits mean false/1.
		Version: 9,
		Name:    "audit_v2",
		Before:  auditBefore,
		SQL: []string{
			`ALTER TABLE duct.duct_audit ADD COLUMN audit_id VARCHAR`,
			`ALTER TABLE duct.duct_audit ADD COLUMN cached BOOLEAN`,
			`ALTER TABLE duct.duct_audit ADD COLUMN hits BIGINT`,
			`CREATE TABLE IF NOT EXISTS duct.duct_audit_state (
				state_key VARCHAR PRIMARY KEY,
				state_value VARCHAR NOT NULL,
				updated_at TIMESTAMP NOT NULL
			)`,
		},
	},
	{
		// Semantic preaggregates (ADR 0011): what is built in the lake schema
		// semantic_cache for each role, and under which model, role views and
		// lake snapshot, so a table is only used while all three still hold.
		Version: 10,
		Name:    "semantic_preagg",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_semantic_preagg (
				role VARCHAR NOT NULL,
				preagg VARCHAR NOT NULL,
				table_name VARCHAR NOT NULL,
				definition_hash VARCHAR NOT NULL,
				views_hash VARCHAR NOT NULL,
				source_snapshot BIGINT NOT NULL,
				num_rows BIGINT NOT NULL,
				built_at TIMESTAMP NOT NULL,
				PRIMARY KEY (role, preagg)
			)`,
		},
	},
	{
		// Alerts v2 (ADR 0009): coordination between several `duct alerts`
		// instances (status/holder/lease/seen/notified/claim on the firing row),
		// and silences. A NULL status is an alert persisted by an older version:
		// firing and already notified. The instants are epoch microseconds in
		// BIGINT columns: the SQLite scanner of DuckDB cannot UPDATE a TIMESTAMP
		// column added by ALTER TABLE.
		Version: 11,
		Name:    "alerts_v2",
		SQL: []string{
			`ALTER TABLE duct.duct_alert_state ADD COLUMN status VARCHAR`,
			`ALTER TABLE duct.duct_alert_state ADD COLUMN holder VARCHAR`,
			`ALTER TABLE duct.duct_alert_state ADD COLUMN lease_until_us BIGINT`,
			`ALTER TABLE duct.duct_alert_state ADD COLUMN seen_by VARCHAR`,
			`ALTER TABLE duct.duct_alert_state ADD COLUMN seen_at_us BIGINT`,
			`ALTER TABLE duct.duct_alert_state ADD COLUMN notified_at_us BIGINT`,
			`ALTER TABLE duct.duct_alert_state ADD COLUMN claim VARCHAR`,
			`CREATE TABLE IF NOT EXISTS duct.duct_alert_resolved (
				rule VARCHAR NOT NULL,
				alert_key VARCHAR NOT NULL,
				resolved_at_us BIGINT NOT NULL,
				PRIMARY KEY (rule, alert_key)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_alert_silences (
				id BIGINT PRIMARY KEY,
				rule VARCHAR NOT NULL,
				alert_key VARCHAR NOT NULL,
				reason VARCHAR NOT NULL,
				created_by VARCHAR NOT NULL,
				created_at TIMESTAMP NOT NULL,
				expires_at TIMESTAMP NOT NULL
			)`,
		},
	},
	{
		// Notebooks (ADR 0006): every version Duct saw of a notebook file (with its
		// content, so it can be restored) and the comments on notebooks. Instants
		// are epoch microseconds (BIGINT). Notebook names are file names without
		// ".py"; the rows outlive the file (a deleted notebook keeps its history).
		Version: 12,
		Name:    "notebooks",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_notebook_versions (
				id BIGINT PRIMARY KEY,
				notebook VARCHAR NOT NULL,
				content_hash VARCHAR NOT NULL,
				author VARCHAR NOT NULL,
				action VARCHAR NOT NULL,
				created_at_us BIGINT NOT NULL,
				size_bytes BIGINT NOT NULL,
				lines_added INTEGER NOT NULL,
				lines_removed INTEGER NOT NULL,
				restored_from BIGINT,
				content VARCHAR NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_notebook_comments (
				id BIGINT PRIMARY KEY,
				notebook VARCHAR NOT NULL,
				ref VARCHAR,
				author VARCHAR NOT NULL,
				body VARCHAR NOT NULL,
				created_at_us BIGINT NOT NULL,
				resolved BOOLEAN NOT NULL,
				resolved_by VARCHAR,
				resolved_at_us BIGINT
			)`,
		},
	},
	{
		// Sharing history (ADR 0014): which shared tables keep their versions
		// (time travel and change data feed) and, per recipient copy, every
		// retained version with its files and change files. Instants are epoch
		// milliseconds (BIGINT).
		Version: 13,
		Name:    "sharing_history",
		SQL: []string{
			`CREATE TABLE IF NOT EXISTS duct.duct_share_history (
				share VARCHAR NOT NULL,
				alias VARCHAR NOT NULL,
				enabled_at TIMESTAMP NOT NULL,
				PRIMARY KEY (share, alias)
			)`,
			`CREATE TABLE IF NOT EXISTS duct.duct_share_versions (
				share VARCHAR NOT NULL,
				recipient VARCHAR NOT NULL,
				alias VARCHAR NOT NULL,
				version BIGINT NOT NULL,
				source_snapshot BIGINT NOT NULL,
				ts_ms BIGINT NOT NULL,
				kind VARCHAR NOT NULL,
				policy_hash VARCHAR NOT NULL,
				schema_string VARCHAR NOT NULL,
				files VARCHAR NOT NULL,
				cdf_files VARCHAR NOT NULL,
				num_rows BIGINT NOT NULL,
				total_bytes BIGINT NOT NULL,
				PRIMARY KEY (share, recipient, alias, version)
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
		if m.Before != nil {
			if err := m.Before(ctx, eng, alias); err != nil {
				return applied, fmt.Errorf("migration %d (%s): %w", m.Version, m.Name, err)
			}
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
