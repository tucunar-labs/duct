package contract

import (
	"context"
	"time"
)

// RoleSchemaPrefix is prepended to a role name to form the schema of that
// role's views (ADR 0007): role "analistas" reads schema "role_analistas".
const RoleSchemaPrefix = "role_"

// RoleSchema returns the schema holding the views a role may query.
func RoleSchema(role string) string { return RoleSchemaPrefix + role }

// Addresser is an OPTIONAL interface of a Protocol: after Init it reports the
// address it is bound to ("host:port"), so callers can discover a random port
// ("127.0.0.1:0" in the config).
type Addresser interface {
	Addr() string
}

// AuditRecord is one statement run by a principal.
type AuditRecord struct {
	At       time.Time
	User     string
	Role     string
	Protocol string // "pg", "http", ...
	SQL      string // credentials already redacted
	Duration time.Duration
	Rows     int64
	// Err is empty on success; otherwise a short reason (refused or failed).
	Err string
	// Cached marks a result served from a cache (dashboards) without running
	// SQL; Duration is then the cache lookup. Hits > 1 means that many
	// identical cached answers were coalesced into this record (0 or 1: one).
	Cached bool
	Hits   int64
}

// Administrator is an OPTIONAL interface of a QueryHandler for protocols that
// run statements by themselves (serve-pg). Administer is asked about every
// statement first: handled=false means "not an administrative statement, treat
// it as a normal query"; handled=true with an error means it was refused or
// failed (the protocol reports the error, which also audits it); handled=true
// without an error means it ran, and resultSQL is a trusted constant SELECT
// with its result (a command tag or rows) that the protocol returns instead of
// the statement. Only an admin identity gets handled && err == nil.
type Administrator interface {
	Administer(ctx context.Context, id Identity, sql string) (resultSQL string, handled bool, err error)
}

// Auditor is an OPTIONAL interface of a QueryHandler. A Protocol that runs
// statements by itself (the Postgres wire protocol has its own DuckDB session)
// reports each one through it so every protocol lands in the same audit log.
// Audit must not block the caller for long and never fails the statement.
type Auditor interface {
	Audit(ctx context.Context, rec AuditRecord)
}
