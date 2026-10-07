package contract

import "context"

// Limiter is an OPTIONAL interface of a QueryHandler (like Auditor): the
// per-role quotas of ADR 0013. A Protocol that runs statements on its own
// DuckDB session (serve-pg) calls Admit before each statement so it shares the
// concurrency counter of the handler with HTTP, MCP and the web.
type Limiter interface {
	// Admit takes a concurrency slot for the identity's role. The error of a
	// refusal implements SQLState() string (53400) and is a quota error.
	Admit(ctx context.Context, id Identity) (Admission, error)
}

// Admission is one admitted statement. Release gives the slot back (idempotent).
type Admission interface {
	// MaxRows is the most rows the statement may return; 0 means unlimited.
	MaxRows() int64
	// RowsExceeded counts the rejection and returns the error to report when
	// the statement tried to return more than MaxRows rows.
	RowsExceeded() error
	Release()
}
