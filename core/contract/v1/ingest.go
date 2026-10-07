package contract

import "context"

// InputView is the name under which Transactor.InTx exposes its input Stream
// to the SQL run inside the transaction.
const InputView = "duct_input"

// IngestState is what the core remembers between runs of one ingestion.
type IngestState struct {
	// Watermark is the highest value of the watermark column loaded by the
	// last successful run, as text. Empty on the first run or in full mode.
	Watermark string
}

// SQLPlan is a source expressed as SQL for the engine to execute.
type SQLPlan struct {
	// Setup statements run first (INSTALL/LOAD of extensions, ...). They must
	// be idempotent.
	Setup []string
	// Select is one SELECT statement producing the rows to load. It already
	// applies the incremental filter derived from IngestState.
	Select string
}

// SQLSource is an OPTIONAL interface of a KindSource module whose data can be
// read by the engine itself (DuckDB readers, ODBC scanner, ...). The module
// never touches data or the engine: it only returns SQL, so it stays decoupled
// from every other module. The core runs Setup, evaluates Select and loads the
// rows into Bronze. Modules that produce batches themselves (Bento) implement
// Source instead.
//
// Invariants: Plan is pure (no I/O) and deterministic for the same state; it
// is called after Init and may be called repeatedly.
type SQLSource interface {
	Module
	Plan(ctx context.Context, st IngestState) (SQLPlan, error)
}

// Tx is a handle bound to one engine connection inside a transaction.
type Tx interface {
	Exec(ctx context.Context, sql string, args ...any) error
	// Query streams a result; the caller must drain and Release it before the
	// next call on the same Tx.
	Query(ctx context.Context, sql string, args ...any) (Stream, error)
}

// Transactor is an OPTIONAL interface of an Engine. InTx runs fn inside one
// transaction on one connection: commit if fn returns nil, rollback
// otherwise. If input is non-nil it is exposed to the SQL in fn as the view
// InputView (consumed once; the engine does not Release input). This is how
// the core loads Arrow batches without any module touching the engine.
type Transactor interface {
	InTx(ctx context.Context, input Stream, fn func(ctx context.Context, tx Tx) error) error
}
