package contract

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Kind classifies a module by the contract it implements. It is the first
// half of a module's registry key (Kind + name).
type Kind string

// Module kinds defined by contract v1.
const (
	KindSource       Kind = "source"
	KindSink         Kind = "sink"
	KindTaskType     Kind = "task"
	KindAlertChannel Kind = "alert.channel"
	KindProtocol     Kind = "protocol"
	KindCompiler     Kind = "compiler"
	KindAuthProvider Kind = "auth"
	KindEngine       Kind = "engine"
)

// Config is the free-form configuration handed to a module in Init. Keys are
// module-defined; values come from YAML/JSON/env decoding (string, float64,
// bool, []any, map[string]any). Modules must not retain or mutate the map.
type Config map[string]any

// Module is the lifecycle shared by every extension.
//
// Invariants:
//   - Name is constant, non-empty, lowercase, and matches the name used at
//     registration.
//   - Init is called exactly once, before any other method, and must not
//     block beyond ctx.
//   - Close is idempotent, safe to call without Init, and releases everything
//     Init acquired.
type Module interface {
	Name() string
	Init(ctx context.Context, cfg Config) error
	Close() error
}

// Batch is the unit of data exchanged between modules: an Apache Arrow
// record batch (the format DuckDB, Bento bridges and `duct serve` speak).
//
// Ownership: a Batch passed to a callee is valid only for the duration of
// the call. A callee that keeps it must call Retain and later Release. The
// producer releases its own reference after the call returns.
type Batch = arrow.RecordBatch

// Stream is a sequence of batches sharing one schema. The receiver must
// call Release when done.
type Stream = array.RecordReader

// Source produces data from an external system.
//
// Invariants: Read calls emit sequentially, never concurrently; it stops and
// returns emit's error if emit fails; it returns ctx.Err() when cancelled;
// every emitted Batch is non-empty and has the same schema within a call.
type Source interface {
	Module
	Read(ctx context.Context, emit func(Batch) error) error
}

// Sink consumes batches.
//
// Invariants: Write is atomic per Batch (all rows or error) and does not keep
// the Batch after returning unless it retains it; Write may be
// called repeatedly after Init and must tolerate retries of the same Batch
// only if the module documents idempotency.
type Sink interface {
	Module
	Write(ctx context.Context, b Batch) error
}

// TaskSpec is a unit of work for a TaskType.
type TaskSpec struct {
	Name   string // task name inside the pipeline or job
	Params Config // task-type specific parameters
}

// TaskResult is the outcome of a successful task run.
type TaskResult struct {
	Output string // short human-readable summary or log tail
}

// TaskType runs one kind of orchestrator task (python, shell, notebook...).
//
// Invariants: Run returns only after the work stopped; on ctx cancellation it
// stops the work and returns ctx.Err(); a non-nil error means the task
// failed and the orchestrator may retry according to its policy.
type TaskType interface {
	Module
	Run(ctx context.Context, spec TaskSpec) (TaskResult, error)
}

// AlertState is the lifecycle state of an alert.
type AlertState string

// Alert states. A channel is notified on both transitions.
const (
	AlertFiring   AlertState = "firing"
	AlertResolved AlertState = "resolved"
)

// Alert is the notification payload delivered to an AlertChannel.
// Rule+Key identify the alert for deduplication.
type Alert struct {
	Rule    string
	Key     string
	State   AlertState
	Message string
	Labels  map[string]string
	At      time.Time
}

// AlertChannel delivers alerts to a human or system (webhook, ntfy, ...).
//
// Invariants: Notify is safe for concurrent use after Init; it returns
// promptly when ctx is cancelled; it returns an error if delivery was not
// confirmed, and never panics on a zero-value optional field.
type AlertChannel interface {
	Module
	Notify(ctx context.Context, a Alert) error
}

// Identity is an authenticated principal.
type Identity struct {
	User string
	Role string
}

// QueryHandler is what the core exposes to a Protocol. It always executes
// as the given identity, through the per-role views. Results are streamed;
// the caller must Release the Stream.
type QueryHandler interface {
	Query(ctx context.Context, id Identity, sql string) (Stream, error)
}

// Protocol is a network listener of `duct serve` (Postgres wire, HTTP...).
//
// Invariants: Serve blocks until ctx is cancelled or a fatal error occurs,
// then shuts down gracefully and returns nil on a clean cancellation. A
// Protocol never touches data except through the QueryHandler.
type Protocol interface {
	Module
	Serve(ctx context.Context, h QueryHandler, auth AuthProvider) error
}

// Artifact is one compiled output, typically a named SQL statement.
type Artifact struct {
	Name string
	SQL  string
}

// Compiler turns a declarative source (semantic layer, pipeline, dashboard)
// into SQL artifacts.
//
// Invariants: Compile is pure and deterministic (same input, same output),
// does no I/O, and reports problems as errors, not partial results.
type Compiler interface {
	Module
	Compile(ctx context.Context, src []byte) ([]Artifact, error)
}

// Credentials carry what a client presented to log in.
type Credentials struct {
	Scheme string // e.g. "password", "bearer"
	User   string
	Secret string
}

// AuthProvider maps credentials to an Identity.
//
// Invariants: Authenticate returns ErrUnauthenticated (wrapped or not) for
// bad credentials, any other error means the provider failed; it never
// logs Secret.
type AuthProvider interface {
	Module
	Authenticate(ctx context.Context, c Credentials) (Identity, error)
}

// Engine is the SQL compute engine over the lake (DuckDB today, ADR 0002).
// The core never touches data except through it.
//
// Init config is engine-defined; the DuckDB engine documents its keys in
// modules/engine-duckdb/AGENTS.md. After Init the lake is attached under the
// catalog name "lake" and is the default catalog, so unqualified table names
// resolve to lake tables. Duct-owned tables (ADR 0001) live in the separate
// catalog "duct" and are always referenced as duct.<table>.
//
// Invariants: methods are safe for concurrent use after Init; Query returns
// a Stream the caller must Release exactly once (it holds a connection until
// then); ExecTx applies all statements or none; every method returns
// ctx.Err() on cancellation.
type Engine interface {
	Module
	// Exec runs one statement (or several, separated by semicolons) that
	// does not need its result. args bind positional parameters.
	Exec(ctx context.Context, sql string, args ...any) error
	// ExecTx runs the statements in a single transaction.
	ExecTx(ctx context.Context, stmts ...string) error
	// Query runs a statement and streams its result as Arrow batches.
	Query(ctx context.Context, sql string, args ...any) (Stream, error)
}
