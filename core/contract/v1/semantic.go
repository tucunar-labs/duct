package contract

import (
	"context"
	"errors"
)

// ErrNoSemanticModel is returned by SemanticQuerier methods when the server
// has no semantic model (semantic/*.ossie.yaml).
var ErrNoSemanticModel = errors.New("no semantic model")

// SemanticFilter is one structured condition of a SemanticRequest. Op is one
// of = != < <= > >= in not_in like not_like between is_null is_not_null; Value
// is a scalar, a []any for in/not_in or a [low, high] []any for between. It is
// never SQL.
//
// Set Field to filter rows (WHERE) or Metric to filter groups by the value of
// a metric (HAVING; like/not_like do not apply); never both.
type SemanticFilter struct {
	Field  string
	Op     string
	Value  any
	Metric string
}

// SemanticOrder sorts by a requested dimension or measure.
type SemanticOrder struct {
	By   string
	Desc bool
}

// SemanticRequest is a semantic query (the arguments of query_metrics).
type SemanticRequest struct {
	Dims     []string
	Measures []string
	Filters  []SemanticFilter
	Limit    int
	Order    []SemanticOrder
}

// SemanticColumn describes one result column: Kind is "dimension" or "measure".
type SemanticColumn struct {
	Name     string
	Kind     string
	Datatype string
}

// SemanticResult is the outcome of SemanticQuerier.SemanticQuery. The caller
// must Release Stream. SQL is the statement that ran, over the role views.
type SemanticResult struct {
	Stream  Stream
	Columns []SemanticColumn
	SQL     string
}

// SemanticQuerier is an OPTIONAL interface of a QueryHandler (like Auditor
// or Administrator): protocols that draw metrics (dashboards, MCP) ask for them
// instead of writing SQL. SemanticQuery compiles the request for the
// identity's role and runs it through the governed path (guard, role views,
// audit) as that identity; it returns an error wrapping ErrNoSemanticModel
// when the server has no model. LakeSnapshot returns the id of the latest
// DuckLake snapshot (any change to data, views or policies bumps it), which
// callers use as a cache-invalidation key.
type SemanticQuerier interface {
	SemanticQuery(ctx context.Context, id Identity, req SemanticRequest) (*SemanticResult, error)
	LakeSnapshot(ctx context.Context) (string, error)
}
