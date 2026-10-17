package contract

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
)

// ScalarFunction is a user-defined scalar function (ADR 0005) that the engine
// evaluates by handing whole batches to Call, so the body can live outside the
// engine's process (a Python worker, an executable).
//
// ArgTypes and ReturnType are SQL type names of the engine in the grammar of
// ParseSQLType (BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
// VARCHAR, DATE, TIMESTAMP, DECIMAL(p,s), T[], STRUCT(a T, ...)). Call receives one
// Arrow array per argument (all the same length, NULLs included) and returns
// one array of the same length, of the Arrow type matching ReturnType. The
// engine applies the default NULL handling itself: a row with a NULL argument
// yields NULL whatever Call returns for it.
type ScalarFunction struct {
	Name       string
	ArgTypes   []string
	ReturnType string
	Call       func(ctx context.Context, args []arrow.Array) (arrow.Array, error)
	// Close, if set, is called once when the Engine closes (stop a worker process).
	Close func() error
}

// ScalarRegistrar is an OPTIONAL interface of an Engine: it can register
// scalar functions that are then callable from any statement it runs. A
// function stays registered until the Engine is closed, which also calls its Close.
type ScalarRegistrar interface {
	RegisterScalar(ctx context.Context, f ScalarFunction) error
}

// TableColumn is one column of a TableFunction.
type TableColumn struct {
	Name string
	Type string // ParseSQLType grammar
}

// TableFunction is a user-defined table function (`SELECT * FROM f(1, 'x')`).
// Its arguments are constants of the query: Call receives them as arrays of one
// element (a NULL argument is a null element) and returns the whole result as
// Arrow batches whose columns are Columns, in order, with the Arrow type of each
// column's SQL type. The engine slices it into its own chunks.
type TableFunction struct {
	Name     string
	ArgTypes []string
	Columns  []TableColumn
	Call     func(ctx context.Context, args []arrow.Array) ([]arrow.RecordBatch, error)
	// Close, if set, is called once when the Engine closes.
	Close func() error
}

// TableRegistrar is an OPTIONAL interface of an Engine: it can register table
// functions. Like scalar ones they stay registered until the Engine is closed.
type TableRegistrar interface {
	RegisterTable(ctx context.Context, f TableFunction) error
}
