package contract

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
)

// ScalarFunction is a user-defined scalar function (ADR 0005) that the engine
// evaluates by handing whole batches to Call, so the body can live outside the
// engine's process (a Python worker, an executable).
//
// ArgTypes and ReturnType are SQL type names of the engine (DuckDB: BOOLEAN,
// TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR). Call receives one
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
