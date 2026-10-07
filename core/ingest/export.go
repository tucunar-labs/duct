package ingest

import (
	"context"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// EvolveSchema adds to target the columns of stage that it lacks (ALTER TABLE
// ... ADD COLUMN), inside tx. Shared with core/stream so every path into
// Bronze evolves schemas the same way.
func EvolveSchema(ctx context.Context, tx contract.Tx, target, stage string) error {
	return evolve(ctx, tx, target, stage)
}

// EnsureSchema is CREATE SCHEMA IF NOT EXISTS, retried on a commit conflict.
func EnsureSchema(ctx context.Context, eng contract.Engine, name string) error {
	return ensureSchema(ctx, eng, name)
}
