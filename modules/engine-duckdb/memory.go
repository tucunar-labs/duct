package engineduckdb

import (
	"context"
	"database/sql"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// initMemory opens a plain in-memory DuckDB with no lake, no Duct state and
// no extensions (Init config key "memory": true, nothing else). The default
// catalog is the in-memory one ("memory"), so unqualified names resolve
// there. `duct alerts` keeps the last minutes of each Kafka stream in such an
// engine (ADR 0009). The Engine and Transactor methods work as usual; the
// lake and Duct catalogs simply do not exist.
func (e *Engine) initMemory(ctx context.Context) error {
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return fmt.Errorf("engine-duckdb: open: %w", err)
	}
	db := sql.OpenDB(connector)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("engine-duckdb: connect: %w", err)
	}
	e.mu.Lock()
	e.db = db
	e.mu.Unlock()
	return nil
}
