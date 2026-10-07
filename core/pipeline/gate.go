package pipeline

import (
	"context"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Gate lets another layer (data contracts, core/contracts) judge a refresh
// before it reaches the lake. Options.Gate is optional.
type Gate interface {
	// Governs reports whether the table (Table.ID form) has a contract.
	Governs(id string) bool
	// Expectations are extra per-row expectations merged into the table's own
	// (DROP ROW rules of the contract).
	Expectations(id string) []Expectation
	// Check runs on the staged result, already without dropped rows, exposed
	// as the relation rel (a quoted name). Violations are recorded in the run
	// history; a non-nil error is a FAIL UPDATE: the previous version stays.
	Check(ctx context.Context, eng contract.Engine, id, rel string) ([]Violation, error)
}
