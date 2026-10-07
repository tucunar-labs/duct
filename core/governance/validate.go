package governance

import (
	"context"
	"fmt"
	"strings"
)

// ValidateRowFilter checks a row-filter predicate over a relation exactly as
// SetRowFilter does (structure, deny list, binds against the lake) without
// storing it. Other features that take a user-written predicate over a table
// (data sharing's partition filters) reuse it.
func (s *Store) ValidateRowFilter(ctx context.Context, relation, predicate string) error {
	schema, table, err := NormalizeRelation(relation)
	if err != nil {
		return err
	}
	predicate = strings.TrimSpace(predicate)
	if predicate == "" {
		return fmt.Errorf("empty predicate")
	}
	base := ident(schema) + "." + ident(table)
	if err := s.checkPolicy(ctx, predicate, schema, table); err != nil {
		return fmt.Errorf("row filter: %w", err)
	}
	if err := s.checkExpr(ctx, "SELECT * FROM "+base+" WHERE ("+predicate+"\n)"); err != nil {
		return fmt.Errorf("row filter: %w", err)
	}
	return nil
}
