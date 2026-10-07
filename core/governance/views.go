package governance

import (
	"context"
	"fmt"
	"sort"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Report is the outcome of Regenerate.
type Report struct {
	Roles   int
	Views   int
	Dropped int      // stale views and schemas removed
	Skipped []string // granted relations that no longer exist
}

// Regenerate makes the lake match the grants: for every role it creates the
// schema role_<name> with one view per granted relation (applying the row
// filter and the masks), replaces changed views and drops stale views and the
// schemas of deleted roles. It is idempotent and cheap; call it after any
// change to roles, grants, filters or masks and after every pipeline refresh
// (a refreshed table may have new columns). The engine must not be locked.
func (s *Store) Regenerate(ctx context.Context) (Report, error) {
	var rep Report
	roles, err := s.rows(ctx, `SELECT name FROM duct.duct_roles ORDER BY name`)
	if err != nil {
		return rep, err
	}
	keep := map[string]bool{}
	for _, r := range roles {
		role := r[0]
		schema := contract.RoleSchema(role)
		keep[schema] = true
		rep.Roles++
		stmts := []string{"CREATE SCHEMA IF NOT EXISTS " + ident(lakeCatalog) + "." + ident(schema)}
		want := map[string]bool{}

		grants, err := s.rows(ctx, `SELECT relation FROM duct.duct_grants WHERE role = ? AND privilege = ? ORDER BY relation`, role, privSelect)
		if err != nil {
			return rep, err
		}
		for _, g := range grants {
			schemaName, table, err := NormalizeRelation(g[0])
			if err != nil {
				return rep, err
			}
			cols, err := s.columns(ctx, schemaName, table)
			if err != nil {
				return rep, err
			}
			if len(cols) == 0 {
				rep.Skipped = append(rep.Skipped, role+": "+g[0])
				continue
			}
			sql, err := s.viewSQL(ctx, role, schemaName, table, cols)
			if err != nil {
				return rep, err
			}
			want[table] = true
			stmts = append(stmts, sql)
			rep.Views++
		}
		stale, err := s.rows(ctx, `SELECT view_name FROM duckdb_views() WHERE database_name = 'lake' AND schema_name = `+lit(schema)+` AND NOT internal`)
		if err != nil {
			return rep, err
		}
		for _, v := range stale {
			if !want[v[0]] {
				stmts = append(stmts, "DROP VIEW IF EXISTS "+ident(lakeCatalog)+"."+ident(schema)+"."+ident(v[0]))
				rep.Dropped++
			}
		}
		if err := s.eng.ExecTx(ctx, stmts...); err != nil {
			return rep, fmt.Errorf("role %s: %w", role, err)
		}
	}
	// Schemas of roles that no longer exist.
	all, err := s.rows(ctx, `SELECT schema_name FROM duckdb_schemas() WHERE database_name = 'lake' AND starts_with(schema_name, `+lit(contract.RoleSchemaPrefix)+`)`)
	if err != nil {
		return rep, err
	}
	for _, a := range all {
		if !keep[a[0]] {
			if err := s.eng.Exec(ctx, "DROP SCHEMA "+ident(lakeCatalog)+"."+ident(a[0])+" CASCADE"); err != nil {
				return rep, err
			}
			rep.Dropped++
		}
	}
	sort.Strings(rep.Skipped)
	return rep, nil
}

// viewSQL builds the view of one relation for one role. Relations inside the
// view are schema-qualified but not catalog-qualified: the catalog is named
// differently in other DuckDB instances (serve-pg attaches it as "ducklake").
func (s *Store) viewSQL(ctx context.Context, role, schema, table string, cols []string) (string, error) {
	rel := schema + "." + table
	filters, err := s.rows(ctx, `SELECT predicate FROM duct.duct_row_filters WHERE role = ? AND relation = ?`, role, rel)
	if err != nil {
		return "", err
	}
	masks, err := s.rows(ctx, `SELECT column_name, expr FROM duct.duct_masks WHERE role = ? AND relation = ?`, role, rel)
	if err != nil {
		return "", err
	}
	maskOf := map[string]string{}
	for _, m := range masks {
		maskOf[strings.ToLower(m[0])] = m[1]
	}
	base := ident(schema) + "." + ident(table)
	inner := "SELECT * FROM " + base
	if len(filters) > 0 {
		// The newline closes a trailing -- comment before the parenthesis.
		inner += " WHERE (" + filters[0][0] + "\n)"
	}
	// The mask expressions read the base columns (the row filter's subquery
	// has no aliases to shadow them).
	sel := "*"
	if len(maskOf) > 0 {
		parts := make([]string, len(cols))
		for i, c := range cols {
			if e, ok := maskOf[strings.ToLower(c)]; ok {
				parts[i] = "(" + e + "\n) AS " + ident(c)
			} else {
				parts[i] = ident(c)
			}
		}
		sel = strings.Join(parts, ", ")
	}
	return "CREATE OR REPLACE VIEW " + ident(lakeCatalog) + "." + ident(contract.RoleSchema(role)) + "." + ident(table) +
		" AS SELECT " + sel + " FROM (" + inner + ") AS " + ident("duct_src"), nil
}
