package governance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// AdminRole is the built-in administrator role. A user whose role is
// duct_admin may run governance statements over `duct serve`; nobody else can,
// whatever grants the role has. Create it like any role (`duct role create
// duct_admin`) and give it users; it can also be granted tables to read.
const AdminRole = "duct_admin"

// IsAdmin reports whether the identity may run governance statements.
func IsAdmin(id contract.Identity) bool { return id.Role == AdminRole }

// Result is the outcome of a governance statement: a command tag, and for SHOW
// a table.
type Result struct {
	Tag     string // "CREATE ROLE", "GRANT", ...
	Columns []string
	Rows    [][]string
}

// SQL returns a constant SELECT that yields the result (every cell text), so a
// protocol can return it through its normal result path. It is built here from
// escaped literals only; callers run it without further checks.
func (r *Result) SQL() string {
	if len(r.Columns) == 0 {
		return "SELECT " + lit(r.Tag) + " AS result"
	}
	if len(r.Rows) == 0 {
		cols := make([]string, len(r.Columns))
		for i, c := range r.Columns {
			cols[i] = "CAST(NULL AS VARCHAR) AS " + ident(c)
		}
		return "SELECT " + strings.Join(cols, ", ") + " WHERE false"
	}
	rows := make([]string, len(r.Rows))
	for i, row := range r.Rows {
		cells := make([]string, len(row))
		for j, c := range row {
			cells[j] = lit(c)
		}
		rows[i] = "(" + strings.Join(cells, ", ") + ")"
	}
	cols := make([]string, len(r.Columns))
	for i, c := range r.Columns {
		cols[i] = ident(c)
	}
	return "SELECT * FROM (VALUES " + strings.Join(rows, ", ") + ") AS t(" + strings.Join(cols, ", ") + ")"
}

// Execute runs a parsed governance statement with the same effects as the CLI
// (`duct role|grant|policy`): it changes the Duct tables and then regenerates
// the role views. The engine must be one that can write the lake (the locked
// engine of `duct serve` can; ADR 0007 only blocks external access).
func (s *Store) Execute(ctx context.Context, st *Stmt) (*Result, error) {
	res := &Result{}
	var err error
	switch st.Kind {
	case KindCreateRole:
		res.Tag, err = "CREATE ROLE", s.CreateRole(ctx, st.Role)
	case KindDropRole:
		res.Tag, err = "DROP ROLE", s.DropRole(ctx, st.Role)
	case KindGrant:
		res.Tag, err = "GRANT", s.Grant(ctx, st.Role, st.Relation)
	case KindRevoke:
		res.Tag, err = "REVOKE", s.Revoke(ctx, st.Role, st.Relation)
	case KindGrantRole:
		res.Tag, err = "GRANT ROLE", s.SetUserRole(ctx, st.User, st.Role)
		if err == nil { // no view changes: the role is resolved at login
			return res, nil
		}
	case KindShowGrants:
		return s.showGrants(ctx, st)
	case KindSetRowFilter:
		res.Tag, err = "ALTER TABLE SET ROW FILTER", s.SetRowFilter(ctx, st.Role, st.Relation, st.Expr)
	case KindDropRowFilter:
		res.Tag, err = "ALTER TABLE DROP ROW FILTER", s.SetRowFilter(ctx, st.Role, st.Relation, "")
	case KindSetMask:
		res.Tag, err = "ALTER TABLE SET MASK", s.SetMask(ctx, st.Role, st.Relation, st.Column, st.Expr)
	case KindDropMask:
		res.Tag, err = "ALTER TABLE DROP MASK", s.SetMask(ctx, st.Role, st.Relation, st.Column, "")
	default:
		return nil, fmt.Errorf("unsupported governance statement %q", st.Kind)
	}
	if err != nil {
		return nil, err
	}
	if _, err := s.Regenerate(ctx); err != nil {
		return nil, fmt.Errorf("regenerate role views: %w", err)
	}
	return res, nil
}

// ExecuteSQL parses and runs one governance statement. It returns
// ErrNotGovernance (unwrapped) for any other text.
func (s *Store) ExecuteSQL(ctx context.Context, sql string) (*Result, error) {
	st, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	return s.Execute(ctx, st)
}

func (s *Store) showGrants(ctx context.Context, st *Stmt) (*Result, error) {
	rel := ""
	if st.Relation != "" {
		sch, tbl, err := NormalizeRelation(st.Relation)
		if err != nil {
			return nil, err
		}
		rel = sch + "." + tbl
	}
	if st.Role != "" {
		if err := s.requireRole(ctx, st.Role); err != nil {
			return nil, err
		}
	}
	roles, err := s.Roles(ctx)
	if err != nil {
		return nil, err
	}
	res := &Result{Tag: "SHOW GRANTS", Columns: []string{"role", "privilege", "object", "row_filter", "masks"}}
	for _, r := range roles {
		if st.Role != "" && r.Name != st.Role {
			continue
		}
		for _, g := range r.Grants {
			if rel != "" && g != rel {
				continue
			}
			var masks []string
			for k, e := range r.Masks {
				if c, ok := strings.CutPrefix(k, g+"."); ok {
					masks = append(masks, c+" = "+e)
				}
			}
			sort.Strings(masks)
			res.Rows = append(res.Rows, []string{r.Name, "SELECT", g, r.Filters[g], strings.Join(masks, "; ")})
		}
	}
	return res, nil
}

// Administer implements contract.Administrator. Governance statements are
// recognised BEFORE the read-only and role-schema checks of the normal path
// and are allowed only for the admin role; anyone else is denied (the caller
// audits the refusal). Statements that are not governance are not handled.
func (h *Handler) Administer(ctx context.Context, id contract.Identity, sql string) (string, bool, error) {
	st, perr := Parse(sql)
	if errors.Is(perr, ErrNotGovernance) {
		return "", false, nil
	}
	if !IsAdmin(id) { // also hides syntax details from non-admins
		return "", true, denied("governance statements require the %s role", AdminRole)
	}
	if perr != nil {
		return "", true, perr
	}
	if h.opt.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.opt.Timeout)
		defer cancel()
	}
	h.adminMu.Lock() // one change at a time: Regenerate is not meant to interleave
	defer h.adminMu.Unlock()
	res, err := NewStore(h.eng).Execute(ctx, st)
	if err != nil {
		return "", true, err
	}
	return res.SQL(), true, nil
}

var _ contract.Administrator = (*Handler)(nil)
