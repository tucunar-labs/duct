package contracts

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/alonsoburon/duct/core/governance"
)

var roleNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)

// RolePlan is what the contract's roles[] give one Duct role on the table.
type RolePlan struct {
	Role   string
	Filter string            // duct.row_filter, "" = none
	Masks  map[string]string // duct.mask.<column> -> SQL expression
}

// rolePlans reads roles[]: access "read" (or empty) becomes a SELECT grant;
// any other access is not applied (Duct has no write privilege).
func (c *Contract) rolePlans() ([]RolePlan, error) {
	cols := map[string]bool{}
	for _, col := range c.Columns() {
		cols[strings.ToLower(col.Name)] = true
	}
	var out []RolePlan
	var errs []string
	for _, r := range c.DC.Roles {
		switch strings.ToLower(r.Access) {
		case "", "read", "select":
		default:
			continue
		}
		if !roleNameRe.MatchString(r.Role) {
			errs = append(errs, fmt.Sprintf("role %q: use lowercase letters, digits and _ (max 40, start with a letter)", r.Role))
			continue
		}
		rp := RolePlan{Role: r.Role, Masks: map[string]string{}}
		for _, cp := range r.CustomProperties {
			if !strings.HasPrefix(cp.Property, extPrefix) {
				continue
			}
			if !knownExtension("role", cp.Property) {
				errs = append(errs, fmt.Sprintf("role %q: unknown extension %q", r.Role, cp.Property))
				continue
			}
			v := strings.TrimSpace(fmt.Sprint(cp.Value))
			if cp.Property == extRowFilter {
				rp.Filter = v
				continue
			}
			col := strings.ToLower(strings.TrimPrefix(cp.Property, extMaskPrefix))
			if !cols[col] {
				errs = append(errs, fmt.Sprintf("role %q: mask on unknown column %q", r.Role, col))
				continue
			}
			rp.Masks[col] = v
		}
		out = append(out, rp)
	}
	if len(errs) > 0 {
		return out, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return out, nil
}

// skippedRoles lists roles whose access Duct does not apply.
func (c *Contract) skippedRoles() []string {
	var out []string
	for _, r := range c.DC.Roles {
		switch strings.ToLower(r.Access) {
		case "", "read", "select":
		default:
			out = append(out, fmt.Sprintf("role %s: access %q is not applied (Duct only grants SELECT)", r.Role, r.Access))
		}
	}
	return out
}

// ApplyRoles makes the contract the source of truth for the roles it names on
// its table: it creates a missing role, grants SELECT, sets or clears the row
// filter and sets the declared masks (clearing the other masks of that role on
// the table). Roles the contract does not name are untouched. It returns one
// line per change. The table must exist in the lake. Callers regenerate the
// role views afterwards (governance Store.Regenerate).
func ApplyRoles(ctx context.Context, st *governance.Store, c *Contract) ([]string, error) {
	plan, err := c.Plan()
	if err != nil {
		return nil, err
	}
	infos, err := st.Roles(ctx)
	if err != nil {
		return nil, err
	}
	have := map[string]governance.RoleInfo{}
	for _, ri := range infos {
		have[ri.Name] = ri
	}
	var log []string
	rel := c.Table
	for _, rp := range plan.Roles {
		ri, exists := have[rp.Role]
		if !exists {
			if err := st.CreateRole(ctx, rp.Role); err != nil {
				return log, err
			}
			log = append(log, "create role "+rp.Role)
		}
		granted := false
		for _, g := range ri.Grants {
			granted = granted || g == rel
		}
		if !granted {
			if err := st.Grant(ctx, rp.Role, rel); err != nil {
				return log, fmt.Errorf("%s: %w", c.File, err)
			}
			log = append(log, fmt.Sprintf("grant select on %s to %s", rel, rp.Role))
		}
		if ri.Filters[rel] != rp.Filter {
			if err := st.SetRowFilter(ctx, rp.Role, rel, rp.Filter); err != nil {
				return log, fmt.Errorf("%s: %w", c.File, err)
			}
			if rp.Filter == "" {
				log = append(log, fmt.Sprintf("clear row filter of %s on %s", rp.Role, rel))
			} else {
				log = append(log, fmt.Sprintf("row filter of %s on %s: %s", rp.Role, rel, rp.Filter))
			}
		}
		prefix := rel + "."
		var drop []string
		for k := range ri.Masks {
			if col, ok := strings.CutPrefix(k, prefix); ok {
				if _, keep := rp.Masks[col]; !keep {
					drop = append(drop, col)
				}
			}
		}
		sort.Strings(drop)
		for _, col := range drop {
			if err := st.SetMask(ctx, rp.Role, rel, col, ""); err != nil {
				return log, err
			}
			log = append(log, fmt.Sprintf("clear mask of %s on %s.%s", rp.Role, rel, col))
		}
		cols := make([]string, 0, len(rp.Masks))
		for col := range rp.Masks {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		for _, col := range cols {
			if ri.Masks[prefix+col] == rp.Masks[col] {
				continue
			}
			if err := st.SetMask(ctx, rp.Role, rel, col, rp.Masks[col]); err != nil {
				return log, fmt.Errorf("%s: %w", c.File, err)
			}
			log = append(log, fmt.Sprintf("mask of %s on %s.%s: %s", rp.Role, rel, col, rp.Masks[col]))
		}
	}
	return log, nil
}
