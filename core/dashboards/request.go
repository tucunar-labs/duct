package dashboards

import (
	"fmt"
	"sort"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// State is what the viewer chose: filter values (by filter id) and the
// cross-filter selections (source chart id -> selected values of its first dim).
// Values come from the browser: Request validates them again.
type State struct {
	Filters map[string][]string
	Cross   map[string][]string
}

// CrossDim is the dimension a selection on the chart filters.
func (c *Chart) CrossDim() string {
	if len(c.Dims) == 0 {
		return ""
	}
	return c.Dims[0]
}

// Request is the semantic query of a chart under the state: its dims,
// measures, sort and limit, the static where, the dashboard filters that
// apply and the cross-filters of the OTHER charts.
func (d *Dashboard) Request(c *Chart, st State) (contract.SemanticRequest, error) {
	req := contract.SemanticRequest{
		Dims:     append([]string(nil), c.Dims...),
		Measures: append([]string(nil), c.Measures...),
		Limit:    c.Limit,
	}
	for _, s := range c.Sort {
		req.Order = append(req.Order, contract.SemanticOrder{By: s.By, Desc: s.Desc})
	}
	for _, w := range c.Where {
		req.Filters = append(req.Filters, contract.SemanticFilter{Field: w.Field, Op: w.Op, Value: w.Value})
	}
	for i := range d.Filters {
		f := &d.Filters[i]
		if !f.Applies(c.ID) {
			continue
		}
		cond, err := f.Conditions(st.Filters[f.ID])
		if err != nil {
			return req, err
		}
		req.Filters = append(req.Filters, cond...)
	}
	if c.Receives() {
		ids := make([]string, 0, len(st.Cross))
		for id := range st.Cross {
			ids = append(ids, id)
		}
		sort.Strings(ids) // stable request, stable cache key
		for _, id := range ids {
			src, ok := d.Chart(id)
			if !ok || !src.Emits() {
				return req, fmt.Errorf("cross-filter from unknown chart %q", id)
			}
			if id == c.ID || len(st.Cross[id]) == 0 {
				continue
			}
			vals, err := cleanValues(st.Cross[id])
			if err != nil {
				return req, err
			}
			req.Filters = append(req.Filters, inFilter(src.CrossDim(), vals))
		}
	}
	return req, nil
}

// ValuesRequest lists the values a select or multiselect offers: the
// dimension grouped by one measure (the filter's, or the first measure of a
// chart it applies to). It is never filtered by the rest of the state.
func (d *Dashboard) ValuesRequest(f *Filter) (contract.SemanticRequest, error) {
	m := f.Measure
	if m == "" {
		for i := range d.Charts {
			if f.Applies(d.Charts[i].ID) && len(d.Charts[i].Measures) > 0 {
				m = d.Charts[i].Measures[0]
				break
			}
		}
	}
	if m == "" {
		return contract.SemanticRequest{}, fmt.Errorf("filter %q: no measure to list its values", f.ID)
	}
	return contract.SemanticRequest{Dims: []string{f.Dimension}, Measures: []string{m}, Limit: 500,
		Order: []contract.SemanticOrder{{By: f.Dimension}}}, nil
}

// Conditions turns a filter value into semantic filters; no value, no filter.
// A daterange takes [from, to] (either may be empty).
func (f *Filter) Conditions(vals []string) ([]contract.SemanticFilter, error) {
	if f.Type == FilterDateRange {
		var from, to string
		if len(vals) > 0 {
			from = vals[0]
		}
		if len(vals) > 1 {
			to = vals[1]
		}
		return f.rangeConditions(from, to)
	}
	vals = nonEmpty(vals)
	if len(vals) == 0 {
		return nil, nil
	}
	vals, err := cleanValues(vals)
	if err != nil {
		return nil, fmt.Errorf("filter %q: %w", f.ID, err)
	}
	switch f.Type {
	case FilterSelect:
		return []contract.SemanticFilter{{Field: f.Dimension, Op: "=", Value: vals[0]}}, nil
	case FilterMultiselect:
		return []contract.SemanticFilter{inFilter(f.Dimension, vals)}, nil
	}
	return nil, fmt.Errorf("filter %q: unknown type %q", f.ID, f.Type)
}

// rangeConditions: from and to are YYYY-MM-DD or empty (open end).
func (f *Filter) rangeConditions(from, to string) ([]contract.SemanticFilter, error) {
	for _, v := range []string{from, to} {
		if v != "" && !dateRe.MatchString(v) {
			return nil, fmt.Errorf("filter %q: %q is not a YYYY-MM-DD date", f.ID, v)
		}
	}
	switch {
	case from != "" && to != "":
		return []contract.SemanticFilter{{Field: f.Dimension, Op: "between", Value: []any{from, to}}}, nil
	case from != "":
		return []contract.SemanticFilter{{Field: f.Dimension, Op: ">=", Value: from}}, nil
	case to != "":
		return []contract.SemanticFilter{{Field: f.Dimension, Op: "<=", Value: to}}, nil
	}
	return nil, nil
}

func inFilter(field string, vals []string) contract.SemanticFilter {
	if len(vals) == 1 {
		return contract.SemanticFilter{Field: field, Op: "=", Value: vals[0]}
	}
	list := make([]any, len(vals))
	for i, v := range vals {
		list[i] = v
	}
	return contract.SemanticFilter{Field: field, Op: "in", Value: list}
}

func nonEmpty(vs []string) []string {
	var out []string
	for _, v := range vs {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func cleanValues(vs []string) ([]string, error) {
	if len(vs) > maxValues {
		return nil, fmt.Errorf("at most %d values", maxValues)
	}
	for _, v := range vs {
		if len(v) > maxValueLen {
			return nil, fmt.Errorf("value longer than %d characters", maxValueLen)
		}
	}
	return vs, nil
}
