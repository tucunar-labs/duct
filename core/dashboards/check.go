package dashboards

import (
	"errors"
	"fmt"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Check compiles every query the dashboard can issue (each chart alone, with
// each filter, with a selection from each emitting chart, and the value lists
// of the pickers) with compile, typically semantic.Model.Query. It reports
// unknown names and filters that cannot reach a chart (a dimension on the
// "many" side of a fact), the cases that would show an error panel at runtime.
func (d *Dashboard) Check(compile func(contract.SemanticRequest) error) error {
	var errs []error
	try := func(what string, req contract.SemanticRequest, err error) {
		if err == nil {
			err = compile(req)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", what, err))
		}
	}
	for i := range d.Charts {
		c := &d.Charts[i]
		req, err := d.Request(c, State{})
		try(fmt.Sprintf("chart %q", c.ID), req, err)
		for j := range d.Filters {
			f := &d.Filters[j]
			if !f.Applies(c.ID) {
				continue
			}
			req, err := d.Request(c, State{Filters: map[string][]string{f.ID: sample(f)}})
			try(fmt.Sprintf("chart %q with filter %q", c.ID, f.ID), req, err)
		}
		for j := range d.Charts {
			src := &d.Charts[j]
			if src.ID == c.ID || !src.Emits() || !c.Receives() {
				continue
			}
			req, err := d.Request(c, State{Cross: map[string][]string{src.ID: {"x"}}})
			try(fmt.Sprintf("chart %q with the cross-filter of %q (set cross_filter: false on one of them)", c.ID, src.ID), req, err)
		}
	}
	for i := range d.Filters {
		f := &d.Filters[i]
		if f.Type == FilterDateRange {
			continue
		}
		req, err := d.ValuesRequest(f)
		try(fmt.Sprintf("filter %q values", f.ID), req, err)
	}
	return errors.Join(errs...)
}

func sample(f *Filter) []string {
	if f.Type == FilterDateRange {
		return []string{"2020-01-01", "2020-12-31"}
	}
	return []string{"x"}
}
