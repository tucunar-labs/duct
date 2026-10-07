package contracts

import (
	"context"
	"fmt"
	"math"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pipeline"
)

// Enforcer applies contracts to `duct run`: it implements pipeline.Gate.
//
//   - schema: columns and types of the refreshed result must match the contract
//     (missing, mistyped and undeclared columns all fail);
//   - rules with severity critical (or error), the implicit required/unique
//     checks of the schema, and rules that cannot be measured: FAIL UPDATE;
//   - any other severity: recorded as a warn violation, the refresh goes on;
//   - rules marked duct.on_violation: drop_row: DROP ROW per row.
//
// Everything is evaluated on the staged result, before the lake is touched.
type Enforcer struct {
	plans map[string]*planned
}

type planned struct {
	c    *Contract
	plan *Plan
}

var _ pipeline.Gate = (*Enforcer)(nil)

// NewEnforcer compiles every contract; a malformed one is an error.
func NewEnforcer(cs []*Contract) (*Enforcer, error) {
	e := &Enforcer{plans: map[string]*planned{}}
	for _, c := range cs {
		p, err := c.Plan()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", c.File, err)
		}
		e.plans[c.Table] = &planned{c: c, plan: p}
	}
	return e, nil
}

// Governs implements pipeline.Gate.
func (e *Enforcer) Governs(id string) bool { _, ok := e.plans[id]; return ok }

// Expectations implements pipeline.Gate: the per-row rules.
func (e *Enforcer) Expectations(id string) []pipeline.Expectation {
	p := e.plans[id]
	if p == nil {
		return nil
	}
	var out []pipeline.Expectation
	for _, ch := range p.plan.Checks {
		if expr, ok := ch.RowExpr(); ok && ch.DropRow {
			out = append(out, pipeline.Expectation{Name: "contract:" + ch.Name, Expr: expr, Action: pipeline.DropRow})
		}
	}
	return out
}

// Check implements pipeline.Gate.
func (e *Enforcer) Check(ctx context.Context, eng contract.Engine, id, rel string) ([]pipeline.Violation, error) {
	p := e.plans[id]
	if p == nil {
		return nil, nil
	}
	var viol []pipeline.Violation
	var fail []string
	actual, err := ActualColumns(ctx, eng, rel)
	if err != nil {
		return nil, fmt.Errorf("contract %s: %w", p.c.File, err)
	}
	if issues := p.c.SchemaIssues(actual); len(issues) > 0 {
		viol = append(viol, pipeline.Violation{Name: "contract:schema", Action: pipeline.FailUpdate, Count: int64(len(issues))})
		fail = append(fail, "schema: "+strings.Join(issues, "; "))
	}
	for _, ch := range p.plan.Checks {
		if ch.DropRow {
			continue // enforced per row by Expectations
		}
		r := measure(ctx, eng, ch, rel)
		if !r.Failed() {
			continue
		}
		act, count := pipeline.Warn, int64(1)
		if r.Err == nil {
			count = max(1, int64(math.Round(r.Value)))
		}
		var msg string
		if r.Err != nil {
			msg = fmt.Sprintf("%s (%v)", ch.Name, r.Err)
		} else {
			msg = fmt.Sprintf("%s = %s, must be %s", ch.Name, fmtNum(r.Value), ch.Expect)
		}
		if ch.Blocking {
			act = pipeline.FailUpdate
			fail = append(fail, msg)
		}
		viol = append(viol, pipeline.Violation{Name: "contract:" + ch.Name, Action: act, Count: count})
	}
	if len(fail) > 0 {
		return viol, fmt.Errorf("contract %s violated: %s", p.c.File, strings.Join(fail, "; "))
	}
	return viol, nil
}
