package contracts

import (
	"context"
	"fmt"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/pipeline"
)

// Refresh is the outcome of RunPipeline.
type Refresh struct {
	Report *pipeline.Report
	// Log lists what was applied after the refresh (contract roles, role views)
	// and Warnings what failed without stopping it.
	Log      []string
	Warnings []string
}

// RunPipeline is the one way to refresh pipelines/ tables, shared by `duct run`
// and the orchestrator: it loads the project's contracts (a broken one stops
// the run), refreshes with the contract Gate, syncs the roles of every
// contract whose table refreshed and rebuilds the per-role views. A nil
// Report means nothing ran.
func RunPipeline(ctx context.Context, eng contract.Engine, root string, tables []pipeline.Table, opt pipeline.Options) (Refresh, error) {
	var out Refresh
	cs, err := Load(root)
	if err != nil {
		return out, err
	}
	if len(cs) > 0 {
		if opt.Gate, err = NewEnforcer(cs); err != nil {
			return out, err
		}
	}
	rep, runErr := pipeline.Run(ctx, eng, tables, opt)
	out.Report = rep
	st := governance.NewStore(eng)
	if rep != nil {
		refreshed := map[string]bool{}
		for _, r := range rep.Results {
			refreshed[r.ID] = r.Status == pipeline.StatusSuccess
		}
		for _, c := range cs {
			if !refreshed[c.Table] {
				continue
			}
			lines, aerr := ApplyRoles(ctx, st, c)
			for _, l := range lines {
				out.Log = append(out.Log, fmt.Sprintf("applied %s: %s", c.File, l))
			}
			if aerr != nil {
				out.Warnings = append(out.Warnings, "contract roles: "+aerr.Error())
			}
		}
	}
	// Refreshed tables may have new columns: rebuild the per-role views.
	if rr, gerr := st.Regenerate(ctx); gerr != nil {
		out.Warnings = append(out.Warnings, "regenerate role views: "+gerr.Error())
	} else {
		if rr.Roles > 0 || rr.Dropped > 0 {
			out.Log = append(out.Log, fmt.Sprintf("role views: %d roles, %d views, %d dropped", rr.Roles, rr.Views, rr.Dropped))
		}
		for _, s := range rr.Skipped {
			out.Warnings = append(out.Warnings, "granted relation no longer exists ("+s+")")
		}
	}
	return out, runErr
}
