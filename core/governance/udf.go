package governance

import (
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// deniedFunctionsFor returns the user-defined functions id may not call: all of
// them unless id is the administrator.
func (h *Handler) deniedFunctionsFor(id contract.Identity) map[string]bool {
	if len(h.opt.AdminOnlyFunctions) == 0 || IsAdmin(id) {
		return nil
	}
	m := make(map[string]bool, len(h.opt.AdminOnlyFunctions))
	for _, f := range h.opt.AdminOnlyFunctions {
		m[strings.ToLower(f)] = true
	}
	return m
}

func (rw *rewriter) checkUDF(v map[string]any) error {
	name, _ := v["function_name"].(string)
	if rw.udf[strings.ToLower(name)] {
		return denied("function %s is reserved to the %s role", name, AdminRole)
	}
	return nil
}
