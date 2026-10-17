package governance

import (
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// deniedFunctionsFor returns the user-defined functions id may not call: all of
// them unless id is the administrator.
func (h *Handler) deniedFunctionsFor(id contract.Identity) map[string]bool {
	names := h.opt.AdminOnlyFunctions
	if p := h.udfNames.Load(); p != nil {
		names = *p
	}
	if len(names) == 0 || IsAdmin(id) {
		return nil
	}
	m := make(map[string]bool, len(names))
	for _, f := range names {
		m[strings.ToLower(f)] = true
	}
	return m
}

// SetAdminOnlyFunctions replaces the set of user-defined functions that only the
// admin role may call (a hot reload of functions/ can add names). It applies to
// the statements that start after the call.
func (h *Handler) SetAdminOnlyFunctions(names []string) {
	cp := append([]string(nil), names...)
	h.udfNames.Store(&cp)
}

func (rw *rewriter) checkUDF(v map[string]any) error {
	name, _ := v["function_name"].(string)
	if rw.udf[strings.ToLower(name)] {
		return denied("function %s is reserved to the %s role", name, AdminRole)
	}
	return nil
}
