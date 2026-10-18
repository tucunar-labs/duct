package project

import (
	"fmt"
	"time"
)

// NotebooksSettings is the [notebooks] table of duct.toml (ADR 0006): the
// notebooks of the web console, one marimo process per user.
//
//	[notebooks]
//	editors      = ["duct_admin", "analistas"]  # roles that open edit mode; default ["duct_admin"]
//	idle_timeout = "30m"                         # a user's process dies after this long without requests
//	max_age      = "12h"                         # hard limit of a process (and of its DUCT_TOKEN)
//	max_kernels  = 8                             # processes alive at once; more are refused
type NotebooksSettings struct {
	Editors     []string `toml:"editors"`
	IdleTimeout string   `toml:"idle_timeout"`
	MaxAge      string   `toml:"max_age"`
	MaxKernels  int      `toml:"max_kernels"`
}

// Durations returns idle_timeout and max_age with their defaults applied.
func (n NotebooksSettings) Durations() (idle, maxAge time.Duration, err error) {
	idle, maxAge = 30*time.Minute, 12*time.Hour
	for _, f := range []struct {
		key string
		s   string
		dst *time.Duration
	}{{"idle_timeout", n.IdleTimeout, &idle}, {"max_age", n.MaxAge, &maxAge}} {
		if f.s == "" {
			continue
		}
		d, err := time.ParseDuration(f.s)
		if err != nil || d <= 0 {
			return 0, 0, fmt.Errorf("[notebooks] %s: %q is not a positive duration (like \"30m\")", f.key, f.s)
		}
		*f.dst = d
	}
	return idle, maxAge, nil
}

// EditorRoles are the roles that may edit notebooks.
func (n NotebooksSettings) EditorRoles() []string {
	if len(n.Editors) == 0 {
		return []string{"duct_admin"}
	}
	return n.Editors
}
