package project

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AuditSettings is the [audit] table of duct.toml (ADR 0007):
//
//	[audit]
//	retention = "90d"   # rows older than this are deleted by `duct gc` and hourly by `duct serve`; 0 = keep forever
//	export    = false   # true: append new rows to bronze.duct_audit_log in the lake (every minute in `duct serve`)
//	cli       = true    # false: `duct query|sql|run|ingest` do not write audit rows
type AuditSettings struct {
	// Retention is a duration ("90d", "36h", "30m") or 0. Empty means the
	// default (DefaultAuditRetention). It is `any` so that both retention = 0
	// and retention = "0" are accepted.
	Retention any   `toml:"retention"`
	Export    bool  `toml:"export"`
	CLI       *bool `toml:"cli"`
}

// DefaultAuditRetention is how long audit rows are kept when [audit] retention
// is not set.
const DefaultAuditRetention = 90 * 24 * time.Hour

// RetentionDuration is the retention of the audit log; 0 keeps it forever.
func (a AuditSettings) RetentionDuration() (time.Duration, error) {
	switch v := a.Retention.(type) {
	case nil:
		return DefaultAuditRetention, nil
	case int64:
		if v == 0 {
			return 0, nil
		}
		return 0, fmt.Errorf("[audit] retention: write a duration with a unit (\"90d\"), or 0")
	case string:
		return ParseAuditDuration(v)
	}
	return 0, fmt.Errorf("[audit] retention: want a string like \"90d\" or 0, got %v", a.Retention)
}

// CLIEnabled reports whether local CLI commands are audited (default true).
func (a AuditSettings) CLIEnabled() bool { return a.CLI == nil || *a.CLI }

// ParseAuditDuration parses "90d", "2w", "36h", "30m", "45s" or "0"; an empty
// string is the default retention. Plain time.ParseDuration forms also work.
func ParseAuditDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "":
		return DefaultAuditRetention, nil
	case "0":
		return 0, nil
	}
	if n := len(s); n > 1 && (s[n-1] == 'd' || s[n-1] == 'w') {
		days, err := strconv.ParseFloat(s[:n-1], 64)
		if err != nil || days < 0 {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		unit := 24 * time.Hour
		if s[n-1] == 'w' {
			unit = 7 * 24 * time.Hour
		}
		return time.Duration(days * float64(unit)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("bad duration %q (use 90d, 36h, 30m or 0)", s)
	}
	return d, nil
}
