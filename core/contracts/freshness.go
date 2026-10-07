package contracts

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// SLA is the freshness part of the contract's slaProperties. Zero = not set.
type SLA struct {
	Latency   time.Duration // max age of the data (property "latency")
	Frequency time.Duration // expected refresh interval (property "frequency")
}

// Max is the age beyond which the table is stale: the latency when given,
// otherwise the frequency. 0 means no freshness SLA.
func (s SLA) Max() (time.Duration, string) {
	if s.Latency > 0 {
		return s.Latency, "latency"
	}
	if s.Frequency > 0 {
		return s.Frequency, "frequency"
	}
	return 0, ""
}

func (c *Contract) sla() SLA {
	var s SLA
	for _, p := range c.DC.SLAProperties {
		d, ok := duration(p.Value, p.Unit)
		if !ok {
			continue
		}
		switch strings.ToLower(p.Property) {
		case "latency":
			s.Latency = d
		case "frequency":
			s.Frequency = d
		}
	}
	return s
}

// duration converts an ODCS SLA value and unit (d, h, min, s, w, mo, y ...).
func duration(value any, unit string) (time.Duration, bool) {
	f, ok := toFloat(value)
	if !ok || f <= 0 {
		return 0, false
	}
	var u time.Duration
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "ms", "millisecond", "milliseconds":
		u = time.Millisecond
	case "s", "sec", "secs", "second", "seconds":
		u = time.Second
	case "m", "min", "mins", "minute", "minutes":
		u = time.Minute
	case "h", "hr", "hrs", "hour", "hours":
		u = time.Hour
	case "d", "day", "days":
		u = 24 * time.Hour
	case "w", "wk", "week", "weeks":
		u = 7 * 24 * time.Hour
	case "mo", "month", "months":
		u = 30 * 24 * time.Hour
	case "y", "yr", "yrs", "year", "years":
		u = 365 * 24 * time.Hour
	default:
		return 0, false
	}
	return time.Duration(f * float64(u)), true
}

// Freshness statuses.
const (
	Fresh   = "fresh"
	Stale   = "stale"
	Unknown = "unknown" // never refreshed by `duct run`
	NoSLA   = "no_sla"
)

// Status is the freshness of one governed table.
type Status struct {
	Table       string     `json:"table"`
	Contract    string     `json:"contract"`
	Status      string     `json:"status"`
	LastRefresh *time.Time `json:"last_refresh,omitempty"`
	AgeSeconds  int64      `json:"age_seconds"`
	MaxSeconds  int64      `json:"max_seconds,omitempty"`
	Source      string     `json:"source,omitempty"` // latency | frequency
}

// Evaluate computes the status of a contract given the time of its last
// successful refresh (zero = never) and now. Never refreshed is "unknown" when
// an SLA exists; it counts as stale for exit codes (see IsStale).
func Evaluate(c *Contract, last, now time.Time) Status {
	max, src := c.sla().Max()
	st := Status{Table: c.Table, Contract: c.File, MaxSeconds: int64(max / time.Second), Source: src}
	if !last.IsZero() {
		l := last.UTC()
		st.LastRefresh = &l
		st.AgeSeconds = int64(now.Sub(last) / time.Second)
	}
	switch {
	case max == 0:
		st.Status = NoSLA
	case last.IsZero():
		st.Status = Unknown
	case now.Sub(last) > max:
		st.Status = Stale
	default:
		st.Status = Fresh
	}
	return st
}

// IsStale is true for stale and for never-refreshed tables with an SLA.
func (s Status) IsStale() bool { return s.Status == Stale || s.Status == Unknown }

// FreshnessOf reports the freshness of every contract, using the last
// successful refresh recorded in duct_pipeline_runs. Tables written by other
// means (ingest, manual SQL) show as unknown: bind the contract to a
// pipeline target. It is plain data for the orchestrator and alerts.
func FreshnessOf(ctx context.Context, eng contract.Engine, cs []*Contract, now time.Time) ([]Status, error) {
	last := map[string]time.Time{}
	rows, err := queryRows(ctx, eng, "SELECT target, CAST(epoch_ms(max(CAST(finished_at AS TIMESTAMP))) AS VARCHAR) FROM duct.duct_pipeline_runs WHERE status = 'success' GROUP BY target")
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		ms, err := strconv.ParseInt(r[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("freshness of %s: %w", r[0], err)
		}
		last[r[0]] = time.UnixMilli(ms)
	}
	out := make([]Status, 0, len(cs))
	for _, c := range cs {
		out = append(out, Evaluate(c, last[c.Table], now))
	}
	return out, nil
}
