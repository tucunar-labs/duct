package orchestrator

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	_ "time/tzdata" // timezone names work on minimal images too
)

// Schedule gives the instant of the next tick strictly after t.
type Schedule interface {
	Next(t time.Time) time.Time
}

// every is "@every <duration>" aligned to the Unix epoch (robfig's own
// version counts from the process start, so two machines would tick at
// different instants and fire twice).
type every time.Duration

func (e every) Next(t time.Time) time.Time {
	d := time.Duration(e)
	return t.Truncate(d).Add(d)
}

// ParseSchedule reads a 5-field cron expression ("*/5 * * * *"), a descriptor
// (@hourly, @daily, @weekly...) or "@every 30s" (at least one second). The
// timezone (IANA name) applies to cron expressions and descriptors.
func ParseSchedule(spec, tz string) (Schedule, error) {
	spec = strings.TrimSpace(spec)
	if rest, ok := strings.CutPrefix(spec, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(rest))
		if err != nil {
			return nil, fmt.Errorf("%q: %w", spec, err)
		}
		if d < time.Second {
			return nil, fmt.Errorf("%q: the interval must be at least 1s", spec)
		}
		return every(d), nil
	}
	if strings.HasPrefix(spec, "CRON_TZ=") || strings.HasPrefix(spec, "TZ=") {
		return nil, errors.New("set the timezone with the timezone key, not inside the expression")
	}
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("timezone %q: %w", tz, err)
		}
		spec = "CRON_TZ=" + tz + " " + spec
	}
	s, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", strings.TrimPrefix(spec, "CRON_TZ="+tz+" "), err)
	}
	return s, nil
}

// advance moves a schedule cursor to now: next is the first tick not yet
// handled. It returns the latest tick that is due (at or before now), if any,
// and the new cursor. Missed ticks are coalesced into one (no catch-up storm
// after a pause).
func advance(s Schedule, next, now time.Time) (tick, newNext time.Time, due bool) {
	for !next.After(now) {
		tick, due = next, true
		next = s.Next(next)
		if next.IsZero() { // schedule exhausted
			break
		}
	}
	return tick, next, due
}
