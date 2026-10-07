package alerts

import (
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Hit is one row of a rule's result: an alert that is firing now.
type Hit struct {
	Key string
	Row map[string]string // every column of the row, as text
}

// Firing is an alert known to be firing.
type Firing struct {
	Key   string
	Since time.Time
	Row   map[string]string
}

// Transition is a change of state of one alert.
type Transition struct {
	Rule    string
	Key     string
	State   contract.AlertState
	Message string
	Row     map[string]string
	At      time.Time
	Since   time.Time // when it started firing
}

// Tracker is the state machine of one rule: every key is either not firing
// or firing, and only changes notify (deduplication per rule + key).
//
//	not firing --(key in result)-->  firing   : Transition(firing)
//	firing     --(key in result)-->  firing   : nothing (row refreshed)
//	firing     --(key absent)---->   resolved : Transition(resolved)
//
// A Tracker is used by one goroutine.
type Tracker struct {
	rule   Rule
	firing map[string]*Firing
}

// NewTracker starts a tracker, optionally with alerts that were already
// firing before a restart (they will not notify again while they keep firing).
func NewTracker(r Rule, seed []Firing) *Tracker {
	t := &Tracker{rule: r, firing: map[string]*Firing{}}
	for i := range seed {
		f := seed[i]
		t.firing[f.Key] = &f
	}
	return t
}

// Len is the number of alerts firing.
func (t *Tracker) Len() int { return len(t.firing) }

// Apply compares the keys firing now with the known ones and returns the
// transitions: firing ones first, then resolved ones.
// Call it only after a successful evaluation: on an error, skip it so no
// alert is resolved by accident.
func (t *Tracker) Apply(now time.Time, hits []Hit) []Transition {
	var out []Transition
	current := make(map[string]bool, len(hits))
	for _, h := range hits {
		if current[h.Key] {
			continue // one alert per key
		}
		current[h.Key] = true
		if f, ok := t.firing[h.Key]; ok {
			f.Row = h.Row // deduplicated: no notification
			continue
		}
		t.firing[h.Key] = &Firing{Key: h.Key, Since: now, Row: h.Row}
		out = append(out, t.transition(contract.AlertFiring, h.Key, h.Row, now, now))
	}
	for key, f := range t.firing {
		if current[key] {
			continue
		}
		delete(t.firing, key)
		out = append(out, t.transition(contract.AlertResolved, key, f.Row, now, f.Since))
	}
	return out
}

func (t *Tracker) transition(st contract.AlertState, key string, row map[string]string, now, since time.Time) Transition {
	vars := make(map[string]string, len(row)+2)
	for k, v := range row {
		vars[k] = v
	}
	vars["rule"], vars["key"] = t.rule.Name, key
	return Transition{Rule: t.rule.Name, Key: key, State: st, Message: Render(t.rule.Message, vars), Row: row, At: now, Since: since}
}
