package alerts

import (
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// StateReminder is the state recorded in Bronze for a reminder: the alert is
// still firing and the REPEAT period went by. Channels receive it as a
// firing alert labelled reminder=true.
const StateReminder contract.AlertState = "reminder"

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
	// NotifiedAt is when it was last notified (firing or reminder); zero when
	// unknown, which counts as "now" for reminders.
	NotifiedAt time.Time
}

// Transition is a change of state of one alert.
type Transition struct {
	Rule    string
	Key     string
	State   contract.AlertState // firing, resolved, or StateReminder
	Message string
	Row     map[string]string
	At      time.Time
	Since   time.Time // when it started firing
}

// Tracker is the state machine of one rule: every key is either not firing
// or firing, and only changes notify (deduplication per rule + key).
//
//	not firing --(in result for FOR)-->  firing   : Transition(firing)
//	firing     --(in result)--------->   firing   : nothing (row refreshed);
//	                                                 every REPEAT: Transition(reminder)
//	firing     --(absent for RESOLVE_FOR)-> resolved : Transition(resolved)
//
// FOR, RESOLVE_FOR and REPEAT are optional (0: at once / never): a reading
// that flaps for less than FOR never fires, and one that returns before
// RESOLVE_FOR never resolves. The waiting clocks are not persisted: a restart
// starts them again. A Tracker is used by one goroutine.
type Tracker struct {
	rule     Rule
	firing   map[string]*Firing
	pending  map[string]time.Time // first evaluation in a row in which the key was in the result
	clearing map[string]time.Time // first evaluation in a row in which a firing key was absent
}

// NewTracker starts a tracker, optionally with alerts that were already
// firing before a restart (they will not notify again while they keep firing).
func NewTracker(r Rule, seed []Firing) *Tracker {
	t := &Tracker{rule: r, firing: map[string]*Firing{}, pending: map[string]time.Time{}, clearing: map[string]time.Time{}}
	for i := range seed {
		f := seed[i]
		t.firing[f.Key] = &f
	}
	return t
}

// Len is the number of alerts firing.
func (t *Tracker) Len() int { return len(t.firing) }

// IsFiring reports whether key is currently firing.
func (t *Tracker) IsFiring(key string) bool { _, ok := t.firing[key]; return ok }

// Keys lists the keys currently firing.
func (t *Tracker) Keys() []string {
	out := make([]string, 0, len(t.firing))
	for k := range t.firing {
		out = append(out, k)
	}
	return out
}

// Drop forgets key without any notification (another instance owns it now).
func (t *Tracker) Drop(key string) {
	delete(t.firing, key)
	delete(t.clearing, key)
}

// Adopt starts tracking an alert another instance (or an earlier run) already
// notified: it will not notify again, only resolve or remind.
func (t *Tracker) Adopt(f Firing) {
	if _, ok := t.firing[f.Key]; !ok {
		t.firing[f.Key] = &f
	}
}

// Apply compares the keys firing now with the known ones and returns the
// transitions: firing ones first, then reminders, then resolved ones.
// Call it only after a successful evaluation: on an error, skip it so no
// alert is resolved by accident.
func (t *Tracker) Apply(now time.Time, hits []Hit) []Transition {
	var out, reminders []Transition
	current := make(map[string]bool, len(hits))
	for _, h := range hits {
		if current[h.Key] {
			continue // one alert per key
		}
		current[h.Key] = true
		if f, ok := t.firing[h.Key]; ok {
			f.Row = h.Row // deduplicated: no notification
			delete(t.clearing, h.Key)
			if t.rule.Repeat > 0 {
				if f.NotifiedAt.IsZero() {
					f.NotifiedAt = now
				} else if now.Sub(f.NotifiedAt) >= t.rule.Repeat {
					f.NotifiedAt = now
					reminders = append(reminders, t.transition(StateReminder, h.Key, h.Row, now, f.Since))
				}
			}
			continue
		}
		since, waiting := t.pending[h.Key]
		if !waiting {
			since = now
			t.pending[h.Key] = now
		}
		if now.Sub(since) < t.rule.For {
			continue
		}
		delete(t.pending, h.Key)
		t.firing[h.Key] = &Firing{Key: h.Key, Since: since, Row: h.Row, NotifiedAt: now}
		out = append(out, t.transition(contract.AlertFiring, h.Key, h.Row, now, since))
	}
	for key := range t.pending {
		if !current[key] {
			delete(t.pending, key) // the condition did not hold long enough
		}
	}
	out = append(out, reminders...)
	for key, f := range t.firing {
		if current[key] {
			continue
		}
		if t.rule.ResolveFor > 0 {
			first, ok := t.clearing[key]
			if !ok {
				t.clearing[key] = now
				first = now
			}
			if now.Sub(first) < t.rule.ResolveFor {
				continue
			}
		}
		delete(t.firing, key)
		delete(t.clearing, key)
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
