package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/txretry"
)

// Coordination between several `duct alerts` instances (ADR 0009).
//
// Every instance may evaluate every rule over whatever its consumer-group
// partitions (or its own group) gave it. The shared Duct state decides who
// notifies, with one lease per (rule, key) in duct.duct_alert_state:
//
//	(no row)       --ClaimFiring (INSERT, PK)-->   firing_pending (holder, lease)
//	firing_pending --FiringDone (after sending)--> firing
//	firing_pending, lease expired                  another instance takes it over and notifies
//	firing         --Confirm (each seen_every)-->  seen_by/seen_at refreshed by whoever sees it
//	firing         --ClaimReminder-------------->  notified_at moved (UPDATE + read-back of claim token)
//	firing         --ClaimResolve--------------->  resolving (holder, lease)
//	resolving      --ResolveDone (after sending)-> row deleted
//
// Only the instance that wins a claim sends the notification, so two instances
// never both notify one transition; a crash between claim and send leaves a
// lease that expires, and another instance finishes the job (at-least-once,
// like the single instance always was). An instance only resolves an alert
// nobody else has confirmed for a lease period: when a partition moves to
// another instance, the old one drops the key silently and the new one owns it.
// Claims are plain statements against Postgres or SQLite through txretry; a
// lost race shows up as a duplicate key (INSERT) or as the read-back naming
// someone else (UPDATE).

// Row statuses of duct.duct_alert_state.
const (
	statusPending   = "firing_pending"
	statusFiring    = "firing"
	statusResolving = "resolving"
)

// Coord is the view of the shared state of one instance.
type Coord struct {
	Eng contract.Engine
	// ID names this instance (unique among the running ones).
	ID  string
	Now func() time.Time

	seq atomic.Int64
}

func (c *Coord) now() time.Time {
	if c.Now != nil {
		return c.Now().UTC()
	}
	return time.Now().UTC()
}

// ts renders an instant as the epoch microseconds the *_us columns hold.
func ts(t time.Time) string { return fmt.Sprintf("%d", t.UTC().UnixMicro()) }

// tsLit renders an instant for a TIMESTAMP column.
func tsLit(t time.Time) string { return fmt.Sprintf("make_timestamp(%d)", t.UTC().UnixMicro()) }

func epochOf(col string) string { return "CAST(epoch_us(CAST(" + col + " AS TIMESTAMP)) AS BIGINT)" }

type stateRow struct {
	Status, Holder, SeenBy, Claim  string
	LeaseUntil, SeenAt, NotifiedAt time.Time
	Since                          time.Time
	Labels                         map[string]string
	Severity, Message              string
}

func microTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	var us int64
	if _, err := fmt.Sscan(s, &us); err != nil {
		return time.Time{}
	}
	return time.UnixMicro(us).UTC()
}

func (c *Coord) rows(ctx context.Context, q string, args ...any) ([][]string, error) {
	st, err := c.Eng.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for j := range row {
				if rec.Column(j).IsValid(i) {
					row[j] = strings.Clone(rec.Column(j).ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	return out, st.Err()
}

const stateCols = `COALESCE(status, ''), COALESCE(holder, ''), COALESCE(seen_by, ''), COALESCE(claim, ''), ` +
	`COALESCE(labels, '{}'), COALESCE(severity, ''), COALESCE(message, ''), ` +
	`COALESCE(CAST(lease_until_us AS VARCHAR), ''), COALESCE(CAST(seen_at_us AS VARCHAR), ''), ` +
	`COALESCE(CAST(notified_at_us AS VARCHAR), ''), COALESCE(CAST(epoch_us(CAST(firing_since AS TIMESTAMP)) AS VARCHAR), '')`

func (c *Coord) get(ctx context.Context, rule, key string) (*stateRow, error) {
	rs, err := c.rows(ctx, "SELECT "+stateCols+" FROM duct.duct_alert_state WHERE rule = "+lit(rule)+" AND alert_key = "+lit(key))
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	r := rs[0]
	row := &stateRow{Status: r[0], Holder: r[1], SeenBy: r[2], Claim: r[3], Severity: r[5], Message: r[6],
		LeaseUntil: microTime(r[7]), SeenAt: microTime(r[8]), NotifiedAt: microTime(r[9]), Since: microTime(r[10])}
	_ = json.Unmarshal([]byte(r[4]), &row.Labels)
	if row.Status == "" { // persisted by an older version: firing and notified
		row.Status = statusFiring
	}
	return row, nil
}

func isDuplicate(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(err.Error())
	for _, f := range []string{"duplicate key", "unique constraint", "constraint failed", "primary key constraint", "violates unique"} {
		if strings.Contains(m, f) {
			return true
		}
	}
	return false
}

func (c *Coord) token() string { return fmt.Sprintf("%s/%d", c.ID, c.seq.Add(1)) }

// ClaimFiring tries to become the notifier of a new firing alert. It reports
// true when this instance must send the notification (and call FiringDone
// afterwards); false when another instance already did, or is doing it.
func (c *Coord) ClaimFiring(ctx context.Context, rule Rule, t Transition, ttl time.Duration) (bool, error) {
	var won bool
	err := txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		won = false
		row, err := c.get(ctx, rule.Name, t.Key)
		if err != nil {
			return err
		}
		now := c.now()
		if row == nil {
			labels, _ := json.Marshal(t.Row)
			err := c.Eng.Exec(ctx, fmt.Sprintf(
				"INSERT INTO duct.duct_alert_state (rule, alert_key, severity, message, labels, firing_since, status, holder, lease_until_us, seen_by, seen_at_us) "+
					"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
				lit(rule.Name), lit(t.Key), lit(rule.Severity), lit(t.Message), lit(string(labels)), tsLit(t.Since),
				lit(statusPending), lit(c.ID), ts(now.Add(ttl)), lit(c.ID), ts(now)))
			switch {
			case err == nil:
				won = true
				return nil
			case isDuplicate(err):
				return nil // another instance inserted first
			}
			return err
		}
		if row.Status == statusPending && row.Holder != c.ID && row.LeaseUntil.Before(now) {
			// The instance that claimed it died before notifying: finish its job.
			won, err = c.takeOver(ctx, rule.Name, t.Key, row.Holder, statusPending, now.Add(ttl))
			return err
		}
		// Already firing and notified by someone: this instance sees it too.
		return c.Eng.Exec(ctx, fmt.Sprintf(
			"UPDATE duct.duct_alert_state SET seen_by = %s, seen_at_us = %s WHERE rule = %s AND alert_key = %s",
			lit(c.ID), ts(now), lit(rule.Name), lit(t.Key)))
	})
	return won, err
}

// takeOver moves an expired lease to this instance; true if it won.
func (c *Coord) takeOver(ctx context.Context, rule, key, from, status string, until time.Time) (bool, error) {
	tok := c.token()
	err := c.Eng.Exec(ctx, fmt.Sprintf(
		"UPDATE duct.duct_alert_state SET holder = %s, claim = %s, lease_until_us = %s WHERE rule = %s AND alert_key = %s AND status = %s AND holder = %s AND lease_until_us < %s",
		lit(c.ID), lit(tok), ts(until), lit(rule), lit(key), lit(status), lit(from), ts(c.now())))
	if err != nil {
		return false, err
	}
	row, err := c.get(ctx, rule, key)
	return err == nil && row != nil && row.Claim == tok, err
}

// FiringDone records that the notification went out.
func (c *Coord) FiringDone(ctx context.Context, rule Rule, key string, ttl time.Duration) error {
	now := c.now()
	return txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return c.Eng.Exec(ctx, fmt.Sprintf(
			"UPDATE duct.duct_alert_state SET status = %s, notified_at_us = %s, lease_until_us = %s WHERE rule = %s AND alert_key = %s AND holder = %s AND status = %s",
			lit(statusFiring), ts(now), ts(now.Add(ttl)), lit(rule.Name), lit(key), lit(c.ID), lit(statusPending)))
	})
}

// Confirm says this instance still sees the alert firing. It returns notify =
// true when this instance must now send a firing notification: the row is
// gone (it was resolved by someone while we still see it) or a dead instance
// left it half done. Call it at most every ttl/3 per key.
func (c *Coord) Confirm(ctx context.Context, rule Rule, t Transition, ttl time.Duration) (notify bool, err error) {
	row, err := c.get(ctx, rule.Name, t.Key)
	if err != nil {
		return false, err
	}
	if row == nil {
		// Someone resolved it a moment ago and this instance, whose data lags,
		// still sees it: not a new alert. It resolves silently when its data catches up.
		if recent, err := c.recentlyResolved(ctx, rule.Name, t.Key, ttl); err != nil || recent {
			return false, err
		}
		return c.ClaimFiring(ctx, rule, t, ttl)
	}
	if row.Status == statusPending && row.Holder != c.ID && row.LeaseUntil.Before(c.now()) {
		return c.ClaimFiring(ctx, rule, t, ttl)
	}
	now := c.now()
	err = txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return c.Eng.Exec(ctx, fmt.Sprintf(
			"UPDATE duct.duct_alert_state SET seen_by = %s, seen_at_us = %s WHERE rule = %s AND alert_key = %s",
			lit(c.ID), ts(now), lit(rule.Name), lit(t.Key)))
	})
	return false, err
}

// ClaimReminder moves notified_at when a REPEAT period went by since the last
// notification of the alert; true means this instance sends the reminder.
func (c *Coord) ClaimReminder(ctx context.Context, rule Rule, key string) (bool, error) {
	var won bool
	err := txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		won = false
		now := c.now()
		tok := c.token()
		// A little under the period: clocks of instances differ by a bit.
		due := now.Add(-rule.Repeat + rule.Repeat/10)
		if err := c.Eng.Exec(ctx, fmt.Sprintf(
			"UPDATE duct.duct_alert_state SET notified_at_us = %s, claim = %s WHERE rule = %s AND alert_key = %s AND status = %s AND (notified_at_us IS NULL OR notified_at_us <= %s)",
			ts(now), lit(tok), lit(rule.Name), lit(key), lit(statusFiring), ts(due))); err != nil {
			return err
		}
		row, err := c.get(ctx, rule.Name, key)
		won = err == nil && row != nil && row.Claim == tok
		return err
	})
	return won, err
}

// ClaimResolve tries to become the one that resolves an alert this instance no
// longer sees firing. It reports false when the alert is gone, another
// instance confirmed it recently (it is theirs now) or someone is already
// resolving it; the caller drops its local copy silently in that case.
//
// lastSeen is when this instance last saw the alert firing: another
// instance's confirmation only blocks the resolution if it is newer than that
// (and within a lease). Two instances that watched the same data and both lost
// it therefore both try, and the claim picks one; an instance whose partition
// moved away is blocked by the one that took it over.
func (c *Coord) ClaimResolve(ctx context.Context, rule Rule, key string, ttl time.Duration, lastSeen time.Time) (bool, error) {
	var won bool
	err := txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		won = false
		row, err := c.get(ctx, rule.Name, key)
		if err != nil || row == nil {
			return err
		}
		now := c.now()
		if row.SeenBy != "" && row.SeenBy != c.ID && row.SeenAt.After(now.Add(-ttl)) && row.SeenAt.After(lastSeen) {
			return nil // someone else is seeing it firing
		}
		tok := c.token()
		until := now.Add(ttl)
		var where string
		switch row.Status {
		case statusResolving:
			if row.Holder == c.ID {
				won = true // our own earlier claim (retry)
				return nil
			}
			if row.LeaseUntil.After(now) {
				return nil
			}
			where = fmt.Sprintf("status = %s AND holder = %s AND lease_until_us < %s", lit(statusResolving), lit(row.Holder), ts(now))
		default:
			where = fmt.Sprintf("status IN (%s, %s)", lit(statusPending), lit(statusFiring))
		}
		if err := c.Eng.Exec(ctx, fmt.Sprintf(
			"UPDATE duct.duct_alert_state SET status = %s, holder = %s, claim = %s, lease_until_us = %s WHERE rule = %s AND alert_key = %s AND %s",
			lit(statusResolving), lit(c.ID), lit(tok), ts(until), lit(rule.Name), lit(key), where)); err != nil {
			return err
		}
		row, err = c.get(ctx, rule.Name, key)
		won = err == nil && row != nil && row.Status == statusResolving && row.Claim == tok
		return err
	})
	return won, err
}

// ResolveDone deletes the alert after the resolved notification went out and
// leaves a short-lived tombstone: an instance whose data lags and still sees
// the alert firing must not announce it again (see Confirm).
func (c *Coord) ResolveDone(ctx context.Context, rule Rule, key string) error {
	now := c.now()
	return txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return c.Eng.ExecTx(ctx,
			fmt.Sprintf("DELETE FROM duct.duct_alert_state WHERE rule = %s AND alert_key = %s AND holder = %s AND status = %s",
				lit(rule.Name), lit(key), lit(c.ID), lit(statusResolving)),
			fmt.Sprintf("DELETE FROM duct.duct_alert_resolved WHERE (rule = %s AND alert_key = %s) OR resolved_at_us < %s",
				lit(rule.Name), lit(key), ts(now.Add(-time.Hour))),
			fmt.Sprintf("INSERT INTO duct.duct_alert_resolved (rule, alert_key, resolved_at_us) VALUES (%s, %s, %s)",
				lit(rule.Name), lit(key), ts(now)))
	})
}

// recentlyResolved: some instance resolved (rule, key) within ttl.
func (c *Coord) recentlyResolved(ctx context.Context, rule, key string, ttl time.Duration) (bool, error) {
	rs, err := c.rows(ctx, fmt.Sprintf("SELECT count(*) FROM duct.duct_alert_resolved WHERE rule = %s AND alert_key = %s AND resolved_at_us > %s",
		lit(rule), lit(key), ts(c.now().Add(-ttl))))
	return err == nil && len(rs) > 0 && rs[0][0] != "0", err
}

// Stale lists the firing alerts of rule that nobody has confirmed for longer
// than age (their owner is gone, or the data that fired them): candidates for
// ClaimResolve.
func (c *Coord) Stale(ctx context.Context, rule string, age time.Duration) ([]Firing, error) {
	cut := c.now().Add(-age)
	rs, err := c.rows(ctx, "SELECT alert_key, COALESCE(labels, '{}'), "+epochOf("firing_since")+" FROM duct.duct_alert_state WHERE rule = "+lit(rule)+
		" AND COALESCE(status, 'firing') IN ('firing', 'firing_pending') AND (seen_at_us IS NULL OR seen_at_us < "+ts(cut)+")")
	if err != nil {
		return nil, err
	}
	var out []Firing
	for _, r := range rs {
		f := Firing{Key: r[0], Since: microTime(r[2])}
		_ = json.Unmarshal([]byte(r[1]), &f.Row)
		out = append(out, f)
	}
	return out, nil
}

// Silence is a mute on a rule (and optionally one key).
type Silence struct {
	ID        int64
	Rule, Key string // Key "" = every key of the rule
	Reason    string
	By        string
	Created   time.Time
	Expires   time.Time
}

// Matches reports whether the silence covers (rule, key).
func (s Silence) Matches(rule, key string) bool {
	return s.Rule == rule && (s.Key == "" || s.Key == key)
}

// AddSilence mutes rule (and key, if not empty) for d.
func AddSilence(ctx context.Context, eng contract.Engine, rule, key, reason, by string, d time.Duration, now time.Time) (Silence, error) {
	id := now.UnixMicro()*1000 + rand.Int64N(1000)
	s := Silence{ID: id, Rule: rule, Key: key, Reason: reason, By: by, Created: now.UTC(), Expires: now.UTC().Add(d)}
	err := txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return eng.Exec(ctx, fmt.Sprintf(
			"INSERT INTO duct.duct_alert_silences (id, rule, alert_key, reason, created_by, created_at, expires_at) VALUES (%d, %s, %s, %s, %s, %s, %s)",
			s.ID, lit(rule), lit(key), lit(reason), lit(by), tsLit(s.Created), tsLit(s.Expires)))
	})
	return s, err
}

// RemoveSilence deletes a silence (by id); it reports whether there was one.
func RemoveSilence(ctx context.Context, eng contract.Engine, id int64) (bool, error) {
	c := &Coord{Eng: eng}
	rs, err := c.rows(ctx, fmt.Sprintf("SELECT count(*) FROM duct.duct_alert_silences WHERE id = %d", id))
	if err != nil || len(rs) == 0 || rs[0][0] == "0" {
		return false, err
	}
	return true, eng.Exec(ctx, fmt.Sprintf("DELETE FROM duct.duct_alert_silences WHERE id = %d", id))
}

// ActiveSilences lists the silences that have not expired at now, newest first.
func ActiveSilences(ctx context.Context, eng contract.Engine, now time.Time) ([]Silence, error) {
	c := &Coord{Eng: eng}
	rs, err := c.rows(ctx, "SELECT id, rule, alert_key, reason, created_by, "+epochOf("created_at")+", "+epochOf("expires_at")+
		" FROM duct.duct_alert_silences WHERE CAST(expires_at AS TIMESTAMP) > "+tsLit(now)+" ORDER BY id DESC")
	if err != nil {
		return nil, err
	}
	var out []Silence
	for _, r := range rs {
		s := Silence{Rule: r[1], Key: r[2], Reason: r[3], By: r[4], Created: microTime(r[5]), Expires: microTime(r[6])}
		_, _ = fmt.Sscan(r[0], &s.ID)
		out = append(out, s)
	}
	return out, nil
}

// SilencedBy returns the active silence that covers (rule, key), if any.
func SilencedBy(ctx context.Context, eng contract.Engine, rule, key string, now time.Time) (*Silence, error) {
	all, err := ActiveSilences(ctx, eng, now)
	if err != nil {
		return nil, err
	}
	for _, s := range all {
		if s.Matches(rule, key) {
			return &s, nil
		}
	}
	return nil, nil
}

// StateRow is one alert of duct.duct_alert_state as `duct alerts status` shows it.
type StateRow struct {
	Rule, Key, Severity, Status, Holder string
	Since                               time.Time
	NotifiedAt                          time.Time
}

// ListState returns every alert in the shared state.
func ListState(ctx context.Context, eng contract.Engine) ([]StateRow, error) {
	c := &Coord{Eng: eng}
	rs, err := c.rows(ctx, "SELECT rule, alert_key, severity, COALESCE(status, 'firing'), COALESCE(holder, ''), "+epochOf("firing_since")+", COALESCE(CAST(notified_at_us AS VARCHAR), '') FROM duct.duct_alert_state ORDER BY rule, alert_key")
	if err != nil {
		return nil, err
	}
	var out []StateRow
	for _, r := range rs {
		out = append(out, StateRow{Rule: r[0], Key: r[1], Severity: r[2], Status: r[3], Holder: r[4], Since: microTime(r[5]), NotifiedAt: microTime(r[6])})
	}
	return out, nil
}
