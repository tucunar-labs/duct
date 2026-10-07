package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/ingest"
	"github.com/alonsoburon/duct/core/txretry"
)

// AlertsTable is the Bronze table with one row per transition.
const AlertsTable = "bronze.duct_alerts"

const alertsDDL = `CREATE TABLE IF NOT EXISTS ` + AlertsTable + ` (
	occurred_at TIMESTAMPTZ, rule VARCHAR, alert_key VARCHAR, state VARCHAR, severity VARCHAR,
	message VARCHAR, labels VARCHAR, firing_since TIMESTAMPTZ,
	channels VARCHAR, delivered BOOLEAN)`

// Entry is what is recorded for one transition.
type Entry struct {
	Transition Transition
	Severity   string
	Channels   string // delivery summary
	Delivered  bool
}

// Store records transitions in bronze.duct_alerts and keeps the firing set
// in duct.duct_alert_state (so a restart neither forgets nor repeats them).
type Store struct{ Eng contract.Engine }

// Init creates the Bronze table (and schema) if needed.
func (s Store) Init(ctx context.Context) error {
	if err := ingest.EnsureSchema(ctx, s.Eng, "bronze"); err != nil {
		return err
	}
	return txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return s.Eng.Exec(ctx, alertsDDL)
	})
}

func lit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// Record writes the Bronze rows and updates the firing set.
func (s Store) Record(ctx context.Context, es []Entry) error {
	var stateStmts []string
	for _, e := range es {
		t := e.Transition
		labels, _ := json.Marshal(t.Row)
		err := txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
			return s.Eng.Exec(ctx, "INSERT INTO "+AlertsTable+" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				t.At.UTC(), t.Rule, t.Key, string(t.State), e.Severity, t.Message, string(labels),
				t.Since.UTC(), e.Channels, e.Delivered)
		})
		if err != nil {
			return fmt.Errorf("record alert: %w", err)
		}
		stateStmts = append(stateStmts, "DELETE FROM duct.duct_alert_state WHERE rule = "+lit(t.Rule)+" AND alert_key = "+lit(t.Key))
		if t.State == contract.AlertFiring {
			stateStmts = append(stateStmts, fmt.Sprintf(
				"INSERT INTO duct.duct_alert_state (rule, alert_key, severity, message, labels, firing_since) VALUES (%s, %s, %s, %s, %s, make_timestamp(%d))",
				lit(t.Rule), lit(t.Key), lit(e.Severity), lit(t.Message), lit(string(labels)), t.Since.UnixMicro()))
		}
	}
	return txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		return s.Eng.ExecTx(ctx, stateStmts...)
	})
}

// Firing returns the persisted firing alerts, grouped by rule.
func (s Store) Firing(ctx context.Context) (map[string][]Firing, error) {
	st, err := s.Eng.Query(ctx, "SELECT rule, alert_key, labels, CAST(epoch_us(CAST(firing_since AS TIMESTAMP)) AS BIGINT) FROM duct.duct_alert_state")
	if err != nil {
		return nil, fmt.Errorf("read alert state (run `duct migrate`): %w", err)
	}
	defer st.Release()
	out := map[string][]Firing{}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			rule, key := strings.Clone(rec.Column(0).ValueStr(i)), strings.Clone(rec.Column(1).ValueStr(i))
			row := map[string]string{}
			_ = json.Unmarshal([]byte(rec.Column(2).ValueStr(i)), &row)
			var micros int64
			if _, err := fmt.Sscan(rec.Column(3).ValueStr(i), &micros); err != nil {
				return nil, err
			}
			out[rule] = append(out[rule], Firing{Key: key, Since: time.UnixMicro(micros).UTC(), Row: row})
		}
	}
	return out, st.Err()
}

// Forget drops the persisted alerts of a rule that no longer exists.
func (s Store) Forget(ctx context.Context, rule string) error {
	return s.Eng.Exec(ctx, "DELETE FROM duct.duct_alert_state WHERE rule = "+lit(rule))
}
