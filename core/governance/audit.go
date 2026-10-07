package governance

import (
	"context"
	"log/slog"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// AuditTable is the Duct-owned table with one row per audited statement.
const AuditTable = "duct.duct_audit"

const maxAuditSQL = 8 << 10

// auditLog writes records to AuditTable from one goroutine, so callers (a
// query hot path, a pgwire session) never wait for the state database.
type auditLog struct {
	eng  contract.Engine
	in   chan auditItem
	done chan struct{}
	mu   sync.Mutex
	shut bool
}

type auditItem struct {
	rec   contract.AuditRecord
	flush chan struct{}
}

func newAuditLog(eng contract.Engine) *auditLog {
	a := &auditLog{eng: eng, in: make(chan auditItem, 4096), done: make(chan struct{})}
	go a.run()
	return a
}

func (a *auditLog) add(rec contract.AuditRecord) {
	if len(rec.SQL) > maxAuditSQL {
		rec.SQL = rec.SQL[:maxAuditSQL] + "…"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.shut {
		return
	}
	select {
	case a.in <- auditItem{rec: rec}:
	default:
		slog.Warn("audit queue full, record dropped", "user", rec.User)
	}
}

func (a *auditLog) run() {
	defer close(a.done)
	for it := range a.in {
		if it.flush != nil {
			close(it.flush)
			continue
		}
		r := it.rec
		status := "ok"
		if r.Err != "" {
			status = "error"
			if len(r.Err) >= 17 && r.Err[:17] == "permission denied" {
				status = "denied"
			}
		}
		var errText any
		if r.Err != "" {
			errText = r.Err
		}
		err := retryLocked(func() error {
			return a.eng.Exec(context.Background(),
				`INSERT INTO `+AuditTable+` (occurred_at, user_name, role, protocol, sql_text, duration_ms, rows_returned, status, error) VALUES (CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?, ?, ?)`,
				r.At.UTC().Format("2006-01-02 15:04:05.000000"), r.User, r.Role, r.Protocol, r.SQL, r.Duration.Milliseconds(), r.Rows, status, errText)
		})
		if err != nil {
			slog.Error("audit write failed", "error", err)
		}
	}
}

func (a *auditLog) flush(ctx context.Context) error {
	ch := make(chan struct{})
	a.mu.Lock()
	if a.shut {
		a.mu.Unlock()
		return nil
	}
	a.in <- auditItem{flush: ch}
	a.mu.Unlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Minute):
		return context.DeadlineExceeded
	}
}

func (a *auditLog) close() error {
	a.mu.Lock()
	if !a.shut {
		a.shut = true
		close(a.in)
	}
	a.mu.Unlock()
	<-a.done
	return nil
}
