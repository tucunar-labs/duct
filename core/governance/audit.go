package governance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/metrics"
	"github.com/alonsoburon/duct/core/project"
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
	// quiet logs write failures at debug level (local CLI rows).
	quiet bool
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

var (
	queriesTotal = metrics.Default().Counter("duct_queries_total", "Statements by protocol and outcome (ok, error, denied, quota).", "protocol", "status")
	queryTime    = metrics.Default().Histogram("duct_query_duration_seconds", "Statement duration by protocol, from authorisation to the last row.", nil, "protocol")
	rowsReturned = metrics.Default().Counter("duct_rows_returned_total", "Rows returned to clients by protocol.", "protocol")
)

// observe counts one audited statement.
func observe(rec contract.AuditRecord) {
	proto := rec.Protocol
	if proto == "" {
		proto = "unknown"
	}
	status := "ok"
	switch {
	case rec.Err == "":
	case strings.HasPrefix(rec.Err, "permission denied"):
		status = "denied"
	case IsQuotaError(errors.New(rec.Err)):
		status = "quota"
	default:
		status = "error"
	}
	queriesTotal.Inc(proto, status)
	queryTime.Observe(rec.Duration.Seconds(), proto)
	if rec.Rows > 0 {
		rowsReturned.Add(float64(rec.Rows), proto)
	}
}

func (a *auditLog) add(rec contract.AuditRecord) {
	observe(rec)
	rec.SQL = RedactSQL(rec.SQL)
	rec.Err = RedactSQL(rec.Err)
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

// cacheAuditWindow is how long cache-hit records with the same user, role,
// protocol and statement are coalesced into one row (hits = how many). A
// dashboard that refreshes every few seconds therefore writes at most one row
// per widget and viewer per window instead of one per refresh.
var cacheAuditWindow = time.Minute

// maxPendingCached bounds the coalescing map; beyond it everything is written.
const maxPendingCached = 10000

type cachedAgg struct {
	rec  contract.AuditRecord
	hits int64
	dur  time.Duration
}

func (a *auditLog) run() {
	defer close(a.done)
	pending := map[string]*cachedAgg{}
	writePending := func(all bool) {
		now := time.Now()
		for k, c := range pending {
			if !all && now.Sub(c.rec.At) < cacheAuditWindow {
				continue
			}
			c.rec.Hits = c.hits
			c.rec.Duration = c.dur / time.Duration(c.hits)
			a.write(c.rec)
			delete(pending, k)
		}
	}
	tick := cacheAuditWindow / 4
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case it, ok := <-a.in:
			if !ok {
				writePending(true)
				return
			}
			switch {
			case it.flush != nil:
				writePending(true)
				close(it.flush)
			case it.rec.Cached:
				r := it.rec
				h := max(r.Hits, 1)
				key := r.User + "\x00" + r.Role + "\x00" + r.Protocol + "\x00" + r.SQL
				if c, ok := pending[key]; ok {
					c.hits += h
					c.dur += r.Duration * time.Duration(h)
				} else {
					pending[key] = &cachedAgg{rec: r, hits: h, dur: r.Duration * time.Duration(h)}
				}
				if len(pending) >= maxPendingCached {
					writePending(true)
				}
			default:
				a.write(it.rec)
			}
		case <-ticker.C:
			writePending(false)
		}
	}
}

func newAuditID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// write stores one record. occurred_at goes in as fixed-width text (see
// project.AuditTimeLayout): a TIMESTAMP on Postgres, text on SQLite.
func (a *auditLog) write(r contract.AuditRecord) {
	status := "ok"
	if r.Err != "" {
		status = "error"
		if strings.HasPrefix(r.Err, "permission denied") {
			status = "denied"
		}
	}
	var errText any
	if r.Err != "" {
		errText = r.Err
	}
	hits := r.Hits
	if hits < 1 {
		hits = 1
	}
	err := retryLocked(func() error {
		return a.eng.Exec(context.Background(),
			`INSERT INTO `+AuditTable+` (occurred_at, user_name, role, protocol, sql_text, duration_ms, rows_returned, status, error, audit_id, cached, hits) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.At.UTC().Format(project.AuditTimeLayout), r.User, r.Role, r.Protocol, r.SQL, r.Duration.Milliseconds(), r.Rows, status, errText, newAuditID(), r.Cached, hits)
	})
	if err != nil {
		if a.quiet {
			slog.Debug("audit write failed", "error", err)
		} else {
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
