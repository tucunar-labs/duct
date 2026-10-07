package governance

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alonsoburon/duct/core/metrics"
)

// Quota caps what a role may consume. A nil limit means unlimited. The
// handler enforces both over the protocols it runs itself (HTTP, MCP, web);
// the Postgres wire protocol runs its own DuckDB session and is not covered
// (ADR 0013).
type Quota struct {
	Role          string
	MaxConcurrent *int   // statements running at the same time, per role, per process
	MaxRows       *int64 // rows one statement may return
}

// quotaPrefix starts every quota error, so protocols can map it to 429.
const quotaPrefix = "quota exceeded"

// IsQuotaError reports whether err was produced by a quota limit.
func IsQuotaError(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), quotaPrefix)
}

func quotaErr(format string, a ...any) error {
	return errors.New(quotaPrefix + ": " + fmt.Sprintf(format, a...))
}

var quotaRejections = metrics.Default().Counter("duct_quota_rejections_total", "Statements refused or cut by a role quota.", "kind")

// SetQuota creates or replaces the quota of a role. A nil limit clears it;
// limits must be positive.
func (s *Store) SetQuota(ctx context.Context, role string, maxConcurrent *int, maxRows *int64) error {
	if err := s.requireRole(ctx, role); err != nil {
		return err
	}
	if maxConcurrent != nil && *maxConcurrent < 1 {
		return errors.New("max_concurrent must be at least 1")
	}
	if maxRows != nil && *maxRows < 1 {
		return errors.New("max_rows must be at least 1")
	}
	conc, rows := "NULL", "NULL"
	if maxConcurrent != nil {
		conc = strconv.Itoa(*maxConcurrent)
	}
	if maxRows != nil {
		rows = strconv.FormatInt(*maxRows, 10)
	}
	return s.execTx(ctx,
		"DELETE FROM duct.duct_quotas WHERE role = "+lit(role),
		"INSERT INTO duct.duct_quotas (role, max_concurrent, max_rows, updated_at) VALUES ("+lit(role)+", "+conc+", "+rows+", now())")
}

// DeleteQuota removes the quota of a role (no error if it had none).
func (s *Store) DeleteQuota(ctx context.Context, role string) error {
	return s.exec(ctx, "DELETE FROM duct.duct_quotas WHERE role = ?", role)
}

// Quotas lists every quota.
func (s *Store) Quotas(ctx context.Context) ([]Quota, error) {
	rs, err := s.rows(ctx, `SELECT role, CAST(max_concurrent AS VARCHAR), CAST(max_rows AS VARCHAR) FROM duct.duct_quotas ORDER BY role`)
	if err != nil {
		return nil, err
	}
	out := make([]Quota, 0, len(rs))
	for _, r := range rs {
		q := Quota{Role: r[0]}
		if n, err := strconv.Atoi(r[1]); err == nil {
			q.MaxConcurrent = &n
		}
		if n, err := strconv.ParseInt(r[2], 10, 64); err == nil {
			q.MaxRows = &n
		}
		out = append(out, q)
	}
	return out, nil
}

// quotaTTL is how long the handler trusts a quota it read; changes made by
// another process (the CLI) apply after it, the admin API invalidates at once.
const quotaTTL = 5 * time.Second

type quotaCache struct {
	mu     sync.Mutex
	at     time.Time
	byRole map[string]Quota
	active map[string]int
}

func (c *quotaCache) get(ctx context.Context, s *Store, role string) (Quota, error) {
	c.mu.Lock()
	fresh := c.byRole != nil && time.Since(c.at) < quotaTTL
	c.mu.Unlock()
	if !fresh {
		qs, err := s.Quotas(ctx)
		if err != nil {
			return Quota{}, err
		}
		m := make(map[string]Quota, len(qs))
		for _, q := range qs {
			m[q.Role] = q
		}
		c.mu.Lock()
		c.byRole, c.at = m, time.Now()
		c.mu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byRole[role], nil
}

// admit takes a concurrency slot for role when it has a limit, and returns the
// quota in force and the function that gives the slot back.
func (c *quotaCache) admit(ctx context.Context, s *Store, role string) (Quota, func(), error) {
	q, err := c.get(ctx, s, role)
	if err != nil {
		return Quota{}, nil, err
	}
	if q.MaxConcurrent == nil {
		return q, func() {}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active == nil {
		c.active = map[string]int{}
	}
	if c.active[role] >= *q.MaxConcurrent {
		quotaRejections.Inc("concurrent")
		return q, nil, quotaErr("role %s may run %d statements at a time", role, *q.MaxConcurrent)
	}
	c.active[role]++
	var once sync.Once
	return q, func() {
		once.Do(func() {
			c.mu.Lock()
			c.active[role]--
			c.mu.Unlock()
		})
	}, nil
}

// InvalidateQuotas makes the handler re-read quotas on the next statement.
func (h *Handler) InvalidateQuotas() {
	h.quotas.mu.Lock()
	h.quotas.byRole = nil
	h.quotas.mu.Unlock()
}

// WithAdmin runs fn with the Store of the handler's engine while holding the
// lock that serialises governance changes (the same one governance SQL uses).
func (h *Handler) WithAdmin(fn func(*Store) error) error {
	h.adminMu.Lock()
	defer h.adminMu.Unlock()
	return fn(NewStore(h.eng))
}
