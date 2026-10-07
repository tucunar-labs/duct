package servepg

import (
	"context"
	"sync"

	"github.com/posthog/duckgres/server"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// quotaExecutor applies the per-role quotas of ADR 0013 to the statements a
// duckgres session runs, through the handler's contract.Limiter so the
// concurrency counter is the same one HTTP, MCP and the web use. Each
// statement that returns rows takes a slot until its row set is closed, and
// the row set stops with a 53400 error when it would pass max_rows. The error
// carries SQLState() so the duckgres fork (embedderSQLState) reports 53400.
//
// It wraps only the executor given to the client connection: the relation
// lookup and the session metadata setup use the raw one. Catalog queries from
// tools (\d, JDBC metadata) pass through it too and count like any statement.
type quotaExecutor struct {
	server.QueryExecutor
	lim contract.Limiter
	id  contract.Identity

	mu   sync.Mutex
	open map[*quotaRows]struct{}
}

func newQuotaExecutor(inner server.QueryExecutor, lim contract.Limiter, id contract.Identity) *quotaExecutor {
	return &quotaExecutor{QueryExecutor: inner, lim: lim, id: id, open: map[*quotaRows]struct{}{}}
}

func (q *quotaExecutor) QueryContext(ctx context.Context, query string, args ...any) (server.RowSet, error) {
	adm, err := q.lim.Admit(ctx, q.id)
	if err != nil {
		return nil, err
	}
	rs, err := q.QueryExecutor.QueryContext(ctx, query, args...)
	if err != nil {
		adm.Release()
		return nil, err
	}
	return q.track(rs, adm), nil
}

func (q *quotaExecutor) Query(query string, args ...any) (server.RowSet, error) {
	return q.QueryContext(context.Background(), query, args...)
}

func (q *quotaExecutor) track(rs server.RowSet, adm contract.Admission) server.RowSet {
	r := &quotaRows{RowSet: rs, adm: adm, q: q}
	q.mu.Lock()
	q.open[r] = struct{}{}
	q.mu.Unlock()
	return r
}

// Close gives back the slots of row sets the session left open.
func (q *quotaExecutor) Close() error {
	q.mu.Lock()
	open := q.open
	q.open = map[*quotaRows]struct{}{}
	q.mu.Unlock()
	for r := range open {
		r.adm.Release()
	}
	return q.QueryExecutor.Close()
}

type quotaRows struct {
	server.RowSet
	adm  contract.Admission
	q    *quotaExecutor
	n    int64
	err  error
	once sync.Once
}

func (r *quotaRows) Next() bool {
	if r.err != nil {
		return false
	}
	if !r.RowSet.Next() {
		return false
	}
	if max := r.adm.MaxRows(); max > 0 && r.n >= max {
		r.err = r.adm.RowsExceeded()
		r.adm.Release()
		return false
	}
	r.n++
	return true
}

func (r *quotaRows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.RowSet.Err()
}

func (r *quotaRows) Close() error {
	r.once.Do(func() {
		r.adm.Release()
		r.q.mu.Lock()
		delete(r.q.open, r)
		r.q.mu.Unlock()
	})
	return r.RowSet.Close()
}
