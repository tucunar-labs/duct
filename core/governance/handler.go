package governance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// HandlerOptions tune a Handler.
type HandlerOptions struct {
	// Timeout caps one statement (default 60 s; negative disables it).
	Timeout time.Duration
	// AdminOnlyFunctions are user-defined functions (functions/ of the project,
	// ADR 0005): they run user code, so only the admin role may call them. Any
	// other role gets "permission denied", whether or not the name resolves.
	AdminOnlyFunctions []string
}

// Handler is the shared contract.QueryHandler of `duct serve`. It executes
// each statement as its Identity: the statement is parsed with DuckDB's own
// parser, must be a single SELECT, may only reference views of the role's
// schema (role_<role>) and no table function, and is rewritten to fully
// qualified names before it runs. Every statement, allowed or not, is audited.
//
// The Engine should be the locked one (ADR 0007); the guard does not depend on
// the lock, which is the second layer.
type Handler struct {
	eng   contract.Engine
	opt   HandlerOptions
	audit *auditLog

	adminMu sync.Mutex // serialises governance statements
	quotas  quotaCache // per-role limits (quota.go)

	semModel atomic.Pointer[semanticModel] // see semantic.go; nil: no model
	udfNames atomic.Pointer[[]string]      // current admin-only functions (udf.go); nil: opt's
	fresh    freshCache                    // preaggregates that may serve a role (preagg.go)
	console  consoleHooks                  // admin API and jobs, for the web console (console.go)
}

var (
	_ contract.QueryHandler = (*Handler)(nil)
	_ contract.Auditor      = (*Handler)(nil)
	_ contract.Limiter      = (*Handler)(nil)
)

// NewHandler returns a Handler over eng. Close it to flush the audit log.
func NewHandler(eng contract.Engine, opt HandlerOptions) *Handler {
	if opt.Timeout == 0 {
		opt.Timeout = time.Minute
	}
	return &Handler{eng: eng, opt: opt, audit: newAuditLog(eng)}
}

// Close flushes the audit log and stops its writer. It does not close eng.
func (h *Handler) Close() error { return h.audit.close() }

// FlushAudit blocks until every audit record accepted so far is written.
func (h *Handler) FlushAudit(ctx context.Context) error { return h.audit.flush(ctx) }

// Audit implements contract.Auditor.
func (h *Handler) Audit(_ context.Context, rec contract.AuditRecord) { h.audit.add(rec) }

// ForProtocol returns a view of h that labels its audit records with the
// protocol name.
func (h *Handler) ForProtocol(name string) contract.QueryHandler {
	return &protocolHandler{h: h, name: name}
}

type protocolHandler struct {
	h    *Handler
	name string
}

func (p *protocolHandler) Query(ctx context.Context, id contract.Identity, sql string) (contract.Stream, error) {
	return p.h.query(ctx, p.name, id, sql)
}

// Audit forwards, so a Protocol still finds the Auditor on the value it got.
func (p *protocolHandler) Audit(ctx context.Context, rec contract.AuditRecord) {
	if rec.Protocol == "" {
		rec.Protocol = p.name
	}
	p.h.Audit(ctx, rec)
}

// Query implements contract.QueryHandler (audit protocol "unknown").
func (h *Handler) Query(ctx context.Context, id contract.Identity, sql string) (contract.Stream, error) {
	return h.query(ctx, "unknown", id, sql)
}

func (h *Handler) query(ctx context.Context, proto string, id contract.Identity, sql string) (contract.Stream, error) {
	start := time.Now()
	rec := contract.AuditRecord{At: start.UTC(), User: id.User, Role: id.Role, Protocol: proto, SQL: sql}
	fail := func(err error) (contract.Stream, error) {
		rec.Err = err.Error()
		rec.Duration = time.Since(start)
		h.audit.add(rec)
		return nil, err
	}
	// Governance statements first: they are neither SELECTs nor on the role
	// schema, and only the admin role may run them.
	rewritten, handled, err := h.Administer(ctx, id, sql)
	if err != nil {
		return fail(err)
	}
	switch {
	case handled:
	case compiledByUs(ctx):
		// SQL the semantic compiler produced for a preaggregate (semantic.go):
		// it reads semantic_cache, which the role schema guard does not allow.
		rewritten = sql
	default:
		if rewritten, err = h.authorize(ctx, id, sql); err != nil {
			return fail(err)
		}
	}
	quota, release, err := h.quotas.admit(ctx, NewStore(h.eng), id.Role)
	if err != nil {
		return fail(err)
	}
	qctx, cancel := ctx, context.CancelFunc(func() {})
	if h.opt.Timeout > 0 {
		qctx, cancel = context.WithTimeout(ctx, h.opt.Timeout)
	}
	st, err := h.eng.Query(qctx, rewritten)
	if err != nil {
		cancel()
		release()
		return fail(err)
	}
	s := &auditedStream{Stream: st, cancel: cancel, release: release, role: id.Role, h: h, rec: rec, start: start}
	if quota.MaxRows != nil {
		s.maxRows = *quota.MaxRows
	}
	s.refs.Store(1)
	return s, nil
}

// authorize returns the SQL to run for id: the parsed, checked and
// rewritten statement.
func (h *Handler) authorize(ctx context.Context, id contract.Identity, sql string) (string, error) {
	if id.Role == "" || !roleRe.MatchString(id.Role) {
		return "", errEmptyIdentity
	}
	if strings.TrimSpace(sql) == "" {
		return "", errors.New("empty statement")
	}
	views, err := h.roleViews(ctx, id.Role)
	if err != nil {
		return "", err
	}
	js, err := h.scalar(ctx, "SELECT CAST(json_serialize_sql("+lit(sql)+") AS VARCHAR)")
	if err != nil {
		return "", fmt.Errorf("syntax error: %w", err)
	}
	p, err := parseAST(js)
	if err != nil {
		return "", err
	}
	rewritten, err := p.rewrite(id.Role, views, h.deniedFunctionsFor(id))
	if err != nil {
		return "", err
	}
	out, err := h.scalar(ctx, "SELECT json_deserialize_sql(CAST("+lit(rewritten)+" AS JSON))")
	if err != nil {
		return "", fmt.Errorf("cannot rebuild statement: %w", err)
	}
	return out, nil
}

func (h *Handler) scalar(ctx context.Context, q string, args ...any) (string, error) {
	st, err := h.eng.Query(ctx, q, args...)
	if err != nil {
		return "", err
	}
	defer st.Release()
	var out string
	for st.Next() {
		rec := st.RecordBatch()
		if rec.NumRows() > 0 && out == "" {
			out = strings.Clone(rec.Column(0).ValueStr(0))
		}
	}
	return out, st.Err()
}

func (h *Handler) roleViews(ctx context.Context, role string) (map[string]bool, error) {
	st, err := h.eng.Query(ctx, `SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = `+lit(contract.RoleSchema(role)))
	if err != nil {
		return nil, err
	}
	defer st.Release()
	views := map[string]bool{}
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			views[strings.ToLower(strings.Clone(rec.Column(0).ValueStr(i)))] = true
		}
	}
	return views, st.Err()
}

// auditedStream counts rows and, on the last Release, closes the timeout and
// writes the audit record.
type auditedStream struct {
	contract.Stream
	cancel context.CancelFunc
	h      *Handler
	rec    contract.AuditRecord
	start  time.Time
	rows   atomic.Int64
	refs   atomic.Int64
	once   sync.Once

	release func() // gives back the role's concurrency slot
	role    string
	maxRows int64       // 0: unlimited
	cut     atomic.Bool // the row quota stopped the stream
}

func (s *auditedStream) Next() bool {
	ok := s.Stream.Next()
	if ok {
		n := s.Stream.RecordBatch().NumRows()
		if s.maxRows > 0 && s.rows.Load()+n > s.maxRows {
			// The batch that would cross the limit is never delivered.
			s.cut.Store(true)
			quotaRejections.Inc("rows")
			return false
		}
		s.rows.Add(n)
	}
	return ok
}

// Err reports the row quota before any error of the underlying stream.
func (s *auditedStream) Err() error {
	if s.cut.Load() {
		return quotaErr("role %s may receive at most %d rows per statement", s.role, s.maxRows)
	}
	return s.Stream.Err()
}

func (s *auditedStream) Retain() {
	s.refs.Add(1)
	s.Stream.Retain()
}

func (s *auditedStream) Release() {
	err := s.Err()
	s.Stream.Release()
	if s.refs.Add(-1) != 0 {
		return
	}
	s.once.Do(func() {
		s.cancel()
		s.release()
		s.rec.Rows = s.rows.Load()
		s.rec.Duration = time.Since(s.start)
		if err != nil {
			s.rec.Err = err.Error()
		}
		s.h.audit.add(s.rec)
	})
}

// Catalog lists the columns of every view in the identity's role schema
// (table_schema, table_name, column_name, data_type, is_nullable). It runs a
// fixed query with the engine's privileges, filtered to the role schema, so
// UIs can show a catalog without information_schema being open to user
// statements. is_nullable is "YES"/"NO" (information_schema); older callers
// ignore the extra column.
func (h *Handler) Catalog(ctx context.Context, id contract.Identity) (contract.Stream, error) {
	if id.Role == "" || !roleRe.MatchString(id.Role) {
		return nil, errEmptyIdentity
	}
	return h.eng.Query(ctx, `SELECT table_schema, table_name, column_name, data_type, is_nullable FROM information_schema.columns
		WHERE table_catalog = 'lake' AND table_schema = `+lit(contract.RoleSchema(id.Role))+` ORDER BY table_name, ordinal_position`)
}

// Administer forwards to the Handler.
func (p *protocolHandler) Administer(ctx context.Context, id contract.Identity, sql string) (string, bool, error) {
	return p.h.Administer(ctx, id, sql)
}

// Admit forwards to the Handler.
func (p *protocolHandler) Admit(ctx context.Context, id contract.Identity) (contract.Admission, error) {
	return p.h.Admit(ctx, id)
}

// Catalog forwards to the Handler.
func (p *protocolHandler) Catalog(ctx context.Context, id contract.Identity) (contract.Stream, error) {
	return p.h.Catalog(ctx, id)
}
