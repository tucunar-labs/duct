// Package admin is the admin HTTP API of `duct serve` (ADR 0013): users, roles,
// grants, tokens, quotas and a status page, as JSON under /admin/v1, plus the
// /readyz probe. It is plain net/http over the governance store; the HTTP
// protocol module mounts it (see modules/serve-http Mount).
//
// Every request authenticates with the local auth provider (Bearer token or
// Basic) and must belong to the duct_admin role: no credentials is 401, any
// other role 403. Every request, allowed or not, is audited with protocol
// "admin". Secrets (tokens, temporary passwords) are returned once, in the
// response of the call that created them, and never logged or listed.
//
// The OpenAPI description is specs/admin.openapi.yaml.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
	"github.com/alonsoburon/duct/core/project"
)

// Prefix is where the API lives.
const Prefix = "/admin/v1"

// Config wires the API.
type Config struct {
	Auth    contract.AuthProvider
	Handler *governance.Handler
	Engine  contract.Engine
	Project *project.Project
	Version string
	// Components reports the health of the optional components (jobs, stream,
	// alerts) for /status; may be nil.
	Components func(ctx context.Context) map[string]any
	Now        func() time.Time
}

// API is the http.Handler of /admin/v1.
type API struct {
	cfg     Config
	mux     *http.ServeMux
	started time.Time
}

// New returns the API handler. Mount it at Prefix + "/".
func New(cfg Config) *API {
	a := &API{cfg: cfg, mux: http.NewServeMux(), started: time.Now()}
	a.routes()
	return a
}

// ServeHTTP implements http.Handler.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) { a.mux.ServeHTTP(w, r) }

// errStatus is an error with an HTTP status.
type errStatus struct {
	code int
	msg  string
}

func (e *errStatus) Error() string { return e.msg }

func bad(format string, a ...any) error {
	return &errStatus{http.StatusBadRequest, fmt.Sprintf(format, a...)}
}

// classify maps a store error to a status.
func classify(err error) *errStatus {
	var es *errStatus
	if errors.As(err, &es) {
		return es
	}
	msg := err.Error()
	switch {
	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return &errStatus{http.StatusServiceUnavailable, msg}
	case strings.Contains(msg, "does not exist"):
		return &errStatus{http.StatusNotFound, msg}
	case strings.Contains(msg, "already exists"), strings.Contains(msg, "still has users"), strings.Contains(msg, "already has"):
		return &errStatus{http.StatusConflict, msg}
	}
	return &errStatus{http.StatusBadRequest, msg}
}

// call is what a route handler returns: a status and a JSON body (nil: none),
// plus a note for the audit record (never a secret).
type call struct {
	status int
	body   any
	note   string
}

type route func(r *http.Request, id contract.Identity) (call, error)

func (a *API) handle(pattern string, fn route) {
	a.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := contract.AuditRecord{At: start.UTC(), Protocol: "admin", User: "-"}
		finish := func(status int, note, errText string) {
			rec.SQL = r.Method + " " + r.URL.Path
			if note != "" {
				rec.SQL += " " + note
			}
			rec.Duration = time.Since(start)
			rec.Err = errText
			a.cfg.Handler.Audit(r.Context(), rec)
		}
		id, err := a.authenticate(r)
		switch {
		case errors.Is(err, contract.ErrUnauthenticated):
			w.Header().Set("WWW-Authenticate", `Bearer realm="duct-admin"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
			finish(http.StatusUnauthorized, "", "unauthenticated")
			return
		case err != nil:
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "authentication unavailable"})
			finish(http.StatusInternalServerError, "", "authentication unavailable")
			return
		}
		rec.User, rec.Role = id.User, id.Role
		if !governance.IsAdmin(id) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "the admin API requires the " + governance.AdminRole + " role"})
			finish(http.StatusForbidden, "", "permission denied: the admin API requires the "+governance.AdminRole+" role")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		res, err := fn(r.WithContext(ctx), id)
		if err != nil {
			es := classify(err)
			writeJSON(w, es.code, map[string]string{"error": es.msg})
			finish(es.code, res.note, es.msg)
			return
		}
		if res.status == 0 {
			res.status = http.StatusOK
		}
		writeJSON(w, res.status, res.body)
		finish(res.status, res.note, "")
	})
}

func (a *API) authenticate(r *http.Request) (contract.Identity, error) {
	h := r.Header.Get("Authorization")
	switch {
	case strings.HasPrefix(h, "Bearer "):
		return a.cfg.Auth.Authenticate(r.Context(), contract.Credentials{Scheme: "bearer", Secret: strings.TrimSpace(h[7:])})
	case strings.HasPrefix(h, "Basic "):
		if user, pass, ok := r.BasicAuth(); ok {
			return a.cfg.Auth.Authenticate(r.Context(), contract.Credentials{Scheme: "password", User: user, Secret: pass})
		}
	}
	return contract.Identity{}, contract.ErrUnauthenticated
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if body == nil {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(body)
}

// decode reads a JSON body (max 64 KiB, unknown fields refused).
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		if errors.Is(err, io.EOF) {
			return bad("a JSON body is required")
		}
		return bad("invalid JSON body: %v", err)
	}
	return nil
}

// admin runs fn with the governance store under the admin lock.
func (a *API) admin(fn func(*governance.Store) error) error { return a.cfg.Handler.WithAdmin(fn) }

// read runs fn with a store for read-only calls (no lock needed).
func (a *API) read() *governance.Store { return governance.NewStore(a.cfg.Engine) }

func (a *API) routes() {
	p := Prefix
	a.handle("GET "+p+"/status", a.status)

	a.handle("GET "+p+"/users", a.listUsers)
	a.handle("POST "+p+"/users", a.createUser)
	a.handle("GET "+p+"/users/{name}", a.getUser)
	a.handle("DELETE "+p+"/users/{name}", a.deleteUser)
	a.handle("POST "+p+"/users/{name}/disable", a.setDisabled(true))
	a.handle("POST "+p+"/users/{name}/enable", a.setDisabled(false))
	a.handle("POST "+p+"/users/{name}/password-reset", a.passwordReset)
	a.handle("PUT "+p+"/users/{name}/role", a.setUserRole)

	a.handle("GET "+p+"/roles", a.listRoles)
	a.handle("POST "+p+"/roles", a.createRole)
	a.handle("DELETE "+p+"/roles/{role}", a.deleteRole)
	a.handle("GET "+p+"/roles/{role}/grants", a.listGrants)
	a.handle("PUT "+p+"/roles/{role}/grants/{relation}", a.grant)
	a.handle("DELETE "+p+"/roles/{role}/grants/{relation}", a.revoke)
	a.handle("PUT "+p+"/roles/{role}/grants/{relation}/row-filter", a.setRowFilter)
	a.handle("DELETE "+p+"/roles/{role}/grants/{relation}/row-filter", a.dropRowFilter)
	a.handle("PUT "+p+"/roles/{role}/grants/{relation}/masks/{column}", a.setMask)
	a.handle("DELETE "+p+"/roles/{role}/grants/{relation}/masks/{column}", a.dropMask)

	a.handle("GET "+p+"/tokens", a.listTokens)
	a.handle("POST "+p+"/tokens", a.createToken)
	a.handle("DELETE "+p+"/tokens/{id}", a.revokeToken)

	a.handle("GET "+p+"/quotas", a.listQuotas)
	a.handle("PUT "+p+"/quotas/{role}", a.setQuota)
	a.handle("DELETE "+p+"/quotas/{role}", a.deleteQuota)
}

// ---- users ----

type userJSON struct {
	Name     string `json:"name"`
	Role     string `json:"role"`
	Disabled bool   `json:"disabled"`
}

func (a *API) listUsers(r *http.Request, _ contract.Identity) (call, error) {
	us, err := a.read().Users(r.Context())
	if err != nil {
		return call{}, err
	}
	out := make([]userJSON, len(us))
	for i, u := range us {
		out[i] = userJSON{u.Name, u.Role, u.Disabled}
	}
	return call{body: map[string]any{"users": out}}, nil
}

func (a *API) getUser(r *http.Request, _ contract.Identity) (call, error) {
	u, ok, err := a.read().UserByName(r.Context(), r.PathValue("name"))
	if err != nil {
		return call{}, err
	}
	if !ok {
		return call{}, &errStatus{http.StatusNotFound, fmt.Sprintf("user %q does not exist", r.PathValue("name"))}
	}
	return call{body: userJSON{u.Name, u.Role, u.Disabled}}, nil
}

func (a *API) createUser(r *http.Request, _ contract.Identity) (call, error) {
	var in struct {
		Name     string `json:"name"`
		Role     string `json:"role"`
		Password string `json:"password"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if in.Name == "" || in.Role == "" {
		return call{}, bad("name and role are required")
	}
	out := map[string]any{"name": in.Name, "role": in.Role}
	err := a.admin(func(s *governance.Store) error {
		if in.Password != "" {
			return s.CreateUser(r.Context(), in.Name, in.Role, in.Password)
		}
		// No password given: create with a random one, returned once.
		tmp, err := randomSecret()
		if err != nil {
			return err
		}
		if err := s.CreateUser(r.Context(), in.Name, in.Role, tmp); err != nil {
			return err
		}
		out["temporary_password"] = tmp
		return nil
	})
	if err != nil {
		return call{}, err
	}
	return call{status: http.StatusCreated, body: out, note: "name=" + in.Name + " role=" + in.Role}, nil
}

func (a *API) deleteUser(r *http.Request, id contract.Identity) (call, error) {
	name := r.PathValue("name")
	if name == id.User {
		return call{}, &errStatus{http.StatusConflict, "an admin cannot delete themselves"}
	}
	if err := a.admin(func(s *governance.Store) error { return s.DropUser(r.Context(), name) }); err != nil {
		return call{}, err
	}
	return call{status: http.StatusNoContent, note: "name=" + name}, nil
}

func (a *API) setDisabled(disabled bool) route {
	return func(r *http.Request, id contract.Identity) (call, error) {
		name := r.PathValue("name")
		if disabled && name == id.User {
			return call{}, &errStatus{http.StatusConflict, "an admin cannot disable themselves"}
		}
		if err := a.admin(func(s *governance.Store) error { return s.SetDisabled(r.Context(), name, disabled) }); err != nil {
			return call{}, err
		}
		return call{body: map[string]any{"name": name, "disabled": disabled}, note: "name=" + name}, nil
	}
}

// passwordReset gives the user a new random password and returns it once. The
// user's tokens stay valid: revoke them separately when the reset answers a
// compromise.
func (a *API) passwordReset(r *http.Request, _ contract.Identity) (call, error) {
	name := r.PathValue("name")
	var pw string
	err := a.admin(func(s *governance.Store) (err error) {
		pw, err = s.ResetPassword(r.Context(), name)
		return err
	})
	if err != nil {
		return call{}, err
	}
	return call{body: map[string]any{"name": name, "temporary_password": pw}, note: "name=" + name}, nil
}

func (a *API) setUserRole(r *http.Request, _ contract.Identity) (call, error) {
	name := r.PathValue("name")
	var in struct {
		Role string `json:"role"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if in.Role == "" {
		return call{}, bad("role is required")
	}
	if err := a.admin(func(s *governance.Store) error { return s.SetUserRole(r.Context(), name, in.Role) }); err != nil {
		return call{}, err
	}
	return call{body: map[string]any{"name": name, "role": in.Role}, note: "name=" + name + " role=" + in.Role}, nil
}

// ---- roles and grants ----

type roleJSON struct {
	Name       string            `json:"name"`
	Users      int               `json:"users"`
	Grants     []string          `json:"grants"`
	RowFilters map[string]string `json:"row_filters"`
	Masks      map[string]string `json:"masks"`
}

func roleOut(r governance.RoleInfo) roleJSON {
	g := r.Grants
	if g == nil {
		g = []string{}
	}
	return roleJSON{r.Name, r.Users, g, r.Filters, r.Masks}
}

func (a *API) listRoles(r *http.Request, _ contract.Identity) (call, error) {
	rs, err := a.read().Roles(r.Context())
	if err != nil {
		return call{}, err
	}
	out := make([]roleJSON, len(rs))
	for i, x := range rs {
		out[i] = roleOut(x)
	}
	return call{body: map[string]any{"roles": out}}, nil
}

func (a *API) execStmt(r *http.Request, st *governance.Stmt) error {
	return a.admin(func(s *governance.Store) error {
		_, err := s.Execute(r.Context(), st)
		return err
	})
}

func (a *API) createRole(r *http.Request, _ contract.Identity) (call, error) {
	var in struct {
		Name string `json:"name"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if in.Name == "" {
		return call{}, bad("name is required")
	}
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindCreateRole, Role: in.Name}); err != nil {
		return call{}, err
	}
	return call{status: http.StatusCreated, body: map[string]string{"name": in.Name}, note: "name=" + in.Name}, nil
}

func (a *API) deleteRole(r *http.Request, _ contract.Identity) (call, error) {
	name := r.PathValue("role")
	if name == governance.AdminRole {
		return call{}, &errStatus{http.StatusConflict, "the " + governance.AdminRole + " role cannot be deleted"}
	}
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindDropRole, Role: name}); err != nil {
		return call{}, err
	}
	_ = a.admin(func(s *governance.Store) error { return s.DeleteQuota(r.Context(), name) })
	a.cfg.Handler.InvalidateQuotas()
	return call{status: http.StatusNoContent, note: "name=" + name}, nil
}

func (a *API) listGrants(r *http.Request, _ contract.Identity) (call, error) {
	rs, err := a.read().Roles(r.Context())
	if err != nil {
		return call{}, err
	}
	for _, x := range rs {
		if x.Name == r.PathValue("role") {
			return call{body: roleOut(x)}, nil
		}
	}
	return call{}, &errStatus{http.StatusNotFound, fmt.Sprintf("role %q does not exist", r.PathValue("role"))}
}

func (a *API) grant(r *http.Request, _ contract.Identity) (call, error) {
	role, rel := r.PathValue("role"), r.PathValue("relation")
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindGrant, Role: role, Relation: rel}); err != nil {
		return call{}, err
	}
	return call{body: map[string]string{"role": role, "relation": rel, "privilege": "select"}, note: "role=" + role + " relation=" + rel}, nil
}

func (a *API) revoke(r *http.Request, _ contract.Identity) (call, error) {
	role, rel := r.PathValue("role"), r.PathValue("relation")
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindRevoke, Role: role, Relation: rel}); err != nil {
		return call{}, err
	}
	return call{status: http.StatusNoContent, note: "role=" + role + " relation=" + rel}, nil
}

func (a *API) setRowFilter(r *http.Request, _ contract.Identity) (call, error) {
	role, rel := r.PathValue("role"), r.PathValue("relation")
	var in struct {
		Predicate string `json:"predicate"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if strings.TrimSpace(in.Predicate) == "" {
		return call{}, bad("predicate is required")
	}
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindSetRowFilter, Role: role, Relation: rel, Expr: in.Predicate}); err != nil {
		return call{}, err
	}
	return call{body: map[string]string{"role": role, "relation": rel, "predicate": in.Predicate}, note: "role=" + role + " relation=" + rel}, nil
}

func (a *API) dropRowFilter(r *http.Request, _ contract.Identity) (call, error) {
	role, rel := r.PathValue("role"), r.PathValue("relation")
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindDropRowFilter, Role: role, Relation: rel}); err != nil {
		return call{}, err
	}
	return call{status: http.StatusNoContent, note: "role=" + role + " relation=" + rel}, nil
}

func (a *API) setMask(r *http.Request, _ contract.Identity) (call, error) {
	role, rel, col := r.PathValue("role"), r.PathValue("relation"), r.PathValue("column")
	var in struct {
		Expr string `json:"expr"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if strings.TrimSpace(in.Expr) == "" {
		return call{}, bad("expr is required")
	}
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindSetMask, Role: role, Relation: rel, Column: col, Expr: in.Expr}); err != nil {
		return call{}, err
	}
	return call{body: map[string]string{"role": role, "relation": rel, "column": col, "expr": in.Expr}, note: "role=" + role + " relation=" + rel + " column=" + col}, nil
}

func (a *API) dropMask(r *http.Request, _ contract.Identity) (call, error) {
	role, rel, col := r.PathValue("role"), r.PathValue("relation"), r.PathValue("column")
	if err := a.execStmt(r, &governance.Stmt{Kind: governance.KindDropMask, Role: role, Relation: rel, Column: col}); err != nil {
		return call{}, err
	}
	return call{status: http.StatusNoContent, note: "role=" + role + " relation=" + rel + " column=" + col}, nil
}

// ---- tokens ----

type tokenJSON struct {
	ID        string `json:"id"`
	User      string `json:"user"`
	Label     string `json:"label,omitempty"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at,omitempty"`
	Revoked   bool   `json:"revoked"`
}

func (a *API) listTokens(r *http.Request, _ contract.Identity) (call, error) {
	ts, err := a.read().Tokens(r.Context(), r.URL.Query().Get("user"))
	if err != nil {
		return call{}, err
	}
	out := make([]tokenJSON, len(ts))
	for i, t := range ts {
		out[i] = tokenJSON{t.ID, t.User, t.Label, t.CreatedAt, t.ExpiresAt, t.Revoked}
	}
	return call{body: map[string]any{"tokens": out}}, nil
}

func (a *API) createToken(r *http.Request, _ contract.Identity) (call, error) {
	var in struct {
		User       string `json:"user"`
		Label      string `json:"label"`
		TTLSeconds int64  `json:"ttl_seconds"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if in.User == "" {
		return call{}, bad("user is required")
	}
	if in.TTLSeconds < 0 {
		return call{}, bad("ttl_seconds must be 0 (no expiry) or more")
	}
	var tok, id string
	err := a.admin(func(s *governance.Store) (err error) {
		tok, id, err = s.CreateToken(r.Context(), in.User, in.Label, time.Duration(in.TTLSeconds)*time.Second)
		return err
	})
	if err != nil {
		return call{}, err
	}
	out := map[string]any{"id": id, "user": in.User, "label": in.Label, "token": tok}
	if in.TTLSeconds > 0 {
		out["expires_in_seconds"] = in.TTLSeconds
	}
	return call{status: http.StatusCreated, body: out, note: "id=" + id + " user=" + in.User}, nil
}

func (a *API) revokeToken(r *http.Request, _ contract.Identity) (call, error) {
	id := r.PathValue("id")
	if err := a.admin(func(s *governance.Store) error { return s.RevokeToken(r.Context(), id) }); err != nil {
		return call{}, err
	}
	return call{body: map[string]any{"id": id, "revoked": true}, note: "id=" + id}, nil
}

// ---- quotas ----

type quotaJSON struct {
	Role          string `json:"role"`
	MaxConcurrent *int   `json:"max_concurrent"`
	MaxRows       *int64 `json:"max_rows"`
}

func (a *API) listQuotas(r *http.Request, _ contract.Identity) (call, error) {
	qs, err := a.read().Quotas(r.Context())
	if err != nil {
		return call{}, err
	}
	out := make([]quotaJSON, len(qs))
	for i, q := range qs {
		out[i] = quotaJSON{q.Role, q.MaxConcurrent, q.MaxRows}
	}
	return call{body: map[string]any{"quotas": out}}, nil
}

func (a *API) setQuota(r *http.Request, _ contract.Identity) (call, error) {
	role := r.PathValue("role")
	var in struct {
		MaxConcurrent *int   `json:"max_concurrent"`
		MaxRows       *int64 `json:"max_rows"`
	}
	if err := decode(r, &in); err != nil {
		return call{}, err
	}
	if in.MaxConcurrent == nil && in.MaxRows == nil {
		return call{}, bad("set max_concurrent, max_rows or both (DELETE removes the quota)")
	}
	if err := a.admin(func(s *governance.Store) error { return s.SetQuota(r.Context(), role, in.MaxConcurrent, in.MaxRows) }); err != nil {
		return call{}, err
	}
	a.cfg.Handler.InvalidateQuotas()
	note := "role=" + role
	if in.MaxConcurrent != nil {
		note += fmt.Sprintf(" max_concurrent=%d", *in.MaxConcurrent)
	}
	if in.MaxRows != nil {
		note += fmt.Sprintf(" max_rows=%d", *in.MaxRows)
	}
	return call{body: quotaJSON{role, in.MaxConcurrent, in.MaxRows}, note: note}, nil
}

func (a *API) deleteQuota(r *http.Request, _ contract.Identity) (call, error) {
	role := r.PathValue("role")
	if err := a.admin(func(s *governance.Store) error { return s.DeleteQuota(r.Context(), role) }); err != nil {
		return call{}, err
	}
	a.cfg.Handler.InvalidateQuotas()
	return call{status: http.StatusNoContent, note: "role=" + role}, nil
}

// ---- status ----

func (a *API) status(r *http.Request, _ contract.Identity) (call, error) {
	ctx := r.Context()
	s := a.cfg.Project
	out := map[string]any{
		"version":        a.cfg.Version,
		"go":             runtime.Version(),
		"uptime_seconds": int64(time.Since(a.started).Seconds()),
	}
	q := func(sql string) ([][]string, error) { return queryRows(ctx, a.cfg.Engine, sql) }
	if rows, err := q("SELECT version()"); err == nil && len(rows) > 0 {
		out["duckdb"] = rows[0][0]
	}
	if rows, err := q("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'"); err == nil && len(rows) > 0 {
		out["ducklake"] = rows[0][0]
	}
	cat := "sqlite"
	if s.CatalogIsPostgres() {
		cat = "postgres"
	}
	out["catalog"] = map[string]any{"type": cat, "ref": s.CatalogRef()}
	kind := "local"
	if s.StorageIsS3() {
		kind = "s3"
	}
	out["storage"] = map[string]any{"kind": kind, "path": s.Config.Storage}

	applied := []int{}
	if rows, err := q("SELECT CAST(version AS VARCHAR) FROM duct.duct_migrations ORDER BY version"); err == nil {
		for _, r := range rows {
			var v int
			_, _ = fmt.Sscan(r[0], &v)
			applied = append(applied, v)
		}
	}
	have := map[int]bool{}
	for _, v := range applied {
		have[v] = true
	}
	pending := []int{}
	latest := 0
	for _, m := range project.Migrations {
		latest = max(latest, m.Version)
		if !have[m.Version] {
			pending = append(pending, m.Version)
		}
	}
	out["migrations"] = map[string]any{"applied": applied, "latest": latest, "pending": pending}

	if rows, err := q("SELECT CAST(snapshot_id AS VARCHAR), CAST(snapshot_time AS VARCHAR) FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC LIMIT 1"); err == nil && len(rows) > 0 {
		var id int64
		_, _ = fmt.Sscan(rows[0][0], &id)
		out["snapshot"] = map[string]any{"id": id, "time": rows[0][1]}
	}
	if a.cfg.Components != nil {
		out["components"] = a.cfg.Components(ctx)
	}
	return call{body: out}, nil
}

func queryRows(ctx context.Context, eng contract.Engine, sql string) ([][]string, error) {
	st, err := eng.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				if col := rec.Column(c); !col.IsNull(i) {
					row[c] = strings.Clone(col.ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	return out, st.Err()
}
