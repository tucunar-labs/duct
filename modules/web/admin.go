package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Admin (ADR 0007, 0013): users, roles and grants, row filters and masks,
// tokens, quotas, status and the audit viewer, for duct_admin only (403
// otherwise, before anything else). Every change goes through the optional
// contract.AdminCaller, which runs the very same code as the REST API under
// /admin/v1: validation errors of governance reach the form unchanged, and
// nothing here duplicates a rule. Secrets (temporary passwords, new tokens)
// come back once from the call that creates them and are shown once through
// the flash store (flash.go); they are never listed, logged or stored.

const auditPageSize = 50

func (a *app) adminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin", a.authed(a.adminStatus))
	mux.HandleFunc("GET /admin/users", a.authed(a.adminUsers))
	mux.HandleFunc("POST /admin/users", a.authed(a.adminUserCreate))
	mux.HandleFunc("POST /admin/users/{name}/disable", a.authed(a.adminUserDisabled(true)))
	mux.HandleFunc("POST /admin/users/{name}/enable", a.authed(a.adminUserDisabled(false)))
	mux.HandleFunc("POST /admin/users/{name}/reset-password", a.authed(a.adminUserReset))
	mux.HandleFunc("POST /admin/users/{name}/role", a.authed(a.adminUserRole))

	mux.HandleFunc("GET /admin/roles", a.authed(a.adminRoles))
	mux.HandleFunc("POST /admin/roles", a.authed(a.adminRoleCreate))
	mux.HandleFunc("GET /admin/roles/{role}", a.authed(a.adminRole))
	mux.HandleFunc("POST /admin/roles/{role}/grants", a.authed(a.adminGrant))
	mux.HandleFunc("POST /admin/roles/{role}/grants/revoke", a.authed(a.adminRevoke))
	mux.HandleFunc("POST /admin/roles/{role}/row-filter", a.authed(a.adminFilterSet))
	mux.HandleFunc("POST /admin/roles/{role}/row-filter/drop", a.authed(a.adminFilterDrop))
	mux.HandleFunc("POST /admin/roles/{role}/masks", a.authed(a.adminMaskSet))
	mux.HandleFunc("POST /admin/roles/{role}/masks/drop", a.authed(a.adminMaskDrop))
	mux.HandleFunc("POST /admin/roles/{role}/quota", a.authed(a.adminQuotaSet))
	mux.HandleFunc("POST /admin/roles/{role}/quota/drop", a.authed(a.adminQuotaDrop))

	mux.HandleFunc("GET /admin/tokens", a.authed(a.adminTokens))
	mux.HandleFunc("POST /admin/tokens", a.authed(a.adminTokenCreate))
	mux.HandleFunc("POST /admin/tokens/{id}/revoke", a.authed(a.adminTokenRevoke))

	mux.HandleFunc("GET /admin/audit", a.authed(a.adminAudit))
}

// ---- plumbing ----

func (a *app) adminPage(sess session, title, section string) pageData {
	p := a.page(sess, title, "admin")
	p.Crumbs = []crumb{{Label: "Admin", Href: "/admin"}, {Label: title}}
	if section == "status" {
		p.Crumbs = []crumb{{Label: "Admin"}}
	}
	return p
}

// adminGate answers 403 to anyone but duct_admin. ok=false: already answered.
func (a *app) adminGate(w http.ResponseWriter, r *http.Request, sess session) (contract.AdminCaller, bool) {
	if !isAdminSession(sess) {
		a.forbidden(w, r, sess, "La administración es solo para el rol duct_admin.")
		return nil, false
	}
	c, ok := a.q.(contract.AdminCaller)
	if !ok {
		a.html(w, http.StatusNotImplemented)
		_ = notFoundPage(a.adminPage(sess, "Admin", "status"), "Este servidor no expone la API de administración.").Render(r.Context(), w)
		return nil, false
	}
	return c, true
}

// adminJSON calls the admin API; a non-2xx status becomes an error carrying the
// API's own message. in is JSON-encoded (nil: no body); out, if given, decodes a 2xx body.
func (a *app) adminJSON(ctx context.Context, c contract.AdminCaller, sess session, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		body, _ = json.Marshal(in)
	}
	ctx, cancel := context.WithTimeout(ctx, a.cfg.queryTimeout)
	defer cancel()
	status, resp, err := c.AdminCall(ctx, sess.who, method, "/admin/v1"+path, body)
	if err != nil {
		return err
	}
	if status < 200 || status > 299 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(resp, &e)
		if e.Error == "" {
			e.Error = "la API respondió " + strconv.Itoa(status)
		}
		return &apiError{status: status, msg: e.Error}
	}
	if out != nil && len(resp) > 0 {
		return json.Unmarshal(resp, out)
	}
	return nil
}

type apiError struct {
	status int
	msg    string
}

func (e *apiError) Error() string { return e.msg }

func esc(s string) string { return url.PathEscape(s) }

// ---- data shapes of the admin API ----

type adminUser struct {
	Name     string `json:"name"`
	Role     string `json:"role"`
	Disabled bool   `json:"disabled"`
}

type adminRole struct {
	Name       string            `json:"name"`
	Users      int               `json:"users"`
	Grants     []string          `json:"grants"`
	RowFilters map[string]string `json:"row_filters"`
	Masks      map[string]string `json:"masks"`
}

type adminToken struct {
	ID        string `json:"id"`
	User      string `json:"user"`
	Label     string `json:"label"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at"`
	Revoked   bool   `json:"revoked"`
}

type adminQuota struct {
	Role          string `json:"role"`
	MaxConcurrent *int   `json:"max_concurrent"`
	MaxRows       *int64 `json:"max_rows"`
}

func (a *app) loadUsers(ctx context.Context, c contract.AdminCaller, sess session) ([]adminUser, error) {
	var out struct {
		Users []adminUser `json:"users"`
	}
	err := a.adminJSON(ctx, c, sess, "GET", "/users", nil, &out)
	return out.Users, err
}

func (a *app) loadRoles(ctx context.Context, c contract.AdminCaller, sess session) ([]adminRole, error) {
	var out struct {
		Roles []adminRole `json:"roles"`
	}
	err := a.adminJSON(ctx, c, sess, "GET", "/roles", nil, &out)
	return out.Roles, err
}

func (a *app) loadQuotas(ctx context.Context, c contract.AdminCaller, sess session) (map[string]adminQuota, error) {
	var out struct {
		Quotas []adminQuota `json:"quotas"`
	}
	err := a.adminJSON(ctx, c, sess, "GET", "/quotas", nil, &out)
	m := map[string]adminQuota{}
	for _, q := range out.Quotas {
		m[q.Role] = q
	}
	return m, err
}

// ---- status ----

type statusView struct {
	Version       string `json:"version"`
	Go            string `json:"go"`
	DuckDB        string `json:"duckdb"`
	DuckLake      string `json:"ducklake"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Catalog       struct {
		Type string `json:"type"`
		Ref  string `json:"ref"`
	} `json:"catalog"`
	Storage struct {
		Kind string `json:"kind"`
		Path string `json:"path"`
	} `json:"storage"`
	Migrations struct {
		Applied []int `json:"applied"`
		Latest  int   `json:"latest"`
		Pending []int `json:"pending"`
	} `json:"migrations"`
	Snapshot *struct {
		ID   int64  `json:"id"`
		Time string `json:"time"`
	} `json:"snapshot"`
	Audit struct {
		RetentionSeconds int64 `json:"retention_seconds"`
		Export           bool  `json:"export"`
	} `json:"audit"`
	Components map[string]map[string]any `json:"components"`
}

func (a *app) adminStatus(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.adminGate(w, r, sess)
	if !ok {
		return
	}
	var st statusView
	errMsg := ""
	if err := a.adminJSON(r.Context(), c, sess, "GET", "/status", nil, &st); err != nil {
		slog.Warn("web: admin status failed", "user", sess.who.User, "err", err)
		errMsg = "No se pudo leer el estado: " + err.Error()
	}
	a.html(w, http.StatusOK)
	_ = adminStatusPage(a.adminPage(sess, "Estado", "status"), st, errMsg, a.takeFlash(r, sess)).Render(r.Context(), w)
}

// ---- users ----

type usersView struct {
	Users []adminUser
	Roles []string
	Err   string
	Flash *flash
	CSRF  string
	Me    string
}

func (a *app) adminUsers(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.adminGate(w, r, sess)
	if !ok {
		return
	}
	v := usersView{Flash: a.takeFlash(r, sess), CSRF: a.csrf(sess), Me: sess.who.User}
	us, err := a.loadUsers(r.Context(), c, sess)
	if err != nil {
		v.Err = "No se pudieron leer los usuarios: " + err.Error()
	}
	rs, err2 := a.loadRoles(r.Context(), c, sess)
	if err2 != nil && v.Err == "" {
		v.Err = "No se pudieron leer los roles: " + err2.Error()
	}
	for _, x := range rs {
		v.Roles = append(v.Roles, x.Name)
	}
	v.Users = us
	a.html(w, http.StatusOK)
	_ = adminUsersPage(a.adminPage(sess, "Usuarios", "users"), v).Render(r.Context(), w)
}

// adminPost is the shell of every change: admin only (403 first), parse the form,
// run fn, and redirect with the outcome. keep lists the form fields shown again
// when fn fails (never a password).
func (a *app) adminPost(w http.ResponseWriter, r *http.Request, sess session, dest, form string, keep []string,
	fn func(ctx context.Context, c contract.AdminCaller, f url.Values) (msg string, secret *flashSecret, err error)) {
	c, ok := a.adminGate(w, r, sess)
	if !ok {
		return
	}
	f := r.PostForm
	if f == nil {
		_ = r.ParseForm()
		f = r.PostForm
	}
	msg, secret, err := fn(r.Context(), c, f)
	if err != nil {
		slog.Warn("web: admin action failed", "user", sess.who.User, "path", r.URL.Path, "err", err)
		fl := &flash{Kind: "error", Msg: err.Error(), Form: form, Draft: map[string]string{}}
		for _, k := range keep {
			fl.Draft[k] = f.Get(k)
		}
		a.redirectFlash(w, r, sess, dest, fl)
		return
	}
	a.redirectFlash(w, r, sess, dest, &flash{Kind: "ok", Msg: msg, Secret: secret})
}

func (a *app) adminUserCreate(w http.ResponseWriter, r *http.Request, sess session) {
	a.adminPost(w, r, sess, "/admin/users", "user-create", []string{"name", "role"},
		func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
			in := map[string]string{"name": strings.TrimSpace(f.Get("name")), "role": f.Get("role")}
			if pw := f.Get("password"); pw != "" {
				in["password"] = pw
			}
			var out struct {
				Temp string `json:"temporary_password"`
			}
			if err := a.adminJSON(ctx, c, sess, "POST", "/users", in, &out); err != nil {
				return "", nil, err
			}
			msg := "Usuario " + in["name"] + " creado con el rol " + in["role"] + "."
			if out.Temp != "" {
				return msg, &flashSecret{Label: "Contraseña temporal de " + in["name"], Value: out.Temp,
					Note: "Entrégasela por un canal seguro; se muestra una sola vez."}, nil
			}
			return msg, nil, nil
		})
}

func (a *app) adminUserDisabled(disable bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, sess session) {
		name := r.PathValue("name")
		a.adminPost(w, r, sess, "/admin/users", "", nil, func(ctx context.Context, c contract.AdminCaller, _ url.Values) (string, *flashSecret, error) {
			verb, msg := "enable", "Usuario "+name+" habilitado."
			if disable {
				verb, msg = "disable", "Usuario "+name+" deshabilitado: ya no puede ingresar."
			}
			return msg, nil, a.adminJSON(ctx, c, sess, "POST", "/users/"+esc(name)+"/"+verb, nil, nil)
		})
	}
}

func (a *app) adminUserReset(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	a.adminPost(w, r, sess, "/admin/users", "", nil, func(ctx context.Context, c contract.AdminCaller, _ url.Values) (string, *flashSecret, error) {
		var out struct {
			Temp string `json:"temporary_password"`
		}
		if err := a.adminJSON(ctx, c, sess, "POST", "/users/"+esc(name)+"/password-reset", nil, &out); err != nil {
			return "", nil, err
		}
		return "Contraseña de " + name + " restablecida. Sus tokens siguen vigentes: revócalos si hubo una filtración.",
			&flashSecret{Label: "Contraseña temporal de " + name, Value: out.Temp, Note: "Se muestra una sola vez."}, nil
	})
}

func (a *app) adminUserRole(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	a.adminPost(w, r, sess, "/admin/users", "", nil, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		role := f.Get("role")
		if err := a.adminJSON(ctx, c, sess, "PUT", "/users/"+esc(name)+"/role", map[string]string{"role": role}, nil); err != nil {
			return "", nil, err
		}
		return "Usuario " + name + " ahora tiene el rol " + role + ".", nil, nil
	})
}

// ---- roles, grants, filters, masks, quotas ----

type rolesView struct {
	Roles  []adminRole
	Quotas map[string]adminQuota
	Err    string
	Flash  *flash
	CSRF   string
}

func (a *app) adminRoles(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.adminGate(w, r, sess)
	if !ok {
		return
	}
	v := rolesView{Flash: a.takeFlash(r, sess), CSRF: a.csrf(sess)}
	var err error
	if v.Roles, err = a.loadRoles(r.Context(), c, sess); err != nil {
		v.Err = "No se pudieron leer los roles: " + err.Error()
	}
	v.Quotas, _ = a.loadQuotas(r.Context(), c, sess)
	a.html(w, http.StatusOK)
	_ = adminRolesPage(a.adminPage(sess, "Roles y permisos", "roles"), v).Render(r.Context(), w)
}

func (a *app) adminRoleCreate(w http.ResponseWriter, r *http.Request, sess session) {
	a.adminPost(w, r, sess, "/admin/roles", "role-create", []string{"name"}, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		name := strings.TrimSpace(f.Get("name"))
		if err := a.adminJSON(ctx, c, sess, "POST", "/roles", map[string]string{"name": name}, nil); err != nil {
			return "", nil, err
		}
		return "Rol " + name + " creado. Aún no tiene permisos: otórgale tablas.", nil, nil
	})
}

// grantView is one granted relation with its filter and masks.
type grantView struct {
	Relation string
	Filter   string
	Masks    []maskView
}

type maskView struct{ Column, Expr string }

type roleView struct {
	Role   adminRole
	Grants []grantView
	Quota  *adminQuota
	Err    string
	Flash  *flash
	CSRF   string
}

func buildGrants(r adminRole) []grantView {
	rels := append([]string(nil), r.Grants...)
	sort.Strings(rels)
	out := make([]grantView, len(rels))
	for i, rel := range rels {
		g := grantView{Relation: rel, Filter: r.RowFilters[rel]}
		for k, expr := range r.Masks { // key: schema.table.column
			if col, ok := strings.CutPrefix(k, rel+"."); ok {
				g.Masks = append(g.Masks, maskView{Column: col, Expr: expr})
			}
		}
		sort.Slice(g.Masks, func(x, y int) bool { return g.Masks[x].Column < g.Masks[y].Column })
		out[i] = g
	}
	return out
}

func (a *app) adminRole(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.adminGate(w, r, sess)
	if !ok {
		return
	}
	name := r.PathValue("role")
	v := roleView{Flash: a.takeFlash(r, sess), CSRF: a.csrf(sess)}
	roles, err := a.loadRoles(r.Context(), c, sess)
	if err != nil {
		v.Err = "No se pudieron leer los roles: " + err.Error()
	}
	found := false
	for _, x := range roles {
		if x.Name == name {
			v.Role, found = x, true
		}
	}
	if !found && v.Err == "" {
		a.notFound(w, r, sess, "admin", "No existe el rol "+name+".")
		return
	}
	v.Role.Name = name
	v.Grants = buildGrants(v.Role)
	if qs, err := a.loadQuotas(r.Context(), c, sess); err == nil {
		if q, ok := qs[name]; ok {
			v.Quota = &q
		}
	}
	p := a.adminPage(sess, name, "roles")
	p.Crumbs = []crumb{{Label: "Admin", Href: "/admin"}, {Label: "Roles y permisos", Href: "/admin/roles"}, {Label: name}}
	a.html(w, http.StatusOK)
	_ = adminRolePage(p, v).Render(r.Context(), w)
}

func roleDest(r *http.Request) string { return "/admin/roles/" + esc(r.PathValue("role")) }

func (a *app) adminGrant(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	a.adminPost(w, r, sess, roleDest(r), "grant", []string{"relation"}, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		rel := strings.TrimSpace(f.Get("relation"))
		if err := a.adminJSON(ctx, c, sess, "PUT", "/roles/"+esc(role)+"/grants/"+esc(rel), nil, nil); err != nil {
			return "", nil, err
		}
		return "Permiso de lectura sobre " + rel + " otorgado a " + role + ".", nil, nil
	})
}

func (a *app) adminRevoke(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	a.adminPost(w, r, sess, roleDest(r), "", nil, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		rel := f.Get("relation")
		if err := a.adminJSON(ctx, c, sess, "DELETE", "/roles/"+esc(role)+"/grants/"+esc(rel), nil, nil); err != nil {
			return "", nil, err
		}
		return "Permiso sobre " + rel + " retirado a " + role + " (también su filtro y sus máscaras).", nil, nil
	})
}

func (a *app) adminFilterSet(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	rel := r.PostFormValue("relation")
	a.adminPost(w, r, sess, roleDest(r), "filter:"+rel, []string{"predicate"}, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		pred := strings.TrimSpace(f.Get("predicate"))
		if err := a.adminJSON(ctx, c, sess, "PUT", "/roles/"+esc(role)+"/grants/"+esc(rel)+"/row-filter", map[string]string{"predicate": pred}, nil); err != nil {
			return "", nil, err
		}
		return "Filtro de filas de " + rel + " guardado para " + role + ".", nil, nil
	})
}

func (a *app) adminFilterDrop(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	a.adminPost(w, r, sess, roleDest(r), "", nil, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		rel := f.Get("relation")
		if err := a.adminJSON(ctx, c, sess, "DELETE", "/roles/"+esc(role)+"/grants/"+esc(rel)+"/row-filter", nil, nil); err != nil {
			return "", nil, err
		}
		return "Filtro de filas de " + rel + " quitado para " + role + ".", nil, nil
	})
}

func (a *app) adminMaskSet(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	rel := r.PostFormValue("relation")
	a.adminPost(w, r, sess, roleDest(r), "mask:"+rel, []string{"column", "expr"}, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		col, expr := strings.TrimSpace(f.Get("column")), strings.TrimSpace(f.Get("expr"))
		if col == "" {
			return "", nil, errors.New("indica la columna a enmascarar")
		}
		if err := a.adminJSON(ctx, c, sess, "PUT", "/roles/"+esc(role)+"/grants/"+esc(rel)+"/masks/"+esc(col), map[string]string{"expr": expr}, nil); err != nil {
			return "", nil, err
		}
		return "Máscara de " + rel + "." + col + " guardada para " + role + ".", nil, nil
	})
}

func (a *app) adminMaskDrop(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	a.adminPost(w, r, sess, roleDest(r), "", nil, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		rel, col := f.Get("relation"), f.Get("column")
		if err := a.adminJSON(ctx, c, sess, "DELETE", "/roles/"+esc(role)+"/grants/"+esc(rel)+"/masks/"+esc(col), nil, nil); err != nil {
			return "", nil, err
		}
		return "Máscara de " + rel + "." + col + " quitada para " + role + ".", nil, nil
	})
}

func (a *app) adminQuotaSet(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	a.adminPost(w, r, sess, roleDest(r), "quota", []string{"max_concurrent", "max_rows"}, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		in := map[string]any{}
		for _, k := range []string{"max_concurrent", "max_rows"} {
			s := strings.TrimSpace(f.Get(k))
			if s == "" {
				continue
			}
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil || n < 0 {
				return "", nil, fmt.Errorf("%s debe ser un número entero mayor o igual a 0", map[string]string{"max_concurrent": "Consultas simultáneas", "max_rows": "Filas máximas"}[k])
			}
			in[k] = n
		}
		if len(in) == 0 {
			return "", nil, errors.New("indica consultas simultáneas, filas máximas o ambas (para quitar la cuota usa «Quitar cuota»)")
		}
		if err := a.adminJSON(ctx, c, sess, "PUT", "/quotas/"+esc(role), in, nil); err != nil {
			return "", nil, err
		}
		return "Cuota del rol " + role + " guardada.", nil, nil
	})
}

func (a *app) adminQuotaDrop(w http.ResponseWriter, r *http.Request, sess session) {
	role := r.PathValue("role")
	a.adminPost(w, r, sess, roleDest(r), "", nil, func(ctx context.Context, c contract.AdminCaller, _ url.Values) (string, *flashSecret, error) {
		return "Cuota del rol " + role + " quitada.", nil, a.adminJSON(ctx, c, sess, "DELETE", "/quotas/"+esc(role), nil, nil)
	})
}

// ---- tokens ----

type tokensView struct {
	Tokens []adminToken
	Users  []adminUser
	Filter string
	Err    string
	Flash  *flash
	CSRF   string
	Now    time.Time
}

var tokenTTLs = []struct {
	Seconds int
	Label   string
}{
	{3600, "1 hora"}, {86400, "1 día"}, {7 * 86400, "7 días"}, {30 * 86400, "30 días"}, {90 * 86400, "90 días"}, {0, "Sin vencimiento"},
}

func (a *app) adminTokens(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.adminGate(w, r, sess)
	if !ok {
		return
	}
	v := tokensView{Flash: a.takeFlash(r, sess), CSRF: a.csrf(sess), Filter: r.URL.Query().Get("user"), Now: time.Now()}
	path := "/tokens"
	if v.Filter != "" {
		path += "?user=" + url.QueryEscape(v.Filter)
	}
	var out struct {
		Tokens []adminToken `json:"tokens"`
	}
	if err := a.adminJSON(r.Context(), c, sess, "GET", path, nil, &out); err != nil {
		v.Err = "No se pudieron leer los tokens: " + err.Error()
	}
	v.Tokens = out.Tokens
	v.Users, _ = a.loadUsers(r.Context(), c, sess)
	a.html(w, http.StatusOK)
	_ = adminTokensPage(a.adminPage(sess, "Tokens", "tokens"), v).Render(r.Context(), w)
}

func (a *app) adminTokenCreate(w http.ResponseWriter, r *http.Request, sess session) {
	a.adminPost(w, r, sess, "/admin/tokens", "token-create", []string{"user", "label", "ttl"}, func(ctx context.Context, c contract.AdminCaller, f url.Values) (string, *flashSecret, error) {
		ttl, err := strconv.Atoi(f.Get("ttl"))
		if err != nil || ttl < 0 {
			return "", nil, errors.New("elige una vigencia válida")
		}
		in := map[string]any{"user": f.Get("user"), "label": strings.TrimSpace(f.Get("label")), "ttl_seconds": ttl}
		var out struct {
			ID    string `json:"id"`
			Token string `json:"token"`
		}
		if err := a.adminJSON(ctx, c, sess, "POST", "/tokens", in, &out); err != nil {
			return "", nil, err
		}
		return "Token " + out.ID + " creado para " + f.Get("user") + ".",
			&flashSecret{Label: "Token de " + f.Get("user"), Value: out.Token, Note: "Se muestra una sola vez; Duct solo guarda su huella y no podrá mostrarlo de nuevo."}, nil
	})
}

func (a *app) adminTokenRevoke(w http.ResponseWriter, r *http.Request, sess session) {
	id := r.PathValue("id")
	a.adminPost(w, r, sess, "/admin/tokens", "", nil, func(ctx context.Context, c contract.AdminCaller, _ url.Values) (string, *flashSecret, error) {
		return "Token " + id + " revocado.", nil, a.adminJSON(ctx, c, sess, "DELETE", "/tokens/"+esc(id), nil, nil)
	})
}

// tokenState: the list never carries secrets, only these facts.
func tokenState(t adminToken, now time.Time) string {
	if t.Revoked {
		return "revoked"
	}
	if t.ExpiresAt != "" {
		if e, ok := parseAPITime(t.ExpiresAt); ok && now.After(e) {
			return "expired"
		}
	}
	return "active"
}

func parseAPITime(s string) (time.Time, bool) {
	for _, l := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if t, err := time.Parse(l, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ---- audit ----

type auditView struct {
	Entries                      []contract.AuditEntry
	Total                        int64
	Page, Pages                  int
	User, Protocol, Status       string
	From, To                     string
	Err                          string
	PrevURL, NextURL, ExportHint string
}

var auditProtocols = []string{"web", "http", "pg", "admin", "mcp", "cli", "sharing", "notebook"}

func (a *app) adminAudit(w http.ResponseWriter, r *http.Request, sess session) {
	if _, ok := a.adminGate(w, r, sess); !ok {
		return
	}
	qv := r.URL.Query()
	v := auditView{User: strings.TrimSpace(qv.Get("user")), Protocol: qv.Get("protocol"), Status: qv.Get("status"), From: qv.Get("from"), To: qv.Get("to")}
	page, _ := strconv.Atoi(qv.Get("page"))
	page = max(page, 1)
	q := contract.AuditQuery{User: v.User, Protocol: v.Protocol, Status: v.Status, Limit: auditPageSize, Offset: (page - 1) * auditPageSize}
	if t, err := time.Parse("2006-01-02", v.From); err == nil {
		q.From = t
	} else if v.From != "" {
		v.Err = "La fecha «desde» no es válida (usa AAAA-MM-DD)."
	}
	if t, err := time.Parse("2006-01-02", v.To); err == nil {
		q.To = t.AddDate(0, 0, 1) // the whole day, inclusive
	} else if v.To != "" {
		v.Err = "La fecha «hasta» no es válida (usa AAAA-MM-DD)."
	}
	status := http.StatusOK
	if rd, ok := a.q.(contract.AuditReader); !ok {
		v.Err = "Este servidor no expone el registro de auditoría."
	} else if v.Err == "" {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
		defer cancel()
		entries, total, err := rd.AuditPage(ctx, sess.who, q)
		if err != nil {
			slog.Warn("web: audit read failed", "user", sess.who.User, "err", err)
			v.Err, status = "No se pudo leer la auditoría: "+err.Error(), http.StatusBadGateway
		}
		v.Entries, v.Total = entries, total
	}
	v.Page = page
	v.Pages = int((v.Total + auditPageSize - 1) / auditPageSize)
	link := func(p int) string {
		vals := url.Values{}
		for k, val := range map[string]string{"user": v.User, "protocol": v.Protocol, "status": v.Status, "from": v.From, "to": v.To} {
			if val != "" {
				vals.Set(k, val)
			}
		}
		vals.Set("page", strconv.Itoa(p))
		return "/admin/audit?" + vals.Encode()
	}
	if page > 1 {
		v.PrevURL = link(page - 1)
	}
	if page < v.Pages {
		v.NextURL = link(page + 1)
	}
	a.html(w, status)
	_ = adminAuditPage(a.adminPage(sess, "Auditoría", "audit"), v).Render(r.Context(), w)
}

// ---- small helpers for the templates ----

func uptimeText(sec int64) string {
	return fmtDuration(time.Duration(sec) * time.Second)
}

func retentionText(sec int64) string {
	if sec <= 0 {
		return "sin límite (se conserva todo)"
	}
	d := time.Duration(sec) * time.Second
	if d%(24*time.Hour) == 0 {
		return strconv.Itoa(int(d/(24*time.Hour))) + " días"
	}
	return fmtDuration(d)
}

func intsText(v []int) string {
	if len(v) == 0 {
		return "ninguna"
	}
	parts := make([]string, len(v))
	for i, n := range v {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ", ")
}

func healthBadgeClass(h string) string {
	switch h {
	case "ok":
		return "badge-success"
	case "degraded", "firing":
		return "badge-danger"
	}
	return "badge-muted"
}

func healthLabel(h string) string {
	switch h {
	case "ok":
		return "Sano"
	case "degraded":
		return "Degradado"
	case "firing":
		return "En alerta"
	case "idle":
		return "Inactivo"
	case "disabled":
		return "No configurado"
	}
	return h
}

var componentNames = map[string]string{"jobs": "Trabajos", "stream": "Streaming", "alerts": "Alertas"}

// componentDetails flattens a component's extra fields into "clave: valor".
func componentDetails(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		if k != "health" && k != "configured" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		switch v := m[k].(type) {
		case map[string]any:
			var parts []string
			sub := make([]string, 0, len(v))
			for sk := range v {
				sub = append(sub, sk)
			}
			sort.Strings(sub)
			for _, sk := range sub {
				parts = append(parts, fmt.Sprintf("%s %v", detailLabel(sk), v[sk]))
			}
			out = append(out, detailLabel(k)+": "+strings.Join(parts, ", "))
		case []any:
			out = append(out, detailLabel(k)+": "+strconv.Itoa(len(v)))
		case nil:
		default:
			out = append(out, fmt.Sprintf("%s: %v", detailLabel(k), v))
		}
	}
	return out
}

func auditStatusClass(s string) string {
	switch s {
	case "ok":
		return "badge-success"
	case "error":
		return "badge-danger"
	case "denied":
		return "badge-warn"
	case "quota":
		return "badge-info"
	}
	return "badge-muted"
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

// detailLabels names the fields the status API reports for the components.
var detailLabels = map[string]string{
	"last_run_status": "última ejecución", "runs_24h": "ejecuciones 24 h", "streams": "streams", "firing": "alertas activas",
	"success": "correctas", "failed": "fallidas", "running": "en curso", "cancelled": "canceladas",
}

func detailLabel(k string) string {
	if l, ok := detailLabels[k]; ok {
		return l
	}
	return k
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func intPtrText(p *int) string {
	if p == nil {
		return ""
	}
	return strconv.Itoa(*p)
}

func int64PtrText(p *int64) string {
	if p == nil {
		return ""
	}
	return strconv.FormatInt(*p, 10)
}

func quotaText(q adminQuota, ok bool) string {
	if !ok {
		return "sin cuota"
	}
	var parts []string
	if q.MaxConcurrent != nil {
		parts = append(parts, strconv.Itoa(*q.MaxConcurrent)+" simultáneas")
	}
	if q.MaxRows != nil {
		parts = append(parts, strconv.FormatInt(*q.MaxRows, 10)+" filas")
	}
	return strings.Join(parts, " · ")
}

func draftVal(f *flash, form, key, fallback string) string {
	if f != nil && f.Form == form && f.Draft != nil {
		if v, ok := f.Draft[key]; ok {
			return v
		}
	}
	return fallback
}
