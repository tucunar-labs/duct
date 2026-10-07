package web

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/starfederation/datastar-go/datastar"
)

const (
	sessionCookie = "duct_session"
	csrfCookie    = "duct_csrf" // login form only (double submit)
	maxFormBytes  = 1 << 20
)

type appConfig struct {
	rowLimit      int
	queryTimeout  time.Duration
	sessionTTL    time.Duration // absolute session timeout
	idleTimeout   time.Duration // 0: default (2h, capped to sessionTTL)
	keys          [][]byte      // session keys, current first; none: ephemeral
	secure        *bool         // nil: decide per request
	trustProxy    bool          // honour X-Forwarded-For for the client IP
	userMax       int           // failed logins per username before lockout (0: 5)
	ipMax         int           // failed logins per client IP before lockout (0: 20)
	lockout       time.Duration // base lockout (0: 15m), doubles on repeats
	minLoginTime  time.Duration // floor for failed-login latency
	dashboardsDir string        // dashboards/*.yaml ("": none)
}

type app struct {
	q      contract.QueryHandler
	auth   contract.AuthProvider
	cfg    appConfig
	store  *sessions
	limit  *loginLimiter
	assets assets
	// chartCache holds dashboard query results per (lake snapshot, role, request).
	chartCache *lru
}

func newApp(q contract.QueryHandler, auth contract.AuthProvider, cfg appConfig) *app {
	if cfg.idleTimeout == 0 {
		cfg.idleTimeout = 2 * time.Hour
	}
	if cfg.userMax == 0 {
		cfg.userMax = 5
	}
	if cfg.ipMax == 0 {
		cfg.ipMax = 20
	}
	if cfg.lockout == 0 {
		cfg.lockout = 15 * time.Minute
	}
	store, err := newSessions(cfg.sessionTTL, cfg.idleTimeout, cfg.keys)
	if err != nil { // keys are validated in Init; this only guards direct callers
		panic("web: " + err.Error())
	}
	return &app{q: q, auth: auth, cfg: cfg, store: store, limit: newLoginLimiter(cfg.userMax, cfg.ipMax, cfg.lockout), assets: newAssets(), chartCache: newLRU(cacheEntries, cacheBytes)}
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /static/", staticHandler())
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/catalog", http.StatusSeeOther)
	})
	mux.HandleFunc("GET /login", a.loginForm)
	mux.HandleFunc("POST /login", a.login)
	mux.HandleFunc("POST /logout", a.authed(a.logout))
	mux.HandleFunc("GET /catalog", a.authed(a.catalog))
	mux.HandleFunc("GET /catalog/{schema}/{table}", a.authed(a.catalogTable))
	mux.HandleFunc("GET /sql", a.authed(a.sqlEditor))
	mux.HandleFunc("POST /sql/run", a.authed(a.sqlRun))
	mux.HandleFunc("GET /dashboards", a.authed(a.dashList))
	mux.HandleFunc("GET /dashboards/{name}", a.authed(a.dashView))
	mux.HandleFunc("POST /dashboards/{name}/data", a.authed(a.dashData))
	return securityHeaders(sameOrigin(mux))
}

// securityHeaders sets a strict CSP. Datastar evaluates its expressions with
// Function(), hence 'unsafe-eval'; CodeMirror injects <style> elements, hence
// 'unsafe-inline' for styles. Nothing is loaded from other origins.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "same-origin")
		if !strings.HasPrefix(r.URL.Path, "/static/") {
			h.Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

// sameOrigin rejects state-changing requests that a browser marks as
// cross-site (Sec-Fetch-Site) or whose Origin is another host. It is a second
// layer: every POST also needs its CSRF token.
func sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if s := r.Header.Get("Sec-Fetch-Site"); s != "" && s != "same-origin" && s != "none" {
				http.Error(w, "cross-site request rejected", http.StatusForbidden)
				return
			}
			if o := r.Header.Get("Origin"); o != "" {
				u, err := url.Parse(o)
				if err != nil || u.Host != r.Host {
					http.Error(w, "cross-origin request rejected", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

type authedHandler func(w http.ResponseWriter, r *http.Request, sess session)

func isDatastar(r *http.Request) bool { return r.Header.Get("Datastar-Request") == "true" }

// authed requires a valid session. Page loads are redirected to the login
// page; Datastar requests get an SSE redirect instead of a silent 401.
func (a *app) authed(next authedHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		var sess session
		ok := false
		if err == nil {
			sess, ok = a.store.get(c.Value)
		}
		if ok {
			if s2, fresh, renewed := a.store.touch(sess); renewed {
				sess = s2
				a.setCookie(w, r, sessionCookie, fresh, a.remaining(sess))
			}
		}
		if !ok {
			if isDatastar(r) {
				w.WriteHeader(http.StatusUnauthorized)
				_ = datastar.NewSSE(w, r).Redirect("/login")
				return
			}
			dest := "/login"
			if r.Method == http.MethodGet {
				dest += "?" + url.Values{"next": {r.URL.RequestURI()}}.Encode()
			}
			http.Redirect(w, r, dest, http.StatusSeeOther)
			return
		}
		if r.Method == http.MethodPost && !a.csrfOK(r, sess) {
			http.Error(w, "invalid CSRF token", http.StatusForbidden)
			return
		}
		next(w, r, sess)
	}
}

// remaining is the cookie lifetime left until the absolute timeout.
func (a *app) remaining(sess session) int {
	return max(1, int(sess.expires.Sub(a.store.now()).Seconds()))
}

func (a *app) csrfOK(r *http.Request, sess session) bool {
	got := r.Header.Get("X-CSRF-Token")
	if got == "" {
		r.Body = http.MaxBytesReader(nil, r.Body, maxFormBytes)
		got = r.PostFormValue("csrf")
	}
	return tokensEqual(got, a.store.csrf(sess))
}

func (a *app) page(sess session, title, active string) pageData {
	p := pageData{Title: title, Active: active, User: sess.who.User, Role: sess.who.Role, CSRF: a.store.csrf(sess), Assets: a.assets, IsAdmin: sess.who.Role == "duct_admin"}
	if title != "" {
		p.Crumbs = []crumb{{Label: title}}
	}
	return p
}

func (a *app) isSecure(r *http.Request) bool {
	if a.cfg.secure != nil {
		return *a.cfg.secure
	}
	return r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
}

func (a *app) setCookie(w http.ResponseWriter, r *http.Request, name, value string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name: name, Value: value, Path: "/", MaxAge: maxAge,
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: a.isSecure(r),
	})
}

// safeNext only accepts local absolute paths, never "//host" or "/\host".
func safeNext(n string) string {
	if strings.HasPrefix(n, "/") && !strings.HasPrefix(n, "//") && !strings.HasPrefix(n, "/\\") && !strings.ContainsAny(n, "\r\n") {
		return n
	}
	return "/catalog"
}

func (a *app) renderLogin(w http.ResponseWriter, r *http.Request, status int, next, errMsg string) {
	tok := randomToken()
	a.setCookie(w, r, csrfCookie, tok, 3600)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = loginPage(a.assets, tok, next, errMsg).Render(r.Context(), w)
}

func (a *app) loginForm(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		if _, ok := a.store.get(c.Value); ok {
			http.Redirect(w, r, safeNext(r.URL.Query().Get("next")), http.StatusSeeOther)
			return
		}
	}
	a.renderLogin(w, r, http.StatusOK, safeNext(r.URL.Query().Get("next")), "")
}

func (a *app) login(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	c, err := r.Cookie(csrfCookie)
	if err != nil || !tokensEqual(c.Value, r.PostForm.Get("csrf")) {
		http.Error(w, "invalid CSRF token", http.StatusForbidden)
		return
	}
	next := safeNext(r.PostForm.Get("next"))
	user, pass := r.PostForm.Get("user"), r.PostForm.Get("password")
	ip := clientIP(r, a.cfg.trustProxy)
	if wait := a.limit.blocked(user, ip); wait > 0 {
		slog.Warn("web: login blocked", "user", logUser(user), "ip", ip, "retry_after", wait.Round(time.Second))
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
		a.renderLogin(w, r, http.StatusTooManyRequests, next, "Demasiados intentos. Intenta de nuevo más tarde.")
		return
	}
	start := time.Now()
	who, err := a.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "password", User: user, Secret: pass})
	switch {
	case errors.Is(err, contract.ErrUnauthenticated):
		locked := a.limit.fail(user, ip)
		slog.Warn("web: login failed", "user", logUser(user), "ip", ip, "locked", locked)
		// Same message and (roughly) same latency whatever was wrong.
		if d := a.cfg.minLoginTime - time.Since(start); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
			}
		}
		a.renderLogin(w, r, http.StatusUnauthorized, next, "Usuario o contraseña incorrectos.")
		return
	case err != nil:
		slog.Error("web: auth provider failed", "err", err)
		a.renderLogin(w, r, http.StatusBadGateway, next, "El proveedor de autenticación no respondió. Intenta de nuevo.")
		return
	}
	a.limit.success(user)
	slog.Info("web: login ok", "user", logUser(who.User), "ip", ip)
	// A fresh session id on every login prevents session fixation.
	if old, err := r.Cookie(sessionCookie); err == nil {
		a.store.revokeValue(old.Value)
	}
	sess, val := a.store.create(who)
	a.setCookie(w, r, sessionCookie, val, a.remaining(sess))
	a.setCookie(w, r, csrfCookie, "", -1)
	http.Redirect(w, r, next, http.StatusSeeOther)
}

// logUser keeps attacker-controlled usernames short in log lines.
func logUser(u string) string {
	if len(u) > 64 {
		u = u[:64] + "..."
	}
	return u
}

func (a *app) logout(w http.ResponseWriter, r *http.Request, sess session) {
	a.store.revoke(sess)
	a.setCookie(w, r, sessionCookie, "", -1)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *app) catalog(w http.ResponseWriter, r *http.Request, sess session) {
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	schemas, err := loadCatalog(ctx, a.q, sess.who)
	msg := ""
	status := http.StatusOK
	if err != nil {
		slog.Warn("web: catalog query failed", "user", sess.who.User, "err", err)
		msg, status = "No se pudo leer el catálogo: "+err.Error(), http.StatusBadGateway
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	links, _ := a.dashboardLinks()
	p := a.page(sess, "Catálogo", "catalog")
	p.Pinned = true
	_ = catalogPage(p, schemas, msg, links).Render(r.Context(), w)
}

// catalogTable is the table detail: columns, a 100-row preview and metadata
// (row count through the QueryHandler, last lake snapshot through the optional
// SemanticQuerier) — never outside the governance path.
func (a *app) catalogTable(w http.ResponseWriter, r *http.Request, sess session) {
	sn, tn := r.PathValue("schema"), r.PathValue("table")
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	schemas, err := loadCatalog(ctx, a.q, sess.who)
	if err != nil {
		slog.Warn("web: catalog query failed", "user", sess.who.User, "err", err)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		p := a.page(sess, "Catálogo", "catalog")
		p.Pinned = true
		_ = catalogPage(p, nil, "No se pudo leer el catálogo: "+err.Error(), nil).Render(r.Context(), w)
		return
	}
	t := findTable(schemas, sn, tn)
	if t == nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		p := a.page(sess, "Tabla no encontrada", "catalog")
		p.Pinned = true
		p.Crumbs = []crumb{{Label: "Catálogo", Href: "/catalog"}, {Label: sn + "." + tn}}
		_ = catalogPage(p, schemas, "No existe la tabla "+sn+"."+tn+" (o tu rol no puede verla).", nil).Render(r.Context(), w)
		return
	}
	d := tableDetail{Schema: sn, Name: tn, Columns: t.Columns}
	preview := tablePreview(ctx, a.q, sess.who, sn, tn, 100)
	if n, err := tableRowCount(ctx, a.q, sess.who, sn, tn); err == nil {
		d.RowCount = n
	} else {
		d.CountErr = err.Error()
		slog.Warn("web: row count failed", "user", sess.who.User, "table", sn+"."+tn, "err", err)
	}
	if snap, err := snapshotID(ctx, a.q); err == nil {
		d.Snapshot = snap
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	p := a.page(sess, sn+"."+tn, "catalog")
	p.Pinned = true
	p.Crumbs = []crumb{{Label: "Catálogo", Href: "/catalog"}, {Label: sn}, {Label: tn}}
	_ = tablePage(p, schemas, d, preview).Render(r.Context(), w)
}

func (a *app) sqlEditor(w http.ResponseWriter, r *http.Request, sess session) {
	q := r.URL.Query().Get("q")
	if len(q) > 100000 {
		q = q[:100000]
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	schemas, _ := loadCatalog(ctx, a.q, sess.who) // the tree is a convenience: empty on error
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	p := a.page(sess, "Editor SQL", "sql")
	p.Pinned = true
	_ = sqlPage(p, q, schemas).Render(r.Context(), w)
}

func (a *app) sqlRun(w http.ResponseWriter, r *http.Request, sess session) {
	var sig struct {
		SQL string `json:"sql"`
		Sel string `json:"sel"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)
	if err := datastar.ReadSignals(r, &sig); err != nil {
		http.Error(w, "bad signals", http.StatusBadRequest)
		return
	}
	// A non-empty selection replaces the document: run the selection.
	sql := sig.SQL
	if s := strings.TrimSpace(sig.Sel); s != "" {
		sql = sig.Sel
	}
	sse := datastar.NewSSE(w, r)
	var res *queryResult
	if strings.TrimSpace(sql) == "" {
		res = &queryResult{Err: "Escribe una consulta.", Nonce: time.Now().UnixNano()}
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
		defer cancel()
		var err error
		res, err = runQuery(ctx, a.q, sess.who, sql, a.cfg.rowLimit)
		if err != nil {
			res = &queryResult{Err: err.Error(), Nonce: time.Now().UnixNano()}
		}
	}
	// The results body is a <duct-grid> island fed with compact JSON: replace
	// the container instead of morphing the old subtree (which used to cost
	// ~1 MB of HTML and 15k DOM nodes for 1,000 rows).
	_ = sse.PatchElementTempl(resultsView(res), datastar.WithMode(datastar.ElementPatchModeReplace))
}
