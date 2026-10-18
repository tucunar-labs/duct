package web

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/starfederation/datastar-go/datastar"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Notebooks (ADR 0006, 0012): the list, the page that embeds a user's marimo
// process (an iframe on the separate notebook origin, entered with a ticket), version history with diffs and
// restore, comments, and create / rename / delete. Everything comes through
// the optional contract.NotebooksConsole of the QueryHandler, which does the
// authorization (editor roles, lock) and the auditing.
//
// The embedded marimo is reached through /nb/{mode}/{name}/... on the notebook
// origin (nbauth.go), a reverse proxy for http AND websocket that is only
// answered for a valid notebook cookie whose user has a running process for that
// notebook; the browser never learns the port or marimo's password.

// the first start may create the virtualenv
const openTimeout = 3 * time.Minute

func (a *app) notebooksRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /notebooks", a.authed(a.notebooksList))
	mux.HandleFunc("POST /notebooks", a.authed(a.notebookCreate))
	mux.HandleFunc("GET /notebooks/{name}", a.authed(a.notebookView))
	mux.HandleFunc("GET /notebooks/{name}/history", a.authed(a.notebookHistory))
	mux.HandleFunc("GET /notebooks/{name}/diff", a.authed(a.notebookDiff))
	mux.HandleFunc("POST /notebooks/{name}/rename", a.authed(a.notebookRename))
	mux.HandleFunc("POST /notebooks/{name}/delete", a.authed(a.notebookDelete))
	mux.HandleFunc("POST /notebooks/{name}/restore", a.authed(a.notebookRestore))
	mux.HandleFunc("POST /notebooks/{name}/comments", a.authed(a.commentAdd))
	mux.HandleFunc("POST /notebooks/{name}/comments/{id}/{action}", a.authed(a.commentResolve))
}

func (a *app) notebooksConsole() (contract.NotebooksConsole, bool) {
	c, ok := a.q.(contract.NotebooksConsole)
	return c, ok
}

func (a *app) nbPage(sess session, title string) pageData {
	p := a.page(sess, title, "notebooks")
	p.Crumbs = []crumb{{Label: "Notebooks"}}
	return p
}

// nbMessage turns a service error into text for the user.
func nbMessage(err error) string {
	switch {
	case errors.Is(err, contract.ErrConsoleUnavailable):
		return "Este servidor no expone los notebooks."
	case errors.Is(err, contract.ErrNotebookNotFound):
		return "No existe ese notebook (o esa versión)."
	}
	return err.Error()
}

func nbStatus(err error) int {
	switch {
	case errors.Is(err, contract.ErrNotebookForbidden):
		return http.StatusForbidden
	case errors.Is(err, contract.ErrNotebookNotFound):
		return http.StatusNotFound
	case errors.Is(err, contract.ErrNotebookLocked):
		return http.StatusConflict
	case errors.Is(err, contract.ErrNotebookInvalid):
		return http.StatusBadRequest
	case errors.Is(err, contract.ErrConsoleUnavailable):
		return http.StatusNotFound
	}
	return http.StatusBadGateway
}

type notebooksView struct {
	Notebooks []contract.NotebookInfo
	Err       string
	Flash     *flash
	Now       time.Time
	CanEdit   bool
	Admin     bool
	CSRF      string
	Draft     string
}

func (a *app) notebooksList(w http.ResponseWriter, r *http.Request, sess session) {
	v := notebooksView{Flash: a.takeFlash(r, sess), Now: time.Now(), Admin: isAdminSession(sess), CSRF: a.csrf(sess)}
	status := http.StatusOK
	c, ok := a.notebooksConsole()
	if !ok {
		v.Err = "Este servidor no expone los notebooks."
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
		defer cancel()
		nbs, err := c.Notebooks(ctx, sess.who)
		if err != nil {
			slog.Warn("web: notebooks list failed", "user", sess.who.User, "err", err)
			v.Err, status = nbMessage(err), nbStatus(err)
		}
		v.Notebooks = nbs
		v.CanEdit = c.CanEditNotebooks(sess.who)
	}
	a.html(w, status)
	_ = notebooksPage(a.nbPage(sess, "Notebooks"), v).Render(r.Context(), w)
}

// nbBack redirects to path with an outcome message.
func (a *app) nbDone(w http.ResponseWriter, r *http.Request, sess session, dest string, err error, okMsg string) {
	if err != nil {
		slog.Warn("web: notebook action failed", "user", sess.who.User, "path", r.URL.Path, "err", err)
		a.redirectFlash(w, r, sess, dest, &flash{Kind: "error", Msg: nbMessage(err)})
		return
	}
	a.redirectFlash(w, r, sess, dest, &flash{Kind: "ok", Msg: okMsg})
}

func (a *app) notebookCreate(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.notebooksConsole()
	name := strings.TrimSpace(r.PostFormValue("name"))
	if !ok {
		a.nbDone(w, r, sess, "/notebooks", contract.ErrConsoleUnavailable, "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	if err := c.CreateNotebook(ctx, sess.who, name); err != nil {
		a.nbDone(w, r, sess, "/notebooks", err, "")
		return
	}
	a.redirectFlash(w, r, sess, "/notebooks/"+url.PathEscape(name), &flash{Kind: "ok", Msg: "Notebook " + name + " creado."})
}

type notebookView struct {
	Info     contract.NotebookInfo
	Mode     string
	LockedBy string
	Err      string
	Flash    *flash
	CSRF     string
	Comments []contract.NotebookComment
	Me       string
	CanEdit  bool
	Admin    bool
	Src      string
	Now      time.Time
}

func (a *app) notebookView(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.notebooksConsole()
	name := r.PathValue("name")
	if !ok {
		a.notFound(w, r, sess, "notebooks", "Este servidor no expone los notebooks.")
		return
	}
	qctx, qcancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer qcancel()
	info, err := c.Notebook(qctx, sess.who, name)
	if err != nil {
		if errors.Is(err, contract.ErrNotebookNotFound) {
			a.notFound(w, r, sess, "notebooks", "No existe el notebook "+name+".")
			return
		}
		a.html(w, nbStatus(err))
		_ = notFoundPage(a.nbPage(sess, "Notebooks"), nbMessage(err)).Render(r.Context(), w)
		return
	}
	edit := info.CanEdit
	switch r.URL.Query().Get("mode") {
	case "run":
		edit = false
	case "edit":
		edit = true // a role that may not edit is refused (and audited) by the service below
	}
	v := notebookView{Info: info, Flash: a.takeFlash(r, sess), CSRF: a.csrf(sess), Me: sess.who.User, CanEdit: info.CanEdit, Admin: isAdminSession(sess), Now: time.Now()}
	status := http.StatusOK
	octx, ocancel := context.WithTimeout(r.Context(), openTimeout)
	defer ocancel()
	ses, err := c.OpenNotebook(octx, sess.who, name, edit)
	if errors.Is(err, contract.ErrNotebookForbidden) {
		a.forbidden(w, r, sess, "Tu rol no puede editar notebooks (ver [notebooks] editors en duct.toml). Puedes ejecutarlo.")
		return
	}
	if err != nil {
		slog.Warn("web: opening a notebook failed", "user", sess.who.User, "notebook", name, "err", err)
		v.Err, status = "No se pudo abrir el notebook: "+nbMessage(err), nbStatus(err)
	} else {
		v.Mode, v.LockedBy = ses.Mode, ses.LockedBy
		if v.Src = a.ticketURL(r, sess, ses.Mode, name); v.Src == "" {
			v.Err, status = "Los notebooks no tienen origen propio en este servidor: configura [serve.web] notebooks_listen (y notebooks_url tras un proxy).", http.StatusServiceUnavailable
		} else {
			w.Header().Set("Content-Security-Policy", consoleCSP("frame-src "+a.nbOrigin(r)+"; "))
		}
	}
	if cm, err := c.NotebookComments(qctx, sess.who, name); err == nil {
		v.Comments = cm
	}
	a.html(w, status)
	p := a.nbPage(sess, name)
	p.Crumbs = []crumb{{Label: "Notebooks", Href: "/notebooks"}, {Label: name}}
	p.Pinned = true
	_ = notebookPage(p, v).Render(r.Context(), w)
}

// ---- history ----

type historyView struct {
	Name     string
	Versions []contract.NotebookVersion
	Err      string
	Flash    *flash
	CSRF     string
	Now      time.Time
	Locked   string
	Diff     []contract.DiffLine
	DiffFrom int64
	DiffTo   int64
	ShowDiff bool
	DiffErr  string
}

func (a *app) notebookHistory(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.notebooksConsole()
	name := r.PathValue("name")
	if !ok {
		a.notFound(w, r, sess, "notebooks", "Este servidor no expone los notebooks.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	v := historyView{Name: name, Flash: a.takeFlash(r, sess), CSRF: a.csrf(sess), Now: time.Now()}
	vs, err := c.NotebookVersions(ctx, sess.who, name)
	if errors.Is(err, contract.ErrNotebookForbidden) {
		a.forbidden(w, r, sess, "El historial muestra el código: solo los roles editores lo ven.")
		return
	}
	status := http.StatusOK
	if err != nil {
		v.Err, status = nbMessage(err), nbStatus(err)
	}
	v.Versions = vs
	if info, err := c.Notebook(ctx, sess.who, name); err == nil && info.LockedBy != sess.who.User {
		v.Locked = info.LockedBy
	}
	if q := r.URL.Query(); q.Get("to") != "" || q.Get("from") != "" {
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		v.ShowDiff, v.DiffFrom, v.DiffTo = true, from, to
		if v.Diff, err = c.NotebookDiff(ctx, sess.who, name, from, to); err != nil {
			v.DiffErr = nbMessage(err)
		}
	}
	a.html(w, status)
	p := a.nbPage(sess, "Historial de "+name)
	p.Crumbs = []crumb{{Label: "Notebooks", Href: "/notebooks"}, {Label: name, Href: "/notebooks/" + url.PathEscape(name)}, {Label: "Historial"}}
	_ = historyPage(p, v).Render(r.Context(), w)
}

// notebookDiff is the old address of one diff: it redirects into the history page.
func (a *app) notebookDiff(w http.ResponseWriter, r *http.Request, _ session) {
	http.Redirect(w, r, "/notebooks/"+url.PathEscape(r.PathValue("name"))+"/history?"+r.URL.RawQuery, http.StatusSeeOther)
}

func (a *app) notebookRestore(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	dest := "/notebooks/" + url.PathEscape(name) + "/history"
	c, ok := a.notebooksConsole()
	if !ok {
		a.nbDone(w, r, sess, dest, contract.ErrConsoleUnavailable, "")
		return
	}
	ver, _ := strconv.ParseInt(r.PostFormValue("version"), 10, 64)
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	a.nbDone(w, r, sess, dest, c.RestoreNotebook(ctx, sess.who, name, ver), "Versión restaurada: el notebook tiene su contenido y la restauración quedó como una versión nueva.")
}

// ---- rename / delete ----

func (a *app) notebookRename(w http.ResponseWriter, r *http.Request, sess session) {
	name, to := r.PathValue("name"), strings.TrimSpace(r.PostFormValue("to"))
	back := "/notebooks/" + url.PathEscape(name)
	c, ok := a.notebooksConsole()
	if !ok {
		a.nbDone(w, r, sess, back, contract.ErrConsoleUnavailable, "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	if err := c.RenameNotebook(ctx, sess.who, name, to); err != nil {
		a.nbDone(w, r, sess, back, err, "")
		return
	}
	a.redirectFlash(w, r, sess, "/notebooks/"+url.PathEscape(to), &flash{Kind: "ok", Msg: "Notebook renombrado a " + to + "."})
}

func (a *app) notebookDelete(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	c, ok := a.notebooksConsole()
	if !ok {
		a.nbDone(w, r, sess, "/notebooks", contract.ErrConsoleUnavailable, "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	if err := c.DeleteNotebook(ctx, sess.who, name); err != nil {
		a.nbDone(w, r, sess, "/notebooks/"+url.PathEscape(name), err, "")
		return
	}
	a.redirectFlash(w, r, sess, "/notebooks", &flash{Kind: "ok", Msg: "Notebook " + name + " eliminado. Su historial se conserva."})
}

// ---- comments (Datastar: the panel is patched, the notebook is not reloaded) ----

type commentSignals struct {
	Ref  string `json:"cref"`
	Body string `json:"cbody"`
}

func (a *app) commentsPanel(r *http.Request, sess session, c contract.NotebooksConsole, name, errMsg string) notebookView {
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	v := notebookView{Info: contract.NotebookInfo{Name: name}, CSRF: a.csrf(sess), Me: sess.who.User, Err: errMsg, Now: time.Now()}
	if cm, err := c.NotebookComments(ctx, sess.who, name); err == nil {
		v.Comments = cm
	}
	if info, err := c.Notebook(ctx, sess.who, name); err == nil {
		v.CanEdit = info.CanEdit
	}
	return v
}

func (a *app) commentAdd(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.notebooksConsole()
	if !ok {
		http.Error(w, "notebooks not available", http.StatusNotFound)
		return
	}
	name := r.PathValue("name")
	var sig commentSignals
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)
	if err := datastar.ReadSignals(r, &sig); err != nil {
		http.Error(w, "bad signals", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	err := c.AddNotebookComment(ctx, sess.who, name, sig.Ref, sig.Body)
	sse := datastar.NewSSE(w, r)
	msg := ""
	if err != nil {
		msg = nbMessage(err)
	} else {
		_ = sse.MarshalAndPatchSignals(map[string]string{"cbody": "", "cref": ""})
	}
	_ = sse.PatchElementTempl(commentsPanel(a.commentsPanel(r, sess, c, name, msg)), datastar.WithMode(datastar.ElementPatchModeReplace))
}

func (a *app) commentResolve(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.notebooksConsole()
	if !ok {
		http.Error(w, "notebooks not available", http.StatusNotFound)
		return
	}
	name := r.PathValue("name")
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	action := r.PathValue("action")
	if err != nil || (action != "resolve" && action != "reopen") {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	err = c.ResolveNotebookComment(ctx, sess.who, name, id, action == "resolve")
	sse := datastar.NewSSE(w, r)
	msg := ""
	if err != nil {
		msg = nbMessage(err)
	}
	_ = sse.PatchElementTempl(commentsPanel(a.commentsPanel(r, sess, c, name, msg)), datastar.WithMode(datastar.ElementPatchModeReplace))
}

// ---- the proxy (notebook origin) ----

// nbCSP is the policy of the embedded marimo: it ships inline scripts, workers
// from blob: and evaluates code, so it cannot live under the CSP of the Duct
// pages; it may only be framed by the console that opened it (the ticket carries its origin).
const nbCSP = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' blob:; worker-src 'self' blob:; style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data: blob:; font-src 'self' data:; media-src 'self' data: blob:; connect-src 'self' ws: wss: blob: data:; frame-src 'self' blob: data:; " +
	"object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors "

func isNBPath(p string) bool { return strings.HasPrefix(p, "/nb/") }

// notebookProxy forwards /nb/{mode}/{name}/... (http and websocket) on the
// notebook origin to the marimo process the console started for the user of the
// notebook cookie. That cookie (duct_nb, see nbauth.go) is the only credential
// accepted here: it is bound to one user, notebook and mode, expires with the
// console session and is checked on every request and upgrade. A cross-site
// request is stopped by SameSite=Strict, by the Sec-Fetch-Site and Origin
// checks below (websocket upgrades included) and by marimo's custom headers.
// The browser's credentials never reach marimo.
func (a *app) notebookProxy(w http.ResponseWriter, r *http.Request) {
	mode, name := r.PathValue("mode"), r.PathValue("name")
	var g grant
	ok := false
	if c, err := r.Cookie(nbCookie); err == nil {
		g, ok = a.store.openGrant(nbGrant, c.Value)
	}
	now := a.store.now()
	if ok {
		ok = g.Mode == mode && g.Name == name && now.Before(time.Unix(g.SessExp, 0)) &&
			now.Sub(time.Unix(g.Seen, 0)) <= a.store.idle && !a.store.revoked(g.Sid)
	}
	if !ok {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") { // refused before the upgrade, like the notebook protocol
			http.Error(w, "authentication required", http.StatusForbidden)
			return
		}
		http.Error(w, "authentication required: open the notebook from Duct", http.StatusUnauthorized)
		return
	}
	if now.Sub(time.Unix(g.Seen, 0)) >= min(a.store.idle/4, time.Minute) {
		g.Seen = now.Unix()
		a.setNBCookie(w, r, g)
	}
	// Only the notebook's own pages and scripts may call it. The one cross-origin request
	// allowed is the console's iframe navigating to it.
	switch s := r.Header.Get("Sec-Fetch-Site"); s {
	case "", "same-origin", "none":
	case "same-site":
		if r.Method != http.MethodGet || r.Header.Get("Sec-Fetch-Mode") != "navigate" || r.Header.Get("Sec-Fetch-Dest") != "iframe" {
			http.Error(w, "cross-site request rejected", http.StatusForbidden)
			return
		}
	default:
		http.Error(w, "cross-site request rejected", http.StatusForbidden)
		return
	}
	if o := r.Header.Get("Origin"); o != "" {
		if u, err := url.Parse(o); err != nil || u.Host != r.Host {
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
	}
	c, avail := a.notebooksConsole()
	if !avail {
		http.Error(w, "notebooks not available", http.StatusNotFound)
		return
	}
	ses, err := c.NotebookTarget(r.Context(), g.identity(), name, mode)
	if err != nil {
		code := nbStatus(err)
		msg := nbMessage(err)
		if errors.Is(err, contract.ErrNotebookNotFound) {
			msg = "no hay un proceso de este notebook para tu usuario: ábrelo desde Duct"
		}
		http.Error(w, msg, code)
		return
	}
	anc := safeOrigin(g.Anc)
	if anc == "" {
		anc = "'none'"
	}
	w.Header().Set("Content-Security-Policy", nbCSP+anc)
	target := &url.URL{Scheme: "http", Host: ses.Target}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			h := pr.Out.Header
			h.Del("Cookie")
			h.Del("Authorization")
			h.Del("Referer")
			h.Set("Authorization", ses.Auth)
			if h.Get("Origin") != "" {
				h.Set("Origin", target.String())
			}
			pr.Out.Host = target.Host
		},
		ModifyResponse: func(resp *http.Response) error {
			for _, k := range []string{"Set-Cookie", "Content-Security-Policy", "X-Frame-Options", "Access-Control-Allow-Origin", "Access-Control-Allow-Credentials"} {
				resp.Header.Del(k)
			}
			return nil
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			slog.Warn("web: notebook proxy failed", "user", g.User, "notebook", name, "err", err)
			http.Error(w, "el proceso del notebook no responde: reabre el notebook desde Duct", http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r)
}
