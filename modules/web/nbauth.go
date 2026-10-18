package web

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// The notebook origin (ADR 0006, "Origen aparte"). marimo runs code and renders
// HTML/JS that an editor wrote, so it is never served from the console's origin:
// a second listener (notebooks_listen, ideally a dedicated host name through
// notebooks_url) proxies /nb/{mode}/{name}/... and has no other route.
//
// Access to it is granted by the console, never by the console's cookie:
//
//  1. the page /notebooks/{name} opens the process and mints a TICKET (sealed
//     with the session key, purpose "ticket", valid 30 s, single use) bound to the
//     user, the notebook, the mode, the console session and the console's origin;
//  2. the iframe loads {notebook origin}/nb-auth?t=TICKET; the notebook origin
//     redeems it (once) and sets its own cookie duct_nb: HttpOnly, SameSite=Strict,
//     Path=/nb/{mode}/{name}/, sealed with purpose "cookie", bound to the same
//     user, notebook and mode, idle-limited and ending with the console session
//     (it is rejected as soon as the session is logged out or past its absolute
//     timeout);
//  3. every request and websocket upgrade is authorized by that cookie alone.
//
// The notebook origin never reads duct_session, and has no console endpoint.

const (
	nbCookie  = "duct_nb"
	nbTicket  = "ticket"
	nbGrant   = "cookie"
	ticketTTL = 30 * time.Second
)

// grant is the payload of a ticket and of a notebook cookie.
type grant struct {
	Purpose string `json:"p"`
	ID      string `json:"i"` // ticket id (single use)
	Sid     string `json:"s"` // console session id (revocation)
	User    string `json:"u"`
	Role    string `json:"r"`
	Name    string `json:"n"`
	Mode    string `json:"m"`
	Anc     string `json:"a"` // origin of the console, the only one that may frame the notebook
	Exp     int64  `json:"e"` // ticket: end of its validity
	SessExp int64  `json:"x"` // absolute timeout of the console session that minted it: the cookie never outlives it
	Seen    int64  `json:"t"` // cookie: last use (idle timeout)
}

func (g grant) identity() contract.Identity { return contract.Identity{User: g.User, Role: g.Role} }

// seal / open reuse the AEAD of the session keys, with the purpose as additional
// data: a session cookie is never a valid ticket, nor a ticket a valid cookie.
func (s *sessions) sealGrant(g grant) string {
	p, _ := json.Marshal(g)
	k := s.keys[0]
	hdr := append([]byte{cookieVersion, k.id}, g.Purpose...)
	nonce := make([]byte, k.aead.NonceSize())
	mustRand(nonce)
	return base64.RawURLEncoding.EncodeToString(append(append([]byte{cookieVersion, k.id}, nonce...), k.aead.Seal(nil, nonce, p, hdr)...))
}

func (s *sessions) openGrant(purpose, value string) (grant, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) < 2 || raw[0] != cookieVersion {
		return grant{}, false
	}
	hdr := append([]byte{raw[0], raw[1]}, purpose...)
	rest := raw[2:]
	for _, k := range s.keys {
		n := k.aead.NonceSize()
		if k.id != raw[1] || len(rest) < n {
			continue
		}
		pt, err := k.aead.Open(nil, rest[:n], rest[n:], hdr)
		if err != nil {
			continue
		}
		var g grant
		if json.Unmarshal(pt, &g) != nil || g.Purpose != purpose {
			return grant{}, false
		}
		return g, true
	}
	return grant{}, false
}

// revoked tells whether the console session id was logged out.
func (s *sessions) revoked(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.deny[id]
	return ok
}

// usedTickets remembers redeemed ticket ids until they would have expired anyway.
type usedTickets struct {
	mu   sync.Mutex
	used map[string]time.Time
}

// redeem marks id as used; false if it already was.
func (u *usedTickets) redeem(id string, exp, now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.used == nil {
		u.used = map[string]time.Time{}
	}
	for k, e := range u.used {
		if !now.Before(e) {
			delete(u.used, k)
		}
	}
	if _, dup := u.used[id]; dup {
		return false
	}
	u.used[id] = exp
	return true
}

// nbOrigin is the origin the browser must use for notebooks, as seen from r.
// "" means there is no notebook listener. With notebooks_url it is that URL;
// otherwise the console's host name with the notebook listener's port. That is
// a different origin but NOT a different cookie jar (cookies ignore ports): fine
// to try it out, and the notebook origin never accepts the console's cookie, but in
// production give notebooks a host name of their own (a sibling subdomain, so the
// SameSite cookies still flow to the iframe).
func (a *app) nbOrigin(r *http.Request) string {
	if a.cfg.nbURL != "" {
		return a.cfg.nbURL
	}
	return safeOrigin(a.nbOriginFromHost(r))
}

var originRE = regexp.MustCompile(`^https?://[A-Za-z0-9.:\[\]-]+$`)

// safeOrigin returns o if it is a plain scheme://host[:port] (it goes into CSP headers), else "".
func safeOrigin(o string) string {
	if originRE.MatchString(o) {
		return o
	}
	return ""
}

func (a *app) nbOriginFromHost(r *http.Request) string {
	if a.cfg.nbPort == "" {
		return ""
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(r.Host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	scheme := "http"
	if a.isSecure(r) {
		scheme = "https"
	}
	return scheme + "://" + net.JoinHostPort(host, a.cfg.nbPort)
}

// consoleOrigin is the origin of the console page that frames the notebook.
func (a *app) consoleOrigin(r *http.Request) string {
	scheme := "http"
	if a.isSecure(r) {
		scheme = "https"
	}
	return safeOrigin(scheme + "://" + r.Host)
}

// ticketURL mints the single-use ticket that opens (mode, name) for sess and
// returns the iframe address on the notebook origin.
func (a *app) ticketURL(r *http.Request, sess session, mode, name string) string {
	origin := a.nbOrigin(r)
	if origin == "" {
		return ""
	}
	now := a.store.now()
	t := a.store.sealGrant(grant{
		Purpose: nbTicket, ID: randomToken(), Sid: sess.id, User: sess.who.User, Role: sess.who.Role,
		Name: name, Mode: mode, Anc: a.consoleOrigin(r), Exp: now.Add(ticketTTL).Unix(), SessExp: sess.expires.Unix(),
	})
	return origin + "/nb-auth?t=" + url.QueryEscape(t)
}

// nbRoutes is the handler of the notebook origin: the ticket exchange and the
// proxy, nothing else.
func (a *app) nbRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /nb-auth", a.nbAuth)
	mux.HandleFunc("/nb/{mode}/{name}/", a.notebookProxy)
	return nbHeaders(mux)
}

// nbHeaders: the notebook origin serves marimo under its own policy (set by the
// proxy, which knows who may frame it); everything else is locked down.
func nbHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		if !isNBPath(r.URL.Path) {
			h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
			h.Set("Cache-Control", "no-store")
			h.Set("Referrer-Policy", "no-referrer")
		}
		next.ServeHTTP(w, r)
	})
}

// nbAuth redeems a ticket: once, within its TTL, for a console session that is
// still logged in. It sets the notebook cookie and redirects to the notebook.
func (a *app) nbAuth(w http.ResponseWriter, r *http.Request) {
	now := a.store.now()
	g, ok := a.store.openGrant(nbTicket, r.URL.Query().Get("t"))
	exp := time.Unix(g.Exp, 0)
	if !ok || !now.Before(exp) || !now.Before(time.Unix(g.SessExp, 0)) || exp.After(now.Add(ticketTTL+time.Second)) || a.store.revoked(g.Sid) ||
		(g.Mode != contract.NotebookEdit && g.Mode != contract.NotebookRun) || g.Name == "" || strings.ContainsAny(g.Name, "/\\?#") {
		http.Error(w, "invalid or expired ticket: open the notebook again from Duct", http.StatusForbidden)
		return
	}
	if !a.tickets.redeem(g.ID, exp, now) {
		http.Error(w, "ticket already used: open the notebook again from Duct", http.StatusForbidden)
		return
	}
	// The cookie dies with the console session: its absolute timeout is the session's.
	c := grant{Purpose: nbGrant, Sid: g.Sid, User: g.User, Role: g.Role, Name: g.Name, Mode: g.Mode, Anc: g.Anc, SessExp: g.SessExp, Seen: now.Unix()}
	a.setNBCookie(w, r, c)
	http.Redirect(w, r, nbBasePath(g.Mode, g.Name), http.StatusSeeOther)
}

func nbBasePath(mode, name string) string { return "/nb/" + mode + "/" + url.PathEscape(name) + "/" }

func (a *app) setNBCookie(w http.ResponseWriter, r *http.Request, c grant) {
	maxAge := max(1, int(time.Until(time.Unix(c.SessExp, 0)).Seconds()))
	http.SetCookie(w, &http.Cookie{
		Name: nbCookie, Value: a.store.sealGrant(c), Path: nbBasePath(c.Mode, c.Name), MaxAge: maxAge,
		HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: a.isSecure(r),
	})
}
