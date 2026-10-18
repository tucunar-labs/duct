package web

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// nbFixture is an app with a controllable clock and a logged-in session.
type nbFixture struct {
	a    *app
	sess session
	now  time.Time
}

func newNBFixture(t *testing.T) *nbFixture {
	t.Helper()
	f := &nbFixture{now: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)}
	f.a = newApp(nil, nil, appConfig{sessionTTL: 12 * time.Hour, idleTimeout: time.Hour, nbPort: "8485", rowLimit: 10})
	f.a.store.now = func() time.Time { return f.now }
	f.sess, _ = f.a.store.create(contract.Identity{User: "ana", Role: "analistas"})
	return f
}

func (f *nbFixture) consoleReq() *http.Request {
	r := httptest.NewRequest("GET", "http://127.0.0.1:8484/notebooks/x", http.NoBody)
	return r
}

// ticket mints a ticket the way the console page does and returns its path+query on the notebook origin.
func (f *nbFixture) ticket(t *testing.T, mode, name string) string {
	t.Helper()
	src := f.a.ticketURL(f.consoleReq(), f.sess, mode, name)
	u, err := url.Parse(src)
	if err != nil || u.Host != "127.0.0.1:8485" || u.Path != "/nb-auth" {
		t.Fatalf("ticket url %q (%v)", src, err)
	}
	return u.RequestURI()
}

func (f *nbFixture) redeem(path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.a.nbRoutes().ServeHTTP(w, httptest.NewRequest("GET", "http://127.0.0.1:8485"+path, http.NoBody))
	return w
}

func (f *nbFixture) proxy(method, path, cookie string, hdr map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:8485"+path, http.NoBody)
	if cookie != "" {
		r.Header.Set("Cookie", cookie)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	f.a.nbRoutes().ServeHTTP(w, r)
	return w
}

func nbCookieOf(t *testing.T, w *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range w.Result().Cookies() {
		if c.Name == nbCookie {
			return c
		}
	}
	t.Fatalf("no %s cookie in %v", nbCookie, w.Result().Header)
	return nil
}

func TestNotebookTicketIsSingleUseAndShortLived(t *testing.T) {
	f := newNBFixture(t)
	p := f.ticket(t, "edit", "hello")
	w := f.redeem(p)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/nb/edit/hello/" {
		t.Fatalf("first redeem: %d %s", w.Code, w.Header().Get("Location"))
	}
	c := nbCookieOf(t, w)
	if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode || c.Path != "/nb/edit/hello/" {
		t.Errorf("cookie attributes: %+v", c)
	}
	if time.Duration(c.MaxAge)*time.Second > 12*time.Hour {
		t.Errorf("cookie outlives the console session: max-age %d", c.MaxAge)
	}
	if w := f.redeem(p); w.Code != http.StatusForbidden || len(w.Result().Cookies()) != 0 {
		t.Errorf("a ticket worked twice: %d", w.Code)
	}
	// 31 s old: expired, even if never used
	p = f.ticket(t, "run", "hello")
	f.now = f.now.Add(ticketTTL + time.Second)
	if w := f.redeem(p); w.Code != http.StatusForbidden || len(w.Result().Cookies()) != 0 {
		t.Errorf("an expired ticket worked: %d", w.Code)
	}
	// a ticket of a session that logged out
	f.now = f.now.Add(time.Minute)
	p = f.ticket(t, "run", "hello")
	f.a.store.revoke(f.sess)
	if w := f.redeem(p); w.Code != http.StatusForbidden {
		t.Errorf("a ticket of a logged-out session worked: %d", w.Code)
	}
	// garbage, empty, and a console session cookie used as ticket
	_, val := f.a.store.create(contract.Identity{User: "x", Role: "y"})
	for _, tk := range []string{"", "garbage", val} {
		if w := f.redeem("/nb-auth?t=" + url.QueryEscape(tk)); w.Code != http.StatusForbidden {
			t.Errorf("ticket %.10q: %d", tk, w.Code)
		}
	}
}

func TestNotebookCookieBindingAndLifetime(t *testing.T) {
	f := newNBFixture(t)
	w := f.redeem(f.ticket(t, "run", "hello"))
	c := nbCookieOf(t, w)
	ck := c.Name + "=" + c.Value
	// Valid: it passes the authentication and reaches the (absent) notebook service.
	if w := f.proxy("GET", "/nb/run/hello/x", ck, nil); w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "notebooks not available") {
		t.Errorf("valid cookie: %d %s", w.Code, w.Body.String())
	}
	// Bound to its notebook and mode.
	for _, p := range []string{"/nb/run/other/x", "/nb/edit/hello/x"} {
		if w := f.proxy("GET", p, ck, nil); w.Code != http.StatusUnauthorized {
			t.Errorf("cookie reused on %s: %d", p, w.Code)
		}
	}
	// A websocket upgrade is refused before it happens.
	if w := f.proxy("GET", "/nb/run/other/ws", ck, map[string]string{"Upgrade": "websocket"}); w.Code != http.StatusForbidden {
		t.Errorf("ws with a foreign cookie: %d", w.Code)
	}
	// The console's own cookie is nothing here, nor are cookies of other purposes.
	_, sessVal := f.a.store.create(contract.Identity{User: "ana", Role: "duct_admin"})
	if w := f.proxy("GET", "/nb/run/hello/x", sessionCookie+"="+sessVal, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("the console cookie was accepted on the notebook origin: %d", w.Code)
	}
	if w := f.proxy("GET", "/nb/run/hello/x", nbCookie+"="+sessVal, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a session cookie value was accepted as a notebook cookie: %d", w.Code)
	}
	tk := strings.TrimPrefix(f.ticket(t, "run", "hello"), "/nb-auth?t=")
	tk, _ = url.QueryUnescape(tk)
	if w := f.proxy("GET", "/nb/run/hello/x", nbCookie+"="+tk, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a ticket was accepted as a notebook cookie: %d", w.Code)
	}
	// Cross-site requests are stopped even with the cookie.
	for _, h := range []map[string]string{
		{"Sec-Fetch-Site": "cross-site"},
		{"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors"},
		{"Origin": "http://evil.example"},
	} {
		if w := f.proxy("GET", "/nb/run/hello/x", ck, h); w.Code != http.StatusForbidden {
			t.Errorf("headers %v: %d, want 403", h, w.Code)
		}
	}
	// The console's iframe navigation (same-site, not same-origin) is the one cross-origin request that passes.
	if w := f.proxy("GET", "/nb/run/hello/", ck, map[string]string{"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "iframe"}); w.Code == http.StatusForbidden {
		t.Errorf("the iframe navigation was refused: %d", w.Code)
	}
	// There are no console endpoints on this origin.
	for _, p := range []string{"/catalog", "/sql/run", "/logout", "/login", "/notebooks", "/static/app.css"} {
		if w := f.proxy("GET", p, sessionCookie+"="+sessVal, nil); w.Code != http.StatusNotFound {
			t.Errorf("%s on the notebook origin: %d, want 404", p, w.Code)
		}
	}
	// Idle: it dies after idle_timeout (1 h here) without use.
	f.now = f.now.Add(61 * time.Minute)
	if w := f.proxy("GET", "/nb/run/hello/x", ck, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("idle cookie: %d", w.Code)
	}
	// Logout kills it at once.
	f3 := newNBFixture(t)
	ck3 := nbCookieOf(t, f3.redeem(f3.ticket(t, "run", "hello")))
	if w := f3.proxy("GET", "/nb/run/hello/x", ck3.Name+"="+ck3.Value, nil); w.Code != http.StatusNotFound {
		t.Fatalf("fresh cookie: %d", w.Code)
	}
	f3.a.store.revoke(f3.sess)
	if w := f3.proxy("GET", "/nb/run/hello/x", ck3.Name+"="+ck3.Value, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("the notebook cookie survived the console logout: %d", w.Code)
	}
}

func TestNotebookCookieEndsWithTheSession(t *testing.T) {
	f := newNBFixture(t)
	ck := nbCookieOf(t, f.redeem(f.ticket(t, "run", "hello")))
	val := ck.Name + "=" + ck.Value
	// the idle timeout slides with use (the browser stores the renewed cookie)...
	for range 20 {
		f.now = f.now.Add(30 * time.Minute)
		if f.now.After(f.sess.expires) {
			break
		}
		w := f.proxy("GET", "/nb/run/hello/x", val, nil)
		if w.Code != http.StatusNotFound {
			t.Fatalf("active cookie at %s: %d", f.now.Sub(f.sess.issued), w.Code)
		}
		for _, c := range w.Result().Cookies() {
			if c.Name == nbCookie {
				val = c.Name + "=" + c.Value
			}
		}
	}
	// ...but not past the console session's absolute timeout (12 h).
	f.now = f.sess.expires.Add(time.Second)
	if w := f.proxy("GET", "/nb/run/hello/x", val, nil); w.Code != http.StatusUnauthorized {
		t.Errorf("the notebook cookie outlived the console session: %d", w.Code)
	}
}

func TestConsoleRefusesCrossOriginReads(t *testing.T) {
	f := newNBFixture(t)
	_, val := f.a.store.create(contract.Identity{User: "ana", Role: "analistas"})
	do := func(method string, hdr map[string]string) int {
		r := httptest.NewRequest(method, "http://127.0.0.1:8484/static/app.css", http.NoBody)
		r.Header.Set("Cookie", sessionCookie+"="+val)
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		f.a.routes().ServeHTTP(w, r)
		return w.Code
	}
	// the notebook origin is same-site: the cookie travels, but its scripts get nothing
	for _, h := range []map[string]string{
		{"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors"},
		{"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "no-cors"},
		{"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "cors"},
	} {
		if code := do("GET", h); code != http.StatusForbidden {
			t.Errorf("GET %v: %d, want 403", h, code)
		}
	}
	if code := do("POST", map[string]string{"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors"}); code != http.StatusForbidden {
		t.Errorf("cross-origin POST: %d", code)
	}
	if code := do("GET", map[string]string{"Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors"}); code != http.StatusOK {
		t.Errorf("same-origin GET: %d", code)
	}
	if code := do("GET", map[string]string{"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "navigate"}); code != http.StatusOK {
		t.Errorf("same-site navigation: %d", code)
	}
}
