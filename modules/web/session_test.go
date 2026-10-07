package web

import (
	"encoding/base64"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func testKey(b byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = b + byte(i)
	}
	return k
}

// newClockEnv builds an app with a controllable clock, keys and limits.
func newClockEnv(t *testing.T, cfg appConfig, clk *fakeClock) *env {
	t.Helper()
	cfg.rowLimit, cfg.queryTimeout = 1000, 5*time.Second
	if cfg.sessionTTL == 0 {
		cfg.sessionTTL = time.Hour
	}
	q := &fakeQ{}
	a := newApp(q, &fakeAuth{}, cfg)
	a.store.now, a.limit.now = clk.now, clk.now
	srv := httptest.NewServer(a.routes())
	t.Cleanup(srv.Close)
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &env{t: t, srv: srv, q: q, c: c}
}

func newClock() *fakeClock { return &fakeClock{t: time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)} }

// withCookie makes a second env (new server, same keys) hold the given session cookie.
func (e *env) adopt(name, value string) {
	u, _ := url.Parse(e.srv.URL)
	e.c.Jar.SetCookies(u, []*http.Cookie{{Name: name, Value: value, Path: "/"}})
}

func (e *env) status(path string) int {
	e.t.Helper()
	resp, _ := e.get(path)
	return resp.StatusCode
}

func (e *env) tryLogin(user, pw, xff string) *http.Response {
	e.t.Helper()
	e.get("/login")
	req, _ := http.NewRequest("POST", e.srv.URL+"/login", strings.NewReader(url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {user}, "password": {pw}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	resp, _ := e.do(req)
	return resp
}

func TestSessionSurvivesRestart(t *testing.T) {
	clk := newClock()
	keys := [][]byte{testKey(1)}
	a := newClockEnv(t, appConfig{keys: keys}, clk)
	a.login()
	val := a.cookie(sessionCookie)
	if a.status("/catalog") != http.StatusOK {
		t.Fatal("not logged in")
	}
	tok := a.csrfToken()

	b := newClockEnv(t, appConfig{keys: keys}, clk) // "restart": new instance, same key
	b.adopt(sessionCookie, val)
	if got := b.status("/catalog"); got != http.StatusOK {
		t.Fatalf("old cookie after restart: %d", got)
	}
	if b.csrfToken() != tok {
		t.Error("csrf token changed across restart")
	}

	c := newClockEnv(t, appConfig{keys: [][]byte{testKey(9)}}, clk) // different key
	c.adopt(sessionCookie, val)
	if got := c.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("cookie accepted under another key: %d", got)
	}
	d := newClockEnv(t, appConfig{}, clk) // ephemeral key
	d.adopt(sessionCookie, val)
	if got := d.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("cookie accepted by ephemeral instance: %d", got)
	}
}

func TestTamperedCookieRejected(t *testing.T) {
	clk := newClock()
	e := newClockEnv(t, appConfig{keys: [][]byte{testKey(1)}}, clk)
	e.login()
	val := e.cookie(sessionCookie)
	raw, _ := base64.RawURLEncoding.DecodeString(val)
	for _, i := range []int{0, 1, 5, len(raw) / 2, len(raw) - 1} {
		bad := append([]byte(nil), raw...)
		bad[i] ^= 0x01
		f := newClockEnv(t, appConfig{keys: [][]byte{testKey(1)}}, clk)
		f.adopt(sessionCookie, base64.RawURLEncoding.EncodeToString(bad))
		if got := f.status("/catalog"); got != http.StatusSeeOther {
			t.Errorf("flipped byte %d accepted: %d", i, got)
		}
	}
	for _, junk := range []string{"x", "AAAA", strings.Repeat("A", 200)} {
		f := newClockEnv(t, appConfig{keys: [][]byte{testKey(1)}}, clk)
		f.adopt(sessionCookie, junk)
		if got := f.status("/catalog"); got != http.StatusSeeOther {
			t.Errorf("junk %q accepted: %d", junk, got)
		}
	}
}

func TestSessionAbsoluteAndIdleTimeouts(t *testing.T) {
	clk := newClock()
	cfg := appConfig{keys: [][]byte{testKey(1)}, sessionTTL: time.Hour, idleTimeout: 10 * time.Minute}

	e := newClockEnv(t, cfg, clk)
	e.login()
	// Activity every 5 minutes keeps it alive past the idle timeout...
	for i := 0; i < 8; i++ {
		clk.add(5 * time.Minute)
		if got := e.status("/catalog"); got != http.StatusOK {
			t.Fatalf("active session dropped at step %d: %d", i, got)
		}
	}
	// ...but never past the absolute timeout (40 min so far; 25 more > 1h).
	clk.add(5 * time.Minute)
	e.status("/catalog")
	clk.add(15 * time.Minute)
	if got := e.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("session alive after absolute timeout: %d", got)
	}

	f := newClockEnv(t, cfg, clk)
	f.login()
	clk.add(11 * time.Minute)
	if got := f.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("idle session alive: %d", got)
	}
}

func TestExpiredCookieRejectedAfterRestart(t *testing.T) {
	clk := newClock()
	keys := [][]byte{testKey(1)}
	a := newClockEnv(t, appConfig{keys: keys, sessionTTL: time.Hour}, clk)
	a.login()
	val := a.cookie(sessionCookie)
	clk.add(2 * time.Hour)
	b := newClockEnv(t, appConfig{keys: keys, sessionTTL: time.Hour}, clk)
	b.adopt(sessionCookie, val)
	if got := b.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("expired cookie accepted: %d", got)
	}
}

func TestKeyRotation(t *testing.T) {
	clk := newClock()
	oldK, newK := testKey(1), testKey(50)
	a := newClockEnv(t, appConfig{keys: [][]byte{oldK}}, clk)
	a.login()
	val := a.cookie(sessionCookie)

	// New key current, old one kept for decrypt: the session still works.
	b := newClockEnv(t, appConfig{keys: [][]byte{newK, oldK}}, clk)
	b.adopt(sessionCookie, val)
	if got := b.status("/catalog"); got != http.StatusOK {
		t.Fatalf("old-key cookie with previous key configured: %d", got)
	}
	// Activity re-seals with the new key: it then works with the new key alone.
	clk.add(2 * time.Minute)
	b.status("/catalog")
	resealed := b.cookie(sessionCookie)
	if resealed == val {
		t.Fatal("cookie not re-sealed after activity")
	}
	c := newClockEnv(t, appConfig{keys: [][]byte{newK}}, clk)
	c.adopt(sessionCookie, resealed)
	if got := c.status("/catalog"); got != http.StatusOK {
		t.Errorf("re-sealed cookie under new key only: %d", got)
	}
	// Dropping the old key kills sessions that were not renewed.
	d := newClockEnv(t, appConfig{keys: [][]byte{newK}}, clk)
	d.adopt(sessionCookie, val)
	if got := d.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("old-key cookie accepted without previous key: %d", got)
	}
}

func TestLogoutInvalidatesSession(t *testing.T) {
	clk := newClock()
	e := newClockEnv(t, appConfig{keys: [][]byte{testKey(1)}}, clk)
	e.login()
	val := e.cookie(sessionCookie)
	resp, _ := e.postForm("/logout", url.Values{"csrf": {e.csrfToken()}})
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout: %d", resp.StatusCode)
	}
	// Replaying the captured cookie fails even though it is still authentic.
	e.adopt(sessionCookie, val)
	if got := e.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("replayed cookie after logout: %d", got)
	}
}

func TestLoginRotatesSession(t *testing.T) {
	clk := newClock()
	e := newClockEnv(t, appConfig{keys: [][]byte{testKey(1)}}, clk)
	e.login()
	old := e.cookie(sessionCookie)
	// Log in again while still holding the old cookie (GET /login would redirect).
	e.adopt(csrfCookie, "tok")
	resp, _ := e.postForm("/login", url.Values{"csrf": {"tok"}, "user": {"ana"}, "password": {"pw"}})
	if resp.StatusCode != http.StatusSeeOther || e.cookie(sessionCookie) == old {
		t.Fatalf("relogin: %d, cookie renewed=%v", resp.StatusCode, e.cookie(sessionCookie) != old)
	}
	e.adopt(sessionCookie, old)
	if got := e.status("/catalog"); got != http.StatusSeeOther {
		t.Errorf("pre-login cookie still valid: %d", got)
	}
}

func TestRateLimitPerUser(t *testing.T) {
	clk := newClock()
	e := newClockEnv(t, appConfig{userMax: 3, ipMax: 100}, clk)
	for i := 0; i < 3; i++ {
		if r := e.tryLogin("ana", "bad", ""); r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d: %d", i, r.StatusCode)
		}
	}
	r := e.tryLogin("ana", "bad", "")
	if r.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("4th attempt: %d", r.StatusCode)
	}
	if n, err := strconv.Atoi(r.Header.Get("Retry-After")); err != nil || n < 890 || n > 900 {
		t.Errorf("Retry-After = %q", r.Header.Get("Retry-After"))
	}
	// The right password is refused too while locked, with the same generic
	// message as for unknown users (no enumeration).
	if r := e.tryLogin("ana", "pw", ""); r.StatusCode != http.StatusTooManyRequests {
		t.Errorf("good password while locked: %d", r.StatusCode)
	}
	// Another user is not affected.
	if r := e.tryLogin("bob", "bad", ""); r.StatusCode != http.StatusUnauthorized {
		t.Errorf("other user: %d", r.StatusCode)
	}
	// Lockout expires.
	clk.add(15*time.Minute + time.Second)
	if r := e.tryLogin("ana", "pw", ""); r.StatusCode != http.StatusSeeOther {
		t.Errorf("after lockout: %d", r.StatusCode)
	}
}

func TestRateLimitBackoffDoubles(t *testing.T) {
	clk := newClock()
	e := newClockEnv(t, appConfig{userMax: 2, ipMax: 100}, clk)
	lock := func() time.Duration {
		for i := 0; i < 2; i++ {
			e.tryLogin("ana", "bad", "")
		}
		r := e.tryLogin("ana", "bad", "")
		if r.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("not locked: %d", r.StatusCode)
		}
		n, _ := strconv.Atoi(r.Header.Get("Retry-After"))
		return time.Duration(n) * time.Second
	}
	first := lock()
	clk.add(first + time.Second)
	second := lock()
	if second < 2*first-2*time.Second {
		t.Errorf("second lockout %v not ~double of %v", second, first)
	}
}

func TestRateLimitPerIP(t *testing.T) {
	clk := newClock()
	e := newClockEnv(t, appConfig{userMax: 100, ipMax: 4}, clk)
	for i := 0; i < 4; i++ {
		e.tryLogin("user"+strconv.Itoa(i), "bad", "") // different users, same IP
	}
	r := e.tryLogin("fresh", "bad", "")
	if r.StatusCode != http.StatusTooManyRequests || r.Header.Get("Retry-After") == "" {
		t.Fatalf("ip not limited: %d", r.StatusCode)
	}
	// Even valid credentials from that IP are refused.
	if r := e.tryLogin("ana", "pw", ""); r.StatusCode != http.StatusTooManyRequests {
		t.Errorf("good login from blocked ip: %d", r.StatusCode)
	}
}

func TestForwardedForOnlyWithTrustProxy(t *testing.T) {
	clk := newClock()
	// Not trusted: rotating X-Forwarded-For does not evade the IP limit.
	e := newClockEnv(t, appConfig{userMax: 100, ipMax: 3}, clk)
	for i := 0; i < 3; i++ {
		e.tryLogin("u"+strconv.Itoa(i), "bad", "203.0.113."+strconv.Itoa(i))
	}
	if r := e.tryLogin("x", "bad", "198.51.100.9"); r.StatusCode != http.StatusTooManyRequests {
		t.Errorf("XFF honoured without trust_proxy: %d", r.StatusCode)
	}
	// Trusted: the proxy-appended (right-most) address is the client; a
	// spoofed left-most value does not matter, and distinct clients are separate.
	f := newClockEnv(t, appConfig{userMax: 100, ipMax: 3, trustProxy: true}, clk)
	for i := 0; i < 3; i++ {
		f.tryLogin("u"+strconv.Itoa(i), "bad", "6.6.6."+strconv.Itoa(i)+", 203.0.113.7")
	}
	if r := f.tryLogin("x", "bad", "1.2.3.4, 203.0.113.7"); r.StatusCode != http.StatusTooManyRequests {
		t.Errorf("trusted proxy: same client not limited: %d", r.StatusCode)
	}
	if r := f.tryLogin("x", "bad", "1.2.3.4, 203.0.113.8"); r.StatusCode != http.StatusUnauthorized {
		t.Errorf("trusted proxy: other client wrongly limited: %d", r.StatusCode)
	}
}

func TestFailedLoginMessageIsGeneric(t *testing.T) {
	e := newClockEnv(t, appConfig{}, newClock())
	for _, u := range []string{"ana", "ghost"} {
		e.get("/login")
		resp, body := e.postForm("/login", url.Values{"csrf": {e.cookie(csrfCookie)}, "user": {u}, "password": {"nope"}})
		if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(body, "Usuario o contraseña incorrectos.") {
			t.Errorf("%s: %d", u, resp.StatusCode)
		}
	}
}

func TestFailedLoginHasMinimumLatency(t *testing.T) {
	e := newClockEnv(t, appConfig{minLoginTime: 150 * time.Millisecond}, newClock())
	start := time.Now()
	e.tryLogin("ghost", "nope", "")
	if d := time.Since(start); d < 150*time.Millisecond {
		t.Errorf("failed login answered in %v", d)
	}
}

func TestKeyFileCreatedOnceAndReused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "web_session.key")
	cfg := contract.Config{"session_key_file": path}
	k1, err := resolveKeys(cfg, func(string) string { return "" })
	if err != nil || len(k1) != 1 {
		t.Fatalf("first: %v %v", k1, err)
	}
	if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v %v", st, err)
	}
	k2, err := resolveKeys(cfg, func(string) string { return "" })
	if err != nil || string(k1[0]) != string(k2[0]) {
		t.Fatalf("key changed between runs: %v", err)
	}
	// Env wins over the file; previous key is appended.
	env := map[string]string{
		"DUCT_WEB_SESSION_KEY":          base64.StdEncoding.EncodeToString(testKey(7)),
		"DUCT_WEB_SESSION_KEY_PREVIOUS": base64.StdEncoding.EncodeToString(testKey(8)),
	}
	k3, err := resolveKeys(cfg, func(k string) string { return env[k] })
	if err != nil || len(k3) != 2 || string(k3[0]) != string(testKey(7)) || string(k3[1]) != string(testKey(8)) {
		t.Fatalf("env keys: %v %v", k3, err)
	}
	if _, err := resolveKeys(contract.Config{"session_key": "short"}, func(string) string { return "" }); err == nil {
		t.Error("bad key accepted")
	}
}
