package web

import (
	"net/http"
	"net/url"
	"sync"
	"time"
)

// flash is a one-shot message that survives a redirect (post/redirect/get): a
// result banner, the draft of a form that failed validation, or a secret (a
// new token, a temporary password) that must be shown exactly once. It lives in
// memory, is bound to the session that created it and is deleted when read or
// after flashTTL, so reloading the page, or opening the link elsewhere, shows
// nothing. Secrets never reach a URL, a cookie or the browser's storage.
type flash struct {
	Kind   string // "ok" | "error"
	Msg    string
	Secret *flashSecret
	Draft  map[string]string // form field values to show again, by field key
	Form   string            // which form the Draft belongs to

	owner string
	exp   time.Time
}

type flashSecret struct {
	Label string // "Contraseña temporal de ana"
	Value string
	Note  string
}

const (
	flashTTL = 2 * time.Minute
	flashMax = 512
)

type flashes struct {
	mu sync.Mutex
	m  map[string]*flash
}

func newFlashes() *flashes { return &flashes{m: map[string]*flash{}} }

func (s *flashes) put(sessID string, f *flash) string {
	id := randomToken()
	now := time.Now()
	f.owner, f.exp = sessID, now.Add(flashTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.m {
		if now.After(v.exp) {
			delete(s.m, k)
		}
	}
	if len(s.m) >= flashMax { // only a flood gets here: drop the oldest-expiring
		var oldest string
		for k, v := range s.m {
			if oldest == "" || v.exp.Before(s.m[oldest].exp) {
				oldest = k
			}
		}
		delete(s.m, oldest)
	}
	s.m[id] = f
	return id
}

// take returns and deletes the flash id if it belongs to sessID and is fresh.
func (s *flashes) take(sessID, id string) *flash {
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.m[id]
	if f == nil || f.owner != sessID {
		return nil
	}
	delete(s.m, id)
	if time.Now().After(f.exp) {
		return nil
	}
	return f
}

// redirectFlash stores f and sends the browser to path?f=<id> (303).
func (a *app) redirectFlash(w http.ResponseWriter, r *http.Request, sess session, path string, f *flash) {
	id := a.flash.put(sess.id, f)
	sep := "?"
	if u, err := url.Parse(path); err == nil && u.RawQuery != "" {
		sep = "&"
	}
	http.Redirect(w, r, path+sep+"f="+id, http.StatusSeeOther)
}

// takeFlash reads the flash named by ?f= for this session, if any.
func (a *app) takeFlash(r *http.Request, sess session) *flash {
	return a.flash.take(sess.id, r.URL.Query().Get("f"))
}
