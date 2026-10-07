package web

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// loginLimiter throttles failed logins per username and per client IP.
// N failures inside a window lock the key for a lockout period; each further
// lockout of the same key doubles it (up to maxLockout) until the key has been
// quiet for a whole lockout. State is in memory: a restart forgets it.
//
// A locked username also rejects the right password; that is what makes it
// useful against guessing, at the price that someone can lock a known
// username out (it unlocks by itself). The IP limit is higher than the user
// one so several people behind one NAT do not lock each other.
type loginLimiter struct {
	mu         sync.Mutex
	now        func() time.Time
	userMax    int
	ipMax      int
	window     time.Duration
	lockout    time.Duration
	maxLockout time.Duration
	entries    map[string]*limitEntry
}

type limitEntry struct {
	fails int
	first time.Time // start of the current counting window
	until time.Time // locked until
	level int       // consecutive lockouts, drives the backoff
	last  time.Time // last failure, for garbage collection
}

const maxLimiterEntries = 10000

func newLoginLimiter(userMax, ipMax int, lockout time.Duration) *loginLimiter {
	return &loginLimiter{
		now: time.Now, userMax: userMax, ipMax: ipMax, window: lockout, lockout: lockout,
		maxLockout: 24 * time.Hour, entries: map[string]*limitEntry{},
	}
}

func userKey(user string) string {
	user = strings.ToLower(strings.TrimSpace(user))
	if len(user) > 256 {
		user = user[:256]
	}
	return "u:" + user
}

func ipKey(ip string) string { return "i:" + ip }

// blocked reports how long the caller must wait (0 = may try).
func (l *loginLimiter) blocked(user, ip string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var wait time.Duration
	for _, k := range []string{userKey(user), ipKey(ip)} {
		if e := l.entries[k]; e != nil && now.Before(e.until) {
			wait = max(wait, e.until.Sub(now))
		}
	}
	return wait
}

// fail records a failed attempt and returns true if it triggered a lockout.
func (l *loginLimiter) fail(user, ip string) (locked bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.gc(now)
	for _, c := range []struct {
		key string
		max int
	}{{userKey(user), l.userMax}, {ipKey(ip), l.ipMax}} {
		e := l.entries[c.key]
		if e == nil {
			e = &limitEntry{first: now}
			l.entries[c.key] = e
		}
		if now.Sub(e.first) > l.window && !now.Before(e.until) {
			e.fails, e.first = 0, now
			if now.Sub(e.until) > l.lockout { // quiet for a whole lockout: forget the backoff
				e.level = 0
			}
		}
		e.last = now
		e.fails++
		if e.fails >= c.max {
			e.level++
			d := l.lockout
			for i := 1; i < e.level && d < l.maxLockout; i++ {
				d *= 2
			}
			e.until, e.fails, e.first = now.Add(min(d, l.maxLockout)), 0, now
			locked = true
		}
	}
	return locked
}

// success clears the username's counters (not the IP's: an attacker with one
// valid account must not be able to reset the IP budget).
func (l *loginLimiter) success(user string) {
	l.mu.Lock()
	delete(l.entries, userKey(user))
	l.mu.Unlock()
}

// gc drops entries that are neither locked nor recent; if the table is still
// too big (attack with random usernames) it drops unlocked ones arbitrarily.
func (l *loginLimiter) gc(now time.Time) {
	if len(l.entries) < maxLimiterEntries {
		return
	}
	for k, e := range l.entries {
		if !now.Before(e.until) && now.Sub(e.last) > l.window {
			delete(l.entries, k)
		}
	}
	for k, e := range l.entries {
		if len(l.entries) < maxLimiterEntries {
			break
		}
		if !now.Before(e.until) {
			delete(l.entries, k)
		}
	}
}

// clientIP is the peer address; with trustProxy the right-most
// X-Forwarded-For entry (the one appended by our own proxy; the left ones are
// client-controlled). IPv6 is grouped by /64.
func clientIP(r *http.Request, trustProxy bool) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if v := strings.TrimSpace(parts[len(parts)-1]); net.ParseIP(v) != nil {
				host = v
			}
		}
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}
	if ip.To4() == nil {
		return ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
	}
	return ip.String()
}
