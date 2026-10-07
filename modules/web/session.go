package web

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/chacha20poly1305"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Sessions are stateless: the cookie is the session. Its value is
//
//	base64url( version(1) | keyID(1) | nonce(24) | XChaCha20-Poly1305(payload) )
//
// with version and keyID as additional authenticated data. The payload holds
// the identity, a session id, a per-session CSRF secret and the timestamps.
// Nothing is stored on the server except a small in-memory denylist of
// logged-out session ids (see sessions.revoke for the trade-off).
const cookieVersion = 1

// session is the decrypted cookie payload.
type session struct {
	id      string
	who     contract.Identity
	csrfKey string
	issued  time.Time
	seen    time.Time // last activity, refreshed by touch
	expires time.Time // absolute timeout
}

type payload struct {
	ID   string `json:"i"`
	User string `json:"u"`
	Role string `json:"r"`
	CSRF string `json:"c"`
	Iat  int64  `json:"a"`
	Seen int64  `json:"s"`
	Exp  int64  `json:"e"`
}

type sessionKey struct {
	id   byte // first byte of sha256(key): a hint to pick the key, not a secret
	aead interface {
		Seal(dst, nonce, plaintext, additionalData []byte) []byte
		Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error)
		NonceSize() int
	}
}

func newSessionKey(raw []byte) (sessionKey, error) {
	if len(raw) != chacha20poly1305.KeySize {
		return sessionKey{}, fmt.Errorf("session key must be %d bytes, got %d", chacha20poly1305.KeySize, len(raw))
	}
	a, err := chacha20poly1305.NewX(raw)
	if err != nil {
		return sessionKey{}, err
	}
	h := sha256.Sum256(raw)
	return sessionKey{id: h[0], aead: a}, nil
}

// parseKey decodes a base64 (std or URL, padded or not) 32-byte key.
func parseKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) == chacha20poly1305.KeySize {
			return b, nil
		}
	}
	return nil, errors.New("not base64 of 32 bytes")
}

// loadKeyFile reads the key persisted at path, or creates it (mode 0600,
// never overwriting an existing file) on first use.
func loadKeyFile(path string) ([]byte, error) {
	if b, err := os.ReadFile(path); err == nil {
		return parseKey(string(b))
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key := make([]byte, chacha20poly1305.KeySize)
	mustRand(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) { // lost a race with another process
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return parseKey(string(b))
	}
	if err != nil {
		return nil, err
	}
	_, werr := f.WriteString(base64.StdEncoding.EncodeToString(key) + "\n")
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(path)
		return nil, werr
	}
	return key, nil
}

type sessions struct {
	keys []sessionKey // keys[0] seals; all of them open (rotation)
	ttl  time.Duration
	idle time.Duration
	now  func() time.Time

	mu   sync.Mutex
	deny map[string]time.Time // revoked session id -> its expiry
}

// newSessions builds the store. rawKeys[0] is the current key, the rest only
// decrypt. Without keys an ephemeral one is generated: sessions then die with
// the process.
func newSessions(ttl, idle time.Duration, rawKeys [][]byte) (*sessions, error) {
	if len(rawKeys) == 0 {
		k := make([]byte, chacha20poly1305.KeySize)
		mustRand(k)
		rawKeys = [][]byte{k}
	}
	s := &sessions{ttl: ttl, idle: idle, now: time.Now, deny: map[string]time.Time{}}
	for _, raw := range rawKeys {
		k, err := newSessionKey(raw)
		if err != nil {
			return nil, err
		}
		s.keys = append(s.keys, k)
	}
	if s.idle <= 0 || s.idle > s.ttl {
		s.idle = s.ttl
	}
	return s, nil
}

func mustRand(b []byte) {
	if _, err := rand.Read(b); err != nil {
		panic("web: no entropy: " + err.Error())
	}
}

func randomToken() string {
	b := make([]byte, 32)
	mustRand(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// create starts a session for who and returns it with its cookie value.
func (s *sessions) create(who contract.Identity) (session, string) {
	now := s.now()
	sess := session{id: randomToken(), who: who, csrfKey: randomToken(), issued: now, seen: now, expires: now.Add(s.ttl)}
	return sess, s.seal(sess)
}

func (s *sessions) seal(sess session) string {
	p, _ := json.Marshal(payload{
		ID: sess.id, User: sess.who.User, Role: sess.who.Role, CSRF: sess.csrfKey,
		Iat: sess.issued.Unix(), Seen: sess.seen.Unix(), Exp: sess.expires.Unix(),
	})
	k := s.keys[0]
	hdr := []byte{cookieVersion, k.id}
	nonce := make([]byte, k.aead.NonceSize())
	mustRand(nonce)
	out := append(append(hdr, nonce...), k.aead.Seal(nil, nonce, p, hdr)...)
	return base64.RawURLEncoding.EncodeToString(out)
}

// decode authenticates and decrypts a cookie value, ignoring time and denylist.
func (s *sessions) decode(value string) (session, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) < 2 || raw[0] != cookieVersion {
		return session{}, false
	}
	hdr, rest := raw[:2], raw[2:]
	for _, k := range s.keys {
		n := k.aead.NonceSize()
		if k.id != hdr[1] || len(rest) < n {
			continue
		}
		pt, err := k.aead.Open(nil, rest[:n], rest[n:], hdr)
		if err != nil {
			continue
		}
		var p payload
		if json.Unmarshal(pt, &p) != nil || p.ID == "" {
			return session{}, false
		}
		return session{
			id: p.ID, who: contract.Identity{User: p.User, Role: p.Role}, csrfKey: p.CSRF,
			issued: time.Unix(p.Iat, 0), seen: time.Unix(p.Seen, 0), expires: time.Unix(p.Exp, 0),
		}, true
	}
	return session{}, false
}

// get validates a cookie value: authentic, not past the absolute timeout, not
// idle for longer than the idle timeout, not logged out.
func (s *sessions) get(value string) (session, bool) {
	if value == "" {
		return session{}, false
	}
	sess, ok := s.decode(value)
	if !ok {
		return session{}, false
	}
	now := s.now()
	if !now.Before(sess.expires) || now.Sub(sess.seen) > s.idle || sess.issued.After(now.Add(time.Minute)) {
		return session{}, false
	}
	s.mu.Lock()
	_, revoked := s.deny[sess.id]
	s.mu.Unlock()
	if revoked {
		return session{}, false
	}
	return sess, true
}

// touch slides the idle timeout: if the last activity is old enough it returns
// a re-sealed cookie value (and the updated session). It also re-seals with the
// current key, so a rotated-out key stops mattering once users are active.
func (s *sessions) touch(sess session) (session, string, bool) {
	now := s.now()
	if now.Sub(sess.seen) < min(s.idle/4, time.Minute) {
		return sess, "", false
	}
	sess.seen = now
	return sess, s.seal(sess), true
}

// revoke (logout) puts the session id on an in-memory denylist until the
// session would have expired anyway. Trade-off: the denylist is not persisted,
// so a restart forgets it and a cookie stolen before logout works again until
// its idle/absolute timeout; keep session_ttl short, or rotate the key to
// kill everything at once.
func (s *sessions) revoke(sess session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for id, exp := range s.deny { // lazy purge keeps the map bounded
		if !now.Before(exp) {
			delete(s.deny, id)
		}
	}
	s.deny[sess.id] = sess.expires
}

// revokeValue revokes the session in a cookie value, if it is authentic.
func (s *sessions) revokeValue(value string) {
	if sess, ok := s.decode(value); ok {
		s.revoke(sess)
	}
}

// csrf is the per-session anti-CSRF token. It lives inside the encrypted
// cookie, so only the holder of the session (and the server) can know it.
func (s *sessions) csrf(sess session) string { return sess.csrfKey }

func tokensEqual(a, b string) bool {
	return a != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// resolveKeys gathers session keys from config/env/file: the current key first,
// then the previous one (rotation). Returns nil keys (ephemeral) when nothing
// is configured, after warning.
func resolveKeys(cfg contract.Config, getenv func(string) string) ([][]byte, error) {
	var keys [][]byte
	cur := ""
	if v, ok := cfg["session_key"].(string); ok {
		cur = v
	}
	if cur == "" {
		cur = getenv("DUCT_WEB_SESSION_KEY")
	}
	switch {
	case cur != "":
		k, err := parseKey(cur)
		if err != nil {
			return nil, fmt.Errorf("web: session_key: %w", err)
		}
		keys = append(keys, k)
	default:
		if path, ok := cfg["session_key_file"].(string); ok && path != "" {
			k, err := loadKeyFile(path)
			if err != nil {
				return nil, fmt.Errorf("web: session_key_file %s: %w", path, err)
			}
			keys = append(keys, k)
		} else {
			slog.Warn("web: no session_key / session_key_file: sessions will not survive a restart")
			return nil, nil
		}
	}
	prev := ""
	if v, ok := cfg["session_key_previous"].(string); ok {
		prev = v
	}
	if prev == "" {
		prev = getenv("DUCT_WEB_SESSION_KEY_PREVIOUS")
	}
	if prev != "" {
		k, err := parseKey(prev)
		if err != nil {
			return nil, fmt.Errorf("web: session_key_previous: %w", err)
		}
		keys = append(keys, k)
	}
	return keys, nil
}
