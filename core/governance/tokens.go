package governance

import (
	"context"
	"crypto/rand"
	"encoding/base64"
)

// TokenInfo describes one API token. The secret is never stored or listed.
type TokenInfo struct {
	ID        string
	User      string
	Label     string
	CreatedAt string
	ExpiresAt string // empty: never
	Revoked   bool
}

// Tokens lists tokens, newest first; user filters by owner when not empty.
func (s *Store) Tokens(ctx context.Context, user string) ([]TokenInfo, error) {
	q := `SELECT id, user_name, COALESCE(label, ''), CAST(created_at AS VARCHAR), COALESCE(CAST(expires_at AS VARCHAR), ''), CAST(revoked AS VARCHAR)
		FROM duct.duct_tokens`
	var args []any
	if user != "" {
		q += " WHERE user_name = ?"
		args = append(args, user)
	}
	rs, err := s.rows(ctx, q+" ORDER BY created_at DESC, id", args...)
	if err != nil {
		return nil, err
	}
	out := make([]TokenInfo, 0, len(rs))
	for _, r := range rs {
		out = append(out, TokenInfo{ID: r[0], User: r[1], Label: r[2], CreatedAt: r[3], ExpiresAt: r[4], Revoked: isTrue(r[5])})
	}
	return out, nil
}

// ResetPassword gives a user a new random password and returns it. The caller
// shows it once; only its hash is kept.
func (s *Store) ResetPassword(ctx context.Context, name string) (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	pw := base64.RawURLEncoding.EncodeToString(raw)
	if err := s.SetPassword(ctx, name, pw); err != nil {
		return "", err
	}
	return pw, nil
}

// UserByName returns one user, or ok=false.
func (s *Store) UserByName(ctx context.Context, name string) (UserInfo, bool, error) {
	us, err := s.Users(ctx)
	if err != nil {
		return UserInfo{}, false, err
	}
	for _, u := range us {
		if u.Name == name {
			return u, true, nil
		}
	}
	return UserInfo{}, false, nil
}
