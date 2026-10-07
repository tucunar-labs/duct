package governance

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/metrics"
)

// Store reads and writes the governance tables through an Engine. It also is
// the default contract.AuthProvider ("local": passwords and bearer tokens).
// The tables must exist (project.Migrate).
type Store struct {
	eng contract.Engine
}

var _ contract.AuthProvider = (*Store)(nil)

// NewStore returns a Store over eng.
func NewStore(eng contract.Engine) *Store { return &Store{eng: eng} }

// Name implements contract.Module.
func (*Store) Name() string { return "local" }

// Init implements contract.Module (nothing to do: the engine is given).
func (*Store) Init(context.Context, contract.Config) error { return nil }

// Close implements contract.Module (the engine is owned by the caller).
func (*Store) Close() error { return nil }

var (
	roleRe  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,39}$`)
	userRe  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.@-]{0,63}$`)
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
)

// Privilege is the only privilege there is for now.
const privSelect = "select"

func lit(s string) string   { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func ident(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// rows runs a query and returns every cell as text (NULL becomes "").
func (s *Store) rows(ctx context.Context, q string, args ...any) ([][]string, error) {
	st, err := s.eng.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer st.Release()
	var out [][]string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			row := make([]string, rec.NumCols())
			for c := range row {
				col := rec.Column(c)
				if !col.IsNull(i) {
					row[c] = strings.Clone(col.ValueStr(i))
				}
			}
			out = append(out, row)
		}
	}
	return out, st.Err()
}

func (s *Store) exists(ctx context.Context, q string, args ...any) (bool, error) {
	r, err := s.rows(ctx, q, args...)
	return len(r) > 0, err
}

// NormalizeRelation turns "Gold.Revenue" or "revenue" into "gold.revenue" /
// "main.revenue" and validates both parts.
func NormalizeRelation(rel string) (schema, table string, err error) {
	rel = strings.ToLower(strings.TrimSpace(rel))
	parts := strings.Split(rel, ".")
	switch len(parts) {
	case 1:
		parts = []string{"main", parts[0]}
	case 2:
	default:
		return "", "", fmt.Errorf("relation %q: use schema.table", rel)
	}
	for _, p := range parts {
		if !identRe.MatchString(p) {
			return "", "", fmt.Errorf("relation %q: invalid name %q", rel, p)
		}
	}
	if strings.HasPrefix(parts[0], contract.RoleSchemaPrefix) {
		return "", "", fmt.Errorf("relation %q: role schemas cannot be granted", rel)
	}
	return parts[0], parts[1], nil
}

func (s *Store) relationExists(ctx context.Context, schema, table string) (bool, error) {
	return s.exists(ctx, `SELECT 1 FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = ? AND table_name = ?`, schema, table)
}

// ---- roles ----

// CreateRole adds a role.
func (s *Store) CreateRole(ctx context.Context, name string) error {
	if !roleRe.MatchString(name) {
		return fmt.Errorf("role name %q: use lowercase letters, digits and _ (max 40, start with a letter)", name)
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_roles WHERE name = ?`, name); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("role %q already exists", name)
	}
	return s.exec(ctx, `INSERT INTO duct.duct_roles (name, created_at) VALUES (?, now())`, name)
}

// RoleInfo describes one role.
type RoleInfo struct {
	Name    string
	Users   int
	Grants  []string // schema.table
	Filters map[string]string
	Masks   map[string]string // schema.table.column -> expr
}

// Roles lists roles with their grants, filters and masks.
func (s *Store) Roles(ctx context.Context) ([]RoleInfo, error) {
	rs, err := s.rows(ctx, `SELECT r.name, CAST((SELECT count(*) FROM duct.duct_users u WHERE u.role = r.name) AS VARCHAR) FROM duct.duct_roles r ORDER BY r.name`)
	if err != nil {
		return nil, err
	}
	var out []RoleInfo
	idx := map[string]int{}
	for _, r := range rs {
		var n int
		_, _ = fmt.Sscan(r[1], &n)
		idx[r[0]] = len(out)
		out = append(out, RoleInfo{Name: r[0], Users: n, Filters: map[string]string{}, Masks: map[string]string{}})
	}
	gs, err := s.rows(ctx, `SELECT role, relation FROM duct.duct_grants ORDER BY role, relation`)
	if err != nil {
		return nil, err
	}
	for _, g := range gs {
		if i, ok := idx[g[0]]; ok {
			out[i].Grants = append(out[i].Grants, g[1])
		}
	}
	fs, err := s.rows(ctx, `SELECT role, relation, predicate FROM duct.duct_row_filters`)
	if err != nil {
		return nil, err
	}
	for _, f := range fs {
		if i, ok := idx[f[0]]; ok {
			out[i].Filters[f[1]] = f[2]
		}
	}
	ms, err := s.rows(ctx, `SELECT role, relation, column_name, expr FROM duct.duct_masks`)
	if err != nil {
		return nil, err
	}
	for _, m := range ms {
		if i, ok := idx[m[0]]; ok {
			out[i].Masks[m[1]+"."+m[2]] = m[3]
		}
	}
	return out, nil
}

// DropRole removes a role with its grants, filters and masks. It refuses while
// users still have the role. Run Regenerate afterwards to drop its schema.
func (s *Store) DropRole(ctx context.Context, name string) error {
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_users WHERE role = ?`, name); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("role %q still has users; drop or move them first", name)
	}
	return s.execTx(ctx,
		"DELETE FROM duct.duct_grants WHERE role = "+lit(name),
		"DELETE FROM duct.duct_row_filters WHERE role = "+lit(name),
		"DELETE FROM duct.duct_masks WHERE role = "+lit(name),
		"DELETE FROM duct.duct_roles WHERE name = "+lit(name),
	)
}

func (s *Store) requireRole(ctx context.Context, name string) error {
	ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_roles WHERE name = ?`, name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("role %q does not exist (duct role create %s)", name, name)
	}
	return nil
}

// ---- users ----

// CreateUser adds a user with a role and a password.
func (s *Store) CreateUser(ctx context.Context, name, role, password string) error {
	if !userRe.MatchString(name) {
		return fmt.Errorf("user name %q: letters, digits and _.@- only (max 64)", name)
	}
	if err := s.requireRole(ctx, role); err != nil {
		return err
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_users WHERE name = ?`, name); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("user %q already exists", name)
	}
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	return s.exec(ctx, `INSERT INTO duct.duct_users (name, role, password_hash, disabled, created_at) VALUES (?, ?, ?, false, now())`, name, role, hash)
}

// SetPassword replaces a user's password.
func (s *Store) SetPassword(ctx context.Context, name, password string) error {
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	return s.mustChange(ctx, name, `UPDATE duct.duct_users SET password_hash = ? WHERE name = ?`, hash, name)
}

// SetDisabled blocks or unblocks a user (its tokens stop working too).
func (s *Store) SetDisabled(ctx context.Context, name string, disabled bool) error {
	return s.mustChange(ctx, name, `UPDATE duct.duct_users SET disabled = ? WHERE name = ?`, disabled, name)
}

func (s *Store) mustChange(ctx context.Context, user, q string, args ...any) error {
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_users WHERE name = ?`, user); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("user %q does not exist", user)
	}
	return s.exec(ctx, q, args...)
}

// DropUser deletes a user and its tokens.
func (s *Store) DropUser(ctx context.Context, name string) error {
	return s.mustChange2(ctx, name,
		"DELETE FROM duct.duct_tokens WHERE user_name = "+lit(name),
		"DELETE FROM duct.duct_users WHERE name = "+lit(name))
}

func (s *Store) mustChange2(ctx context.Context, user string, stmts ...string) error {
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_users WHERE name = ?`, user); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("user %q does not exist", user)
	}
	return s.execTx(ctx, stmts...)
}

// UserInfo describes one user (never its hash).
type UserInfo struct {
	Name     string
	Role     string
	Disabled bool
}

// Users lists users.
func (s *Store) Users(ctx context.Context) ([]UserInfo, error) {
	rs, err := s.rows(ctx, `SELECT name, role, CAST(disabled AS VARCHAR) FROM duct.duct_users ORDER BY name`)
	if err != nil {
		return nil, err
	}
	out := make([]UserInfo, 0, len(rs))
	for _, r := range rs {
		out = append(out, UserInfo{Name: r[0], Role: r[1], Disabled: isTrue(r[2])})
	}
	return out, nil
}

// ---- tokens ----

const tokenPrefix = "dct_"

func hashToken(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

// CreateToken issues a bearer token for a user. The plaintext is returned
// once; only its SHA-256 is stored. ttl <= 0 means no expiry.
func (s *Store) CreateToken(ctx context.Context, user, label string, ttl time.Duration) (token, id string, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token = tokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	hash := hashToken(token)
	id = hash[:12]
	var expires any
	if ttl > 0 {
		expires = time.Now().Add(ttl).UTC().Format("2006-01-02 15:04:05")
	}
	err = s.mustChange(ctx, user, `INSERT INTO duct.duct_tokens (id, user_name, token_hash, label, created_at, expires_at, revoked) VALUES (?, ?, ?, ?, now(), CAST(? AS TIMESTAMP), false)`,
		id, user, hash, label, expires)
	if err != nil {
		return "", "", err
	}
	return token, id, nil
}

// RevokeToken disables a token by id.
func (s *Store) RevokeToken(ctx context.Context, id string) error {
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_tokens WHERE id = ?`, id); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("token %q does not exist", id)
	}
	return s.exec(ctx, `UPDATE duct.duct_tokens SET revoked = true WHERE id = ?`, id)
}

// ---- authentication ----

// Authenticate implements contract.AuthProvider. Schemes: "password"
// (User + Secret) and "bearer" (Secret is a token; User is ignored).
func (s *Store) Authenticate(ctx context.Context, c contract.Credentials) (contract.Identity, error) {
	var (
		id  contract.Identity
		err error
	)
	switch c.Scheme {
	case "password":
		id, err = s.byPassword(ctx, c.User, c.Secret)
	case "bearer":
		id, err = s.byToken(ctx, c.Secret)
	default:
		err = fmt.Errorf("%w: unsupported scheme %q", contract.ErrUnauthenticated, c.Scheme)
	}
	if errors.Is(err, contract.ErrUnauthenticated) {
		authFailures.Inc(c.Scheme)
	}
	return id, err
}

var authFailures = metrics.Default().Counter("duct_auth_failures_total", "Logins refused (bad or missing credentials, disabled user, revoked or expired token), by scheme.", "scheme")

func (s *Store) byPassword(ctx context.Context, user, password string) (contract.Identity, error) {
	if !userRe.MatchString(user) {
		VerifyPassword(dummyHash, password)
		return contract.Identity{}, contract.ErrUnauthenticated
	}
	rs, err := s.rows(ctx, `SELECT role, password_hash, CAST(disabled AS VARCHAR) FROM duct.duct_users WHERE name = ?`, user)
	if err != nil {
		return contract.Identity{}, err
	}
	if len(rs) == 0 {
		VerifyPassword(dummyHash, password)
		return contract.Identity{}, contract.ErrUnauthenticated
	}
	if !VerifyPassword(rs[0][1], password) || isTrue(rs[0][2]) {
		return contract.Identity{}, contract.ErrUnauthenticated
	}
	return contract.Identity{User: user, Role: rs[0][0]}, nil
}

func (s *Store) byToken(ctx context.Context, token string) (contract.Identity, error) {
	if !strings.HasPrefix(token, tokenPrefix) || len(token) > 200 {
		return contract.Identity{}, contract.ErrUnauthenticated
	}
	rs, err := s.rows(ctx, `SELECT u.name, u.role, CAST(u.disabled AS VARCHAR), CAST(t.revoked AS VARCHAR), CAST(t.expires_at AS VARCHAR)
		FROM duct.duct_tokens t JOIN duct.duct_users u ON u.name = t.user_name WHERE t.token_hash = ?`, hashToken(token))
	if err != nil {
		return contract.Identity{}, err
	}
	if len(rs) == 0 {
		return contract.Identity{}, contract.ErrUnauthenticated
	}
	r := rs[0]
	if isTrue(r[2]) || isTrue(r[3]) {
		return contract.Identity{}, contract.ErrUnauthenticated
	}
	if r[4] != "" {
		exp, ok := parseTime(r[4])
		if !ok || !exp.After(time.Now()) { // unreadable expiry fails closed
			return contract.Identity{}, contract.ErrUnauthenticated
		}
	}
	return contract.Identity{User: r[0], Role: r[1]}, nil
}

// isTrue reads a BOOLEAN that the SQLite state file may hold as 0/1.
func isTrue(s string) bool { return s == "1" || strings.EqualFold(s, "true") }

// parseTime reads a TIMESTAMP as stored in SQLite or Postgres (no zone means UTC).
func parseTime(s string) (time.Time, bool) {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999",
	} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

var errNoRelation = errors.New("relation does not exist in the lake")

// ---- grants, filters, masks ----

// Grant gives a role SELECT on a relation (schema.table).
func (s *Store) Grant(ctx context.Context, role, relation string) error {
	if err := s.requireRole(ctx, role); err != nil {
		return err
	}
	schema, table, err := NormalizeRelation(relation)
	if err != nil {
		return err
	}
	if ok, err := s.relationExists(ctx, schema, table); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s.%s: %w", schema, table, errNoRelation)
	}
	rel := schema + "." + table
	// The view is named after the table: two granted relations may not share it.
	others, err := s.rows(ctx, `SELECT relation FROM duct.duct_grants WHERE role = ? AND relation <> ?`, role, rel)
	if err != nil {
		return err
	}
	for _, o := range others {
		if _, t, _ := NormalizeRelation(o[0]); t == table {
			return fmt.Errorf("role %s already has %s, whose view is also named %q", role, o[0], table)
		}
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_grants WHERE role = ? AND relation = ?`, role, rel); err != nil || ok {
		return err
	}
	return s.exec(ctx, `INSERT INTO duct.duct_grants (role, relation, privilege, granted_at) VALUES (?, ?, ?, now())`, role, rel, privSelect)
}

// Revoke removes a grant together with the role's filter and masks on it.
func (s *Store) Revoke(ctx context.Context, role, relation string) error {
	schema, table, err := NormalizeRelation(relation)
	if err != nil {
		return err
	}
	rel := schema + "." + table
	return s.execTx(ctx,
		"DELETE FROM duct.duct_grants WHERE role = "+lit(role)+" AND relation = "+lit(rel),
		"DELETE FROM duct.duct_row_filters WHERE role = "+lit(role)+" AND relation = "+lit(rel),
		"DELETE FROM duct.duct_masks WHERE role = "+lit(role)+" AND relation = "+lit(rel))
}

func (s *Store) requireGrant(ctx context.Context, role, relation string) (string, error) {
	schema, table, err := NormalizeRelation(relation)
	if err != nil {
		return "", err
	}
	rel := schema + "." + table
	if err := s.requireRole(ctx, role); err != nil {
		return "", err
	}
	ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_grants WHERE role = ? AND relation = ?`, role, rel)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("role %s has no grant on %s (duct grant select on %s to %s)", role, rel, rel, role)
	}
	return rel, nil
}

// SetRowFilter sets the predicate (a SQL boolean expression over the base
// table's columns) that limits the rows a role sees. An empty predicate drops it.
func (s *Store) SetRowFilter(ctx context.Context, role, relation, predicate string) error {
	rel, err := s.requireGrant(ctx, role, relation)
	if err != nil {
		return err
	}
	predicate = strings.TrimSpace(predicate)
	if predicate == "" {
		return s.exec(ctx, `DELETE FROM duct.duct_row_filters WHERE role = ? AND relation = ?`, role, rel)
	}
	schema, table, _ := NormalizeRelation(rel)
	base := ident(schema) + "." + ident(table)
	if err := s.checkPolicy(ctx, predicate, schema, table); err != nil {
		return fmt.Errorf("row filter: %w", err)
	}
	if err := s.checkExpr(ctx, "SELECT * FROM "+base+" WHERE ("+predicate+"\n)"); err != nil {
		return fmt.Errorf("row filter: %w", err)
	}
	return s.execTx(ctx,
		"DELETE FROM duct.duct_row_filters WHERE role = "+lit(role)+" AND relation = "+lit(rel),
		"INSERT INTO duct.duct_row_filters (role, relation, predicate) VALUES ("+lit(role)+", "+lit(rel)+", "+lit(predicate)+")")
}

// SetMask replaces a column by an expression (over the base table's columns)
// in the role's view. An empty expression drops the mask.
func (s *Store) SetMask(ctx context.Context, role, relation, column, expr string) error {
	rel, err := s.requireGrant(ctx, role, relation)
	if err != nil {
		return err
	}
	column = strings.ToLower(strings.TrimSpace(column))
	if !identRe.MatchString(column) {
		return fmt.Errorf("column %q: invalid name", column)
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return s.exec(ctx, `DELETE FROM duct.duct_masks WHERE role = ? AND relation = ? AND column_name = ?`, role, rel, column)
	}
	schema, table, _ := NormalizeRelation(rel)
	cols, err := s.columns(ctx, schema, table)
	if err != nil {
		return err
	}
	found := false
	for _, c := range cols {
		found = found || strings.EqualFold(c, column)
	}
	if !found {
		return fmt.Errorf("%s has no column %q", rel, column)
	}
	if err := s.checkPolicy(ctx, expr, schema, table); err != nil {
		return fmt.Errorf("mask: %w", err)
	}
	if err := s.checkExpr(ctx, "SELECT ("+expr+"\n) AS "+ident(column)+" FROM "+ident(schema)+"."+ident(table)); err != nil {
		return fmt.Errorf("mask: %w", err)
	}
	return s.execTx(ctx,
		"DELETE FROM duct.duct_masks WHERE role = "+lit(role)+" AND relation = "+lit(rel)+" AND column_name = "+lit(column),
		"INSERT INTO duct.duct_masks (role, relation, column_name, expr) VALUES ("+lit(role)+", "+lit(rel)+", "+lit(column)+", "+lit(expr)+")")
}

// checkPolicy parses `SELECT (<expr>) FROM schema.table` with DuckDB's parser
// and applies the structural rules of checkPolicyExpr.
func (s *Store) checkPolicy(ctx context.Context, expr, schema, table string) error {
	rs, err := s.rows(ctx, `SELECT CAST(json_serialize_sql(`+lit("SELECT ("+expr+"\n) FROM "+ident(schema)+"."+ident(table))+`) AS VARCHAR)`)
	if err != nil {
		return err
	}
	if len(rs) != 1 {
		return errors.New("cannot parse")
	}
	return checkPolicyExpr(rs[0][0], schema, table)
}

// SetUserRole changes the (single) role of a user. Sessions already open keep
// the old role until they log in again.
func (s *Store) SetUserRole(ctx context.Context, user, role string) error {
	if err := s.requireRole(ctx, role); err != nil {
		return err
	}
	return s.mustChange(ctx, user, `UPDATE duct.duct_users SET role = ? WHERE name = ?`, role, user)
}

// checkExpr parses a statement with DuckDB's parser (exactly one SELECT) and
// binds it against the lake, so unknown columns fail now and not at query time.
func (s *Store) checkExpr(ctx context.Context, stmt string) error {
	rs, err := s.rows(ctx, `SELECT CAST(json_serialize_sql(`+lit(stmt)+`) AS VARCHAR)`)
	if err != nil {
		return err
	}
	if len(rs) != 1 {
		return errors.New("cannot parse")
	}
	if _, err := parseAST(rs[0][0]); err != nil {
		return err
	}
	return s.exec(ctx, stmt+" LIMIT 0")
}

func (s *Store) columns(ctx context.Context, schema, table string) ([]string, error) {
	rs, err := s.rows(ctx, `SELECT column_name FROM information_schema.columns WHERE table_catalog = 'lake' AND table_schema = ? AND table_name = ? ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r[0]
	}
	return out, nil
}

// SQLite (the state file) can answer "database is locked" when another
// process writes at the same moment; the write is safe to repeat.
func retryLocked(fn func() error) error {
	var err error
	for i := range 8 {
		if err = fn(); err == nil || !strings.Contains(err.Error(), "database is locked") {
			return err
		}
		time.Sleep(time.Duration(50<<i) * time.Millisecond)
	}
	return err
}

func (s *Store) exec(ctx context.Context, q string, args ...any) error {
	return retryLocked(func() error { return s.eng.Exec(ctx, q, args...) })
}

func (s *Store) execTx(ctx context.Context, stmts ...string) error {
	return retryLocked(func() error { return s.eng.ExecTx(ctx, stmts...) })
}
