package sharing

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/governance"
)

// RolePrefix is prepended to a recipient name to form its role and user.
const RolePrefix = "share_"

var (
	nameRe  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
	aliasRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// RoleOf is the role (and user) of a recipient.
func RoleOf(recipient string) string { return RolePrefix + recipient }

// RecipientOf maps an authenticated identity back to its recipient name; ok is
// false for any identity that is not a recipient's own user.
func RecipientOf(id contract.Identity) (string, bool) {
	name, ok := strings.CutPrefix(id.Role, RolePrefix)
	if !ok || id.User != id.Role || !nameRe.MatchString(name) {
		return "", false
	}
	return name, true
}

// Store reads and writes the sharing tables. It needs the migrated Duct tables
// and uses governance for roles, users, tokens, grants, filters and masks.
type Store struct {
	eng contract.Engine
	gov *governance.Store
}

// NewStore returns a Store over eng.
func NewStore(eng contract.Engine) *Store {
	return &Store{eng: eng, gov: governance.NewStore(eng)}
}

// Governance is the governance store the Store works through.
func (s *Store) Governance() *governance.Store { return s.gov }

func lit(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

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
				if col := rec.Column(c); !col.IsNull(i) {
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

func (s *Store) exec(ctx context.Context, q string, args ...any) error {
	return retryLocked(func() error { return s.eng.Exec(ctx, q, args...) })
}

func (s *Store) execTx(ctx context.Context, stmts ...string) error {
	return retryLocked(func() error { return s.eng.ExecTx(ctx, stmts...) })
}

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

// ---- model types ----

// TableDef is one table of a share.
type TableDef struct {
	Share, Alias, Relation string // Relation is schema.table in the lake
	PartitionFilter        string // extra predicate applied for every recipient
	History                bool   // keep versions: time travel and change data feed
}

// Schema is the schema the table is exposed under (the lake schema).
func (t TableDef) Schema() string { s, _, _ := strings.Cut(t.Relation, "."); return s }

// ShareInfo describes a share.
type ShareInfo struct {
	Name       string
	Tables     []TableDef
	Recipients []string
}

// RecipientInfo describes a recipient.
type RecipientInfo struct {
	Name     string
	Role     string
	Disabled bool
	Shares   []string
}

// ---- shares ----

// CreateShare adds an empty share.
func (s *Store) CreateShare(ctx context.Context, name string) error {
	if !nameRe.MatchString(name) {
		return fmt.Errorf("share name %q: lowercase letters, digits and _ (max 32, start with a letter)", name)
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_shares WHERE name = ?`, name); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("share %q already exists", name)
	}
	return s.exec(ctx, `INSERT INTO duct.duct_shares (name, created_at) VALUES (?, now())`, name)
}

func (s *Store) requireShare(ctx context.Context, name string) error {
	ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_shares WHERE name = ?`, name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("share %q does not exist (duct share create %s)", name, name)
	}
	return nil
}

func (s *Store) requireRecipient(ctx context.Context, name string) error {
	ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_recipients WHERE name = ?`, name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("recipient %q does not exist (duct recipient create %s)", name, name)
	}
	return nil
}

// Shares lists shares with their tables and recipients.
func (s *Store) Shares(ctx context.Context) ([]ShareInfo, error) {
	rs, err := s.rows(ctx, `SELECT name FROM duct.duct_shares ORDER BY name`)
	if err != nil {
		return nil, err
	}
	out := make([]ShareInfo, len(rs))
	idx := map[string]int{}
	for i, r := range rs {
		out[i].Name = r[0]
		idx[r[0]] = i
	}
	ts, err := s.rows(ctx, `SELECT share, alias, relation, partition_filter FROM duct.duct_share_tables ORDER BY share, alias`)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		if i, ok := idx[t[0]]; ok {
			out[i].Tables = append(out[i].Tables, TableDef{Share: t[0], Alias: t[1], Relation: t[2], PartitionFilter: t[3]})
		}
	}
	hs, err := s.rows(ctx, `SELECT share, alias FROM duct.duct_share_history`)
	if err != nil {
		return nil, err
	}
	for _, h := range hs {
		if i, ok := idx[h[0]]; ok {
			for j := range out[i].Tables {
				if out[i].Tables[j].Alias == h[1] {
					out[i].Tables[j].History = true
				}
			}
		}
	}
	gs, err := s.rows(ctx, `SELECT share, recipient FROM duct.duct_share_grants ORDER BY share, recipient`)
	if err != nil {
		return nil, err
	}
	for _, g := range gs {
		if i, ok := idx[g[0]]; ok {
			out[i].Recipients = append(out[i].Recipients, g[1])
		}
	}
	return out, nil
}

// Tables lists the tables of a share.
func (s *Store) Tables(ctx context.Context, share string) ([]TableDef, error) {
	rs, err := s.rows(ctx, `SELECT alias, relation, partition_filter FROM duct.duct_share_tables WHERE share = ? ORDER BY alias`, share)
	if err != nil {
		return nil, err
	}
	hs, err := s.rows(ctx, `SELECT alias FROM duct.duct_share_history WHERE share = ?`, share)
	if err != nil {
		return nil, err
	}
	hist := map[string]bool{}
	for _, h := range hs {
		hist[h[0]] = true
	}
	out := make([]TableDef, len(rs))
	for i, r := range rs {
		out[i] = TableDef{Share: share, Alias: r[0], Relation: r[1], PartitionFilter: r[2], History: hist[r[0]]}
	}
	return out, nil
}

// SetHistory turns the version history of a shared table on or off. With it
// on, every new version of each recipient's copy is kept (see Options.KeepVersions)
// and served by version, timestamp and change feed; changes made before it was
// turned on are not available. Turning it off deletes the retained versions
// from the state (their files go with the next `duct share gc`).
func (s *Store) SetHistory(ctx context.Context, share, alias string, on bool) error {
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return err
	}
	found := false
	for _, t := range ts {
		found = found || t.Alias == alias
	}
	if !found {
		return fmt.Errorf("share %s has no table %q", share, alias)
	}
	del := "DELETE FROM duct.duct_share_history WHERE share = " + lit(share) + " AND alias = " + lit(alias)
	if !on {
		return s.execTx(ctx, del, "DELETE FROM duct.duct_share_versions WHERE share = "+lit(share)+" AND alias = "+lit(alias))
	}
	return s.execTx(ctx, del, "INSERT INTO duct.duct_share_history (share, alias, enabled_at) VALUES ("+lit(share)+", "+lit(alias)+", now())")
}

// Table finds a table by the schema and name it is exposed under.
func (s *Store) Table(ctx context.Context, share, schema, alias string) (TableDef, bool, error) {
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return TableDef{}, false, err
	}
	for _, t := range ts {
		if t.Alias == alias && t.Schema() == schema {
			return t, true, nil
		}
	}
	return TableDef{}, false, nil
}

// AddTable adds schema.table to a share under alias (default: the table name).
// partitionFilter, if not empty, is a predicate over the table's columns
// applied to the copy of every recipient (validated like a row filter). Every
// recipient already granted the share gets SELECT on the table.
func (s *Store) AddTable(ctx context.Context, share, relation, alias, partitionFilter string) error {
	if err := s.requireShare(ctx, share); err != nil {
		return err
	}
	schema, table, err := governance.NormalizeRelation(relation)
	if err != nil {
		return err
	}
	rel := schema + "." + table
	if ok, err := s.exists(ctx, `SELECT 1 FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = ? AND table_name = ?`, schema, table); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s: relation does not exist in the lake", rel)
	}
	if alias == "" {
		alias = table
	}
	alias = strings.ToLower(alias)
	if !aliasRe.MatchString(alias) {
		return fmt.Errorf("alias %q: lowercase letters, digits and _ (start with a letter)", alias)
	}
	partitionFilter = strings.TrimSpace(partitionFilter)
	if partitionFilter != "" {
		if err := s.gov.ValidateRowFilter(ctx, rel, partitionFilter); err != nil {
			return fmt.Errorf("partition filter: %w", err)
		}
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_share_tables WHERE share = ? AND alias = ?`, share, alias); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("share %s already has a table named %q", share, alias)
	}
	// Two tables named alike would collide in the recipient's role views.
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return err
	}
	for _, t := range ts {
		if _, tn, _ := governance.NormalizeRelation(t.Relation); tn == table {
			return fmt.Errorf("share %s already has %s, whose view is also named %q", share, t.Relation, table)
		}
	}
	var pf any
	if partitionFilter != "" {
		pf = partitionFilter
	}
	rcps, err := s.recipientsOf(ctx, share)
	if err != nil {
		return err
	}
	for _, r := range rcps { // fail before storing anything
		if err := s.gov.Grant(ctx, RoleOf(r), rel); err != nil {
			return fmt.Errorf("recipient %s: %w", r, err)
		}
	}
	return s.exec(ctx, `INSERT INTO duct.duct_share_tables (share, alias, relation, partition_filter, added_at) VALUES (?, ?, ?, ?, now())`, share, alias, rel, pf)
}

// RemoveTable takes a table out of a share. It returns the copies (storage
// prefixes) to delete. Roles keep the table only if another share they are
// granted still carries it.
func (s *Store) RemoveTable(ctx context.Context, share, alias string) ([]string, error) {
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return nil, err
	}
	var def *TableDef
	for i := range ts {
		if ts[i].Alias == alias {
			def = &ts[i]
		}
	}
	if def == nil {
		return nil, fmt.Errorf("share %s has no table %q", share, alias)
	}
	rcps, err := s.recipientsOf(ctx, share)
	if err != nil {
		return nil, err
	}
	prefixes, err := s.copyPrefixes(ctx, `share = `+lit(share)+` AND alias = `+lit(alias))
	if err != nil {
		return nil, err
	}
	if err := s.execTx(ctx,
		"DELETE FROM duct.duct_share_tables WHERE share = "+lit(share)+" AND alias = "+lit(alias),
		"DELETE FROM duct.duct_share_history WHERE share = "+lit(share)+" AND alias = "+lit(alias),
		"DELETE FROM duct.duct_share_versions WHERE share = "+lit(share)+" AND alias = "+lit(alias),
		"DELETE FROM duct.duct_share_copies WHERE share = "+lit(share)+" AND alias = "+lit(alias)); err != nil {
		return nil, err
	}
	for _, r := range rcps {
		if err := s.revokeIfUnused(ctx, r, def.Relation); err != nil {
			return prefixes, err
		}
	}
	return prefixes, nil
}

// DropShare deletes a share, its grants and copies; it returns the copies'
// prefixes to delete from storage.
func (s *Store) DropShare(ctx context.Context, share string) ([]string, error) {
	if err := s.requireShare(ctx, share); err != nil {
		return nil, err
	}
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return nil, err
	}
	rcps, err := s.recipientsOf(ctx, share)
	if err != nil {
		return nil, err
	}
	prefixes, err := s.copyPrefixes(ctx, `share = `+lit(share))
	if err != nil {
		return nil, err
	}
	if err := s.execTx(ctx,
		"DELETE FROM duct.duct_share_copies WHERE share = "+lit(share),
		"DELETE FROM duct.duct_share_versions WHERE share = "+lit(share),
		"DELETE FROM duct.duct_share_history WHERE share = "+lit(share),
		"DELETE FROM duct.duct_share_grants WHERE share = "+lit(share),
		"DELETE FROM duct.duct_share_tables WHERE share = "+lit(share),
		"DELETE FROM duct.duct_shares WHERE name = "+lit(share)); err != nil {
		return nil, err
	}
	for _, r := range rcps {
		for _, t := range ts {
			if err := s.revokeIfUnused(ctx, r, t.Relation); err != nil {
				return prefixes, err
			}
		}
	}
	return prefixes, nil
}

func (s *Store) recipientsOf(ctx context.Context, share string) ([]string, error) {
	rs, err := s.rows(ctx, `SELECT recipient FROM duct.duct_share_grants WHERE share = ? ORDER BY recipient`, share)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r[0]
	}
	return out, nil
}

// revokeIfUnused removes the role's SELECT on a relation (with its policies)
// unless another share granted to the recipient still carries it.
func (s *Store) revokeIfUnused(ctx context.Context, recipient, relation string) error {
	used, err := s.exists(ctx, `SELECT 1 FROM duct.duct_share_grants g JOIN duct.duct_share_tables t ON t.share = g.share
		WHERE g.recipient = ? AND t.relation = ?`, recipient, relation)
	if err != nil || used {
		return err
	}
	return s.gov.Revoke(ctx, RoleOf(recipient), relation)
}

func (s *Store) copyPrefixes(ctx context.Context, where string) ([]string, error) {
	rs, err := s.rows(ctx, `SELECT prefix FROM duct.duct_share_copies WHERE `+where)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r[0]
	}
	return out, nil
}

// ---- recipients ----

// Issued is a new bearer token.
type Issued struct {
	Token     string
	TokenID   string
	ExpiresAt time.Time // zero: never
}

// CreateRecipient creates the recipient's role, user and first token. ttl <= 0
// means the token does not expire.
func (s *Store) CreateRecipient(ctx context.Context, name string, ttl time.Duration) (Issued, error) {
	if !nameRe.MatchString(name) {
		return Issued{}, fmt.Errorf("recipient name %q: lowercase letters, digits and _ (max 32, start with a letter)", name)
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_recipients WHERE name = ?`, name); err != nil {
		return Issued{}, err
	} else if ok {
		return Issued{}, fmt.Errorf("recipient %q already exists", name)
	}
	role := RoleOf(name)
	if err := s.gov.CreateRole(ctx, role); err != nil {
		return Issued{}, err
	}
	// The password is random and thrown away: a recipient only has its token.
	pw := make([]byte, 32)
	if _, err := rand.Read(pw); err != nil {
		return Issued{}, err
	}
	if err := s.gov.CreateUser(ctx, role, role, hex.EncodeToString(pw)); err != nil {
		_ = s.gov.DropRole(ctx, role)
		return Issued{}, err
	}
	if err := s.exec(ctx, `INSERT INTO duct.duct_recipients (name, role, created_at) VALUES (?, ?, now())`, name, role); err != nil {
		_ = s.gov.DropUser(ctx, role)
		_ = s.gov.DropRole(ctx, role)
		return Issued{}, err
	}
	return s.issue(ctx, name, ttl)
}

func (s *Store) issue(ctx context.Context, name string, ttl time.Duration) (Issued, error) {
	tok, id, err := s.gov.CreateToken(ctx, RoleOf(name), "sharing", ttl)
	if err != nil {
		return Issued{}, err
	}
	is := Issued{Token: tok, TokenID: id}
	if ttl > 0 {
		is.ExpiresAt = time.Now().Add(ttl).UTC().Truncate(time.Second)
	}
	return is, nil
}

// NewToken issues another token for a recipient and re-enables it. With
// rotate the previous tokens are revoked first.
func (s *Store) NewToken(ctx context.Context, name string, ttl time.Duration, rotate bool) (Issued, error) {
	if err := s.requireRecipient(ctx, name); err != nil {
		return Issued{}, err
	}
	if rotate {
		if err := s.revokeTokens(ctx, name); err != nil {
			return Issued{}, err
		}
	}
	if err := s.gov.SetDisabled(ctx, RoleOf(name), false); err != nil {
		return Issued{}, err
	}
	return s.issue(ctx, name, ttl)
}

func (s *Store) revokeTokens(ctx context.Context, name string) error {
	return s.exec(ctx, `UPDATE duct.duct_tokens SET revoked = true WHERE user_name = ?`, RoleOf(name))
}

// RevokeRecipient cuts a recipient off: its tokens are revoked and its user
// disabled (URLs already issued still work until they expire, minutes).
func (s *Store) RevokeRecipient(ctx context.Context, name string) error {
	if err := s.requireRecipient(ctx, name); err != nil {
		return err
	}
	if err := s.revokeTokens(ctx, name); err != nil {
		return err
	}
	return s.gov.SetDisabled(ctx, RoleOf(name), true)
}

// DropRecipient removes a recipient completely and returns the prefixes of its
// copies. Run governance Regenerate afterwards.
func (s *Store) DropRecipient(ctx context.Context, name string) ([]string, error) {
	if err := s.requireRecipient(ctx, name); err != nil {
		return nil, err
	}
	prefixes, err := s.copyPrefixes(ctx, `recipient = `+lit(name))
	if err != nil {
		return nil, err
	}
	if err := s.execTx(ctx,
		"DELETE FROM duct.duct_share_copies WHERE recipient = "+lit(name),
		"DELETE FROM duct.duct_share_versions WHERE recipient = "+lit(name),
		"DELETE FROM duct.duct_share_grants WHERE recipient = "+lit(name),
		"DELETE FROM duct.duct_recipients WHERE name = "+lit(name)); err != nil {
		return nil, err
	}
	role := RoleOf(name)
	if err := s.gov.DropUser(ctx, role); err != nil {
		return prefixes, err
	}
	return prefixes, s.gov.DropRole(ctx, role)
}

// Recipients lists recipients with the shares granted to each.
func (s *Store) Recipients(ctx context.Context) ([]RecipientInfo, error) {
	rs, err := s.rows(ctx, `SELECT r.name, r.role, CAST(u.disabled AS VARCHAR) FROM duct.duct_recipients r
		LEFT JOIN duct.duct_users u ON u.name = r.role ORDER BY r.name`)
	if err != nil {
		return nil, err
	}
	out := make([]RecipientInfo, len(rs))
	idx := map[string]int{}
	for i, r := range rs {
		out[i] = RecipientInfo{Name: r[0], Role: r[1], Disabled: r[2] == "1" || strings.EqualFold(r[2], "true")}
		idx[r[0]] = i
	}
	gs, err := s.rows(ctx, `SELECT recipient, share FROM duct.duct_share_grants ORDER BY recipient, share`)
	if err != nil {
		return nil, err
	}
	for _, g := range gs {
		if i, ok := idx[g[0]]; ok {
			out[i].Shares = append(out[i].Shares, g[1])
		}
	}
	return out, nil
}

// IsRecipient reports whether name is a recipient.
func (s *Store) IsRecipient(ctx context.Context, name string) (bool, error) {
	return s.exists(ctx, `SELECT 1 FROM duct.duct_recipients WHERE name = ?`, name)
}

// ---- grants ----

// GrantOptions are the policies of a grant. They go on the recipient's role,
// per relation, as governance row filters and masks (so they are shared by
// every share of that recipient that carries the table).
type GrantOptions struct {
	Table     string            // alias of the table the policies apply to
	RowFilter string            // predicate over the table's columns
	Masks     map[string]string // column -> expression
}

// Grant gives a recipient a share: SELECT on each of its tables for the
// recipient's role, plus the policies in opt. Run governance Regenerate
// afterwards to build the role views.
func (s *Store) Grant(ctx context.Context, share, recipient string, opt GrantOptions) error {
	if err := s.requireShare(ctx, share); err != nil {
		return err
	}
	if err := s.requireRecipient(ctx, recipient); err != nil {
		return err
	}
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return err
	}
	if len(ts) == 0 {
		return fmt.Errorf("share %s has no tables (duct share add %s schema.table)", share, share)
	}
	hasPolicy := opt.RowFilter != "" || len(opt.Masks) > 0
	var target *TableDef
	if hasPolicy {
		switch {
		case opt.Table != "":
			for i := range ts {
				if ts[i].Alias == strings.ToLower(opt.Table) {
					target = &ts[i]
				}
			}
			if target == nil {
				return fmt.Errorf("share %s has no table %q", share, opt.Table)
			}
		case len(ts) == 1:
			target = &ts[0]
		default:
			return fmt.Errorf("share %s has several tables: say which one the policy is for with --table", share)
		}
	} else if opt.Table != "" {
		return errors.New("--table is only for --row-filter and --mask")
	}
	role := RoleOf(recipient)
	for _, t := range ts {
		if err := s.gov.Grant(ctx, role, t.Relation); err != nil {
			return err
		}
	}
	if hasPolicy {
		if opt.RowFilter != "" {
			if err := s.gov.SetRowFilter(ctx, role, target.Relation, opt.RowFilter); err != nil {
				return err
			}
		}
		cols := make([]string, 0, len(opt.Masks))
		for c := range opt.Masks {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		for _, c := range cols {
			if err := s.gov.SetMask(ctx, role, target.Relation, c, opt.Masks[c]); err != nil {
				return err
			}
		}
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_share_grants WHERE share = ? AND recipient = ?`, share, recipient); err != nil || ok {
		return err
	}
	return s.exec(ctx, `INSERT INTO duct.duct_share_grants (share, recipient, granted_at) VALUES (?, ?, now())`, share, recipient)
}

// Revoke takes a share away from a recipient and returns its copies'
// prefixes. Run governance Regenerate afterwards.
func (s *Store) Revoke(ctx context.Context, share, recipient string) ([]string, error) {
	if err := s.requireShare(ctx, share); err != nil {
		return nil, err
	}
	if err := s.requireRecipient(ctx, recipient); err != nil {
		return nil, err
	}
	if ok, err := s.exists(ctx, `SELECT 1 FROM duct.duct_share_grants WHERE share = ? AND recipient = ?`, share, recipient); err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("share %s is not granted to %s", share, recipient)
	}
	ts, err := s.Tables(ctx, share)
	if err != nil {
		return nil, err
	}
	prefixes, err := s.copyPrefixes(ctx, `share = `+lit(share)+` AND recipient = `+lit(recipient))
	if err != nil {
		return nil, err
	}
	if err := s.execTx(ctx,
		"DELETE FROM duct.duct_share_copies WHERE share = "+lit(share)+" AND recipient = "+lit(recipient),
		"DELETE FROM duct.duct_share_versions WHERE share = "+lit(share)+" AND recipient = "+lit(recipient),
		"DELETE FROM duct.duct_share_grants WHERE share = "+lit(share)+" AND recipient = "+lit(recipient)); err != nil {
		return nil, err
	}
	for _, t := range ts {
		if err := s.revokeIfUnused(ctx, recipient, t.Relation); err != nil {
			return prefixes, err
		}
	}
	return prefixes, nil
}

// Granted lists the shares a recipient may read, sorted.
func (s *Store) Granted(ctx context.Context, recipient string) ([]string, error) {
	rs, err := s.rows(ctx, `SELECT share FROM duct.duct_share_grants WHERE recipient = ? ORDER BY share`, recipient)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r[0]
	}
	return out, nil
}

// IsGranted reports whether the share is granted to the recipient.
func (s *Store) IsGranted(ctx context.Context, share, recipient string) (bool, error) {
	return s.exists(ctx, `SELECT 1 FROM duct.duct_share_grants WHERE share = ? AND recipient = ?`, share, recipient)
}

// Policy summarises what a recipient sees of one relation (for listings).
func (s *Store) Policy(ctx context.Context, recipient, relation string) (rowFilter string, masks []string, err error) {
	role := RoleOf(recipient)
	roles, err := s.gov.Roles(ctx)
	if err != nil {
		return "", nil, err
	}
	for _, r := range roles {
		if r.Name != role {
			continue
		}
		rowFilter = r.Filters[relation]
		for k, e := range r.Masks {
			if c, ok := strings.CutPrefix(k, relation+"."); ok {
				masks = append(masks, c+" = "+e)
			}
		}
	}
	sort.Strings(masks)
	return rowFilter, masks, nil
}

// policyOf returns what the recipient's role applies to a relation: the row
// filter and the masks (column, lowercase, to expression).
func (s *Store) policyOf(ctx context.Context, recipient, relation string) (filter string, masks map[string]string, err error) {
	role := RoleOf(recipient)
	roles, err := s.gov.Roles(ctx)
	if err != nil {
		return "", nil, err
	}
	masks = map[string]string{}
	for _, r := range roles {
		if r.Name != role {
			continue
		}
		filter = r.Filters[relation]
		for k, e := range r.Masks {
			if c, ok := strings.CutPrefix(k, relation+"."); ok {
				masks[strings.ToLower(c)] = e
			}
		}
	}
	return filter, masks, nil
}
