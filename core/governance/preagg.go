package governance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/semantic"
	"github.com/alonsoburon/duct/core/txretry"
)

// Semantic preaggregates (ADR 0011).
//
// Safety model, in one paragraph: a preaggregate is built PER ROLE, from that
// role's own views (role_<role>), so row filters and masks are applied to the
// data before it is aggregated; a role never reads a table built for another
// role. The table lives in the lake schema semantic_cache, which no guard path
// opens: only SQL compiled by the semantic layer reads it (compiledByUs), and
// the compiler routes to it only when the role's table is *fresh*: built from
// the current model definition, under the current definitions of the role's
// views, and with no data or policy change in the lake since the snapshot it
// was built from. A policy change rewrites the views (Regenerate), which is a
// lake snapshot, so a filtered role can never be served aggregates computed
// before its filter existed: they go stale at once and queries fall back to the
// views until the next refresh.

const (
	cacheAuthor  = "duct"
	cacheMessage = "semantic_cache"
	// preaggTTL bounds how long a freshness verdict is reused for the same lake
	// snapshot (a refresh by another process does not change the snapshot id).
	preaggTTL = 5 * time.Second
)

type compiledKey struct{}

// withCompiled marks ctx as running SQL produced by the semantic compiler.
func withCompiled(ctx context.Context) context.Context {
	return context.WithValue(ctx, compiledKey{}, true)
}

func compiledByUs(ctx context.Context) bool {
	v, _ := ctx.Value(compiledKey{}).(bool)
	return v
}

type freshEntry struct {
	snapshot string
	model    *semantic.Model
	at       time.Time
	tables   map[string]string
}

type freshCache struct {
	mu sync.Mutex
	m  map[string]freshEntry
}

func sha(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// viewsHash fingerprints the definitions of the role's views as they are in
// the lake now: what a preaggregate built from them was filtered/masked by.
func viewsHash(ctx context.Context, s *Store, role string) (string, error) {
	rows, err := s.rows(ctx, `SELECT view_name, sql FROM duckdb_views() WHERE database_name = 'lake' AND schema_name = `+
		lit(contract.RoleSchema(role))+` AND NOT internal ORDER BY view_name`)
	if err != nil {
		return "", err
	}
	var parts []string
	for _, r := range rows {
		parts = append(parts, r[0], r[1])
	}
	return sha(parts...), nil
}

func lakeSnapshotID(ctx context.Context, s *Store) (int64, error) {
	r, err := s.rows(ctx, "SELECT CAST(COALESCE(max(snapshot_id), 0) AS BIGINT) FROM lake.snapshots()")
	if err != nil || len(r) == 0 {
		return 0, err
	}
	var n int64
	_, err = fmt.Sscan(r[0][0], &n)
	return n, err
}

// freshPreaggs lists the preaggregates that may serve the role right now
// (name -> table in semantic_cache). On any doubt it returns fewer, never more.
func (h *Handler) freshPreaggs(ctx context.Context, m *semantic.Model, sc semantic.Scope) map[string]string {
	if len(m.Preaggs()) == 0 {
		return nil
	}
	snap, err := h.LakeSnapshot(ctx)
	if err != nil {
		return nil
	}
	h.fresh.mu.Lock()
	if e, ok := h.fresh.m[sc.Role]; ok && e.snapshot == snap && e.model == m && time.Since(e.at) < preaggTTL {
		h.fresh.mu.Unlock()
		return e.tables
	}
	h.fresh.mu.Unlock()
	tables := h.computeFresh(ctx, m, sc)
	h.fresh.mu.Lock()
	if h.fresh.m == nil {
		h.fresh.m = map[string]freshEntry{}
	}
	h.fresh.m[sc.Role] = freshEntry{snapshot: snap, model: m, at: time.Now(), tables: tables}
	h.fresh.mu.Unlock()
	return tables
}

func (h *Handler) computeFresh(ctx context.Context, m *semantic.Model, sc semantic.Scope) map[string]string {
	s := NewStore(h.eng)
	rows, err := s.rows(ctx, `SELECT preagg, table_name, definition_hash, views_hash, CAST(source_snapshot AS BIGINT) FROM duct.duct_semantic_preagg WHERE role = ?`, sc.Role)
	if err != nil || len(rows) == 0 {
		return nil
	}
	vh, err := viewsHash(ctx, s, sc.Role)
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, r := range rows {
		p, ok := m.Preagg(r[0])
		if !ok || r[1] != semantic.PreaggTable(sc.Role, p.Name) {
			continue
		}
		sql, err := m.PreaggSQL(sc, p)
		if err != nil || sha(sql) != r[2] || r[3] != vh {
			continue
		}
		var src int64
		if _, err := fmt.Sscan(r[4], &src); err != nil {
			continue
		}
		if ok, err := untouchedSince(ctx, s, src); err != nil || !ok {
			continue
		}
		out[p.Name] = r[1]
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// PreaggOptions select what RefreshPreaggs does.
type PreaggOptions struct {
	Role   string // only this role ("" = every role)
	Force  bool   // rebuild even if fresh
	Manual bool   // also the preaggregates declared refresh: manual
}

// PreaggReport is the outcome of RefreshPreaggs.
type PreaggReport struct {
	Built   int
	Current int // already fresh
	Skipped int // the role cannot read the datasets
	Dropped int // tables of roles/preaggregates that no longer exist
	Notes   []string
}

// RefreshPreaggs (re)builds the preaggregates of m for the roles of the
// project. It needs an engine that may write the lake (not the locked one).
// Safe to run from several processes: the table is replaced in one lake
// transaction and its record upserted afterwards.
func RefreshPreaggs(ctx context.Context, eng contract.Engine, m *semantic.Model, opt PreaggOptions) (PreaggReport, error) {
	var rep PreaggReport
	s := NewStore(eng)
	if m == nil {
		m = &semantic.Model{}
	}
	roles, err := s.rows(ctx, `SELECT name FROM duct.duct_roles ORDER BY name`)
	if err != nil {
		return rep, err
	}
	want := map[string]bool{} // table names that should exist
	for _, r := range roles {
		role := r[0]
		scope, err := roleScope(ctx, s, role)
		if err != nil {
			return rep, err
		}
		for _, p := range m.Preaggs() {
			if p.Refresh == semantic.RefreshManual && !opt.Manual {
				want[semantic.PreaggTable(role, p.Name)] = true // keep what exists
				continue
			}
			if opt.Role != "" && opt.Role != role {
				want[semantic.PreaggTable(role, p.Name)] = true
				continue
			}
			sql, err := m.PreaggSQL(scope, p)
			if err != nil {
				rep.Skipped++
				rep.Notes = append(rep.Notes, fmt.Sprintf("%s/%s: %v", role, p.Name, err))
				continue
			}
			table := semantic.PreaggTable(role, p.Name)
			want[table] = true
			if !opt.Force {
				if cur, err := isCurrent(ctx, s, role, p.Name, table, sql); err == nil && cur {
					rep.Current++
					continue
				}
			}
			if err := buildPreagg(ctx, eng, s, role, p.Name, table, sql); err != nil {
				return rep, fmt.Errorf("preaggregate %s for role %s: %w", p.Name, role, err)
			}
			rep.Built++
		}
	}
	// Drop what the model or the roles no longer define (only with the whole
	// picture: a single-role run leaves the others alone).
	if opt.Role == "" {
		have, err := s.rows(ctx, `SELECT role, preagg, table_name FROM duct.duct_semantic_preagg`)
		if err != nil {
			return rep, err
		}
		for _, h := range have {
			if want[h[2]] {
				continue
			}
			if err := dropPreagg(ctx, eng, h[0], h[1], h[2]); err != nil {
				return rep, err
			}
			rep.Dropped++
		}
		{
			// Tables without a record (an interrupted build) are removed too.
			tabs, err := s.rows(ctx, `SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = `+lit(semantic.PreaggSchema))
			if err == nil {
				for _, t := range tabs {
					if !want[t[0]] {
						if err := eng.ExecTx(ctx,
							fmt.Sprintf("CALL %s.set_commit_message(%s, %s)", ident(lakeCatalog), lit(cacheAuthor), lit(cacheMessage)),
							"DROP TABLE IF EXISTS "+ident(lakeCatalog)+"."+ident(semantic.PreaggSchema)+"."+ident(t[0])); err != nil {
							return rep, err
						}
						rep.Dropped++
					}
				}
			}
		}
	}
	sort.Strings(rep.Notes)
	return rep, nil
}

func roleScope(ctx context.Context, s *Store, role string) (semantic.Scope, error) {
	rows, err := s.rows(ctx, `SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake' AND table_schema = `+lit(contract.RoleSchema(role)))
	if err != nil {
		return semantic.Scope{}, err
	}
	sc := semantic.Scope{Role: role, Tables: map[string]bool{}}
	for _, r := range rows {
		sc.Tables[strings.ToLower(r[0])] = true
	}
	return sc, nil
}

// isCurrent: recorded, same definition, same views, nothing but our own builds
// in the lake since its source snapshot.
func isCurrent(ctx context.Context, s *Store, role, name, table, sql string) (bool, error) {
	rows, err := s.rows(ctx, `SELECT definition_hash, views_hash, CAST(source_snapshot AS BIGINT) FROM duct.duct_semantic_preagg WHERE role = ? AND preagg = ?`, role, name)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	vh, err := viewsHash(ctx, s, role)
	if err != nil || rows[0][0] != sha(sql) || rows[0][1] != vh {
		return false, err
	}
	ok, err := s.rows(ctx, `SELECT count(*) FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = `+lit(semantic.PreaggSchema)+` AND table_name = `+lit(table))
	if err != nil || len(ok) == 0 || ok[0][0] != "1" {
		return false, err
	}
	var src int64
	if _, err := fmt.Sscan(rows[0][2], &src); err != nil {
		return false, err
	}
	return untouchedSince(ctx, s, src)
}

// untouchedSince: the lake has had no change since snapshot src other than our
// own preaggregate builds. src must still exist: an expired snapshot (duct gc)
// would hide the changes that came right after it.
func untouchedSince(ctx context.Context, s *Store, src int64) (bool, error) {
	n, err := s.rows(ctx, `SELECT (SELECT count(*) FROM lake.snapshots() WHERE snapshot_id = ?), `+
		`(SELECT count(*) FROM lake.snapshots() WHERE snapshot_id > ? AND NOT (COALESCE(author, '') = `+lit(cacheAuthor)+
		` AND COALESCE(commit_message, '') = `+lit(cacheMessage)+`))`, src, src)
	if err != nil || len(n) == 0 {
		return false, err
	}
	return n[0][0] == "1" && n[0][1] == "0", nil
}

func buildPreagg(ctx context.Context, eng contract.Engine, s *Store, role, name, table, sql string) error {
	return txretry.Do(ctx, txretry.Default, func(ctx context.Context) error {
		src, err := lakeSnapshotID(ctx, s)
		if err != nil {
			return err
		}
		vh, err := viewsHash(ctx, s, role)
		if err != nil {
			return err
		}
		target := ident(lakeCatalog) + "." + ident(semantic.PreaggSchema) + "." + ident(table)
		if err := eng.ExecTx(ctx,
			fmt.Sprintf("CALL %s.set_commit_message(%s, %s)", ident(lakeCatalog), lit(cacheAuthor), lit(cacheMessage)),
			"CREATE SCHEMA IF NOT EXISTS "+ident(lakeCatalog)+"."+ident(semantic.PreaggSchema),
			"CREATE OR REPLACE TABLE "+target+" AS "+sql,
		); err != nil {
			return err
		}
		// The views must not have changed while we read them.
		if vh2, err := viewsHash(ctx, s, role); err != nil || vh2 != vh {
			_ = eng.Exec(ctx, "DROP TABLE IF EXISTS "+target)
			if err != nil {
				return err
			}
			return fmt.Errorf("transaction conflict: the views of role %s changed while building", role)
		}
		n, err := s.rows(ctx, "SELECT count(*) FROM "+target)
		if err != nil || len(n) == 0 {
			return err
		}
		return eng.ExecTx(ctx,
			`DELETE FROM duct.duct_semantic_preagg WHERE role = `+lit(role)+` AND preagg = `+lit(name),
			fmt.Sprintf(`INSERT INTO duct.duct_semantic_preagg (role, preagg, table_name, definition_hash, views_hash, source_snapshot, num_rows, built_at)
				VALUES (%s, %s, %s, %s, %s, %d, %s, make_timestamp(%d))`,
				lit(role), lit(name), lit(table), lit(sha(sql)), lit(vh), src, n[0][0], time.Now().UnixMicro()))
	})
}

func dropPreagg(ctx context.Context, eng contract.Engine, role, name, table string) error {
	// Two transactions: one may not write to the lake and to the Duct state.
	if err := eng.ExecTx(ctx,
		fmt.Sprintf("CALL %s.set_commit_message(%s, %s)", ident(lakeCatalog), lit(cacheAuthor), lit(cacheMessage)),
		"DROP TABLE IF EXISTS "+ident(lakeCatalog)+"."+ident(semantic.PreaggSchema)+"."+ident(table),
	); err != nil {
		return err
	}
	return eng.Exec(ctx, `DELETE FROM duct.duct_semantic_preagg WHERE role = `+lit(role)+` AND preagg = `+lit(name))
}

// PreaggStatus lists what is built (for `duct semantic status`).
type PreaggStatus struct {
	Role, Name, Table string
	Rows              int64
	BuiltAt           time.Time
	Fresh             bool
}

// PreaggStatuses reports every recorded preaggregate and whether it would serve now.
func PreaggStatuses(ctx context.Context, eng contract.Engine, m *semantic.Model) ([]PreaggStatus, error) {
	s := NewStore(eng)
	rows, err := s.rows(ctx, `SELECT role, preagg, table_name, CAST(num_rows AS BIGINT), CAST(epoch_us(CAST(built_at AS TIMESTAMP)) AS BIGINT) FROM duct.duct_semantic_preagg ORDER BY role, preagg`)
	if err != nil {
		return nil, err
	}
	var out []PreaggStatus
	for _, r := range rows {
		st := PreaggStatus{Role: r[0], Name: r[1], Table: r[2]}
		var us int64
		_, _ = fmt.Sscan(r[3], &st.Rows)
		_, _ = fmt.Sscan(r[4], &us)
		st.BuiltAt = time.UnixMicro(us).UTC()
		if m != nil {
			if p, ok := m.Preagg(st.Name); ok {
				if sc, err := roleScope(ctx, s, st.Role); err == nil {
					if sql, err := m.PreaggSQL(sc, p); err == nil {
						st.Fresh, _ = isCurrent(ctx, s, st.Role, st.Name, st.Table, sql)
					}
				}
			}
		}
		out = append(out, st)
	}
	return out, nil
}
