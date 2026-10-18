package sharing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// DefaultKeepVersions is how many versions of a copy are retained when the
// table has history (Options.KeepVersions).
const DefaultKeepVersions = 10

// KeepVersionsFrom reads `keep_versions` of the [serve.sharing] table (0, if
// absent: the default). A value that is not a positive integer is an error.
func KeepVersionsFrom(cfg map[string]any) (int, error) {
	var n int64
	switch v := cfg["keep_versions"].(type) {
	case nil:
		return 0, nil
	case int:
		n = int64(v)
	case int64:
		n = v
	case float64:
		n = int64(v)
	default:
		return 0, errors.New("sharing: keep_versions must be an integer")
	}
	if n < 1 || n > 100000 {
		return 0, fmt.Errorf("sharing: keep_versions %d: use a number of versions from 1", n)
	}
	return int(n), nil
}

// Kinds of a version (what its change feed is).
const (
	// KindInitial: the first version of a copy; its change feed is "every row
	// was inserted" and is served as its own files.
	KindInitial = "initial"
	// KindDiff: the version has change files (the net change since the
	// previous version, as the recipient sees it).
	KindDiff = "diff"
	// KindBase: history was turned on while this version was current; there
	// is no feed for it, only for later versions.
	KindBase = "base"
	// KindGap: the version has no feed, because the recipient's policy or the
	// schema changed, or the feed could not be derived safely.
	KindGap = "gap"
)

// Unavailable is a request the history cannot satisfy (a version that is not
// retained, a table without history, a version with no change feed). The
// server answers it with 400.
type Unavailable struct{ Msg string }

func (e *Unavailable) Error() string { return e.Msg }

func unavailable(format string, a ...any) error { return &Unavailable{fmt.Sprintf(format, a...)} }

// Version is one retained version of a copy.
type Version struct {
	Version        int64
	SourceSnapshot int64 // lake snapshot the version was materialized at
	TimestampMS    int64 // instant of that snapshot, ms since the epoch
	Kind           string
	PolicyHash     string // recipient policy and schema the version was built under
	SchemaString   string
	Files          []CopyFile // the table as of this version
	CDF            []CopyFile // the change feed from the previous version (KindDiff)
	NumRows        int64
	TotalBytes     int64
}

const versionCols = `CAST(version AS VARCHAR), CAST(source_snapshot AS VARCHAR), CAST(ts_ms AS VARCHAR), kind, policy_hash, schema_string,
	files, cdf_files, CAST(num_rows AS VARCHAR), CAST(total_bytes AS VARCHAR)`

func parseVersion(r []string) (Version, error) {
	v := Version{Kind: r[3], PolicyHash: r[4], SchemaString: r[5]}
	_, _ = fmt.Sscan(r[0], &v.Version)
	_, _ = fmt.Sscan(r[1], &v.SourceSnapshot)
	_, _ = fmt.Sscan(r[2], &v.TimestampMS)
	_, _ = fmt.Sscan(r[8], &v.NumRows)
	_, _ = fmt.Sscan(r[9], &v.TotalBytes)
	if err := json.Unmarshal([]byte(r[6]), &v.Files); err != nil {
		return v, fmt.Errorf("sharing: corrupt file list of version %d: %w", v.Version, err)
	}
	if err := json.Unmarshal([]byte(r[7]), &v.CDF); err != nil {
		return v, fmt.Errorf("sharing: corrupt change list of version %d: %w", v.Version, err)
	}
	return v, nil
}

// loadVersions returns the retained versions, oldest first.
func (s *Service) loadVersions(ctx context.Context, share, recipient, alias string) ([]Version, error) {
	rs, err := s.Store.rows(ctx, `SELECT `+versionCols+` FROM duct.duct_share_versions WHERE share = ? AND recipient = ? AND alias = ? ORDER BY version`, share, recipient, alias)
	if err != nil {
		return nil, err
	}
	out := make([]Version, len(rs))
	for i, r := range rs {
		if out[i], err = parseVersion(r); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Service) versionRow(ctx context.Context, share, recipient, alias string, version int64) (*Version, error) {
	rs, err := s.Store.rows(ctx, `SELECT `+versionCols+` FROM duct.duct_share_versions WHERE share = ? AND recipient = ? AND alias = ? AND version = ?`, share, recipient, alias, version)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	v, err := parseVersion(rs[0])
	return &v, err
}

func (s *Service) saveVersion(ctx context.Context, share, recipient, alias string, v Version) error {
	files, _ := json.Marshal(v.Files)
	cdf, _ := json.Marshal(v.CDF)
	where := fmt.Sprintf("share = %s AND recipient = %s AND alias = %s AND version = %d", lit(share), lit(recipient), lit(alias), v.Version)
	ins := fmt.Sprintf(`INSERT INTO duct.duct_share_versions (share, recipient, alias, version, source_snapshot, ts_ms, kind, policy_hash, schema_string, files, cdf_files, num_rows, total_bytes)
		VALUES (%s, %s, %s, %d, %d, %d, %s, %s, %s, %s, %s, %d, %d)`,
		lit(share), lit(recipient), lit(alias), v.Version, v.SourceSnapshot, v.TimestampMS, lit(v.Kind), lit(v.PolicyHash), lit(v.SchemaString),
		lit(string(files)), lit(string(cdf)), v.NumRows, v.TotalBytes)
	return s.Store.execTx(ctx, "DELETE FROM duct.duct_share_versions WHERE "+where, ins)
}

// keep is how many versions are retained.
func (s *Service) keep() int64 {
	if s.opt.KeepVersions > 0 {
		return int64(s.opt.KeepVersions)
	}
	return DefaultKeepVersions
}

// prune forgets the versions beyond the retention; their files are swept once
// the grace period has passed.
func (s *Service) prune(ctx context.Context, share, recipient, alias string, newest int64) error {
	return s.Store.exec(ctx, fmt.Sprintf(`DELETE FROM duct.duct_share_versions WHERE share = %s AND recipient = %s AND alias = %s AND version <= %d`,
		lit(share), lit(recipient), lit(alias), newest-s.keep()))
}

// snapshotTime is the instant of a lake snapshot in ms since the epoch.
func (s *Service) snapshotTime(ctx context.Context, snap int64) int64 {
	rs, err := s.Store.rows(ctx, fmt.Sprintf(`SELECT CAST(epoch_ms(snapshot_time) AS VARCHAR) FROM lake.snapshots() WHERE snapshot_id = %d`, snap))
	if err == nil && len(rs) == 1 {
		var ms int64
		if _, err := fmt.Sscan(rs[0][0], &ms); err == nil {
			return ms
		}
	}
	return time.Now().UnixMilli()
}

// policyHash identifies what the recipient's role applies to the table (row
// filter, masks, the share's fixed filter) and the schema it produces. Two
// versions with the same hash differ only by data, so the feed between them can
// be derived from the base table's changes.
func (s *Service) policyHash(ctx context.Context, recipient string, def TableDef, schema string) (string, error) {
	filter, masks, err := s.Store.policyOf(ctx, recipient, def.Relation)
	if err != nil {
		return "", err
	}
	keys := make([]string, 0, len(masks))
	for k := range masks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s", filter, def.PartitionFilter, schema)
	for _, k := range keys {
		_, _ = fmt.Fprintf(h, "\x00%s=%s", k, masks[k])
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// currentVersion makes sure a history table has the row of the current copy.
// It runs only when the copy has just been verified against the lake (so what
// it holds is what the recipient sees now): that happens when history is turned
// on while a copy exists.
func (s *Service) currentVersion(ctx context.Context, recipient string, def TableDef, c *Copy) error {
	v, err := s.versionRow(ctx, def.Share, recipient, def.Alias, c.Version)
	if err != nil {
		return err
	}
	if v == nil {
		mu := s.lock(def.Share + "/" + recipient + "/" + def.Alias)
		mu.Lock()
		defer mu.Unlock()
		if v, err = s.versionRow(ctx, def.Share, recipient, def.Alias, c.Version); err != nil {
			return err
		}
	}
	if v == nil {
		h, err := s.policyHash(ctx, recipient, def, c.SchemaString)
		if err != nil {
			return err
		}
		nv := Version{Version: c.Version, SourceSnapshot: c.SourceSnapshot, TimestampMS: s.snapshotTime(ctx, c.SourceSnapshot), Kind: KindBase,
			PolicyHash: h, SchemaString: c.SchemaString, Files: c.Files, NumRows: c.NumRows, TotalBytes: c.TotalBytes}
		if err := s.saveVersion(ctx, def.Share, recipient, def.Alias, nv); err != nil {
			return err
		}
		v = &nv
	}
	c.TimestampMS = v.TimestampMS
	return nil
}

// ---- the change feed ----

// reserved are the names the change query uses for itself.
var reserved = map[string]bool{
	"_s": true, "_r": true, "_t": true, "_rf": true, "_in": true, "_h": true, "_k": true, "_n1": true, "_n2": true,
	"_change_type": true, "snapshot_id": true, "rowid": true, "change_type": true,
}

// changesSQL builds the query of the recipient's change feed between two lake
// snapshots (inclusive), net per row: the first and the last state of each
// row of the range are compared, so a row changed several times is one change.
// The recipient's row filter and masks are applied to both states, exactly as
// the role view applies them, which gives the semantics:
//
//   - a row that enters the recipient's view (inserted, or updated into the
//     filter) is an insert;
//   - a row that leaves it (deleted, or updated out of the filter) is a delete,
//     with the values the recipient last saw;
//   - a row inside the view before and after is an update (preimage and
//     postimage), unless its visible values are equal (a change in a masked
//     column is not reported);
//   - a row never inside the view does not appear.
//
// filter is the role's row filter, masks maps lowercase column names to
// expressions and partFilter is the share's fixed filter, which sees the masked
// values (as it does on the view).
func changesSQL(def TableDef, cols []column, filter string, masks map[string]string, from, to int64) (string, error) {
	schema, table, _ := strings.Cut(def.Relation, ".")
	for _, c := range cols {
		if reserved[strings.ToLower(c.Name)] {
			return "", fmt.Errorf("column %q collides with the change feed's own columns", c.Name)
		}
	}
	names := make([]string, len(cols))
	masked := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c.Name)
		if e, ok := masks[strings.ToLower(c.Name)]; ok {
			masked[i] = "(" + e + "\n) AS " + quoteIdent(c.Name)
		} else {
			masked[i] = names[i]
		}
	}
	rf := "true"
	if filter != "" {
		rf = "(" + filter + "\n)"
	}
	pf := "true"
	if def.PartitionFilter != "" {
		pf = "(" + def.PartitionFilter + "\n)"
	}
	list := strings.Join(names, ", ")
	prefixed := func(a string) string {
		parts := make([]string, len(cols))
		for i, c := range cols {
			x := a + "." + names[i]
			if c.Cast != "" {
				x = "CAST(" + x + " AS " + c.Cast + ")"
			}
			parts[i] = x + " AS " + names[i]
		}
		return strings.Join(parts, ", ")
	}
	var b strings.Builder
	fmt.Fprintf(&b, `WITH ch AS (SELECT snapshot_id AS "_s", rowid AS "_r", change_type AS "_t", %s, COALESCE(%s, false) AS "_rf" FROM lake.%s.table_changes(%s, %d, %d)),
m AS (SELECT "_s", "_r", "_t", "_rf", %s FROM ch),
p AS (SELECT "_s", "_r", "_t", %s, ("_rf" AND COALESCE(%s, false)) AS "_in", hash(ROW(%s)) AS "_h",
  CASE WHEN "_t" IN ('delete', 'update_preimage') THEN 0 ELSE 1 END AS "_k" FROM m),
e AS (SELECT *, row_number() OVER (PARTITION BY "_r" ORDER BY "_s", "_k") AS "_n1",
  row_number() OVER (PARTITION BY "_r" ORDER BY "_s" DESC, "_k" DESC) AS "_n2" FROM p),
b AS (SELECT * FROM e WHERE "_n1" = 1 AND "_k" = 0),
a AS (SELECT * FROM e WHERE "_n2" = 1 AND "_k" = 1)
`, list, rf, quoteIdent(schema), lit(table), from, to, strings.Join(masked, ", "), list, pf, list)
	fmt.Fprintf(&b, `SELECT %s, 'delete' AS "_change_type" FROM b LEFT JOIN a ON a."_r" = b."_r" WHERE b."_in" AND NOT COALESCE(a."_in", false)
UNION ALL
SELECT %s, 'insert' AS "_change_type" FROM a LEFT JOIN b ON a."_r" = b."_r" WHERE a."_in" AND NOT COALESCE(b."_in", false)
UNION ALL
SELECT %s, 'update_preimage' AS "_change_type" FROM b JOIN a ON a."_r" = b."_r" WHERE b."_in" AND a."_in" AND b."_h" <> a."_h"
UNION ALL
SELECT %s, 'update_postimage' AS "_change_type" FROM b JOIN a ON a."_r" = b."_r" WHERE b."_in" AND a."_in" AND b."_h" <> a."_h"`,
		prefixed("b"), prefixed("a"), prefixed("b"), prefixed("a"))
	return b.String(), nil
}

// writeChanges writes the change files of a version: the net changes the
// recipient sees between lake snapshots from and to. It returns the files and
// the net change in the number of rows (inserts minus deletes). It reads the
// base table's changes with the engine and applies the recipient's own policy
// (the same filter and masks the role view has) to them; the output is
// covered by TestChangeFeedReplays, which replays the feed over the previous
// version and compares with the next.
func (s *Service) writeChanges(ctx context.Context, recipient string, def TableDef, cols []column, from, to int64, dir string) ([]CopyFile, int64, error) {
	filter, masks, err := s.Store.policyOf(ctx, recipient, def.Relation)
	if err != nil {
		return nil, 0, err
	}
	q, err := changesSQL(def, cols, filter, masks, from, to)
	if err != nil {
		return nil, 0, err
	}
	tmp, err := os.MkdirTemp(s.opt.TempDir, "duct-share-*")
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	st, err := s.eng.Query(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	var delta int64
	idx := -1
	for i, f := range st.Schema().Fields() {
		if f.Name == "_change_type" {
			idx = i
		}
	}
	if idx < 0 {
		st.Release()
		return nil, 0, errors.New("sharing: change query has no _change_type column")
	}
	files, werr := writeParquet(st, tmp, s.opt.RowsPerFile, func(rec arrow.RecordBatch) {
		col := rec.Column(idx)
		for i := range int(rec.NumRows()) {
			switch col.ValueStr(i) {
			case "insert":
				delta++
			case "delete":
				delta--
			}
		}
	})
	st.Release()
	if werr != nil {
		return nil, 0, fmt.Errorf("sharing: write change feed: %w", werr)
	}
	out, err := s.putFiles(ctx, dir, "cdf", files)
	return out, delta, err
}

// tableChanged reports whether the base table changed in the lake snapshots
// from..to (inclusive); an error (the table was dropped, say) counts as changed.
func (s *Service) tableChanged(ctx context.Context, def TableDef, from, to int64) bool {
	if from > to {
		return false
	}
	schema, table, _ := strings.Cut(def.Relation, ".")
	rs, err := s.Store.rows(ctx, fmt.Sprintf(`SELECT count(*) FROM (SELECT 1 FROM lake.%s.table_changes(%s, %d, %d) LIMIT 1)`, quoteIdent(schema), lit(table), from, to))
	return err != nil || len(rs) != 1 || rs[0][0] != "0"
}

// ---- reading versions ----

// Version returns the copy of the table as of version n.
func (s *Service) Version(ctx context.Context, c *Copy, n int64) (*Copy, error) {
	if !c.History {
		if n != c.Version {
			return nil, unavailable("version %d is not available: this table is shared without history, only the current version (%d) is served", n, c.Version)
		}
		return c, nil
	}
	vs, err := s.loadVersions(ctx, c.Share, c.Recipient, c.Alias)
	if err != nil {
		return nil, err
	}
	for _, v := range vs {
		if v.Version == n {
			out := *c
			out.Version, out.SourceSnapshot, out.TimestampMS = v.Version, v.SourceSnapshot, v.TimestampMS
			out.SchemaString, out.Files, out.NumRows, out.TotalBytes = v.SchemaString, v.Files, v.NumRows, v.TotalBytes
			return &out, nil
		}
	}
	return nil, unavailableVersion(n, vs, c.Version)
}

func unavailableVersion(n int64, vs []Version, current int64) error {
	if len(vs) == 0 {
		return unavailable("version %d is not available (current version %d)", n, current)
	}
	return unavailable("version %d is not available: versions %d to %d are retained", n, vs[0].Version, vs[len(vs)-1].Version)
}

// VersionByTimestamp maps an instant to a version. With floor it is the latest
// version created at or before t (reads as of t, endingTimestamp); without, the
// earliest created at or after t (startingTimestamp).
func (s *Service) VersionByTimestamp(ctx context.Context, c *Copy, t time.Time, floor bool) (int64, error) {
	if !c.History {
		return 0, unavailable("timestamps are not available: this table is shared without history (duct share history)")
	}
	vs, err := s.loadVersions(ctx, c.Share, c.Recipient, c.Alias)
	if err != nil {
		return 0, err
	}
	ms := t.UnixMilli()
	if floor {
		for i := len(vs) - 1; i >= 0; i-- {
			if vs[i].TimestampMS <= ms {
				return vs[i].Version, nil
			}
		}
		return 0, unavailable("the timestamp %s is before the earliest retained version (%s)", t.UTC().Format(time.RFC3339), tsString(vs))
	}
	for _, v := range vs {
		if v.TimestampMS >= ms {
			return v.Version, nil
		}
	}
	return 0, unavailable("the timestamp %s is after the latest version", t.UTC().Format(time.RFC3339))
}

func tsString(vs []Version) string {
	if len(vs) == 0 {
		return "none"
	}
	return time.UnixMilli(vs[0].TimestampMS).UTC().Format(time.RFC3339)
}

// FileChange is one file of a change feed response.
type FileChange struct {
	Kind        string // "add" (rows of a first version, all inserts) or "cdf"
	Version     int64
	TimestampMS int64
	File        CopyFile
}

// Changes returns the files of the change feed of versions from..to
// (inclusive) and the copy of the last one (for its schema). The table must
// have history, every version of the range must be retained and have a feed.
func (s *Service) Changes(ctx context.Context, c *Copy, from, to int64) ([]FileChange, *Copy, error) {
	if !c.History {
		return nil, nil, unavailable("the change feed is not available: this table is shared without history (duct share history)")
	}
	if from > to {
		return nil, nil, unavailable("startingVersion %d is after endingVersion %d", from, to)
	}
	vs, err := s.loadVersions(ctx, c.Share, c.Recipient, c.Alias)
	if err != nil {
		return nil, nil, err
	}
	byV := map[int64]Version{}
	for _, v := range vs {
		byV[v.Version] = v
	}
	var out []FileChange
	for n := from; n <= to; n++ {
		v, ok := byV[n]
		if !ok {
			return nil, nil, unavailableVersion(n, vs, c.Version)
		}
		switch v.Kind {
		case KindInitial:
			for _, f := range v.Files {
				out = append(out, FileChange{Kind: "add", Version: n, TimestampMS: v.TimestampMS, File: f})
			}
		case KindDiff:
			for _, f := range v.CDF {
				out = append(out, FileChange{Kind: "cdf", Version: n, TimestampMS: v.TimestampMS, File: f})
			}
		case KindBase:
			return nil, nil, unavailable("version %d has no change feed: history was turned on there, changes start at version %d", n, n+1)
		default:
			return nil, nil, unavailable("version %d has no change feed: the schema or the recipient's policy changed there; read the table at version %d instead", n, n)
		}
	}
	last := byV[to]
	end := *c
	end.Version, end.SourceSnapshot, end.TimestampMS = last.Version, last.SourceSnapshot, last.TimestampMS
	end.SchemaString, end.Files, end.NumRows, end.TotalBytes = last.SchemaString, last.Files, last.NumRows, last.TotalBytes
	return out, &end, nil
}

// retainedKeys are the files of every retained version of one copy.
func (s *Service) retainedKeys(ctx context.Context, share, recipient, alias string, into map[string]bool) error {
	vs, err := s.loadVersions(ctx, share, recipient, alias)
	if err != nil {
		return err
	}
	for _, v := range vs {
		for _, f := range v.Files {
			into[f.Key] = true
		}
		for _, f := range v.CDF {
			into[f.Key] = true
		}
	}
	return nil
}

// warnGap logs why a version has no change feed.
func warnGap(c *Copy, why string, err error) {
	slog.Warn("sharing: version has no change feed", "table", c.Prefix, "version", c.Version, "why", why, "error", err)
}
