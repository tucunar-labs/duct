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
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Defaults of Options.
const (
	DefaultURLTTL      = 15 * time.Minute
	DefaultGrace       = time.Hour
	DefaultRowsPerFile = 2_000_000
)

// ErrNotFound is returned for a share, schema or table the recipient may not
// see (it does not say which of the two it is).
var ErrNotFound = errors.New("not found")

// Options tune a Service.
type Options struct {
	// URLTTL is how long a file URL works (default 15 minutes).
	URLTTL time.Duration
	// Grace is how long a superseded copy stays on storage so downloads that
	// started with an older URL can finish (default 1 hour).
	Grace time.Duration
	// RowsPerFile is the target size of a Parquet file (default 2 million rows).
	RowsPerFile int64
	// TempDir is where files are staged before upload (default os.TempDir).
	TempDir string
}

// Service materializes and serves the copies. It reads data only through the
// governed QueryHandler, as the recipient's own role, so a copy holds exactly
// what the recipient's views show.
type Service struct {
	Store *Store
	eng   contract.Engine
	q     contract.QueryHandler
	files Storage
	opt   Options
	locks sync.Map // copy key -> *sync.Mutex
}

// NewService returns a Service. q must run statements as the identity it is
// given over its role views (governance.Handler); eng is the engine with the
// Duct tables (read for the lake snapshot, written for the copies' state).
func NewService(eng contract.Engine, q contract.QueryHandler, files Storage, opt Options) *Service {
	if opt.URLTTL <= 0 {
		opt.URLTTL = DefaultURLTTL
	}
	if opt.Grace <= 0 {
		opt.Grace = DefaultGrace
	}
	if opt.RowsPerFile <= 0 {
		opt.RowsPerFile = DefaultRowsPerFile
	}
	return &Service{Store: NewStore(eng), eng: eng, q: q, files: files, opt: opt}
}

// Storage is where the copies live.
func (s *Service) Storage() Storage { return s.files }

// URLTTL is the lifetime of file URLs.
func (s *Service) URLTTL() time.Duration { return s.opt.URLTTL }

// CopyFile is one Parquet file of a copy.
type CopyFile struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	Rows int64  `json:"rows"`
	ID   string `json:"id"`
}

// Copy is the materialized, filtered and masked copy of one table for one
// recipient.
type Copy struct {
	Share, Recipient, Alias string
	Version                 int64 // what the protocol reports as table version
	SourceSnapshot          int64 // lake snapshot the copy was last checked against
	Fingerprint             string
	Prefix                  string // storage prefix of the table (all its versions)
	SchemaString            string
	TableID                 string
	Files                   []CopyFile
	NumRows, TotalBytes     int64
	CreatedAt               time.Time
}

func tableID(share, recipient, alias string) string {
	h := sha256.Sum256([]byte("duct-share\x00" + share + "\x00" + recipient + "\x00" + alias))
	x := hex.EncodeToString(h[:16])
	return x[:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:32]
}

func (s *Service) loadCopy(ctx context.Context, share, recipient, alias string) (*Copy, error) {
	rs, err := s.Store.rows(ctx, `SELECT CAST(version AS VARCHAR), CAST(source_snapshot AS VARCHAR), fingerprint, prefix, schema_string, files,
		CAST(num_rows AS VARCHAR), CAST(total_bytes AS VARCHAR), table_id, CAST(created_at AS VARCHAR)
		FROM duct.duct_share_copies WHERE share = ? AND recipient = ? AND alias = ?`, share, recipient, alias)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	r := rs[0]
	c := &Copy{Share: share, Recipient: recipient, Alias: alias, Fingerprint: r[2], Prefix: r[3], SchemaString: r[4], TableID: r[8]}
	_, _ = fmt.Sscan(r[0], &c.Version)
	_, _ = fmt.Sscan(r[1], &c.SourceSnapshot)
	_, _ = fmt.Sscan(r[6], &c.NumRows)
	_, _ = fmt.Sscan(r[7], &c.TotalBytes)
	c.CreatedAt, _ = parseTime(r[9])
	if err := json.Unmarshal([]byte(r[5]), &c.Files); err != nil {
		return nil, fmt.Errorf("sharing: corrupt file list of %s/%s/%s: %w", share, recipient, alias, err)
	}
	return c, nil
}

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

func (s *Service) saveCopy(ctx context.Context, c *Copy) error {
	files, _ := json.Marshal(c.Files)
	del := "DELETE FROM duct.duct_share_copies WHERE share = " + lit(c.Share) + " AND recipient = " + lit(c.Recipient) + " AND alias = " + lit(c.Alias)
	ins := fmt.Sprintf(`INSERT INTO duct.duct_share_copies (share, recipient, alias, version, source_snapshot, fingerprint, prefix, schema_string, files, num_rows, total_bytes, table_id, created_at, checked_at)
		VALUES (%s, %s, %s, %d, %d, %s, %s, %s, %s, %d, %d, %s, CAST(%s AS TIMESTAMP), now())`,
		lit(c.Share), lit(c.Recipient), lit(c.Alias), c.Version, c.SourceSnapshot, lit(c.Fingerprint), lit(c.Prefix), lit(c.SchemaString),
		lit(string(files)), c.NumRows, c.TotalBytes, lit(c.TableID), lit(c.CreatedAt.UTC().Format("2006-01-02 15:04:05")))
	return s.Store.execTx(ctx, del, ins)
}

// snapshot is the current snapshot id of the lake.
func (s *Service) snapshot(ctx context.Context) (int64, error) {
	rs, err := s.Store.rows(ctx, `SELECT CAST(id AS VARCHAR) FROM lake.current_snapshot()`)
	if err != nil {
		return 0, err
	}
	if len(rs) != 1 {
		return 0, errors.New("sharing: cannot read the lake snapshot")
	}
	var n int64
	_, err = fmt.Sscan(rs[0][0], &n)
	return n, err
}

// Resolve returns the up-to-date copy of the table the recipient asks for.
// A share the recipient is not granted, or a table the share lacks, is
// ErrNotFound.
func (s *Service) Resolve(ctx context.Context, recipient, share, schema, table string) (*Copy, error) {
	ok, err := s.Store.IsGranted(ctx, share, recipient)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	def, ok, err := s.Store.Table(ctx, share, schema, table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return s.Ensure(ctx, recipient, def)
}

func (s *Service) lock(key string) *sync.Mutex {
	m, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return m.(*sync.Mutex)
}

func identityOf(recipient string) contract.Identity {
	return contract.Identity{User: RoleOf(recipient), Role: RoleOf(recipient)}
}

// Ensure brings the copy of def for recipient up to date with the lake: when
// the lake snapshot moved it fingerprints what the recipient's view returns
// and, only if that changed, writes a new compacted copy (a new version) and
// drops the superseded ones once they are older than the grace period.
func (s *Service) Ensure(ctx context.Context, recipient string, def TableDef) (*Copy, error) {
	snap, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	c, err := s.loadCopy(ctx, def.Share, recipient, def.Alias)
	if err != nil {
		return nil, err
	}
	if c != nil && c.SourceSnapshot == snap {
		return c, nil
	}
	mu := s.lock(def.Share + "/" + recipient + "/" + def.Alias)
	mu.Lock()
	defer mu.Unlock()
	if c, err = s.loadCopy(ctx, def.Share, recipient, def.Alias); err != nil {
		return nil, err
	}
	if c != nil && c.SourceSnapshot >= snap {
		return c, nil
	}
	_, tbl, _ := strings.Cut(def.Relation, ".")
	id := identityOf(recipient)
	from := " FROM " + quoteIdent(tbl)
	where := ""
	if def.PartitionFilter != "" {
		where = " WHERE (" + def.PartitionFilter + "\n)"
	}
	cols, err := s.describe(ctx, id, from)
	if err != nil {
		return nil, err
	}
	schema := schemaString(cols)
	sel := selectList(cols)
	fp, err := s.fingerprint(ctx, id, sel, from+where, schema)
	if err != nil {
		return nil, err
	}
	if c != nil && c.Fingerprint == fp {
		c.SourceSnapshot = snap
		return c, s.saveCopy(ctx, c)
	}
	next := &Copy{Share: def.Share, Recipient: recipient, Alias: def.Alias, Version: 1, SourceSnapshot: snap, Fingerprint: fp,
		Prefix: def.Share + "/" + recipient + "/" + def.Alias, SchemaString: schema, TableID: tableID(def.Share, recipient, def.Alias), CreatedAt: time.Now().UTC()}
	if c != nil {
		next.Version = c.Version + 1
	}
	if err := s.write(ctx, id, next, "SELECT "+sel+from+where); err != nil {
		return nil, err
	}
	if err := s.saveCopy(ctx, next); err != nil {
		return nil, err
	}
	if err := s.sweep(ctx, next.Prefix+"/", liveKeys(next)); err != nil {
		slog.Warn("sharing: cleaning old copies failed", "table", next.Prefix, "error", err)
	}
	return next, nil
}

// describe reads the column types of the recipient's view.
func (s *Service) describe(ctx context.Context, id contract.Identity, from string) ([]column, error) {
	st, err := s.q.Query(ctx, id, "SELECT *"+from+" LIMIT 0")
	if err != nil {
		return nil, err
	}
	defer st.Release()
	cols := planColumns(st.Schema())
	for st.Next() { // drain: the stream is audited when released
	}
	return cols, st.Err()
}

func (s *Service) fingerprint(ctx context.Context, id contract.Identity, sel, fromWhere, schema string) (string, error) {
	st, err := s.q.Query(ctx, id, "SELECT count(*), CAST(sum(CAST(hash(t) AS HUGEINT)) AS VARCHAR) FROM (SELECT "+sel+fromWhere+") AS t")
	if err != nil {
		return "", err
	}
	defer st.Release()
	var n, sum string
	for st.Next() {
		rec := st.RecordBatch()
		if rec.NumRows() > 0 {
			n, sum = strings.Clone(rec.Column(0).ValueStr(0)), strings.Clone(rec.Column(1).ValueStr(0))
		}
	}
	if err := st.Err(); err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(schema + "\x00" + n + "\x00" + sum))
	return hex.EncodeToString(h[:]), nil
}

func (s *Service) write(ctx context.Context, id contract.Identity, c *Copy, query string) error {
	tmp, err := os.MkdirTemp(s.opt.TempDir, "duct-share-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	st, err := s.q.Query(ctx, id, query)
	if err != nil {
		return err
	}
	files, werr := writeParquet(st, tmp, s.opt.RowsPerFile)
	st.Release()
	if werr != nil {
		return fmt.Errorf("sharing: write copy of %s: %w", c.Prefix, werr)
	}
	dir := fmt.Sprintf("%s/v%d", c.Prefix, c.SourceSnapshot)
	for i, f := range files {
		key := fmt.Sprintf("%s/part-%05d.parquet", dir, i)
		if err := s.upload(ctx, key, f); err != nil {
			return err
		}
		h := sha256.Sum256([]byte(key))
		c.Files = append(c.Files, CopyFile{Key: key, Size: f.Size, Rows: f.Rows, ID: hex.EncodeToString(h[:16])})
		c.NumRows += f.Rows
		c.TotalBytes += f.Size
	}
	return nil
}

func (s *Service) upload(ctx context.Context, key string, f localFile) error {
	r, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()
	return s.files.Put(ctx, key, r, f.Size)
}

func liveKeys(cs ...*Copy) map[string]bool {
	m := map[string]bool{}
	for _, c := range cs {
		for _, f := range c.Files {
			m[f.Key] = true
		}
	}
	return m
}

// sweep deletes objects under prefix that no live copy references and that are
// older than the grace period (so a download in flight finishes).
func (s *Service) sweep(ctx context.Context, prefix string, live map[string]bool) error {
	objs, err := s.files.List(ctx, prefix)
	if err != nil {
		return err
	}
	var old []string
	cutoff := time.Now().Add(-s.opt.Grace)
	for _, o := range objs {
		if !live[o.Key] && o.Modified.Before(cutoff) {
			old = append(old, o.Key)
		}
	}
	return s.files.Delete(ctx, old)
}

// GC deletes every stored object that no copy references and is older than the
// grace period. It returns how many it removed.
func (s *Service) GC(ctx context.Context) (int, error) {
	rs, err := s.Store.rows(ctx, `SELECT share, recipient, alias FROM duct.duct_share_copies`)
	if err != nil {
		return 0, err
	}
	live := map[string]bool{}
	for _, r := range rs {
		c, err := s.loadCopy(ctx, r[0], r[1], r[2])
		if err != nil {
			return 0, err
		}
		if c != nil {
			for k := range liveKeys(c) {
				live[k] = true
			}
		}
	}
	objs, err := s.files.List(ctx, "")
	if err != nil {
		return 0, err
	}
	var old []string
	cutoff := time.Now().Add(-s.opt.Grace)
	for _, o := range objs {
		if !live[o.Key] && o.Modified.Before(cutoff) {
			old = append(old, o.Key)
		}
	}
	return len(old), s.files.Delete(ctx, old)
}

// Purge deletes the stored files under the given table prefixes at once (a
// revoked grant must not leave a readable copy).
func (s *Service) Purge(ctx context.Context, prefixes []string) error {
	for _, p := range prefixes {
		if err := DeletePrefix(ctx, s.files, p); err != nil {
			return err
		}
	}
	return nil
}

// RefreshStats is the outcome of Refresh.
type RefreshStats struct{ Checked, Rewritten int }

// Refresh brings every copy of share (all shares when empty) up to date. A
// failure on one table does not stop the others; the first error is returned.
func (s *Service) Refresh(ctx context.Context, share string) (RefreshStats, error) {
	var st RefreshStats
	var first error
	shares, err := s.Store.Shares(ctx)
	if err != nil {
		return st, err
	}
	for _, sh := range shares {
		if share != "" && sh.Name != share {
			continue
		}
		for _, rcp := range sh.Recipients {
			for _, t := range sh.Tables {
				before, _ := s.loadCopy(ctx, sh.Name, rcp, t.Alias)
				c, err := s.Ensure(ctx, rcp, t)
				st.Checked++
				if err != nil {
					if first == nil {
						first = fmt.Errorf("%s/%s/%s: %w", sh.Name, rcp, t.Alias, err)
					}
					continue
				}
				if before == nil || before.Version != c.Version {
					st.Rewritten++
				}
			}
		}
	}
	return st, first
}

// FileURLs returns download URLs for the files of c, valid for URLTTL.
func (s *Service) FileURLs(ctx context.Context, c *Copy) ([]string, error) {
	out := make([]string, len(c.Files))
	for i, f := range c.Files {
		u, err := s.files.URL(ctx, f.Key, s.opt.URLTTL)
		if err != nil {
			return nil, err
		}
		out[i] = u
	}
	return out, nil
}
