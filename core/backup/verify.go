package backup

import (
	"context"
	"fmt"
	"sort"
	"strings"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/project"
)

// Report is the outcome of Verify.
type Report struct {
	SnapshotID int64
	Tables     int
	Rows       int64
	StateRows  int64
	LakeFiles  int
	Problems   []string
}

// OK reports whether everything matched.
func (r *Report) OK() bool { return len(r.Problems) == 0 }

// Verify compares a project (opened through a fresh eng) with a manifest: the
// snapshot id, the lake tables and their row counts, the Duct tables, and that
// every data file the catalog references exists with its size.
func Verify(ctx context.Context, p *project.Project, eng contract.Engine, m *Manifest) (*Report, error) {
	s := session{eng: eng}
	rep := &Report{}
	bad := func(format string, a ...any) { rep.Problems = append(rep.Problems, fmt.Sprintf(format, a...)) }

	snap, err := s.int(ctx, "SELECT max(snapshot_id) FROM ducklake_snapshots('lake')")
	if err != nil {
		return nil, err
	}
	rep.SnapshotID = snap
	if snap != m.SnapshotID {
		bad("snapshot id is %d, the backup has %d", snap, m.SnapshotID)
	}

	rows, err := s.rows(ctx, "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'lake' AND NOT internal ORDER BY 1, 2")
	if err != nil {
		return nil, err
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r[0]+"."+r[1]] = true
	}
	for _, t := range m.Tables {
		name := t.Schema + "." + t.Name
		if !got[name] {
			bad("table %s is missing", name)
			continue
		}
		delete(got, name)
		n, err := s.count(ctx, fmt.Sprintf("lake.%s.%s", qi(t.Schema), qi(t.Name)))
		if err != nil {
			return nil, err
		}
		rep.Tables++
		rep.Rows += n
		if n != t.Rows {
			bad("table %s has %d rows, the backup has %d", name, n, t.Rows)
		}
	}
	extra := make([]string, 0, len(got))
	for n := range got {
		extra = append(extra, n)
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		bad("tables that are not in the backup: %s", strings.Join(extra, ", "))
	}

	for _, t := range m.StateTables {
		if t.Name == "duct_migrations" {
			continue // the target may know newer migrations
		}
		n, err := s.count(ctx, "duct."+qi(t.Name))
		if err != nil {
			return nil, err
		}
		rep.StateRows += n
		if n != t.Rows {
			bad("Duct table %s has %d rows, the backup has %d", t.Name, n, t.Rows)
		}
	}

	dst, err := lakeStore(p)
	if err != nil {
		return nil, err
	}
	objs, err := dst.List(ctx, "")
	if err != nil {
		return nil, err
	}
	size := make(map[string]int64, len(objs))
	for _, o := range objs {
		size[o.Key] = o.Size
	}
	var missing []string
	for _, f := range m.LakeFiles {
		sz, ok := size[f.Path]
		if !ok || (f.Size > 0 && sz != f.Size) {
			missing = append(missing, f.Path)
		}
	}
	rep.LakeFiles = len(m.LakeFiles) - len(missing)
	if len(missing) > 0 {
		if len(missing) > 5 {
			missing = append(missing[:5], fmt.Sprintf("... and %d more", len(missing)-5))
		}
		bad("lake files missing or with another size: %s", strings.Join(missing, ", "))
	}
	return rep, nil
}
