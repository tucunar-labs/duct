package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Triggers (ADR 0005): a run starts when a DuckLake table gets a new snapshot
// or a file appears. Both detectors keep a cursor in duct_jobs_state, so
// several processes see the same changes, and start the run through a fire key
// (duct_jobs_fires), so exactly one of them does. The first look at a trigger
// only records the baseline: existing snapshots and files never start a run.
// While a job has an active run its triggers wait (the cursor does not move),
// which coalesces any number of changes into one run afterwards.

// snapshotKeys are the DuckLake change kinds that alter a table's data or
// schema, with how their values name the table. Maintenance (flushed_inlined,
// merge_adjacent, ...) is deliberately absent: a `duct gc` is not new data.
var snapshotKeys = map[string]bool{ // true: values are table ids, false: schema.table names
	"tables_created":       false,
	"tables_inserted_into": true,
	"tables_deleted_from":  true,
	"tables_altered":       true,
	"inlined_insert":       true,
	"inlined_delete":       true,
}

// matchSnapshot returns the watched tables (schema.table) that one snapshot's
// changes touch. ids maps a table id to schema.table.
func matchSnapshot(changes map[string][]string, watched map[string]bool, ids map[string]string) []string {
	hit := map[string]bool{}
	for key, vals := range changes {
		isID, relevant := snapshotKeys[key]
		if !relevant {
			continue
		}
		for _, v := range vals {
			name := v
			if isID {
				name = ids[v]
			}
			if watched[name] {
				hit[name] = true
			}
		}
	}
	var out []string
	for n := range hit {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (o *Orchestrator) pollSnapshots(ctx context.Context, job Job) error {
	tables := job.SnapshotTables()
	if len(tables) == 0 {
		return nil
	}
	key := "snap:" + job.Name
	curS, err := o.st.scalar(ctx, "SELECT COALESCE(max(snapshot_id), 0) FROM lake.snapshots()")
	if err != nil {
		return fmt.Errorf("snapshots: %w", err)
	}
	cur, _ := strconv.ParseInt(curS, 10, 64)
	prevS, ok, err := o.st.getState(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return o.st.setState(ctx, key, curS, o.now())
	}
	prev, _ := strconv.ParseInt(prevS, 10, 64)
	if cur <= prev {
		return nil
	}
	if n, err := o.st.activeRuns(ctx, job.Name); err != nil || n > 0 {
		return err // wait for the active run; the cursor stays
	}
	snaps, err := o.st.rows(ctx, "SELECT snapshot_id, CAST(to_json(changes) AS VARCHAR) FROM lake.snapshots() WHERE snapshot_id > ? ORDER BY snapshot_id", prev)
	if err != nil {
		return err
	}
	idRows, err := o.st.rows(ctx, "SELECT CAST(t.table_id AS VARCHAR), s.schema_name || '.' || t.table_name "+
		"FROM __ducklake_metadata_lake.ducklake_table t JOIN __ducklake_metadata_lake.ducklake_schema s USING (schema_id) "+
		"WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL")
	if err != nil {
		return err
	}
	ids := map[string]string{}
	for _, r := range idRows {
		ids[r[0]] = r[1]
	}
	watched := map[string]bool{}
	for _, t := range tables {
		watched[t] = true
	}
	var lastHit int64
	touched := map[string]bool{}
	for _, sn := range snaps {
		var ch map[string][]string
		if err := json.Unmarshal([]byte(sn[1]), &ch); err != nil {
			continue
		}
		if hit := matchSnapshot(ch, watched, ids); len(hit) > 0 {
			lastHit, _ = strconv.ParseInt(sn[0], 10, 64)
			for _, h := range hit {
				touched[h] = true
			}
		}
	}
	if lastHit != 0 {
		names := make([]string, 0, len(touched))
		for n := range touched {
			names = append(names, n)
		}
		sort.Strings(names)
		detail, _ := json.Marshal(map[string]any{"snapshot": lastHit, "tables": names})
		if _, err := o.fire(ctx, job, TriggerSnapshot, fmt.Sprintf("snap:%s:%d", job.Name, lastHit), time.Time{}, string(detail)); err != nil {
			return err
		}
	}
	return o.st.setState(ctx, key, curS, o.now())
}

// fileFingerprints maps each file matching glob (relative to root) to
// "mtime:size". Files modified less than settle ago are left out: they may
// still be being written.
func fileFingerprints(root, glob string, settle time.Duration, now time.Time) (map[string]string, map[string]bool, error) {
	matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(glob)))
	if err != nil {
		return nil, nil, err
	}
	out, unsettled := map[string]string{}, map[string]bool{}
	for _, m := range matches {
		st, err := os.Stat(m)
		if err != nil || st.IsDir() {
			continue
		}
		rel, _ := filepath.Rel(root, m)
		rel = filepath.ToSlash(rel)
		if now.Sub(st.ModTime()) < settle {
			unsettled[rel] = true
			continue
		}
		out[rel] = fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
	}
	return out, unsettled, nil
}

// diffFiles lists the files that are new or changed in cur against prev, sorted.
func diffFiles(prev, cur map[string]string) []string {
	var out []string
	for p, fp := range cur {
		if prev[p] != fp {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func (o *Orchestrator) pollFiles(ctx context.Context, job Job) error {
	glob := job.Triggers.OnFile
	if glob == "" {
		return nil
	}
	key := "file:" + job.Name
	now := o.now()
	cur, unsettled, err := fileFingerprints(o.opt.Root, glob, o.opt.FileSettle, now)
	if err != nil {
		return err
	}
	prevS, ok, err := o.st.getState(ctx, key)
	if err != nil {
		return err
	}
	prev := map[string]string{}
	if ok {
		_ = json.Unmarshal([]byte(prevS), &prev)
	}
	// What the cursor remembers: settled files now, plus the old value of
	// files still being written (so they count as changed once they settle).
	next := map[string]string{}
	for p, fp := range cur {
		next[p] = fp
	}
	for p := range unsettled {
		if fp, had := prev[p]; had {
			next[p] = fp
		}
	}
	save := func() error {
		b, _ := json.Marshal(next)
		return o.st.setState(ctx, key, string(b), now)
	}
	if !ok {
		return save()
	}
	changed := diffFiles(prev, cur)
	if len(changed) == 0 {
		if len(next) != len(prev) { // files were deleted
			return save()
		}
		return nil
	}
	if n, err := o.st.activeRuns(ctx, job.Name); err != nil || n > 0 {
		return err
	}
	h := sha256.New()
	for _, p := range changed {
		_, _ = h.Write([]byte(p + "=" + cur[p] + "\n"))
	}
	detail, _ := json.Marshal(map[string]any{"files": changed[:min(len(changed), 20)], "count": len(changed)})
	if _, err := o.fire(ctx, job, TriggerFile, "file:"+job.Name+":"+hex.EncodeToString(h.Sum(nil))[:16], time.Time{}, string(detail)); err != nil {
		return err
	}
	return save()
}

func (o *Orchestrator) pollTriggers(ctx context.Context) {
	for _, name := range o.names {
		job := o.jobs[name]
		if err := o.pollSnapshots(ctx, job); err != nil && ctx.Err() == nil {
			o.log.Warn("snapshot trigger", "job", name, "err", err)
		}
		if err := o.pollFiles(ctx, job); err != nil && ctx.Err() == nil {
			o.log.Warn("file trigger", "job", name, "err", err)
		}
	}
}

// hasTriggers reports whether any job watches snapshots or files.
func (o *Orchestrator) hasTriggers() bool {
	for _, j := range o.jobs {
		if len(j.Triggers.OnSnapshot) > 0 || j.Triggers.OnFile != "" {
			return true
		}
	}
	return false
}
