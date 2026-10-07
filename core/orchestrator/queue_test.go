package orchestrator

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func row(run int64, id, status string, deps ...string) TaskRow {
	return TaskRow{RunID: run, ID: id, Status: status, DependsOn: deps, MaxAttempts: 1}
}

func ids(rs []TaskRow) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}

func TestReadyTasksFollowTheDAG(t *testing.T) {
	now := time.Now()
	rows := []TaskRow{
		row(1, "load", StatusSuccess),
		row(1, "build", StatusPending, "load"),
		row(1, "report", StatusPending, "build"),
		row(1, "side", StatusPending),
		row(1, "wait", StatusPending, "load"),
	}
	rows[4].NotBefore = now.Add(time.Minute) // retry backoff not over
	if got := ids(readyTasks(rows, "me", now)); !reflect.DeepEqual(got, []string{"build", "side"}) {
		t.Fatalf("ready %v", got)
	}
	rows[1].Status = StatusRunning
	if got := ids(readyTasks(rows, "me", now)); !reflect.DeepEqual(got, []string{"side"}) {
		t.Fatalf("ready %v", got)
	}
}

func TestPinnedRunsBelongToTheirProcess(t *testing.T) {
	r := row(1, "a", StatusPending)
	r.pinned = "cli-1"
	if len(readyTasks([]TaskRow{r}, "serve-1", time.Now())) != 0 {
		t.Error("another process must not take pinned work")
	}
	if len(readyTasks([]TaskRow{r}, "cli-1", time.Now())) != 1 {
		t.Error("the pinned process must take it")
	}
}

func TestSkipCascades(t *testing.T) {
	rows := []TaskRow{
		row(1, "a", StatusFailed),
		row(1, "b", StatusPending, "a"),
		row(1, "c", StatusPending, "b"),
		row(1, "d", StatusPending),
		row(2, "a", StatusSuccess),
		row(2, "b", StatusPending, "a"),
	}
	got := map[string]bool{}
	for _, s := range toSkip(rows) {
		got[s.Task+"@"+string(rune('0'+s.RunID))] = true
	}
	if !reflect.DeepEqual(got, map[string]bool{"b@1": true, "c@1": true}) {
		t.Fatalf("skips %v", got)
	}
}

func TestFinishedRuns(t *testing.T) {
	rows := []TaskRow{
		row(1, "a", StatusSuccess), row(1, "b", StatusSuccess), // success
		row(2, "a", StatusFailed), row(2, "b", StatusSkipped, "a"), // failed
		row(3, "a", StatusSuccess), row(3, "b", StatusRunning, "a"), // open
		row(4, "a", StatusCancelled), row(4, "b", StatusSkipped, "a"), // cancelled
		row(5, "a", StatusSuccess), row(5, "b", StatusPending, "a"), // open (pending retry)
	}
	rows[2].Error = "boom"
	got := map[int64]string{}
	for _, f := range finishedRuns(rows) {
		got[f.RunID] = f.Status
	}
	want := map[int64]string{1: StatusSuccess, 2: StatusFailed, 4: StatusCancelled}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("finished %v", got)
	}
	for _, f := range finishedRuns(rows) {
		if f.RunID == 2 && f.Error != "task a failed: boom" {
			t.Errorf("error %q", f.Error)
		}
	}
}

func TestMatchSnapshot(t *testing.T) {
	ids := map[string]string{"2": "bronze.t", "3": "bronze.u"}
	watched := map[string]bool{"bronze.t": true}
	for name, tc := range map[string]struct {
		ch   map[string][]string
		want []string
	}{
		"insert":             {map[string][]string{"tables_inserted_into": {"2"}}, []string{"bronze.t"}},
		"inlined":            {map[string][]string{"inlined_insert": {"2"}}, []string{"bronze.t"}},
		"created":            {map[string][]string{"tables_created": {"bronze.t"}}, []string{"bronze.t"}},
		"other table":        {map[string][]string{"tables_inserted_into": {"3"}}, nil},
		"gc is not new data": {map[string][]string{"flushed_inlined": {"2"}, "merge_adjacent": {"2"}}, nil},
		"schema only":        {map[string][]string{"schemas_created": {"bronze"}}, nil},
	} {
		if got := matchSnapshot(tc.ch, watched, ids); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: %v want %v", name, got, tc.want)
		}
	}
}

func TestFileDetection(t *testing.T) {
	root := t.TempDir()
	put := func(name string, age time.Duration) {
		p := filepath.Join(root, "in", name)
		_ = os.MkdirAll(filepath.Dir(p), 0o750)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		mt := time.Now().Add(-age)
		_ = os.Chtimes(p, mt, mt)
	}
	put("a.csv", time.Hour)
	put("b.csv", time.Hour)
	put("c.txt", time.Hour)
	put("fresh.csv", 0)
	base, unsettled, err := fileFingerprints(root, "in/*.csv", 2*time.Second, time.Now())
	if err != nil || len(base) != 2 || !unsettled["in/fresh.csv"] {
		t.Fatalf("base %v unsettled %v %v", base, unsettled, err)
	}
	if d := diffFiles(base, base); len(d) != 0 {
		t.Errorf("no change expected, got %v", d)
	}
	put("d.csv", time.Hour)
	put("a.csv", 30*time.Minute) // modified
	cur, _, _ := fileFingerprints(root, "in/*.csv", 2*time.Second, time.Now())
	if d := diffFiles(base, cur); !reflect.DeepEqual(d, []string{"in/a.csv", "in/d.csv"}) {
		t.Errorf("diff %v", d)
	}
}
