package orchestrator

import (
	"fmt"
	"sort"
	"time"
)

// The functions in this file are the pure part of scheduling: given the rows of
// the running runs, what can start, what must be skipped, which runs are over.

func terminal(status string) bool {
	switch status {
	case StatusSuccess, StatusFailed, StatusSkipped, StatusCancelled:
		return true
	}
	return false
}

func byRun(rows []TaskRow) map[int64]map[string]*TaskRow {
	m := map[int64]map[string]*TaskRow{}
	for i := range rows {
		r := &rows[i]
		if m[r.RunID] == nil {
			m[r.RunID] = map[string]*TaskRow{}
		}
		m[r.RunID][r.ID] = r
	}
	return m
}

// readyTasks returns the pending tasks this process may start now: not held
// back by a retry delay, not pinned to another process and with every
// dependency successful. Oldest run first, then declaration-independent id order.
func readyTasks(rows []TaskRow, instance string, now time.Time) []TaskRow {
	runs := byRun(rows)
	var out []TaskRow
	for _, r := range rows {
		if r.Status != StatusPending || (r.pinned != "" && r.pinned != instance) || r.NotBefore.After(now) {
			continue
		}
		ok := true
		for _, d := range r.DependsOn {
			if dep := runs[r.RunID][d]; dep == nil || dep.Status != StatusSuccess {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].RunID != out[j].RunID {
			return out[i].RunID < out[j].RunID
		}
		return out[i].ID < out[j].ID
	})
	return out
}

type skip struct {
	RunID  int64
	Task   string
	Reason string
}

// toSkip lists the pending tasks that can never run because a dependency
// failed, was skipped or was cancelled (transitively).
func toSkip(rows []TaskRow) []skip {
	runs := byRun(rows)
	status := map[string]string{}
	key := func(run int64, id string) string { return fmt.Sprintf("%d/%s", run, id) }
	for _, r := range rows {
		status[key(r.RunID, r.ID)] = r.Status
	}
	var out []skip
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if status[key(r.RunID, r.ID)] != StatusPending {
				continue
			}
			for _, d := range r.DependsOn {
				if runs[r.RunID][d] == nil {
					continue
				}
				if st := status[key(r.RunID, d)]; st == StatusFailed || st == StatusSkipped || st == StatusCancelled {
					status[key(r.RunID, r.ID)] = StatusSkipped
					out = append(out, skip{r.RunID, r.ID, fmt.Sprintf("upstream %s %s", d, st)})
					changed = true
					break
				}
			}
		}
	}
	return out
}

type finished struct {
	RunID  int64
	Status string
	Error  string
}

// finishedRuns lists the runs whose tasks are all terminal, with their final
// status: failed if any task failed, else cancelled if any was cancelled, else success.
func finishedRuns(rows []TaskRow) []finished {
	runs := byRun(rows)
	ids := make([]int64, 0, len(runs))
	for id := range runs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []finished
	for _, id := range ids {
		done, failed, cancelled := true, "", ""
		var first string
		tasks := make([]*TaskRow, 0, len(runs[id]))
		for _, t := range runs[id] {
			tasks = append(tasks, t)
		}
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
		for _, t := range tasks {
			switch {
			case !terminal(t.Status):
				done = false
			case t.Status == StatusFailed:
				failed = t.ID
				if first == "" {
					first = fmt.Sprintf("task %s failed: %s", t.ID, t.Error)
				}
			case t.Status == StatusCancelled:
				cancelled = t.ID
			}
		}
		if !done {
			continue
		}
		switch {
		case failed != "":
			out = append(out, finished{id, StatusFailed, first})
		case cancelled != "":
			out = append(out, finished{id, StatusCancelled, "task " + cancelled + " was cancelled"})
		default:
			out = append(out, finished{id, StatusSuccess, ""})
		}
	}
	return out
}
