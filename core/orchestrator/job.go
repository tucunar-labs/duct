// Package orchestrator is Duct's own job orchestrator (ADR 0005): jobs defined
// in jobs/*.yaml run as a DAG of tasks on a schedule or when a trigger fires
// (a new DuckLake snapshot of a table, a new file). Runs, task attempts and the
// work queue live in Duct-owned tables, so several `duct serve` processes on a
// shared Postgres catalog cooperate without any extra service. See AGENTS.md.
package orchestrator

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Dir is the folder of a project that holds the job definitions.
const Dir = "jobs"

// Task types. pipeline and ingest are built into the orchestrator (they need
// the engine and the core packages, which a module may not import); the others
// are TaskType modules found in the registry under KindTaskType.
const (
	TypePipeline = "pipeline"
	TypeIngest   = "ingest"
	TypePython   = "python"
	TypeShell    = "shell"
	TypeNotebook = "notebook"
)

var taskTypes = []string{TypePipeline, TypeIngest, TypePython, TypeShell, TypeNotebook}

// Catch-up policies for the cron ticks missed during downtime.
const (
	CatchupNone = "none" // forget them
	CatchupLast = "last" // one run for the most recent one (default)
	CatchupAll  = "all"  // one run per tick, oldest first, one at a time
)

// Catch-up bounds: at most MaxCatchup ticks of policy all, none older than
// CatchupHorizon (fire keys are kept 7 days, see pruneFires).
const (
	MaxCatchup     = 100
	CatchupHorizon = 6 * 24 * time.Hour
)

// Defaults for a task that does not say.
const (
	DefaultTimeout    = time.Hour
	DefaultBackoff    = 10 * time.Second
	DefaultMaxBackoff = 5 * time.Minute
)

// Duration is a time.Duration written as "30s", "5m" in YAML.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	if v < 0 {
		return fmt.Errorf("line %d: duration must not be negative", n.Line)
	}
	*d = Duration(v)
	return nil
}

// Job is one jobs/<name>.yaml file (schema: specs/jobs.schema.json).
type Job struct {
	Name     string `yaml:"name"`
	Schedule string `yaml:"schedule"`
	Timezone string `yaml:"timezone"`
	// Catchup says what to do with the cron ticks missed while the orchestrator
	// was not running: none, last (default) or all (see CatchupAll).
	Catchup  string   `yaml:"catchup"`
	Triggers Triggers `yaml:"triggers"`
	Tasks    []Task   `yaml:"tasks"`
	Alerts   Alerts   `yaml:"alerts"`

	// File is the path the definition was read from (not part of the YAML).
	File string `yaml:"-"`
}

// Triggers start a run besides the schedule.
type Triggers struct {
	// OnSnapshot lists lake tables (schema.table); a new DuckLake snapshot
	// that changes any of them starts a run. Do not list a table the job
	// itself writes: it would trigger itself.
	OnSnapshot []string `yaml:"on_snapshot"`
	// OnFile is a glob (relative to the project) polled for new or changed files.
	OnFile string `yaml:"on_file"`
}

// Task is one node of the job's DAG.
type Task struct {
	ID         string         `yaml:"id"`
	Type       string         `yaml:"type"`
	DependsOn  []string       `yaml:"depends_on"`
	Params     map[string]any `yaml:"params"`
	Retries    int            `yaml:"retries"`
	Backoff    Duration       `yaml:"backoff"`
	MaxBackoff Duration       `yaml:"max_backoff"`
	Timeout    Duration       `yaml:"timeout"`
}

// Alerts says whom to tell when a run fails (and when it recovers).
type Alerts struct {
	OnFailure []AlertSpec `yaml:"on_failure"`
}

// AlertSpec names a KindAlertChannel module and its Init config, e.g.
// {channel: webhook, config: {url: https://...}}.
type AlertSpec struct {
	Channel string         `yaml:"channel"`
	Config  map[string]any `yaml:"config"`
}

var (
	nameRe  = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	tableRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$`)
)

// Timeout returns the effective per-attempt timeout.
func (t Task) timeout() time.Duration {
	if t.Timeout > 0 {
		return time.Duration(t.Timeout)
	}
	return DefaultTimeout
}

// MaxAttempts is 1 + retries.
func (t Task) MaxAttempts() int { return 1 + t.Retries }

// RetryDelay is the wait before attempt n+1 after attempt n (1-based) failed:
// backoff * 2^(n-1), capped at max_backoff.
func (t Task) RetryDelay(n int) time.Duration {
	base := time.Duration(t.Backoff)
	if base <= 0 {
		base = DefaultBackoff
	}
	max := time.Duration(t.MaxBackoff)
	if max <= 0 {
		max = DefaultMaxBackoff
	}
	d := base
	for i := 1; i < n; i++ {
		if d >= max {
			break
		}
		d *= 2
	}
	return min(d, max)
}

// Table returns "schema.table" for an on_snapshot entry (bare names are in main).
func normalizeTable(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if !strings.Contains(s, ".") {
		return "main." + s
	}
	return s
}

// SnapshotTables returns on_snapshot normalized to schema.table.
func (j Job) SnapshotTables() []string {
	out := make([]string, len(j.Triggers.OnSnapshot))
	for i, t := range j.Triggers.OnSnapshot {
		out[i] = normalizeTable(t)
	}
	return out
}

// Validate checks the definition: names, schedule, DAG (known dependencies, no
// cycles) and the params every task type needs.
func (j *Job) Validate() error {
	if !nameRe.MatchString(j.Name) {
		return fmt.Errorf("name %q must match %s", j.Name, nameRe)
	}
	if len(j.Tasks) == 0 {
		return errors.New("tasks is required (at least one)")
	}
	if j.Schedule != "" {
		if _, err := ParseSchedule(j.Schedule, j.Timezone); err != nil {
			return fmt.Errorf("schedule: %w", err)
		}
	} else if j.Timezone != "" {
		return errors.New("timezone needs a schedule")
	}
	switch j.Catchup {
	case "", CatchupNone, CatchupLast, CatchupAll:
	default:
		return fmt.Errorf("catchup %q must be none, last or all", j.Catchup)
	}
	if j.Catchup != "" && j.Schedule == "" {
		return errors.New("catchup needs a schedule")
	}
	for _, t := range j.Triggers.OnSnapshot {
		if !tableRe.MatchString(strings.ToLower(strings.TrimSpace(t))) {
			return fmt.Errorf("triggers.on_snapshot: %q must be schema.table", t)
		}
	}
	if g := j.Triggers.OnFile; g != "" {
		if _, err := filepath.Match(g, "x"); err != nil {
			return fmt.Errorf("triggers.on_file: %w", err)
		}
		if filepath.IsAbs(g) || strings.Contains(g, "..") {
			return fmt.Errorf("triggers.on_file %q must be relative to the project and stay inside it", g)
		}
	}
	ids := map[string]bool{}
	for i := range j.Tasks {
		t := &j.Tasks[i]
		if !nameRe.MatchString(t.ID) {
			return fmt.Errorf("task id %q must match %s", t.ID, nameRe)
		}
		if ids[t.ID] {
			return fmt.Errorf("task %q is defined twice", t.ID)
		}
		ids[t.ID] = true
		if !contains(taskTypes, t.Type) {
			return fmt.Errorf("task %s: type %q must be one of %s", t.ID, t.Type, strings.Join(taskTypes, ", "))
		}
		if t.Retries < 0 || t.Retries > 20 {
			return fmt.Errorf("task %s: retries must be between 0 and 20", t.ID)
		}
		if err := validateParams(t); err != nil {
			return fmt.Errorf("task %s: %w", t.ID, err)
		}
	}
	for _, t := range j.Tasks {
		for _, d := range t.DependsOn {
			if !ids[d] {
				return fmt.Errorf("task %s: depends_on %q is not a task of this job", t.ID, d)
			}
			if d == t.ID {
				return fmt.Errorf("task %s depends on itself", t.ID)
			}
		}
	}
	if _, err := j.Order(); err != nil {
		return err
	}
	for i, a := range j.Alerts.OnFailure {
		if a.Channel == "" {
			return fmt.Errorf("alerts.on_failure[%d]: channel is required", i)
		}
	}
	return nil
}

func validateParams(t *Task) error {
	str := func(k string) error {
		if s, _ := t.Params[k].(string); s == "" {
			return fmt.Errorf("%s task needs params.%s", t.Type, k)
		}
		return nil
	}
	list := func(k string) error {
		switch v := t.Params[k].(type) {
		case nil, []any:
			return nil
		default:
			return fmt.Errorf("params.%s must be a list, not %T", k, v)
		}
	}
	switch t.Type {
	case TypeShell:
		l, ok := t.Params["command"].([]any)
		if !ok || len(l) == 0 {
			return errors.New("shell task needs params.command as a list: [program, arg...] (no shell is involved)")
		}
	case TypePython:
		if err := str("script"); err != nil {
			return err
		}
		return list("args")
	case TypeNotebook:
		if err := str("notebook"); err != nil {
			return err
		}
		if _, ok := t.Params["args"].(map[string]any); t.Params["args"] != nil && !ok {
			return errors.New("params.args must be a map")
		}
	case TypePipeline:
		return list("targets")
	case TypeIngest:
		return list("names")
	}
	return nil
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// Order returns the task ids in a valid execution order (dependencies first,
// ties by declaration order) or an error naming a cycle.
func (j *Job) Order() ([]string, error) {
	deps := map[string][]string{}
	for _, t := range j.Tasks {
		deps[t.ID] = t.DependsOn
	}
	var out []string
	state := map[string]int{} // 1 visiting, 2 done
	var stack []string
	var visit func(id string) error
	visit = func(id string) error {
		switch state[id] {
		case 2:
			return nil
		case 1:
			i := 0
			for k, s := range stack {
				if s == id {
					i = k
				}
			}
			return fmt.Errorf("dependency cycle: %s", strings.Join(append(append([]string{}, stack[i:]...), id), " -> "))
		}
		state[id] = 1
		stack = append(stack, id)
		for _, d := range deps[id] {
			if err := visit(d); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = 2
		out = append(out, id)
		return nil
	}
	for _, t := range j.Tasks {
		if err := visit(t.ID); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Task returns the task with the given id.
func (j Job) Task(id string) (Task, bool) {
	for _, t := range j.Tasks {
		if t.ID == id {
			return t, true
		}
	}
	return Task{}, false
}

// Parse decodes and validates one definition. Unknown keys are errors.
func Parse(data []byte) (Job, error) {
	var j Job
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&j); err != nil {
		if errors.Is(err, io.EOF) {
			return j, errors.New("empty file")
		}
		return j, err
	}
	return j, j.Validate()
}

// Load reads every jobs/*.yaml (and .yml) of the project root, sorted by name.
// A missing folder yields no jobs. Names must be unique.
func Load(root string) ([]Job, error) {
	dir := filepath.Join(root, Dir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Job
	seen := map[string]string{}
	for _, e := range entries {
		ext := filepath.Ext(e.Name())
		if e.IsDir() || (ext != ".yaml" && ext != ".yml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path) //nolint:gosec // inside the project
		if err != nil {
			return nil, err
		}
		j, err := Parse(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Join(Dir, e.Name()), err)
		}
		j.File = path
		if prev, dup := seen[j.Name]; dup {
			return nil, fmt.Errorf("%s: name %q already defined in %s", e.Name(), j.Name, prev)
		}
		seen[j.Name] = e.Name()
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Name < out[k].Name })
	return out, nil
}
