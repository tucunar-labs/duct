package contract

import (
	"context"
	"errors"
	"time"
)

// ErrConsoleUnavailable is returned by the console interfaces below when the
// server did not wire the capability (for example Jobs without a jobs/ folder).
var ErrConsoleUnavailable = errors.New("not available on this server")

// AdminCaller is an OPTIONAL interface of a QueryHandler for the web console:
// it runs one call of the admin API (/admin/v1, specs/admin.openapi.yaml) as
// the already-authenticated identity, so the console reuses every rule of the
// API instead of duplicating it. path is the full path ("/admin/v1/users"),
// body the JSON request (nil: none). Only the admin role is served: any other
// identity gets status 403. The call is audited under the protocol that made
// it. resp is the JSON response (secrets, such as a new token or temporary
// password, appear only in the response of the call that created them).
type AdminCaller interface {
	AdminCall(ctx context.Context, id Identity, method, path string, body []byte) (status int, resp []byte, err error)
}

// AuditQuery selects audit rows for AuditReader. Zero fields do not filter.
type AuditQuery struct {
	User, Protocol string
	Status         string // ok, error, denied or quota
	From, To       time.Time
	Limit, Offset  int
}

// AuditEntry is one audited statement; SQL and Error are already redacted.
type AuditEntry struct {
	At       time.Time
	User     string
	Role     string
	Protocol string
	Status   string
	Duration time.Duration
	Rows     int64
	Cached   bool
	Hits     int64
	SQL      string
	Error    string
}

// AuditReader is an OPTIONAL interface of a QueryHandler: a page of the audit
// log, newest first, with the total number of rows matching q. Only the admin
// role may read it (others get a permission error).
type AuditReader interface {
	AuditPage(ctx context.Context, id Identity, q AuditQuery) (entries []AuditEntry, total int64, err error)
}

// Job and run statuses as the orchestrator records them.
const (
	RunPending   = "pending"
	RunRunning   = "running"
	RunSuccess   = "success"
	RunFailed    = "failed"
	RunSkipped   = "skipped"
	RunCancelled = "cancelled"
)

// JobTaskDef is a task of a job's DAG as defined in jobs/<name>.yaml.
type JobTaskDef struct {
	ID        string
	Type      string
	DependsOn []string
	Retries   int
	Timeout   time.Duration
}

// JobRunInfo is one run of a job (a row of the run history).
type JobRunInfo struct {
	ID           int64
	Job          string
	Trigger      string // cron, manual, snapshot or file
	Detail       string // free text: who ran it, which files or tables fired it
	Status       string
	ScheduledFor time.Time
	StartedAt    time.Time
	FinishedAt   time.Time // zero while running
	Error        string
	Tasks        int // tasks in the run
	Attempts     int // attempts made so far, summed over its tasks
}

// JobTaskRun is the state of one task inside a run.
type JobTaskRun struct {
	ID          string
	Type        string
	DependsOn   []string
	Status      string
	Attempts    int
	MaxAttempts int
	StartedAt   time.Time
	FinishedAt  time.Time
	Duration    time.Duration
	Error       string
	Output      string // tail of the output, clipped by the orchestrator
}

// JobInfo describes a job: its definition plus what the console shows in a list.
type JobInfo struct {
	Name       string
	Schedule   string // cron expression, descriptor or "@every ..."; "" = no schedule
	Timezone   string
	Catchup    string
	OnSnapshot []string // tables (schema.table) whose new snapshots start a run
	OnFile     string   // glob polled for new files
	Alerts     []string // alert channels notified on failure
	Tasks      []JobTaskDef
	Enabled    bool      // false: schedule and triggers are paused (manual runs still work)
	NextFire   time.Time // next scheduled tick; zero if none or paused
	LastRun    *JobRunInfo
}

// JobsConsole is an OPTIONAL interface of a QueryHandler for the web console:
// read the jobs and their runs (any role) and act on them (admin role only;
// others get a permission error). RunJob starts a run now (trigger "manual")
// and returns its id without waiting; the workers of `duct serve` execute it.
// CancelJobRun stops a running run (its running tasks are interrupted, the
// pending ones are cancelled). SetJobEnabled pauses or resumes the schedule
// and the triggers of a job; the setting lives in the Duct state, so every
// process sees it. Every action is audited.
type JobsConsole interface {
	Jobs(ctx context.Context) ([]JobInfo, error)
	JobRuns(ctx context.Context, job string, limit int) ([]JobRunInfo, error)
	JobRun(ctx context.Context, runID int64) (JobRunInfo, []JobTaskRun, bool, error)
	RunJob(ctx context.Context, id Identity, job string) (runID int64, err error)
	CancelJobRun(ctx context.Context, id Identity, runID int64) error
	SetJobEnabled(ctx context.Context, id Identity, job string, enabled bool) error
}
