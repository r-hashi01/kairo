package kairo

import (
	"context"
	"encoding/json"
)

// Where the embedded runtime keeps runs (ADR 0051): per run its state (the
// core's encoding), its input and its events (appended only), the timers
// armed and the leases of the steps running. The tables are the
// TypeScript and Python SDKs' (sdk/ts/src/store.ts): processes of any of
// them may share one database.

// RunRow is a run as stored.
type RunRow struct {
	ID        string
	Plan      string
	Hash      string
	State     []byte
	Input     json.RawMessage // nil: NULL
	Status    string
	Output    json.RawMessage // nil: NULL
	Error     string
	Seq       int64 // events recorded so far
	CreatedAt int64
	UpdatedAt int64
	// Parent is the run that made this one (a workflow, of its calls):
	// kept and removed with it (ADR 0054). Empty: none.
	Parent string
	// Workflow names the workflow of a workflow run (empty for a call), and
	// Meta is what the application gave it when it started (ADR 0059).
	Workflow string
	Meta     json.RawMessage // nil: NULL
}

// ListFilter selects root runs for List (ADR 0059): those of Workflow, in
// Status, created in [Since, Until) (unix ms; 0: unbounded), after the run
// After in creation order, at most Limit.
type ListFilter struct {
	Workflow, Status string
	Since, Until     int64
	After            string
	Limit            int
}

// TimerRow is an armed timer.
type TimerRow struct {
	Run   string
	Timer uint32
	Act   uint32
	At    int64
}

// LeaseRow is a step held by a process while it runs (ADR 0051). Act 0
// (no step has it) is a workflow's drive lease (ADR 0059): the process that
// runs the workflow's function, with a token in Attempt.
type LeaseRow struct {
	Run     string
	Act     uint32
	Attempt int32
	Owner   string
	Until   int64 // unix ms: past it, the owner is taken to have stopped
}

// Changes is what a transaction on a run writes, computed from the row as
// read.
type Changes struct {
	Events       []json.RawMessage
	Row          *RunRow
	SetTimers    []TimerRow
	DeleteTimers []uint32
	// Clear drops every timer and lease of the run (it ended).
	Clear     bool
	SetLeases []LeaseRow
	// EndLeases: steps (acts) whose leases end; their outcome is in.
	EndLeases []uint32
	// Notify: the run settled; tell the listeners at commit.
	Notify bool
}

// Store keeps runs. WithRun locks a run's row (nil when absent), lets fn
// compute the changes and writes them, in one transaction. Implementations
// must be safe for concurrent use.
type Store interface {
	Init(ctx context.Context) error
	WithRun(ctx context.Context, id string, fn func(row *RunRow) (*Changes, error)) error
	Get(ctx context.Context, id string) (*RunRow, error) // nil, nil: no such run
	// Children are the runs made by parent (ADR 0054's parent column).
	Children(ctx context.Context, parent string) ([]RunRow, error)
	DueTimers(ctx context.Context, now int64, limit int) ([]TimerRow, error)
	ExpiredLeases(ctx context.Context, now int64, limit int) ([]LeaseRow, error)
	RenewLeases(ctx context.Context, owner string, until int64) error
	// ExpireLeases ends owner's leases now (a process that knows its
	// earlier self stopped).
	ExpireLeases(ctx context.Context, owner string, now int64) error
	// HandOver gives a step's lease to l.Owner until l.Until (ADR 0052);
	// nothing if the lease is gone.
	HandOver(ctx context.Context, l LeaseRow) error
	// NextWake is when something is next to do: the earliest timer or
	// lease expiry (ADR 0053); ok false if neither.
	NextWake(ctx context.Context) (at int64, ok bool, err error)
	// RemoveFinished removes finished trees of runs (ADR 0054); at most
	// limit; returns how many.
	RemoveFinished(ctx context.Context, cutoff int64, limit int) (int, error)
	// ClaimDrive sets the drive lease l (act 0) unless another owner holds
	// it unexpired at now; ok says whether it did (ADR 0059).
	ClaimDrive(ctx context.Context, l LeaseRow, now int64) (ok bool, err error)
	// EndDrive ends the drive lease of run held by owner with token: it is
	// removed, or (resume) left expired at now, owned by none (renewals do
	// not touch it), for a tick to take up.
	EndDrive(ctx context.Context, run, owner string, token int32, now int64, resume bool) error
	// List returns root runs (no parent) in creation order (ADR 0059).
	List(ctx context.Context, f ListFilter) ([]RunRow, error)
	Close() error
}

// Notifier is implemented by stores that hear of runs settled in other
// processes (PostgreSQL's LISTEN/NOTIFY, sdk/go/pgnotify).
type Notifier interface {
	Listen(ctx context.Context, settled func(runID string)) (stop func(), err error)
}

var doneStatus = map[string]bool{"completed": true, "failed": true, "cancelled": true}

// settledStatus: a run that will not go on by itself (finished, or stopped
// for review).
func settledStatus(s string) bool { return doneStatus[s] || s == "blocked" }
