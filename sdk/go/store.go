package kairo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
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
	Events []json.RawMessage
	Row    *RunRow
	// GuardParent atomically requires this run to be an unfinished workflow
	// before a new child row is written (ADR 0065).
	GuardParent string
	// GuardTreeSettled requires every descendant to be finished before this
	// workflow's terminal state is written (ADR 0062, 0065).
	GuardTreeSettled bool
	SetTimers        []TimerRow
	DeleteTimers     []uint32
	// Clear drops every timer and lease of the run (it ended).
	Clear     bool
	SetLeases []LeaseRow
	// EndLeases: steps (acts) whose leases end; their outcome is in.
	EndLeases []uint32
	// Notify: the run settled; tell the listeners at commit.
	Notify      bool
	SharedRates []SharedRateSettlement
}

// SharedRateRequest asks a store to admit one start against the shared
// request and actual-weight budgets of a destination (ADR 0078).
type SharedRateRequest struct {
	Destination    string
	RequestsPerMin int
	WeightPerMin   int
}

// SharedRateSettlement records what a completed action actually consumed.
// SQL stores apply it in the same transaction as the action's outcome.
type SharedRateSettlement struct {
	Destination    string
	RequestsPerMin int
	WeightPerMin   int
	Weight         int
	RetryAfter     time.Duration
}

// SharedRateStore is the optional Store capability used by shared
// destination limits. Implementations must apply Changes.SharedRates in the
// same transaction as the other Changes (ADR 0078).
type SharedRateStore interface {
	AdmitDestination(ctx context.Context, req SharedRateRequest) (wait time.Duration, err error)
	PruneDestinationRates(ctx context.Context, limit int) (int, error)
}

// ErrParentUnavailable means a run cannot safely be added to the requested
// parent's tree (ADR 0065).
var ErrParentUnavailable = errors.New("kairo: parent workflow is unavailable")

var errChildrenUnsettled = errors.New("kairo: children changed while settling the workflow")

func checkParent(parent *RunRow, parentID, childID string) error {
	var reason string
	switch {
	case parentID == childID:
		reason = "a run cannot be its own parent"
	case parent == nil:
		reason = "it does not exist"
	case parent.Plan != planWorkflow || parent.Workflow == "":
		reason = "it is not a workflow"
	case doneStatus[parent.Status]:
		reason = "it has finished"
	default:
		return nil
	}
	return fmt.Errorf("%w: %s: %s", ErrParentUnavailable, parentID, reason)
}

// Store keeps runs. WithRun locks a run's row (nil when absent), lets fn
// compute the changes and writes them, in one transaction. GuardParent and
// GuardTreeSettled are part of that transaction's contract (ADR 0065).
// Implementations must be safe for concurrent use. Open marks operation
// errors from custom stores with RetryWorkflow; errors returned by fn remain
// semantic errors.
type Store interface {
	Init(ctx context.Context) error
	WithRun(ctx context.Context, id string, fn func(row *RunRow) (*Changes, error)) error
	Get(ctx context.Context, id string) (*RunRow, error) // nil, nil: no such run
	// Children are the runs made by parent (ADR 0054's parent column).
	Children(ctx context.Context, parent string) ([]RunRow, error)
	DueTimers(ctx context.Context, now int64, limit int) ([]TimerRow, error)
	ExpiredLeases(ctx context.Context, now int64, limit int) ([]LeaseRow, error)
	// Recoverable returns this run's expired step leases and due timers. A
	// parent replay calls it when it reaches the same child again (ADR 0062).
	Recoverable(ctx context.Context, run string, now int64) ([]LeaseRow, []TimerRow, error)
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

// retryStore marks failures of Store operations for workflow replay (ADR
// 0063). Errors returned by WithRun's pure callback are Kairo semantic
// errors, not storage failures, and pass through unchanged.
type retryStore struct{ Store }

func storeRetry(ctx context.Context, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrParentUnavailable) || errors.Is(err, errChildrenUnsettled) || retryWorkflow(err) {
		return err
	}
	return RetryWorkflow(fmt.Errorf("kairo: store: %w", err))
}

func wrapStore(store Store) Store {
	r := retryStore{Store: store}
	if n, ok := store.(Notifier); ok {
		return retryNotifierStore{retryStore: r, notifier: n}
	}
	return r
}

func (s retryStore) Init(ctx context.Context) error { return storeRetry(ctx, s.Store.Init(ctx)) }
func (s retryStore) WithRun(ctx context.Context, id string, fn func(*RunRow) (*Changes, error)) error {
	var callbackErr error
	err := s.Store.WithRun(ctx, id, func(row *RunRow) (*Changes, error) {
		ch, err := fn(row)
		if err != nil {
			callbackErr = err
		}
		return ch, err
	})
	if callbackErr != nil {
		return err
	}
	return storeRetry(ctx, err)
}
func (s retryStore) Get(ctx context.Context, id string) (*RunRow, error) {
	r, err := s.Store.Get(ctx, id)
	return r, storeRetry(ctx, err)
}
func (s retryStore) Children(ctx context.Context, parent string) ([]RunRow, error) {
	r, err := s.Store.Children(ctx, parent)
	return r, storeRetry(ctx, err)
}
func (s retryStore) DueTimers(ctx context.Context, now int64, limit int) ([]TimerRow, error) {
	r, err := s.Store.DueTimers(ctx, now, limit)
	return r, storeRetry(ctx, err)
}
func (s retryStore) ExpiredLeases(ctx context.Context, now int64, limit int) ([]LeaseRow, error) {
	r, err := s.Store.ExpiredLeases(ctx, now, limit)
	return r, storeRetry(ctx, err)
}
func (s retryStore) Recoverable(ctx context.Context, run string, now int64) ([]LeaseRow, []TimerRow, error) {
	l, t, err := s.Store.Recoverable(ctx, run, now)
	return l, t, storeRetry(ctx, err)
}
func (s retryStore) RenewLeases(ctx context.Context, owner string, until int64) error {
	return storeRetry(ctx, s.Store.RenewLeases(ctx, owner, until))
}
func (s retryStore) ExpireLeases(ctx context.Context, owner string, now int64) error {
	return storeRetry(ctx, s.Store.ExpireLeases(ctx, owner, now))
}
func (s retryStore) HandOver(ctx context.Context, l LeaseRow) error {
	return storeRetry(ctx, s.Store.HandOver(ctx, l))
}
func (s retryStore) NextWake(ctx context.Context) (int64, bool, error) {
	at, ok, err := s.Store.NextWake(ctx)
	return at, ok, storeRetry(ctx, err)
}
func (s retryStore) RemoveFinished(ctx context.Context, cutoff int64, limit int) (int, error) {
	n, err := s.Store.RemoveFinished(ctx, cutoff, limit)
	return n, storeRetry(ctx, err)
}
func (s retryStore) ClaimDrive(ctx context.Context, l LeaseRow, now int64) (bool, error) {
	ok, err := s.Store.ClaimDrive(ctx, l, now)
	return ok, storeRetry(ctx, err)
}
func (s retryStore) EndDrive(ctx context.Context, run, owner string, token int32, now int64, resume bool) error {
	return storeRetry(ctx, s.Store.EndDrive(ctx, run, owner, token, now, resume))
}
func (s retryStore) List(ctx context.Context, f ListFilter) ([]RunRow, error) {
	r, err := s.Store.List(ctx, f)
	return r, storeRetry(ctx, err)
}
func (s retryStore) Close() error { return storeRetry(context.Background(), s.Store.Close()) }

type retryNotifierStore struct {
	retryStore
	notifier Notifier
}

func (s retryNotifierStore) Listen(ctx context.Context, settled func(string)) (func(), error) {
	stop, err := s.notifier.Listen(ctx, settled)
	return stop, storeRetry(ctx, err)
}

var doneStatus = map[string]bool{"completed": true, "failed": true, "cancelled": true}

// settledStatus: a run that will not go on by itself (finished, or stopped
// for review).
func settledStatus(s string) bool { return doneStatus[s] || s == "blocked" }
