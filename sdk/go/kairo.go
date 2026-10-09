// Package kairo is kairo's Go SDK (ADR 0058): workflows written as code,
// run by the runtime embedded in this process with a database (ADR 0051).
// The core runs here directly, not through WASM. It speaks the TypeScript
// and Python SDKs' tables, call ids and contracts, so processes of any of
// them may share one database.
//
//	k, _ := kairo.Open(ctx, kairo.Options{Store: kairo.NewSQLStore(db, kairo.SQLite)})
//	kairo.Action(k, "classify", kairo.Unprotected, func(t *kairo.TaskContext, in Req) (Verdict, error) { ... })
//	kairo.Workflow(k, "refund", func(ctx *kairo.Context, in Req) (Receipt, error) {
//		v, err := kairo.Call[Verdict](ctx, "classify", in)
//		...
//	})
//	_ = k.Start(ctx)
//	out, err := kairo.Run[Receipt](ctx, k, "refund", req, kairo.WithID("refund-o-42"))
package kairo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r-hashi01/kairo/wasmcore"
)

const (
	planCall     = "kairo.call/"
	planWait     = "kairo.wait/"
	planWorkflow = "kairo.workflow"
	builtinNow   = "kairo.now"
	builtinRand  = "kairo.random"
	builtinSleep = "kairo.sleep"
)

// ErrSuspended: in suspend mode, the workflow waits (a timer, a signal);
// Tick or Signal drives it on later.
var ErrSuspended = errors.New("kairo: the workflow waits")

// ErrCancelled: the workflow was cancelled.
var ErrCancelled = errors.New("kairo: cancelled")

// ErrStopped: the caller stopped waiting (its context ended), or the
// process is closing. The workflow is not cancelled: it goes on (ADR 0059).
var ErrStopped = errors.New("kairo: stopped waiting for the workflow")

// ErrTimedOut: a wait's timeout came before its signal (WaitTimeout).
var ErrTimedOut = errors.New("kairo: the wait timed out")

// CallError is a call that did not complete: it failed (its action's
// failure, after its retries) or was cancelled (errors.Is ErrCancelled).
type CallError struct {
	RunID   string // the call's run
	Action  string // the action called ("" for a wait)
	Status  string // "failed" or "cancelled"
	Message string
}

func (e *CallError) Error() string {
	what := e.RunID
	if e.Action != "" {
		what += " (" + e.Action + ")"
	}
	if e.Message == "" {
		return "call " + what + " " + e.Status
	}
	return "call " + what + " " + e.Status + ": " + e.Message
}

func (e *CallError) Unwrap() error {
	if e.Status == "cancelled" {
		return ErrCancelled
	}
	return nil
}

// WorkflowError is a workflow that failed: its function returned an error
// (Err, when it failed in this process: errors.As finds a CallError in it)
// or panicked. Message is what is recorded.
type WorkflowError struct {
	ID      string
	Message string
	Err     error
}

func (e *WorkflowError) Error() string { return "workflow " + e.ID + " failed: " + e.Message }
func (e *WorkflowError) Unwrap() error { return e.Err }

// errDrivenElsewhere: another process holds the workflow's drive lease.
var errDrivenElsewhere = errors.New("kairo: the workflow is driven by another process")

// Effect is what an action does to the world (ADR 0049).
type Effect string

const (
	// Unprotected: may run again (an LLM call, a read).
	Unprotected Effect = "unprotected"
	// Real: acts on the world (a payment, a write); recorded before it
	// runs, never run twice, stopped for review when its outcome is unknown.
	Real Effect = "real"
)

// Mode is how calls that wait behave.
type Mode int

const (
	// Wait: a call waits in this process (a resident process).
	Wait Mode = iota
	// Suspend: a call that waits makes the workflow return ErrSuspended;
	// Tick and Signal drive it on (serverless, ADR 0051).
	Suspend
)

// Forever keeps finished runs for ever (Options.KeepFinished).
const Forever time.Duration = -1

// Options configure Open.
type Options struct {
	// Store keeps the runs: NewSQLStore for SQLite or PostgreSQL, or
	// NewMemStore.
	Store Store
	Mode  Mode
	// Owner names this process in the leases (default: random). A process
	// with the name its earlier self had takes that self's steps as stopped
	// when it opens.
	Owner string
	// Lease: how long a step is leased to this process without renewal
	// (default 30s).
	Lease time.Duration
	// KeepFinished: how long a finished tree of runs is kept (ADR 0054).
	// Default: KAIRO_KEEP_FINISHED ("30m", "24h", "7d", "forever"), or 24h.
	KeepFinished time.Duration
	// Wake, in suspend mode, is told when something is next to do before
	// Run, Tick and Signal return (ADR 0053): schedule a Tick then.
	Wake func(at time.Time) error
	// Concurrency: at most this many steps of actions run here at once
	// (default: no limit). Steps over it wait, leased, for a slot.
	Concurrency int
	// Logger takes what goes wrong in the runtime's background work (lease
	// renewals, sweeps, callbacks; default slog.Default()).
	Logger *slog.Logger
	// Observe, if set, is given each Observation as it happens: runs that
	// start and settle, steps' attempts that start and finish. It is called
	// on the runtime's goroutines: it must not block.
	Observe func(Observation)
	// HTTP configures actions over HTTP(S) and /tick (ADR 0052, 0053).
	HTTP HTTPOptions
	// Now is the clock (tests).
	Now func() time.Time
}

// Kairo declares actions and workflows and drives them.
type Kairo struct {
	rt      *runtime
	mode    Mode
	wake    func(time.Time) error
	nowFn   func() time.Time
	closing atomic.Bool
	http    HTTPOptions
	client  *http.Client

	mu         sync.Mutex
	actions    map[string]*actionDef
	workflows  map[string]workflowFn
	planned    map[string]bool
	driving    map[*drive]struct{}
	drivingIDs map[string]bool
	again      map[string]bool
	stopHook   func()

	redriveMu   sync.Mutex
	redriveCond *sync.Cond
	redrivesN   int

	// Wait mode (ADR 0059): workflows driven in the background here (by
	// id), the sweep that takes up those whose driver stopped, and what
	// ends them when the process closes.
	owned     map[string]bool
	bgCtx     context.Context
	bgCancel  context.CancelFunc
	bgWG      sync.WaitGroup
	sweepStop chan struct{}

	concurrency int
	slots       chan struct{}       // Concurrency (nil: no limit)
	limits      map[string]*limiter // by destination
}

type drive struct{ cancel context.CancelFunc }

type workflowFn func(ctx *Context, in json.RawMessage) (any, error)

type actionDef struct {
	spec    map[string]any
	handler func(t *TaskContext, in json.RawMessage) (any, error)
	url     string // called over HTTP(S) (ADR 0052)
	async   bool
	limit   int // steps at once (0: no limit)
	rate    int // steps started a minute (0: no limit)
}

// Open opens the runtime on opts.Store. Declare actions and workflows, then
// Start.
func Open(ctx context.Context, opts Options) (*Kairo, error) {
	if opts.Store == nil {
		return nil, errors.New("kairo: Options.Store is required")
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	lease := opts.Lease
	if lease <= 0 {
		lease = 30 * time.Second
	}
	keep, err := keepFinished(opts.KeepFinished, os.Getenv("KAIRO_KEEP_FINISHED"))
	if err != nil {
		return nil, err
	}
	owner := opts.Owner
	if owner == "" {
		owner = newID()
	}
	keepMs := int64(-1)
	if keep >= 0 {
		keepMs = keep.Milliseconds()
	}
	rt := newRuntime(opts.Store, func() int64 { return nowFn().UnixMilli() }, owner, lease.Milliseconds(), keepMs)
	if opts.Logger != nil {
		rt.logger = opts.Logger
	}
	rt.observer = opts.Observe
	if err := rt.open(ctx, opts.Owner != ""); err != nil {
		return nil, err
	}
	k := &Kairo{rt: rt, mode: opts.Mode, wake: opts.Wake, nowFn: nowFn, http: opts.HTTP, actions: map[string]*actionDef{}, workflows: map[string]workflowFn{},
		planned: map[string]bool{}, driving: map[*drive]struct{}{}, drivingIDs: map[string]bool{}, again: map[string]bool{},
		owned: map[string]bool{}, concurrency: opts.Concurrency, limits: map[string]*limiter{}}
	k.bgCtx, k.bgCancel = context.WithCancel(context.Background())
	k.redriveCond = sync.NewCond(&k.redriveMu)
	k.client = k.httpClient()
	return k, nil
}

var durationRE = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s|m|h|d)$`)

// keepFinished is opt, or KAIRO_KEEP_FINISHED (env), or 24 hours; Forever
// keeps runs for ever.
func keepFinished(opt time.Duration, env string) (time.Duration, error) {
	if opt != 0 {
		return opt, nil
	}
	if env == "" {
		return 24 * time.Hour, nil
	}
	if env == "forever" {
		return Forever, nil
	}
	m := durationRE.FindStringSubmatch(env)
	if m == nil {
		return 0, fmt.Errorf("KAIRO_KEEP_FINISHED=%s: a duration such as 30m, 24h, 7d, or forever", env)
	}
	n, _ := strconv.ParseFloat(m[1], 64)
	unit := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour}[m[2]]
	return time.Duration(n * float64(unit)), nil
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// newToken is a drive lease's token (never 0: no lease).
func newToken() int32 {
	for {
		if t := mrand.Int32(); t != 0 {
			return t
		}
	}
}

// ActionOption configures an action.
type ActionOption func(spec map[string]any)

// Timeout: how long a step may run before its outcome is unknown.
func Timeout(d time.Duration) ActionOption {
	return func(s map[string]any) { s["timeout"] = d.String() }
}

// Destination: the rate-limit key (default: the action).
func Destination(name string) ActionOption {
	return func(s map[string]any) { s["destination"] = name }
}

// MaxAttempts: a step of the action is tried at most n times when it
// fails retryably (Retryable; default 3, or 1 for a Real action without
// IdempotentRetry: such a step is retried only when it did not take
// effect).
func MaxAttempts(n int) ActionOption {
	return func(s map[string]any) { s["max_attempts"] = n }
}

// Backoff: the wait before a step's second attempt, doubled for each one
// after (default 200ms).
func Backoff(d time.Duration) ActionOption {
	return func(s map[string]any) { s["backoff"] = d.String() }
}

// Limit: at most n steps of the action (of its Destination, if it has
// one) run here at once (ADR 0059).
func Limit(n int) ActionOption {
	return func(s map[string]any) { s["$limit"] = n }
}

// Rate: at most perMinute steps of the action (of its Destination) start
// here a minute, spaced evenly (ADR 0059).
func Rate(perMinute int) ActionOption {
	return func(s map[string]any) { s["$rate"] = perMinute }
}

// IdempotentRetry declares that a real action may run again with the same
// idempotency key when its outcome is unknown (instead of stopping for
// review).
func IdempotentRetry() ActionOption {
	return func(s map[string]any) { s["idempotent_retry"] = true }
}

// TaskContext is what an action's handler gets: cancelled when the step is
// (a timeout, its workflow cancelled, the process closing).
type TaskContext struct {
	context.Context
	RunID, StepID, IdempotencyKey string
	Attempt                       int32
	emit                          func([]byte)
}

// Emit sends a chunk of live output to subscribers (Kairo.Subscribe); it
// is not recorded.
func (t *TaskContext) Emit(chunk []byte) { t.emit(chunk) }

// stepError carries how a failure is to be taken.
type stepError struct {
	err       error
	retryable bool
	unknown   bool
}

func (e *stepError) Error() string { return e.err.Error() }
func (e *stepError) Unwrap() error { return e.err }

// Retryable marks an action's failure as one that may be retried (it did
// not take effect).
func Retryable(err error) error { return &stepError{err: err, retryable: true} }

// Unknown marks an action's failure as an unknown outcome (it may have
// taken effect): never taken as success (invariant 5).
func Unknown(err error) error { return &stepError{err: err, unknown: true, retryable: true} }

// Action declares an action; its handler runs the steps of it here.
func Action[I, O any](k *Kairo, name string, effect Effect, fn func(t *TaskContext, in I) (O, error), opts ...ActionOption) {
	if strings.HasPrefix(name, "kairo.") {
		panic(fmt.Sprintf(`kairo: action %s: names beginning with "kairo." are kairo's`, name))
	}
	spec := map[string]any{"action": name, "effect": string(effect)}
	for _, o := range opts {
		o(spec)
	}
	u, _ := spec["$url"].(string)
	async, _ := spec["$async"].(bool)
	limit, _ := spec["$limit"].(int)
	rate, _ := spec["$rate"].(int)
	for _, key := range []string{"$url", "$async", "$limit", "$rate"} {
		delete(spec, key)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.actions[name] = &actionDef{spec: spec, url: u, async: async, limit: limit, rate: rate, handler: func(t *TaskContext, raw json.RawMessage) (any, error) {
		var in I
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, fmt.Errorf("action %s: its input: %w", name, err)
			}
		}
		return fn(t, in)
	}}
}

// Workflow declares a workflow.
func Workflow[I, O any](k *Kairo, name string, fn func(ctx *Context, in I) (O, error)) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.workflows[name] = func(ctx *Context, raw json.RawMessage) (any, error) {
		var in I
		if len(raw) > 0 && string(raw) != "null" {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, fmt.Errorf("workflow %s: its input: %w", name, err)
			}
		}
		return fn(ctx, in)
	}
}

// Start registers the actions and their plans, and starts running the
// actions' steps.
func (k *Kairo) Start(ctx context.Context) error {
	k.mu.Lock()
	specs := make([]map[string]any, 0, len(k.actions)+3)
	remote := false
	if k.concurrency > 0 {
		k.slots = make(chan struct{}, k.concurrency)
	}
	for name, a := range k.actions {
		if a.limit > 0 || a.rate > 0 {
			// Actions of one destination share its limits: the strictest.
			key := destination(name, a)
			l := k.limits[key]
			if l == nil {
				l = &limiter{}
				k.limits[key] = l
			}
			l.tighten(a.limit, a.rate)
		}
		specs = append(specs, a.spec)
		if a.url != "" {
			remote = true
			if err := checkURL(a.url, k.http.AllowInsecure); err != nil {
				k.mu.Unlock()
				return err
			}
		}
	}
	k.mu.Unlock()
	if remote {
		// Actions over HTTP(S) (ADR 0052) take their outcomes on the callback.
		if k.http.Secret == "" || k.http.CallbackURL == "" {
			return errors.New("kairo: actions with a URL need HTTP.Secret and HTTP.CallbackURL")
		}
		if err := checkURL(k.http.CallbackURL, k.http.AllowInsecure); err != nil {
			return err
		}
	}
	for _, b := range []string{builtinNow, builtinRand, builtinSleep} {
		specs = append(specs, map[string]any{"action": b, "effect": "unprotected"})
	}
	js, err := json.Marshal(specs)
	if err != nil {
		return err
	}
	if err := k.rt.registerActions(js, k.serve); err != nil {
		return err
	}
	for _, s := range specs {
		a := s["action"].(string)
		root := map[string]any{"kind": "step", "id": "call", "action": a, "input": map[string]any{"in": "$input.in"}}
		if err := k.plan(planCall+a, root, nil); err != nil {
			return err
		}
	}
	if err := k.planWorkflow(); err != nil {
		return err
	}
	k.rt.mu.Lock()
	k.rt.lostDrive, k.rt.missingPlan = k.lostDrive, k.missingPlan
	k.rt.leaseParents = k.mode == Suspend
	k.rt.mu.Unlock()
	if k.mode == Suspend {
		// A call that settles drives its workflow on (its id is the
		// workflow's id, "/", the call's key).
		k.stopHook = k.rt.onSettled(func(r RunInfo) {
			if i := strings.LastIndex(r.RunID, "/"); i > 0 {
				k.redrive(r.RunID[:i])
			}
		})
		return nil
	}
	// Wait mode: a resident process takes up, once a lease period, what
	// processes that stopped left (ADR 0059): their workflows, steps and
	// timers. One goroutine for the process, not one for each run.
	k.sweepStop = make(chan struct{})
	k.bgWG.Add(1)
	go func(stop chan struct{}) {
		defer k.bgWG.Done()
		t := time.NewTicker(time.Duration(k.rt.leaseMs) * time.Millisecond)
		defer t.Stop()
		for {
			if err := k.rt.tick(k.bgCtx); err != nil && k.bgCtx.Err() == nil {
				k.rt.logger.Warn("kairo: sweeping", "err", err)
			}
			k.rt.recheck(k.bgCtx)
			select {
			case <-stop:
				return
			case <-t.C:
			}
		}
	}(k.sweepStop)
	return nil
}

// missingPlan registers a plan that this process has not used yet but
// another one made a run of: a wait, with its timeout (ADR 0059).
func (k *Kairo) missingPlan(name string) bool {
	rest, ok := strings.CutPrefix(name, planWait)
	if !ok {
		return false
	}
	signal, ms := rest, int64(0)
	if i := strings.LastIndex(rest, "@"); i >= 0 {
		if n, err := strconv.ParseInt(rest[i+1:], 10, 64); err == nil && n > 0 {
			signal, ms = rest[:i], n
		}
	}
	return k.plan(name, waitRoot(signal, ms), nil) == nil
}

func (k *Kairo) planWorkflow() error {
	return k.plan(planWorkflow, map[string]any{"kind": "wait", "id": "done", "signal": "done"},
		map[string]any{"started_at": map[string]any{"type": "integer", "value": 0}})
}

func (k *Kairo) plan(name string, root, vars map[string]any) error {
	k.mu.Lock()
	done := k.planned[name]
	k.mu.Unlock()
	if done {
		return nil
	}
	def := map[string]any{"name": name, "root": root}
	if vars != nil {
		def["vars"] = vars
	}
	js, err := json.Marshal(def)
	if err != nil {
		return err
	}
	if _, err := k.rt.registerPlan(js); err != nil {
		return err
	}
	k.mu.Lock()
	k.planned[name] = true
	k.mu.Unlock()
	return nil
}

// serve runs a dispatched step: a builtin, or a declared action.
func (k *Kairo) serve(ctx context.Context, t task, emit func([]byte)) result {
	var in struct {
		In json.RawMessage `json:"in"`
	}
	if len(t.Input) > 0 {
		_ = json.Unmarshal(t.Input, &in)
	}
	now := k.nowFn().UnixMilli()
	switch t.Action {
	case builtinNow:
		return result{Output: json.RawMessage(strconv.FormatInt(now, 10))}
	case builtinRand:
		out, _ := json.Marshal(mrand.Float64())
		return result{Output: out}
	case builtinSleep:
		// Waits in kairo, not here (ADR 0045).
		var s struct {
			Ms float64 `json:"ms"`
		}
		_ = json.Unmarshal(in.In, &s)
		return result{Wait: &waitResult{Until: now + int64(math.Round(s.Ms))}}
	}
	k.mu.Lock()
	a := k.actions[t.Action]
	k.mu.Unlock()
	if a == nil {
		return result{Err: "no action " + t.Action + " here"}
	}
	// Over a limit, the step waits here for a slot, leased (ADR 0059).
	release, err := k.admit(ctx, t.Action, a)
	if err != nil {
		return result{Err: err.Error(), Retryable: true, ErrType: "cancelled"}
	}
	defer release()
	if a.url != "" {
		return k.callRemote(ctx, t, a, in.In)
	}
	tc := &TaskContext{Context: ctx, RunID: t.RunID, StepID: t.StepID, IdempotencyKey: t.IdemKey, Attempt: t.Attempt, emit: emit}
	return k.runHandler(a, tc, t.Action, in.In)
}

// runHandler runs an action's handler and takes its outcome.
func (k *Kairo) runHandler(a *actionDef, tc *TaskContext, action string, in json.RawMessage) result {
	out, err := a.handler(tc, in)
	if err != nil {
		var se *stepError
		if errors.As(err, &se) {
			return result{Err: err.Error(), Retryable: se.retryable, Unknown: se.unknown, ErrType: "error"}
		}
		return result{Err: err.Error(), ErrType: "error"}
	}
	js, err := json.Marshal(out)
	if err != nil {
		return result{Err: "action " + action + ": its output: " + err.Error()}
	}
	return result{Output: js}
}

// RunOption configures Run.
type RunOption func(*runOpts)

type runOpts struct {
	id   string
	meta map[string]any
}

// WithID runs the workflow as execution id (an idempotency key): running it
// again resumes it, or returns its result.
func WithID(id string) RunOption { return func(o *runOpts) { o.id = id } }

// WithMeta gives the workflow's run meta, kept with it (RunInfo.Meta) and
// returned by List; set when it starts (ADR 0059).
func WithMeta(meta map[string]any) RunOption { return func(o *runOpts) { o.meta = meta } }

// Run runs workflow name, or resumes it, and returns its result: calls that
// finished return their recorded results. In wait mode the workflow is
// driven in the background (by this process, or by whichever process holds
// it): a ctx that ends returns ErrStopped and the workflow goes on. In
// suspend mode it is driven here until it waits: ErrSuspended.
func Run[O any](ctx context.Context, k *Kairo, name string, in any, opts ...RunOption) (O, error) {
	var out O
	id, token, raw, err := k.start(ctx, name, in, opts)
	if err != nil {
		return out, err
	}
	var res json.RawMessage
	if k.mode == Suspend {
		res, err = k.drive(ctx, id, name, raw, "", token)
		if errors.Is(err, errDrivenElsewhere) {
			err = ErrSuspended
		}
		if werr := k.wakeUp(ctx); err == nil && werr != nil {
			err = werr
		}
	} else {
		k.driveBackground(id, name, raw, "", token)
		res, err = k.await(ctx, id)
	}
	if err != nil {
		return out, err
	}
	if len(res) > 0 && string(res) != "null" {
		err = json.Unmarshal(res, &out)
	}
	return out, err
}

// Submit starts workflow name and returns its id once the start is
// recorded, without waiting for it (ADR 0059): Await its result, from any
// process. In wait mode it is driven in the background; in suspend mode it
// is driven here until it waits. The id is an idempotency key, as in Run.
func (k *Kairo) Submit(ctx context.Context, name string, in any, opts ...RunOption) (string, error) {
	id, token, raw, err := k.start(ctx, name, in, opts)
	if err != nil {
		return "", err
	}
	if k.mode == Wait {
		k.driveBackground(id, name, raw, "", token)
		return id, nil
	}
	// Its outcome (suspended, failed, ...) is recorded: Await reads it.
	_, _ = k.drive(ctx, id, name, raw, "", token)
	return id, k.wakeUp(ctx)
}

// Await returns workflow id's result once it has finished. In wait mode
// it waits; a ctx that ends returns ErrStopped (the workflow goes on). In
// suspend mode it does not wait: ErrSuspended while it has not finished.
func Await[O any](ctx context.Context, k *Kairo, id string) (O, error) {
	var out O
	res, err := k.await(ctx, id)
	if err != nil {
		return out, err
	}
	if len(res) > 0 && string(res) != "null" {
		err = json.Unmarshal(res, &out)
	}
	return out, err
}

func (k *Kairo) await(ctx context.Context, id string) (json.RawMessage, error) {
	if k.mode == Suspend {
		ri, err := k.rt.get(ctx, id)
		if err != nil {
			return nil, err
		}
		if !ri.Finished() {
			return nil, ErrSuspended
		}
		return done(id, ri)
	}
	// Until it finishes, the caller stops waiting, or the process closes.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(k.bgCtx, cancel)()
	ri, err := k.rt.waitDone(wctx, id)
	if err != nil {
		if wctx.Err() != nil {
			return nil, fmt.Errorf("workflow %s: %w", id, ErrStopped)
		}
		return nil, err
	}
	return done(id, ri)
}

// start makes workflow name's run (or finds it). A new one is leased to
// this process to drive in the same transaction: token (0: it existed).
func (k *Kairo) start(ctx context.Context, name string, in any, opts []RunOption) (id string, token int32, raw json.RawMessage, err error) {
	o := runOpts{}
	for _, f := range opts {
		f(&o)
	}
	if o.id == "" {
		o.id = newID()
	}
	k.mu.Lock()
	fn := k.workflows[name]
	k.mu.Unlock()
	if fn == nil {
		return "", 0, nil, fmt.Errorf("kairo: no workflow %s", name)
	}
	if raw, err = json.Marshal(in); err != nil {
		return "", 0, nil, err
	}
	var meta json.RawMessage
	if o.meta != nil {
		if meta, err = json.Marshal(o.meta); err != nil {
			return "", 0, nil, err
		}
	}
	if err := k.planWorkflow(); err != nil {
		return "", 0, nil, err
	}
	token = newToken()
	existing, err := k.rt.run(ctx, planWorkflow, map[string]any{"workflow": name, "input": raw}, o.id, runSpec{
		vars: map[string]any{"started_at": k.nowFn().UnixMilli()}, workflow: name, meta: meta, drive: token})
	if err != nil {
		return "", 0, nil, err
	}
	if existing {
		token = 0
	}
	return o.id, token, raw, nil
}

// drive runs workflow id's function here, holding its drive lease (ADR
// 0059): token is the lease set with its start, or 0 to claim it now.
// What it leaves: nothing when it finished (the lease ends with it), no
// lease when it waits (suspend mode: what it waits for drives it on), an
// expired lease when it stopped otherwise (a tick takes it up).
func (k *Kairo) drive(ctx context.Context, id, name string, in json.RawMessage, parent string, token int32) (json.RawMessage, error) {
	if token == 0 {
		ri, err := k.rt.get(ctx, id)
		if err != nil {
			return nil, err
		}
		if ri.Finished() {
			return done(id, ri)
		}
		token = newToken()
		now := k.rt.now()
		ok, err := k.rt.store.ClaimDrive(ctx, LeaseRow{Run: id, Attempt: token, Owner: k.rt.owner, Until: now + k.rt.leaseMs}, now)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errDrivenElsewhere
		}
	}
	k.rt.driving(true)
	defer k.rt.driving(false)
	res, err := k.runAs(ctx, name, in, id, parent, nil)
	if err != nil {
		resume := !errors.Is(err, ErrSuspended)
		if eerr := k.rt.store.EndDrive(context.Background(), id, k.rt.owner, token, k.rt.now(), resume); eerr != nil {
			k.rt.logger.Warn("kairo: ending a drive lease", "workflow", id, "err", eerr)
		}
	}
	return res, err
}

// driveBackground, in wait mode, drives workflow id in the background,
// unless this process drives it already.
func (k *Kairo) driveBackground(id, name string, in json.RawMessage, parent string, token int32) {
	k.mu.Lock()
	if k.owned[id] || k.closing.Load() {
		k.mu.Unlock()
		return
	}
	k.owned[id] = true
	k.bgWG.Add(1)
	k.mu.Unlock()
	go func() {
		defer k.bgWG.Done()
		defer func() {
			k.mu.Lock()
			delete(k.owned, id)
			k.mu.Unlock()
		}()
		if _, err := k.drive(k.bgCtx, id, name, in, parent, token); err != nil && !errors.Is(err, errDrivenElsewhere) &&
			!errors.Is(err, ErrStopped) && !errors.Is(err, ErrCancelled) {
			k.rt.logger.Warn("kairo: driving a workflow", "workflow", id, "err", err)
		}
	}()
}

// lostDrive takes up workflow l.Run, whose driver stopped (its drive lease
// expired, ADR 0059).
func (k *Kairo) lostDrive(ctx context.Context, l LeaseRow) error {
	ri, err := k.rt.get(ctx, l.Run)
	if errors.Is(err, ErrUnknownRun) || (err == nil && (ri.Plan != planWorkflow || ri.Finished())) {
		// Nothing left to drive.
		return k.rt.store.EndDrive(ctx, l.Run, l.Owner, l.Attempt, k.rt.now(), false)
	}
	if err != nil {
		return err
	}
	var in struct {
		Workflow string          `json:"workflow"`
		Input    json.RawMessage `json:"input"`
	}
	if json.Unmarshal(ri.Input, &in) != nil || in.Workflow == "" {
		return nil
	}
	k.mu.Lock()
	fn := k.workflows[in.Workflow]
	k.mu.Unlock()
	if fn == nil {
		return fmt.Errorf("%w: workflow %s", errUnknownPlan, in.Workflow) // another process's
	}
	if k.mode == Suspend {
		k.redrive(l.Run)
		return nil
	}
	k.driveBackground(l.Run, in.Workflow, in.Input, ri.Parent, 0)
	return nil
}

// runAs drives workflow id. parentCancelled reports whether the workflow
// that made this one was cancelled in kairo (nil for a top-level one).
func (k *Kairo) runAs(ctx context.Context, name string, in json.RawMessage, id, parent string, parentCancelled func() bool) (json.RawMessage, error) {
	k.mu.Lock()
	fn := k.workflows[name]
	k.mu.Unlock()
	if fn == nil {
		return nil, fmt.Errorf("kairo: no workflow %s", name)
	}
	if err := k.planWorkflow(); err != nil {
		return nil, err
	}
	wfIn := map[string]any{"workflow": name, "input": in}
	if _, err := k.rt.run(ctx, planWorkflow, wfIn, id, runSpec{vars: map[string]any{"started_at": k.nowFn().UnixMilli()}, parent: parent, workflow: name}); err != nil {
		return nil, err
	}
	ri, err := k.rt.get(ctx, id)
	if err != nil {
		return nil, err
	}
	if ri.Finished() {
		return done(id, ri)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	d := &drive{cancel: cancel}
	k.mu.Lock()
	k.driving[d] = struct{}{}
	k.drivingIDs[id] = true
	k.mu.Unlock()
	defer func() {
		k.mu.Lock()
		delete(k.driving, d)
		delete(k.drivingIDs, id)
		k.mu.Unlock()
	}()
	// Cancelled in kairo (its run, or a workflow above it): only then are
	// its calls cancelled. A context that ends only stops the driving here.
	var self atomic.Bool
	cancelled := func() bool { return self.Load() || (parentCancelled != nil && parentCancelled()) }
	if k.mode == Wait {
		// The workflow's own run ends when it is cancelled: stop the calls.
		go func() {
			if r, err := k.rt.wait(cctx, id); err == nil && r.Status == "cancelled" {
				self.Store(true)
				cancel()
			}
		}()
	}
	c := &Context{Context: cctx, k: k, id: id, seen: map[string]int{}, cancelled: cancelled}
	value, err := callWorkflow(fn, c, in)
	if err != nil {
		switch {
		case errors.Is(err, ErrSuspended):
			return nil, ErrSuspended // goes on later
		case cancelled():
			if !self.Load() {
				// Cancelled with the workflow above it: so is its run.
				_ = k.rt.cancel(context.Background(), id)
			}
			return nil, fmt.Errorf("workflow %s: %w", id, ErrCancelled)
		case cctx.Err() != nil:
			// Not driven here any more; the workflow stays, to go on later.
			return nil, fmt.Errorf("workflow %s: %w", id, ErrStopped)
		}
		_ = k.rt.signal(context.Background(), id, "done", map[string]any{"ok": false, "error": err.Error()})
		return nil, &WorkflowError{ID: id, Message: err.Error(), Err: err}
	}
	js, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if err := k.rt.signal(context.Background(), id, "done", map[string]any{"ok": true, "value": json.RawMessage(js)}); err != nil {
		return nil, err
	}
	return js, nil
}

// callWorkflow runs a workflow's function; a panic is its failure.
func callWorkflow(fn workflowFn, c *Context, in json.RawMessage) (v any, err error) {
	defer func() {
		if p := recover(); p != nil {
			v, err = nil, fmt.Errorf("workflow %s panicked: %v", c.id, p)
		}
	}()
	return fn(c, in)
}

// done is the result of a finished workflow run.
func done(id string, ri RunInfo) (json.RawMessage, error) {
	if ri.Status == "cancelled" {
		return nil, fmt.Errorf("workflow %s: %w", id, ErrCancelled)
	}
	var out struct {
		Payload *struct {
			OK    bool            `json:"ok"`
			Value json.RawMessage `json:"value"`
			Error string          `json:"error"`
		} `json:"payload"`
	}
	if len(ri.Output) > 0 {
		_ = json.Unmarshal(ri.Output, &out)
	}
	if out.Payload == nil {
		return nil, &WorkflowError{ID: id, Message: ri.Status + ": " + ri.Error}
	}
	if !out.Payload.OK {
		return nil, &WorkflowError{ID: id, Message: out.Payload.Error}
	}
	return out.Payload.Value, nil
}

// callRun runs one call: a run of its own, found again by its id.
func (k *Kairo) callRun(c *Context, plan string, root map[string]any, in any, runID string) (json.RawMessage, error) {
	if root != nil {
		if err := k.plan(plan, root, nil); err != nil {
			return nil, err
		}
	}
	// Kept and removed with the workflow that makes it (ADR 0054).
	if _, err := k.rt.run(c, plan, map[string]any{"in": in}, runID, runSpec{parent: c.id}); err != nil {
		return nil, err
	}
	var r RunInfo
	var err error
	if k.mode == Suspend {
		// Whatever this process can do for the call is done once it is
		// idle; a call still going then waits for a timer or a signal.
		k.rt.idle()
		if r, err = k.rt.get(c, runID); err != nil {
			return nil, err
		}
		if !r.Finished() {
			// Waiting, or stopped for review until it is resolved.
			return nil, ErrSuspended
		}
	} else if r, err = k.rt.waitDone(c, runID); err != nil {
		if c.Err() != nil {
			// The workflow was cancelled in kairo: so is the call. If not,
			// the workflow is only no longer driven here: the call goes on.
			if c.cancelled() {
				_ = k.rt.cancel(context.Background(), runID)
				return nil, &CallError{RunID: runID, Action: calledAction(plan), Status: "cancelled"}
			}
			return nil, fmt.Errorf("call %s: %w", runID, ErrStopped)
		}
		return nil, err
	}
	if r.Status != "completed" {
		return nil, &CallError{RunID: runID, Action: calledAction(plan), Status: r.Status, Message: r.Error}
	}
	return r.Output, nil
}

// calledAction is the action a call's plan calls ("" for a wait).
func calledAction(plan string) string {
	a, _ := strings.CutPrefix(plan, planCall)
	if a == plan {
		return ""
	}
	return a
}

// Signal sends a signal to the first wait for it in workflow id that has
// not received one. A wait the workflow has not reached yet receives it
// when it does (ADR 0059): signals of one name go to its waits in order.
func (k *Kairo) Signal(ctx context.Context, id, name string, payload any) error {
	if err := k.plan(planWait+name, waitRoot(name, 0), nil); err != nil {
		return err
	}
	for n := 0; ; n++ {
		runID := callID(id, planWait+name, "null", n)
		r, err := k.rt.get(ctx, runID)
		if errors.Is(err, ErrUnknownRun) {
			made, err := k.signalAhead(ctx, id, name, runID, payload)
			if err != nil {
				return err
			}
			if !made {
				n-- // the workflow made the wait meanwhile: signal it
				continue
			}
			break
		}
		if err != nil {
			return err
		}
		if r.Finished() {
			continue
		}
		if err := k.rt.signal(ctx, runID, name, payload); err != nil {
			return err
		}
		break
	}
	if k.mode == Suspend {
		k.settle()
		return k.wakeUp(ctx)
	}
	return nil
}

// signalAhead makes wait runID of workflow id, as the workflow will when
// it reaches it, with the signal applied in the same transaction.
func (k *Kairo) signalAhead(ctx context.Context, id, name, runID string, payload any) (bool, error) {
	wf, err := k.rt.get(ctx, id)
	if errors.Is(err, ErrUnknownRun) || (err == nil && wf.Plan != planWorkflow) {
		return false, fmt.Errorf("kairo: no workflow %s", id)
	}
	if err != nil {
		return false, err
	}
	if wf.Finished() {
		return false, fmt.Errorf("kairo: workflow %s has finished", id)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	existing, err := k.rt.run(ctx, planWait+name, map[string]any{"in": nil}, runID, runSpec{parent: id,
		then: []wasmcore.Event{{Kind: "signal", At: k.rt.now(), Name: name, Data: data}}})
	return !existing, err
}

// waitRoot is a wait for signal name, with a timeout (ms; 0: none).
func waitRoot(name string, timeoutMs int64) map[string]any {
	root := map[string]any{"kind": "wait", "id": "w", "signal": name}
	if timeoutMs > 0 {
		root["timeout"] = strconv.FormatInt(timeoutMs, 10) + "ms"
	}
	return root
}

// Filter selects workflows for List: of Workflow, in Status, created in
// [Since, Until) (zero: unbounded), after the run After, at most Limit.
type Filter struct {
	Workflow, Status string
	Since, Until     time.Time
	After            string
	Limit            int
}

// List returns workflows started here or elsewhere (root runs: not child
// workflows, not calls), in the order they were created (ADR 0059).
func (k *Kairo) List(ctx context.Context, f Filter) ([]RunInfo, error) {
	lf := ListFilter{Workflow: f.Workflow, Status: f.Status, After: f.After, Limit: f.Limit}
	if !f.Since.IsZero() {
		lf.Since = f.Since.UnixMilli()
	}
	if !f.Until.IsZero() {
		lf.Until = f.Until.UnixMilli()
	}
	rows, err := k.rt.store.List(ctx, lf)
	if err != nil {
		return nil, err
	}
	out := make([]RunInfo, len(rows))
	for i := range rows {
		out[i] = info(&rows[i])
	}
	return out, nil
}

// Cancel cancels workflow id: the calls it waits for are cancelled with it.
func (k *Kairo) Cancel(ctx context.Context, id string) error { return k.rt.cancel(ctx, id) }

// Get describes run id (a workflow or a call).
func (k *Kairo) Get(ctx context.Context, id string) (RunInfo, error) { return k.rt.get(ctx, id) }

// Children are the runs made by run id: a workflow's calls and child
// workflows (ADR 0058).
func (k *Kairo) Children(ctx context.Context, id string) ([]RunInfo, error) {
	rows, err := k.rt.store.Children(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]RunInfo, len(rows))
	for i := range rows {
		out[i] = info(&rows[i])
	}
	return out, nil
}

// Resolve settles a call stopped for review (blocked: a real step whose
// outcome was unknown) with the output it had: it did take effect.
func (k *Kairo) Resolve(ctx context.Context, callID string, output any) error {
	return k.rt.resolve(ctx, callID, output, "")
}

// ResolveFailed settles a call stopped for review as not done: it failed
// with msg.
func (k *Kairo) ResolveFailed(ctx context.Context, callID string, msg string) error {
	if msg == "" {
		msg = "resolved as failed"
	}
	return k.rt.resolve(ctx, callID, nil, msg)
}

// Subscribe delivers the live output (TaskContext.Emit) of run id and of
// every run under it, until stop is called or the process closes. Chunks a
// slow subscriber cannot take are dropped.
func (k *Kairo) Subscribe(id string) (chunks <-chan Chunk, stop func()) {
	return k.rt.subscribe(id, 256)
}

// Tick, in suspend mode, takes up due timers and steps whose process
// stopped, drives on the workflows whose calls settle, and returns once
// that is done, with when something is next to do (ok false: nothing).
func (k *Kairo) Tick(ctx context.Context) (next time.Time, ok bool, err error) {
	if err := k.rt.tick(ctx); err != nil {
		return time.Time{}, false, err
	}
	k.settle()
	at, ok, err := k.rt.nextWake(ctx)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	if k.mode == Suspend && k.wake != nil {
		if err := k.wake(time.UnixMilli(at)); err != nil {
			return time.Time{}, false, err
		}
	}
	return time.UnixMilli(at), true, nil
}

// wakeUp, in suspend mode, tells Wake when something is next to do.
func (k *Kairo) wakeUp(ctx context.Context) error {
	if k.mode != Suspend || k.wake == nil {
		return nil
	}
	at, ok, err := k.rt.nextWake(ctx)
	if err != nil || !ok {
		return err
	}
	return k.wake(time.UnixMilli(at))
}

// settle returns once the work started here and the workflows driven on
// are done.
func (k *Kairo) settle() {
	for {
		k.rt.idle()
		k.redriveMu.Lock()
		for k.redrivesN > 0 {
			k.redriveCond.Wait()
		}
		k.redriveMu.Unlock()
		k.rt.busyMu.Lock()
		idle := k.rt.busyN == 0
		k.rt.busyMu.Unlock()
		if idle {
			return
		}
	}
}

// redrive, in suspend mode, drives workflow id on (again, if it is being
// driven now).
func (k *Kairo) redrive(id string) {
	k.mu.Lock()
	if k.drivingIDs[id] {
		k.again[id] = true
		k.mu.Unlock()
		return
	}
	k.mu.Unlock()
	k.redriveMu.Lock()
	k.redrivesN++
	k.redriveMu.Unlock()
	go func() {
		defer func() {
			k.redriveMu.Lock()
			k.redrivesN--
			if k.redrivesN == 0 {
				k.redriveCond.Broadcast()
			}
			k.redriveMu.Unlock()
		}()
		for {
			k.mu.Lock()
			delete(k.again, id)
			k.mu.Unlock()
			ri, err := k.rt.get(context.Background(), id)
			if err != nil || ri.Plan != planWorkflow || ri.Finished() {
				return
			}
			var in struct {
				Workflow string          `json:"workflow"`
				Input    json.RawMessage `json:"input"`
			}
			if json.Unmarshal(ri.Input, &in) != nil || in.Workflow == "" {
				return
			}
			// Suspended again, failed (recorded), cancelled, or driven elsewhere.
			_, _ = k.drive(context.Background(), id, in.Workflow, in.Input, ri.Parent, 0)
			k.mu.Lock()
			more := k.again[id]
			k.mu.Unlock()
			if !more {
				return
			}
		}
	}()
}

// Close stops serving actions and driving workflows, as a process that
// stops does: the workflows it drove stay unfinished, to be resumed.
// The drive leases it held are left expired: another process takes them
// up at its next sweep or tick (ADR 0059).
func (k *Kairo) Close() error {
	k.mu.Lock()
	k.closing.Store(true)
	k.mu.Unlock()
	if k.stopHook != nil {
		k.stopHook()
	}
	if k.sweepStop != nil {
		close(k.sweepStop)
	}
	k.bgCancel()
	k.mu.Lock()
	for d := range k.driving {
		d.cancel()
	}
	k.mu.Unlock()
	k.bgWG.Wait()
	return k.rt.close()
}

// destination is the key an action's limits are counted by.
func destination(name string, a *actionDef) string {
	if d, ok := a.spec["destination"].(string); ok && d != "" {
		return d
	}
	return name
}

// limiter limits the steps of one destination (ADR 0059): at once
// (slots) and a minute (every: the spacing of their starts).
type limiter struct {
	slots chan struct{}
	every time.Duration
	mu    sync.Mutex
	next  time.Time
}

func (l *limiter) tighten(limit, perMinute int) {
	if limit > 0 && (l.slots == nil || limit < cap(l.slots)) {
		l.slots = make(chan struct{}, limit)
	}
	if perMinute > 0 {
		if every := time.Minute / time.Duration(perMinute); every > l.every {
			l.every = every
		}
	}
}

// admit waits for a step of action a to be let run: its destination's
// slot and turn, then the process's slot. release gives the slots back.
func (k *Kairo) admit(ctx context.Context, action string, a *actionDef) (release func(), err error) {
	var held []chan struct{}
	release = func() {
		for _, s := range held {
			<-s
		}
	}
	take := func(s chan struct{}) error {
		select {
		case s <- struct{}{}:
			held = append(held, s)
			return nil
		case <-ctx.Done():
			release()
			return ctx.Err()
		}
	}
	k.mu.Lock()
	l := k.limits[destination(action, a)]
	k.mu.Unlock()
	if l != nil {
		if l.slots != nil {
			if err := take(l.slots); err != nil {
				return nil, err
			}
		}
		if l.every > 0 {
			l.mu.Lock()
			at := l.next
			if now := time.Now(); at.Before(now) {
				at = now
			}
			l.next = at.Add(l.every)
			l.mu.Unlock()
			if d := time.Until(at); d > 0 {
				t := time.NewTimer(d)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					release()
					return nil, ctx.Err()
				}
			}
		}
	}
	if k.slots != nil {
		if err := take(k.slots); err != nil {
			return nil, err
		}
	}
	return release, nil
}
