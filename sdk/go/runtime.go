package kairo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r-hashi01/kairo/wasmcore"
)

// The embedded runtime (ADR 0051), as the TypeScript and Python SDKs have
// it (sdk/ts/src/embedded.ts), with the core called directly instead of
// through WASM (ADR 0058). Each event is one transaction: lock the run,
// apply the event, append it, replace the state, arm or disarm timers and
// leases. Commands are carried out after the commit; a real step only once
// its intent was committed with it (invariant 4).

// errUnknownPlan: a run whose plan this process does not have (another
// process's to take up).
var errUnknownPlan = errors.New("kairo: plan not registered here")

// ErrEffectWeakened: a call's action is no longer real while a real
// attempt of it is out (ADR 0060). The call waits: restore the action, or
// settle the call with Resolve, ResolveFailed or Cancel.
var ErrEffectWeakened = errors.New("kairo: a real attempt is out and its action is no longer real")

// ErrUnknownRun: no such run.
var ErrUnknownRun = errors.New("kairo: no such run")

// RunInfo describes a run.
type RunInfo struct {
	RunID  string          `json:"run_id"`
	Plan   string          `json:"plan"`
	Status string          `json:"status"`
	Input  json.RawMessage `json:"input,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
	Parent string          `json:"parent,omitempty"`
	// Workflow names a workflow run's workflow; Meta is what it was started
	// with (WithMeta). Created and Updated: when the run started and last
	// changed (ADR 0059).
	Workflow string          `json:"workflow,omitempty"`
	Version  string          `json:"version,omitempty"` // a workflow run's version (ADR 0060)
	Meta     json.RawMessage `json:"meta,omitempty"`
	Created  time.Time       `json:"created_at"`
	Updated  time.Time       `json:"updated_at"`
	// trimmed: finished, its result no longer kept (kairod, ADR 0027).
	trimmed bool
}

// Finished reports whether the run is over (completed, failed or
// cancelled).
func (r RunInfo) Finished() bool { return doneStatus[r.Status] }

func info(row *RunRow) RunInfo {
	var version string
	if row.Plan == planWorkflow {
		_, version, _, _ = workflowOf(row.Input)
	}
	return RunInfo{Version: version, RunID: row.ID, Plan: row.Plan, Status: row.Status, Input: row.Input, Output: row.Output, Error: row.Error, Parent: row.Parent,
		Workflow: row.Workflow, Meta: row.Meta, Created: time.UnixMilli(row.CreatedAt), Updated: time.UnixMilli(row.UpdatedAt)}
}

// task is a dispatched step, as an action handler gets it.
type task struct {
	RunID, StepID, Action, IdemKey string
	Act                            uint32
	Attempt                        int32
	Input                          json.RawMessage
}

// result is a step's outcome.
type result struct {
	Output    json.RawMessage
	Err       string
	ErrType   string
	Retryable bool
	Unknown   bool
	Wait      *waitResult
	// Pending: the step runs elsewhere and its outcome comes later
	// (complete); its lease goes to Owner (ADR 0052).
	Pending *pending
}

type waitResult struct {
	Until  int64
	Output json.RawMessage
}

type pending struct {
	Owner   string
	LeaseMs int64
}

type handlerFunc func(ctx context.Context, t task, emit func([]byte)) result

// Chunk is a step's live output (ADR 0058): delivered to subscribers in
// this process while it is produced, never recorded.
type Chunk struct {
	RunID  string
	StepID string
	Data   []byte
}

type runtime struct {
	core    *wasmcore.Core
	coreMu  sync.RWMutex // Compile appends plans; Apply reads them
	store   Store
	now     func() int64
	owner   string
	leaseMs int64
	keepMs  int64 // < 0: kept for ever

	mu        sync.Mutex
	plans     map[string]wasmcore.Compiled
	handler   handlerFunc
	waiters   map[string]map[chan RunInfo]struct{}
	hooks     map[int]func(RunInfo)
	nextHook  int
	timers    map[string]*time.Timer
	running   map[string]context.CancelFunc
	subs      map[int]*subscription
	nextSub   int
	closed    bool
	renewStop chan struct{}

	busyMu    sync.Mutex
	busyCond  *sync.Cond
	busyN     int // work started here and not done (steps, timers that fired, sweeps)
	lastSweep atomic.Int64
	unlisten  func()

	// Set by Kairo before it starts (ADR 0059). drives counts the workflows
	// this process drives (their leases are renewed with its steps').
	// leaseParents: a run that settles leases its parent workflow to this
	// process, to be driven on (suspend mode). lostDrive takes up a drive
	// lease whose owner stopped; missingPlan registers a plan on demand.
	drives       int
	leaseParents bool
	logger       *slog.Logger
	observer     func(Observation)
	manualTimers bool
	lostDrive    func(ctx context.Context, l LeaseRow) error
	missingPlan  func(name string) bool
}

type subscription struct {
	prefix string
	ch     chan Chunk
}

const removePerTick = 100

func newRuntime(store Store, now func() int64, owner string, leaseMs, keepMs int64) *runtime {
	r := &runtime{core: wasmcore.New(), store: store, now: now, owner: owner, leaseMs: leaseMs, keepMs: keepMs,
		plans: map[string]wasmcore.Compiled{}, waiters: map[string]map[chan RunInfo]struct{}{}, hooks: map[int]func(RunInfo){},
		timers: map[string]*time.Timer{}, running: map[string]context.CancelFunc{}, subs: map[int]*subscription{}}
	r.busyCond = sync.NewCond(&r.busyMu)
	r.logger = slog.Default()
	return r
}

func (r *runtime) open(ctx context.Context, named bool) error {
	if err := r.store.Init(ctx); err != nil {
		return err
	}
	if named {
		// A process of the same name that stopped: its steps are not running.
		if err := r.store.ExpireLeases(ctx, r.owner, r.now()); err != nil {
			return err
		}
	}
	if n, ok := r.store.(Notifier); ok {
		stop, err := n.Listen(context.Background(), r.settledElsewhere)
		if err != nil {
			return err
		}
		r.unlisten = stop
	}
	return nil
}

func (r *runtime) track(f func()) {
	r.busyMu.Lock()
	r.busyN++
	r.busyMu.Unlock()
	go func() {
		defer func() {
			r.busyMu.Lock()
			r.busyN--
			if r.busyN == 0 {
				r.busyCond.Broadcast()
			}
			r.busyMu.Unlock()
		}()
		f()
	}()
}

// idle returns once the work started here (steps, timers that fired) is
// done, including work that work started.
func (r *runtime) idle() {
	r.busyMu.Lock()
	for r.busyN > 0 {
		r.busyCond.Wait()
	}
	r.busyMu.Unlock()
}

func (r *runtime) registerActions(specs []byte, h handlerFunc) error {
	r.coreMu.Lock()
	defer r.coreMu.Unlock()
	if err := r.core.Register(specs); err != nil {
		return err
	}
	r.mu.Lock()
	r.handler = h
	r.mu.Unlock()
	return nil
}

func (r *runtime) registerPlan(def []byte) (wasmcore.Compiled, error) {
	r.coreMu.Lock()
	c, err := r.core.Compile(def)
	r.coreMu.Unlock()
	if err != nil {
		return c, err
	}
	r.mu.Lock()
	r.plans[c.Name] = c
	r.mu.Unlock()
	return c, nil
}

func (r *runtime) plan(name string) (wasmcore.Compiled, bool) {
	r.mu.Lock()
	c, ok := r.plans[name]
	missing := r.missingPlan
	r.mu.Unlock()
	if ok || missing == nil || !missing(name) {
		return c, ok
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok = r.plans[name]
	return c, ok
}

func (r *runtime) onSettled(h func(RunInfo)) func() {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextHook
	r.nextHook++
	r.hooks[id] = h
	return func() {
		r.mu.Lock()
		delete(r.hooks, id)
		r.mu.Unlock()
	}
}

type startInfo struct {
	plan wasmcore.Compiled
	at   int64
	runSpec
}

// runSpec is what a new run starts with, besides its plan and input.
type runSpec struct {
	vars     map[string]any
	parent   string
	workflow string
	meta     json.RawMessage
	// drive, for a workflow run: its drive lease, set with its start
	// (token; 0: none).
	drive int32
	// then: events applied in the start's transaction (a signal received
	// before the wait was reached, ADR 0059).
	then []wasmcore.Event
}

// run starts a run of plan, or finds it: a run id is an idempotency key.
func (r *runtime) run(ctx context.Context, plan string, input any, runID string, spec runSpec) (existing bool, err error) {
	p, ok := r.plan(plan)
	if !ok {
		return false, fmt.Errorf("%w: %s", errUnknownPlan, plan)
	}
	data, err := json.Marshal(input)
	if err != nil {
		return false, err
	}
	at := r.now()
	ev := wasmcore.Event{Kind: "start", At: at, Data: data}
	if spec.vars != nil {
		if ev.Vars, err = json.Marshal(spec.vars); err != nil {
			return false, err
		}
	}
	existing, err = r.process(ctx, runID, append([]wasmcore.Event{ev}, spec.then...), &startInfo{plan: p, at: at, runSpec: spec})
	if err != nil {
		// Two processes starting one id at once: the one that lost finds it.
		if row, gerr := r.store.Get(ctx, runID); gerr == nil && row != nil {
			return true, nil
		}
	}
	return existing, err
}

func (r *runtime) get(ctx context.Context, runID string) (RunInfo, error) {
	row, err := r.store.Get(ctx, runID)
	if err != nil {
		return RunInfo{}, err
	}
	if row == nil {
		return RunInfo{}, fmt.Errorf("%w: %s", ErrUnknownRun, runID)
	}
	return info(row), nil
}

// wait returns once the run has finished or stopped for review.
func (r *runtime) wait(ctx context.Context, runID string) (RunInfo, error) {
	return r.waitUntil(ctx, runID, settledStatus)
}

// waitDone returns once the run has finished: a run stopped for review is
// waited for until it is resolved (ADR 0058).
func (r *runtime) waitDone(ctx context.Context, runID string) (RunInfo, error) {
	return r.waitUntil(ctx, runID, func(s string) bool { return doneStatus[s] })
}

func (r *runtime) waitUntil(ctx context.Context, runID string, ok func(status string) bool) (RunInfo, error) {
	ch := make(chan RunInfo, 4)
	r.mu.Lock()
	if r.waiters[runID] == nil {
		r.waiters[runID] = map[chan RunInfo]struct{}{}
	}
	r.waiters[runID][ch] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.waiters[runID], ch)
		if len(r.waiters[runID]) == 0 {
			delete(r.waiters, runID)
		}
		r.mu.Unlock()
	}()
	// Registered first, then read: an end between the two still wakes us.
	now, err := r.get(ctx, runID)
	if err != nil {
		return RunInfo{}, err
	}
	if ok(now.Status) {
		return now, nil
	}
	for {
		select {
		case ri := <-ch:
			if ok(ri.Status) {
				return ri, nil
			}
		case <-ctx.Done():
			return RunInfo{}, ctx.Err()
		}
	}
}

func (r *runtime) wake(ri RunInfo) {
	r.mu.Lock()
	var chs []chan RunInfo
	for ch := range r.waiters[ri.RunID] {
		chs = append(chs, ch)
	}
	var hooks []func(RunInfo)
	ids := make([]int, 0, len(r.hooks))
	for id := range r.hooks {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		hooks = append(hooks, r.hooks[id])
	}
	r.mu.Unlock()
	for _, ch := range chs {
		select {
		case ch <- ri:
		default:
		}
	}
	for _, h := range hooks {
		h(ri)
	}
}

// settledElsewhere: a run settled in another process (Notifier).
func (r *runtime) settledElsewhere(runID string) {
	r.mu.Lock()
	n := len(r.waiters[runID])
	r.mu.Unlock()
	if n == 0 {
		return
	}
	r.track(func() {
		if ri, err := r.get(context.Background(), runID); err == nil && settledStatus(ri.Status) {
			r.wake(ri)
		}
	})
}

func (r *runtime) signal(ctx context.Context, runID, name string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = r.process(ctx, runID, []wasmcore.Event{{Kind: "signal", At: r.now(), Name: name, Data: data}}, nil)
	return err
}

func (r *runtime) cancel(ctx context.Context, runID string) error {
	_, err := r.process(ctx, runID, []wasmcore.Event{{Kind: "cancel", At: r.now(), Err: "cancelled"}}, nil)
	return err
}

// resolve settles a step stopped for review (ADR 0058): it took effect
// (with output) or it did not (errMsg; the run fails).
func (r *runtime) resolve(ctx context.Context, runID string, output any, errMsg string) error {
	row, err := r.store.Get(ctx, runID)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("%w: %s", ErrUnknownRun, runID)
	}
	in, err := wasmcore.Inspect(row.State)
	if err != nil {
		return err
	}
	// Stopped for review; or a real attempt out that cannot go on because
	// its action is no longer real (ADR 0060): only then, not one out as
	// usual.
	acts, out := in.Review, false
	if len(acts) == 0 && len(in.Intents) > 0 {
		if p, ok := r.plan(row.Plan); ok && p.Hash != row.Hash &&
			errors.Is(adoptable(p, row, []wasmcore.Event{{Kind: "step_err"}}), ErrEffectWeakened) {
			acts, out = in.Intents, true
		}
	}
	if len(acts) == 0 {
		return fmt.Errorf("kairo: run %s has no step stopped for review", runID)
	}
	ev := wasmcore.Event{Kind: "resolve", At: r.now(), Act: acts[0], Err: errMsg, Unknown: out}
	if errMsg == "" {
		if ev.Data, err = json.Marshal(output); err != nil {
			return err
		}
	}
	_, err = r.process(ctx, runID, []wasmcore.Event{ev}, nil)
	return err
}

// tick takes up what no process is doing: steps whose lease expired, timers
// that are due, and finished trees past the time they are kept.
func (r *runtime) tick(ctx context.Context) error {
	r.lastSweep.Store(r.now())
	leases, err := r.store.ExpiredLeases(ctx, r.now(), 1000)
	if err != nil {
		return err
	}
	// One run that cannot go on does not stop the others (ADR 0060): it is
	// logged, and put off a lease period, so that it does not stay first of
	// what each tick takes up.
	for _, l := range leases {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var err error
		if l.Act == 0 {
			// A workflow's driver stopped (ADR 0059).
			if r.lostDrive != nil {
				err = skipUnknownPlan(r.lostDrive(ctx, l))
			}
		} else {
			err = skipUnknownPlan(r.recover(ctx, l))
		}
		if err != nil && ctx.Err() == nil {
			r.stuck(l.Run, err)
			l.Owner, l.Until = "", r.now()+r.leaseMs
			if herr := r.store.HandOver(ctx, l); herr != nil {
				r.logger.Warn("kairo: putting off a lease", "run", l.Run, "err", herr)
			}
		}
	}
	timers, err := r.store.DueTimers(ctx, r.now(), 1000)
	if err != nil {
		return err
	}
	for _, t := range timers {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := skipUnknownPlan(r.fire(ctx, t)); err != nil && ctx.Err() == nil {
			r.stuck(t.Run, err)
			t.At = r.now() + r.leaseMs
			if serr := r.store.WithRun(ctx, t.Run, func(row *RunRow) (*Changes, error) {
				if row == nil {
					return nil, nil
				}
				return &Changes{SetTimers: []TimerRow{t}}, nil
			}); serr != nil {
				r.logger.Warn("kairo: putting off a timer", "run", t.Run, "err", serr)
			}
		}
	}
	if r.keepMs >= 0 {
		if _, err := r.store.RemoveFinished(ctx, r.now()-r.keepMs, removePerTick); err != nil {
			return err
		}
	}
	return nil
}

// recheck, without a Notifier, reads the runs waited for here: one may have
// settled in another process (ADR 0059).
func (r *runtime) recheck(ctx context.Context) {
	if _, ok := r.store.(Notifier); ok {
		return
	}
	r.mu.Lock()
	ids := make([]string, 0, len(r.waiters))
	for id := range r.waiters {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	for _, id := range ids {
		if ri, err := r.get(ctx, id); err == nil && settledStatus(ri.Status) {
			r.wake(ri)
		}
	}
}

// driving counts a workflow driven here (its lease renewed) while it is.
func (r *runtime) driving(on bool) {
	r.mu.Lock()
	if on {
		r.drives++
	} else {
		r.drives--
	}
	r.mu.Unlock()
	r.renew()
}

// stuck logs and observes a run that could not go on now (err nil: it
// could).
func (r *runtime) stuck(runID string, err error) {
	if err == nil {
		return
	}
	r.logger.Warn("kairo: a run cannot go on", "run", runID, "err", err)
	r.observe(Observation{Kind: ObsRunStuck, RunID: runID, Error: err.Error()})
}

// adoptable says whether run row, which started under another version of
// plan, may go on under the current one (ADR 0060). Only the SDK's own
// plans (one node each) may; and not while a real attempt is out whose
// action the current plan does not treat as real (it would be retried on
// an unknown outcome, invariant 5), unless the events settle the call
// (cancel, resolve).
func adoptable(plan wasmcore.Compiled, row *RunRow, events []wasmcore.Event) error {
	if plan.Name != planWorkflow && !strings.HasPrefix(plan.Name, planCall) && !strings.HasPrefix(plan.Name, planWait) {
		return fmt.Errorf("plan %s changed since it started", row.Plan)
	}
	// Events that cannot make a retry of an outcome unknown: a cancel, a
	// resolve, an attempt's definite success or wait.
	settles := len(events) > 0
	for _, e := range events {
		switch e.Kind {
		case "cancel", "resolve", "step_ok", "step_wait":
		default:
			settles = false
		}
	}
	if settles || strictlyReal(plan) {
		return nil
	}
	in, err := wasmcore.Inspect(row.State)
	if err != nil {
		return err
	}
	if in.IntentDurable {
		return fmt.Errorf("%w (%s)", ErrEffectWeakened, row.Plan)
	}
	return nil
}

// strictlyReal: every action of plan is real and not retried on an unknown
// outcome.
func strictlyReal(plan wasmcore.Compiled) bool {
	for a, e := range plan.Effects {
		if e != "real" || plan.Idempotent[a] {
			return false
		}
	}
	return len(plan.Effects) > 0
}

func skipUnknownPlan(err error) error {
	if errors.Is(err, errUnknownPlan) {
		return nil
	}
	return err
}

// recover: the process that ran l's step stopped; its outcome is unknown.
func (r *runtime) recover(ctx context.Context, l LeaseRow) error {
	_, err := r.process(ctx, l.Run, []wasmcore.Event{{Kind: "step_err", At: r.now(), Act: l.Act, Attempt: l.Attempt, Unknown: true,
		Retryable: true, Err: "the process running the step stopped", ErrType: "process_lost"}}, nil)
	return err
}

func (r *runtime) fire(ctx context.Context, t TimerRow) error {
	_, err := r.process(ctx, t.Run, []wasmcore.Event{{Kind: "timer", At: r.now(), Act: t.Act, Timer: t.Timer}}, nil)
	return err
}

// close stops as a process that stops: no new step starts, timers are
// dropped (they stay in the store), steps running here are cancelled.
func (r *runtime) close() error {
	r.mu.Lock()
	r.closed = true
	for k, t := range r.timers {
		t.Stop()
		delete(r.timers, k)
	}
	for k, c := range r.running {
		c()
		delete(r.running, k)
	}
	r.mu.Unlock()
	r.renew()
	r.idle()
	if r.unlisten != nil {
		r.unlisten()
	}
	r.mu.Lock()
	for id, s := range r.subs {
		close(s.ch)
		delete(r.subs, id)
	}
	r.mu.Unlock()
	return r.store.Close()
}

func (r *runtime) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// renew keeps one goroutine renewing this process's leases while it runs
// steps or drives workflows.
func (r *runtime) renew() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if (len(r.running) > 0 || r.drives > 0) && !r.closed {
		if r.renewStop == nil {
			stop := make(chan struct{})
			r.renewStop = stop
			every := time.Duration(max(r.leaseMs/3, 10)) * time.Millisecond
			go func() {
				t := time.NewTicker(every)
				defer t.Stop()
				for {
					select {
					case <-stop:
						return
					case <-t.C:
						if err := r.store.RenewLeases(context.Background(), r.owner, r.now()+r.leaseMs); err != nil {
							r.logger.Warn("kairo: renewing leases", "err", err)
						}
					}
				}
			}()
		}
		return
	}
	if r.renewStop != nil {
		close(r.renewStop)
		r.renewStop = nil
	}
}

// process applies events to run runID in one transaction, then carries out
// the commands. With start, a new run of that plan (or the existing one:
// existing says which).
func (r *runtime) process(ctx context.Context, runID string, events []wasmcore.Event, start *startInfo) (bool, error) {
	var (
		existing, settled bool
		commands          []wasmcore.Command
		newRow            *RunRow
	)
	err := r.store.WithRun(ctx, runID, func(row *RunRow) (*Changes, error) {
		existing, settled, commands, newRow = false, false, nil, nil
		// A step's outcome ends its lease, applied or not (a stale one).
		var endLeases []uint32
		for _, e := range events {
			if e.Kind == "step_ok" || e.Kind == "step_err" || e.Kind == "step_wait" {
				endLeases = append(endLeases, e.Act)
			}
		}
		if row != nil && start != nil {
			existing = true
			return nil, nil
		}
		if row == nil && start == nil {
			return &Changes{EndLeases: endLeases}, nil
		}
		var plan wasmcore.Compiled
		if start != nil {
			plan = start.plan
		} else {
			p, ok := r.plan(row.Plan)
			if !ok {
				return nil, fmt.Errorf("%w: run %s, plan %s", errUnknownPlan, runID, row.Plan)
			}
			plan = p
		}
		if row != nil && row.Hash != plan.Hash {
			if err := adoptable(plan, row, events); err != nil {
				return nil, fmt.Errorf("kairo: run %s: %w", runID, err)
			}
			// Goes on under the current plan (ADR 0060): next has its hash.
		}
		var state []byte
		status, output, errMsg := "running", json.RawMessage(nil), ""
		if row != nil {
			state, status, output, errMsg = row.State, row.Status, row.Output, row.Error
		}
		var recorded []json.RawMessage
		var applyErr error
		var apply func(ev wasmcore.Event)
		apply = func(ev wasmcore.Event) {
			if applyErr != nil {
				return
			}
			evJSON, err := json.Marshal(ev)
			if err != nil {
				applyErr = err
				return
			}
			r.coreMu.RLock()
			s, res, err := r.core.Apply(plan.Plan, runID, state, evJSON, false)
			r.coreMu.RUnlock()
			if err != nil {
				applyErr = err
				return
			}
			if res.Ignored {
				return
			}
			state = s
			recorded = append(recorded, evJSON)
			status, output, errMsg = res.Status, res.Output, res.Error
			for _, c := range res.Commands {
				commands = append(commands, c)
				// A real step's intent, committed with what dispatched it.
				if c.Kind == "dispatch" && c.Effect == "real" {
					apply(wasmcore.Event{Kind: "intent", At: ev.At, Act: c.Act, Attempt: c.Attempt})
				}
			}
		}
		for _, ev := range events {
			apply(ev)
		}
		if applyErr != nil {
			return nil, applyErr
		}
		if len(recorded) == 0 {
			return &Changes{EndLeases: endLeases}, nil
		}
		at := r.now()
		if start != nil {
			at = start.at
		}
		next := &RunRow{ID: runID, Plan: plan.Name, Hash: plan.Hash, State: state, Status: status, Output: output, Error: errMsg, UpdatedAt: at}
		if row != nil {
			next.Input, next.CreatedAt, next.Parent, next.Workflow, next.Meta = row.Input, row.CreatedAt, row.Parent, row.Workflow, row.Meta
		} else {
			next.Input, next.CreatedAt, next.Parent, next.Workflow, next.Meta = events[0].Data, at, start.parent, start.workflow, start.meta
			if next.Input == nil {
				next.Input = json.RawMessage("null")
			}
		}
		ch := &Changes{Events: recorded, Row: next, EndLeases: endLeases, Clear: doneStatus[status]}
		if row == nil && start.drive != 0 && !ch.Clear {
			// Driven by this process from its start (ADR 0059).
			ch.SetLeases = append(ch.SetLeases, LeaseRow{Run: runID, Act: 0, Attempt: start.drive, Owner: r.owner, Until: at + r.leaseMs})
		}
		for _, c := range commands {
			switch c.Kind {
			case "timer":
				ch.SetTimers = append(ch.SetTimers, TimerRow{Run: runID, Timer: c.Timer, Act: c.Act, At: c.At})
			case "cancel_timer":
				ch.DeleteTimers = append(ch.DeleteTimers, c.Timer)
			case "dispatch":
				// Dispatched to this process: leased to it while it runs.
				ch.SetLeases = append(ch.SetLeases, LeaseRow{Run: runID, Act: c.Act, Attempt: c.Attempt, Owner: r.owner, Until: at + r.leaseMs})
			}
		}
		was := ""
		if row != nil {
			was = row.Status
		}
		settled = settledStatus(status) && status != was
		ch.Notify = settled
		if settled && next.Parent != "" && r.leaseParents {
			// Its workflow is to be driven on: by this process, or, if it
			// stops first, by whichever takes the lease up (ADR 0059).
			ch.SetLeases = append(ch.SetLeases, LeaseRow{Run: next.Parent, Act: 0, Attempt: newToken(), Owner: r.owner, Until: at + r.leaseMs})
		}
		newRow = next
		return ch, nil
	})
	if err != nil {
		return false, err
	}
	if r.isClosed() {
		return existing, nil
	}
	if start != nil && !existing && newRow != nil {
		r.observe(Observation{Kind: ObsRunStarted, RunID: runID, Parent: newRow.Parent, Plan: newRow.Plan, Workflow: newRow.Workflow})
	}
	for _, c := range commands {
		r.carryOut(runID, c)
	}
	// Settled by this transaction (not a run found settled already).
	if settled && newRow != nil {
		r.observe(Observation{Kind: ObsRunSettled, RunID: runID, Parent: newRow.Parent, Plan: newRow.Plan, Workflow: newRow.Workflow,
			Status: newRow.Status, Error: newRow.Error})
		r.wake(info(newRow))
	}
	// Along the way: what no process is doing (at most once a lease period).
	if r.now()-r.lastSweep.Load() >= r.leaseMs {
		r.lastSweep.Store(r.now())
		r.track(func() {
			if err := r.tick(context.Background()); err != nil {
				r.logger.Warn("kairo: sweeping", "err", err)
			}
		})
	}
	return existing, nil
}

func (r *runtime) carryOut(runID string, c wasmcore.Command) {
	switch c.Kind {
	case "dispatch":
		r.track(func() { r.dispatch(runID, c) })
	case "timer":
		if r.manualTimers {
			return // fired by Tick only
		}
		key := fmt.Sprintf("%s\x00t%d", runID, c.Timer)
		t := TimerRow{Run: runID, Timer: c.Timer, Act: c.Act, At: c.At}
		r.mu.Lock()
		if old := r.timers[key]; old != nil {
			old.Stop()
		}
		r.timers[key] = time.AfterFunc(time.Duration(max(0, c.At-r.now()))*time.Millisecond, func() {
			r.mu.Lock()
			delete(r.timers, key)
			closed := r.closed
			r.mu.Unlock()
			if !closed {
				r.track(func() {
					if err := skipUnknownPlan(r.fire(context.Background(), t)); err != nil {
						r.logger.Warn("kairo: firing a timer", "run", runID, "err", err)
					}
				})
			}
		})
		r.mu.Unlock()
	case "cancel_timer":
		key := fmt.Sprintf("%s\x00t%d", runID, c.Timer)
		r.mu.Lock()
		if t := r.timers[key]; t != nil {
			t.Stop()
			delete(r.timers, key)
		}
		r.mu.Unlock()
	case "abort":
		r.mu.Lock()
		if cancel := r.running[fmt.Sprintf("%s\x00%d", runID, c.Act)]; cancel != nil {
			cancel()
		}
		r.mu.Unlock()
	}
}

// dispatch runs a dispatched step here and applies its outcome.
func (r *runtime) dispatch(runID string, c wasmcore.Command) {
	r.mu.Lock()
	h := r.handler
	if h == nil || r.closed {
		r.mu.Unlock()
		return
	}
	key := fmt.Sprintf("%s\x00%d", runID, c.Act)
	ctx, cancel := context.WithCancel(context.Background())
	r.running[key] = cancel
	r.mu.Unlock()
	r.renew()
	t := task{RunID: runID, StepID: c.StepID, Act: c.Act, Attempt: c.Attempt, IdemKey: c.IdemKey, Action: c.Action, Input: c.Input}
	r.observe(Observation{Kind: ObsStepStarted, RunID: runID, Action: c.Action, StepID: c.StepID, Attempt: c.Attempt})
	began := time.Now()
	res := r.call(ctx, h, t)
	r.observe(Observation{Kind: ObsStepFinished, RunID: runID, Action: c.Action, StepID: c.StepID, Attempt: c.Attempt,
		Status: stepStatus(res), Error: res.Err, Duration: time.Since(began)})
	r.mu.Lock()
	delete(r.running, key)
	r.mu.Unlock()
	cancel()
	r.renew()
	if r.isClosed() {
		return
	}
	if res.Pending != nil {
		// It runs elsewhere (ADR 0052): its lease goes there until its
		// outcome comes (complete) or the lease expires.
		if err := r.store.HandOver(context.Background(), LeaseRow{Run: runID, Act: c.Act, Attempt: c.Attempt,
			Owner: res.Pending.Owner, Until: r.now() + res.Pending.LeaseMs}); err != nil {
			r.logger.Warn("kairo: handing over a lease", "run", runID, "err", err)
		}
		return
	}
	if _, err := r.process(context.Background(), runID, []wasmcore.Event{outcome(res, c.Act, c.Attempt, r.now())}, nil); err != nil {
		r.logger.Warn("kairo: applying a step's outcome", "run", runID, "err", err)
	}
}

// call runs a handler; a panic is the step's definite failure.
func (r *runtime) call(ctx context.Context, h handlerFunc, t task) (res result) {
	defer func() {
		if p := recover(); p != nil {
			res = result{Err: fmt.Sprint(p), ErrType: "panic"}
		}
	}()
	return h(ctx, t, func(data []byte) { r.publish(Chunk{RunID: t.RunID, StepID: t.StepID, Data: data}) })
}

// complete applies the outcome of a step that ran elsewhere (ADR 0052).
func (r *runtime) complete(ctx context.Context, runID string, act uint32, attempt int32, res result) error {
	r.observe(Observation{Kind: ObsStepFinished, RunID: runID, Attempt: attempt, Status: stepStatus(res), Error: res.Err})
	_, err := r.process(ctx, runID, []wasmcore.Event{outcome(res, act, attempt, r.now())}, nil)
	return err
}

func outcome(res result, act uint32, attempt int32, at int64) wasmcore.Event {
	if res.Err != "" || res.Unknown {
		msg := res.Err
		if msg == "" {
			msg = "outcome unknown"
		}
		return wasmcore.Event{Kind: "step_err", At: at, Act: act, Attempt: attempt, Err: msg, Retryable: res.Retryable,
			Unknown: res.Unknown, ErrType: res.ErrType}
	}
	if res.Wait != nil {
		return wasmcore.Event{Kind: "step_wait", At: at, Act: act, Attempt: attempt, Deadline: res.Wait.Until, Data: orNull(res.Wait.Output)}
	}
	return wasmcore.Event{Kind: "step_ok", At: at, Act: act, Attempt: attempt, Data: orNull(res.Output)}
}

func orNull(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	return b
}

// subscribe delivers the live output of run id and of every run under it
// (ids that start with id + "/": its calls and child workflows). Chunks a
// slow subscriber cannot take are dropped.
func (r *runtime) subscribe(id string, buffer int) (<-chan Chunk, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan Chunk, buffer)
	if r.closed {
		close(ch)
		return ch, func() {}
	}
	n := r.nextSub
	r.nextSub++
	r.subs[n] = &subscription{prefix: id, ch: ch}
	return ch, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if s := r.subs[n]; s != nil {
			close(s.ch)
			delete(r.subs, n)
		}
	}
}

func (r *runtime) publish(c Chunk) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.subs {
		if c.RunID == s.prefix || strings.HasPrefix(c.RunID, s.prefix+"/") {
			select {
			case s.ch <- c:
			default:
			}
		}
	}
}

func (r *runtime) nextWake(ctx context.Context) (int64, bool, error) {
	return r.store.NextWake(ctx)
}
