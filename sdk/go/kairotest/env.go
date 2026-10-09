package kairotest

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// Env is a kairo for an application's tests: in memory, in suspend mode,
// on a clock the test moves. A workflow that waits (a sleep, a signal, a
// timeout) returns kairo.ErrSuspended from Run; Advance moves the clock
// and fires what is due, Signal sends what it waits for, and Run (or
// Await) again gives its result. Nothing waits in real time.
//
//	env := kairotest.New(t)
//	kairo.Action(env.K, "charge", kairo.Real, stubCharge)
//	kairo.Workflow(env.K, "refund", refund)
//	env.Start()
//	_, err := kairo.Run[Receipt](ctx, env.K, "refund", req, kairo.WithID("r-1"))
//	// err is kairo.ErrSuspended: refund sleeps a day before it charges
//	env.Advance(24 * time.Hour)
//	out, err := kairo.Await[Receipt](ctx, env.K, "r-1")
type Env struct {
	K *kairo.Kairo
	t testing.TB

	mu      sync.Mutex
	now     time.Time
	started map[string]int // run id: the order it started in
}

// EnvOption configures New.
type EnvOption func(*envOpts)

type envOpts struct {
	at   time.Time
	opts []func(*kairo.Options)
}

// At starts the clock at t (default 2026-01-01T00:00:00Z).
func At(t time.Time) EnvOption { return func(o *envOpts) { o.at = t } }

// With changes the kairo.Options New opens with (Concurrency, Observe,
// Logger, ...). Store, Mode, Now and ManualTimers are the Env's.
func With(f func(*kairo.Options)) EnvOption {
	return func(o *envOpts) { o.opts = append(o.opts, f) }
}

// New opens an Env; it is closed when the test ends. Declare actions and
// workflows on env.K, then env.Start.
func New(t testing.TB, opts ...EnvOption) *Env {
	t.Helper()
	o := envOpts{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	for _, f := range opts {
		f(&o)
	}
	e := &Env{t: t, now: o.at, started: map[string]int{}}
	ko := kairo.Options{}
	for _, f := range o.opts {
		f(&ko)
	}
	observe := ko.Observe
	ko.Store, ko.Mode, ko.Now, ko.ManualTimers = kairo.NewMemStore(), kairo.Suspend, e.Now, true
	ko.Observe = func(ob kairo.Observation) {
		if ob.Kind == kairo.ObsRunStarted {
			e.mu.Lock()
			e.started[ob.RunID] = len(e.started)
			e.mu.Unlock()
		}
		if observe != nil {
			observe(ob)
		}
	}
	k, err := kairo.Open(context.Background(), ko)
	if err != nil {
		t.Fatalf("kairotest: %v", err)
	}
	e.K = k
	t.Cleanup(func() { k.Close() })
	return e
}

// Start starts env.K (kairo.Kairo.Start), failing the test on an error.
func (e *Env) Start() {
	e.t.Helper()
	if err := e.K.Start(context.Background()); err != nil {
		e.t.Fatalf("kairotest: %v", err)
	}
}

// Now is the Env's clock.
func (e *Env) Now() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

// Advance moves the clock by d, firing what comes due on the way at the
// time it is due (a sleep that ends, then a timeout that starts from
// there), and driving the workflows on.
func (e *Env) Advance(d time.Duration) {
	e.t.Helper()
	e.mu.Lock()
	target := e.now.Add(d)
	e.mu.Unlock()
	for range 10000 {
		next, ok, err := e.K.Tick(context.Background())
		if err != nil {
			e.t.Fatalf("kairotest: tick: %v", err)
		}
		e.mu.Lock()
		if !ok || next.After(target) {
			e.now = target
			e.mu.Unlock()
			return
		}
		if next.After(e.now) {
			e.now = next // to the next thing due
		}
		e.mu.Unlock()
	}
	e.t.Fatalf("kairotest: still due after 10000 ticks: a timer that fires again at once?")
}

// Signal sends a signal (kairo.Kairo.Signal), failing the test on an
// error.
func (e *Env) Signal(id, name string, payload any) {
	e.t.Helper()
	if err := e.K.Signal(context.Background(), id, name, payload); err != nil {
		e.t.Fatalf("kairotest: %v", err)
	}
}

// Call is one call a workflow made: an action's, a wait for a signal, or
// a child workflow.
type Call struct {
	RunID string
	// Kind is "call", "wait" or "workflow"; Name is the action, the
	// signal or the workflow.
	Kind, Name string
	// Input is what it was given; Output is an action's output, a wait's
	// payload (null if it timed out), a workflow's result.
	Input, Output json.RawMessage
	Status        string // completed, failed, cancelled, blocked, running
	Error         string
}

// Calls are the calls workflow id made, in the order it made them.
func (e *Env) Calls(id string) []Call {
	e.t.Helper()
	kids, err := e.K.Children(context.Background(), id)
	if err != nil {
		e.t.Fatalf("kairotest: %v", err)
	}
	e.mu.Lock()
	order := func(id string) int {
		if n, ok := e.started[id]; ok {
			return n
		}
		return len(e.started) // made by another process: last, by id
	}
	sort.SliceStable(kids, func(i, j int) bool { return order(kids[i].RunID) < order(kids[j].RunID) })
	e.mu.Unlock()
	calls := make([]Call, len(kids))
	for i, r := range kids {
		c := Call{RunID: r.RunID, Status: r.Status, Error: r.Error}
		var in struct {
			In    json.RawMessage `json:"in"`
			Input json.RawMessage `json:"input"`
		}
		_ = json.Unmarshal(r.Input, &in)
		switch {
		case strings.HasPrefix(r.Plan, "kairo.call/"):
			c.Kind, c.Name, c.Input, c.Output = "call", strings.TrimPrefix(r.Plan, "kairo.call/"), in.In, r.Output
		case strings.HasPrefix(r.Plan, "kairo.wait/"):
			signal := strings.TrimPrefix(r.Plan, "kairo.wait/")
			if i := strings.LastIndex(signal, "@"); i >= 0 && strings.Trim(signal[i+1:], "0123456789") == "" && i+1 < len(signal) {
				signal = signal[:i] // its timeout (ADR 0059)
			}
			var w struct {
				Payload json.RawMessage `json:"payload"`
			}
			_ = json.Unmarshal(r.Output, &w)
			c.Kind, c.Name, c.Input, c.Output = "wait", signal, in.In, w.Payload
		default:
			var w struct {
				Payload *struct {
					Value json.RawMessage `json:"value"`
				} `json:"payload"`
			}
			_ = json.Unmarshal(r.Output, &w)
			c.Kind, c.Name, c.Input = "workflow", r.Workflow, in.Input
			if w.Payload != nil {
				c.Output = w.Payload.Value
			}
		}
		calls[i] = c
	}
	return calls
}
