package kairo

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Context is what a workflow function calls. It is a context.Context:
// done when the workflow is cancelled, and when it stops being driven here
// (the caller's context ends, the process closes). Only a cancel in kairo
// cancels its calls.
//
// A workflow must take the same path each time it runs again: read the
// clock and randomness through Now and Random, do I/O in actions, and run
// calls at once only through Parallel (not with goroutines of its own).
type Context struct {
	context.Context
	k  *Kairo
	id string
	// lane names a branch of Parallel: its calls' ids carry it, so that
	// branches running at once number their calls without racing.
	lane string
	mu   sync.Mutex
	seen map[string]int
	// cancelled reports whether the workflow (or one above it) was
	// cancelled in kairo.
	cancelled func() bool
}

// ID is the workflow's execution id.
func (c *Context) ID() string { return c.id }

// next is the run id of the next call of kind with input (canonical): the
// TypeScript and Python SDKs' rule (ADR 0049), with the lane inside
// Parallel.
func (c *Context) next(kind, canon string) string {
	key := kind + "\x00" + canon
	c.mu.Lock()
	n := c.seen[key]
	c.seen[key] = n + 1
	c.mu.Unlock()
	if c.lane == "" {
		return callID(c.id, kind, canon, n)
	}
	id := callID(c.id, kind, canon, n)
	// <workflow>/<hash>.<lane>.<n>
	cut := len(id) - len(strconv.Itoa(n))
	return id[:cut] + c.lane + "." + strconv.Itoa(n)
}

func decode[T any](raw json.RawMessage) (T, error) {
	var v T
	if len(raw) == 0 || string(raw) == "null" {
		return v, nil
	}
	err := json.Unmarshal(raw, &v)
	return v, err
}

// Call runs action with in, once, however often the workflow runs again.
func Call[O any](ctx *Context, action string, in any) (O, error) {
	canon, err := canonical(in)
	if err != nil {
		var zero O
		return zero, err
	}
	out, err := ctx.k.callRun(ctx, planCall+action, nil, in, ctx.next(planCall+action, canon))
	if err != nil {
		var zero O
		return zero, err
	}
	return decode[O](out)
}

// WaitOption configures WaitFor.
type WaitOption func(*waitOpts)

type waitOpts struct{ timeout time.Duration }

// WaitTimeout ends the wait with ErrTimedOut if no signal comes within d
// (ADR 0059). It is kept in kairo: the process may stop meanwhile.
func WaitTimeout(d time.Duration) WaitOption { return func(o *waitOpts) { o.timeout = d } }

// WaitFor waits for signal name (Kairo.Signal) and returns its payload.
func WaitFor[P any](ctx *Context, name string, opts ...WaitOption) (P, error) {
	var zero P
	var o waitOpts
	for _, f := range opts {
		f(&o)
	}
	// The timeout is the plan's, not the call's: its id is the same with
	// or without one, so a signal sent ahead finds it (ADR 0059).
	plan, root := planWait+name, waitRoot(name, 0)
	if ms := o.timeout.Milliseconds(); ms > 0 {
		plan, root = plan+"@"+strconv.FormatInt(ms, 10), waitRoot(name, ms)
	}
	out, err := ctx.k.callRun(ctx, plan, root, nil, ctx.next(planWait+name, "null"))
	if err != nil {
		return zero, err
	}
	var w struct {
		TimedOut bool            `json:"timed_out"`
		Payload  json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(out, &w); err != nil {
		return zero, err
	}
	if w.TimedOut {
		return zero, fmt.Errorf("wait for %s: %w", name, ErrTimedOut)
	}
	return decode[P](w.Payload)
}

// Sleep waits d, in kairo: the process may stop meanwhile.
func (c *Context) Sleep(d time.Duration) error {
	_, err := Call[any](c, builtinSleep, map[string]any{"ms": d.Milliseconds()})
	return err
}

// Now is the time, the same each time the workflow runs again.
func (c *Context) Now() (time.Time, error) {
	ms, err := Call[int64](c, builtinNow, nil)
	return time.UnixMilli(ms), err
}

// Random is a number in [0, 1), the same each time the workflow runs again.
func (c *Context) Random() (float64, error) { return Call[float64](c, builtinRand, nil) }

// Child runs workflow name as a child of this one: cancelled with it, kept
// and removed with it (ADR 0054).
func Child[O any](ctx *Context, name string, in any) (O, error) {
	var zero O
	raw, err := json.Marshal(in)
	if err != nil {
		return zero, err
	}
	canon, err := canonical(in)
	if err != nil {
		return zero, err
	}
	out, err := ctx.k.runAs(ctx, name, raw, ctx.next("kairo.workflow/"+name, canon), ctx.id, ctx.cancelled)
	if err != nil {
		return zero, err
	}
	return decode[O](out)
}

// Parallel runs fns at once and returns once all are done, with the first
// error (by position). Each fn gets a Context of its own branch: use it for
// the calls the branch makes.
func Parallel(ctx *Context, fns ...func(ctx *Context) error) error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		lane := "p" + strconv.Itoa(i)
		if ctx.lane != "" {
			lane = ctx.lane + "." + lane
		}
		branch := &Context{Context: ctx.Context, k: ctx.k, id: ctx.id, lane: lane, seen: map[string]int{}, cancelled: ctx.cancelled}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					errs[i] = fmt.Errorf("kairo: parallel branch %d: %v", i, p)
				}
			}()
			errs[i] = fn(branch)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
