// Package kairotest is the behaviour of kairo's Go SDK, as a suite any
// store runs (ADR 0058): sdk/go runs it in memory, sdk/go/storetest on
// SQLite and PostgreSQL.
package kairotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// Opener opens a store on one database: each call is a new store, as a
// new process would open, on the same data.
type Opener func() kairo.Store

// Run runs the suite; fresh makes a new database for each test.
func Run(t *testing.T, fresh func(t *testing.T) Opener) {
	for name, test := range map[string]func(*testing.T, Opener){
		"ResumedElsewhereRunsNothingTwice": testResumedElsewhereRunsNothingTwice,
		"SleepSignalAndChild":              testSleepSignalAndChild,
		"Cancel":                           testCancel,
		"ResolveABlockedCall":              testResolveABlockedCall,
		"LiveOutput":                       testLiveOutput,
		"ServerlessAcrossInvocations":      testServerlessAcrossInvocations,
		"FinishedTreesRemoved":             testFinishedTreesRemoved,
		"StopWaitingIsNotCancel":           testStopWaitingIsNotCancel,
		"CancelReachesChildren":            testCancelReachesChildren,
		"PanicIsAFailure":                  testPanicIsAFailure,
		"StoppedDriverIsTakenUp":           testStoppedDriverIsTakenUp,
		"LiveDriverIsNotDoubled":           testLiveDriverIsNotDoubled,
		"SignalBeforeTheWait":              testSignalBeforeTheWait,
		"WaitTimesOut":                     testWaitTimesOut,
		"Limits":                           testLimits,
		"SubmitAwaitAndList":               testSubmitAwaitAndList,
	} {
		t.Run(name, func(t *testing.T) { test(t, fresh(t)) })
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type counts struct {
	mu sync.Mutex
	m  map[string]int
}

func (c *counts) add(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k]++
}

func (c *counts) get(k string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[k]
}

type Edit struct {
	Answers []string `json:"answers"`
	Wrote   string   `json:"wrote"`
	Again   string   `json:"again"`
}

// A process that stops in the middle: another one resumes the workflow, and
// nothing that finished runs again; the real call runs once.
func testResumedElsewhereRunsNothingTwice(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	var hang atomic.Bool
	hang.Store(true)
	start := func() *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) {
			runs.add("llm")
			return strings.ToUpper(q), nil
		})
		kairo.Action(k, "write", kairo.Real, func(_ *kairo.TaskContext, path string) (string, error) {
			runs.add("write")
			return "wrote " + path, nil
		})
		kairo.Workflow(k, "edit", func(ctx *kairo.Context, files []string) (Edit, error) {
			answers := make([]string, len(files))
			fns := make([]func(*kairo.Context) error, len(files))
			for i, f := range files {
				fns[i] = func(b *kairo.Context) error {
					a, err := kairo.Call[string](b, "llm", f)
					answers[i] = a
					return err
				}
			}
			if err := kairo.Parallel(ctx, fns...); err != nil {
				return Edit{}, err
			}
			w, err := kairo.Call[string](ctx, "write", files[0])
			if err != nil {
				return Edit{}, err
			}
			if hang.Load() {
				<-ctx.Done() // as a process that stops here
				return Edit{}, ctx.Err()
			}
			again, err := kairo.Call[string](ctx, "llm", "done")
			return Edit{Answers: answers, Wrote: w, Again: again}, err
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}

	k1 := start()
	first := make(chan error, 1)
	go func() {
		_, err := kairo.Run[Edit](context.Background(), k1, "edit", []string{"a", "b", "c"}, kairo.WithID("edit-1"))
		first <- err
	}()
	waitUntil(t, func() bool { return runs.get("write") == 1 })
	if runs.get("llm") != 3 {
		t.Fatalf("llm ran %d times", runs.get("llm"))
	}
	k1.Close()
	<-first

	hang.Store(false)
	k2 := start()
	defer k2.Close()
	out, err := kairo.Run[Edit](context.Background(), k2, "edit", []string{"a", "b", "c"}, kairo.WithID("edit-1"))
	if err != nil {
		t.Fatal(err)
	}
	want := Edit{Answers: []string{"A", "B", "C"}, Wrote: "wrote a", Again: "DONE"}
	if !equalJSON(out, want) {
		t.Fatalf("%+v", out)
	}
	if runs.get("write") != 1 || runs.get("llm") != 4 {
		t.Fatalf("write %d, llm %d", runs.get("write"), runs.get("llm"))
	}
	// kairo.Run again: the result, and nothing runs.
	if again, err := kairo.Run[Edit](context.Background(), k2, "edit", []string{"a", "b", "c"}, kairo.WithID("edit-1")); err != nil || !equalJSON(again, want) {
		t.Fatalf("%+v %v", again, err)
	}
	if runs.get("llm") != 4 {
		t.Fatalf("llm ran again")
	}
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func testSleepSignalAndChild(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) { return strings.ToUpper(q), nil })
	type Approval struct {
		By string `json:"by"`
	}
	kairo.Workflow(k, "child", func(ctx *kairo.Context, q string) (string, error) { return kairo.Call[string](ctx, "llm", q) })
	kairo.Workflow(k, "timed", func(ctx *kairo.Context, _ any) (map[string]any, error) {
		t0, err := ctx.Now()
		if err != nil {
			return nil, err
		}
		if err := ctx.Sleep(300 * time.Millisecond); err != nil {
			return nil, err
		}
		t1, err := ctx.Now()
		if err != nil {
			return nil, err
		}
		a, err := kairo.WaitFor[Approval](ctx, "approve")
		if err != nil {
			return nil, err
		}
		c, err := kairo.Child[string](ctx, "child", "kid")
		return map[string]any{"slept": t1.Sub(t0).Milliseconds(), "by": a.By, "child": c}, err
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan map[string]any, 1)
	go func() {
		out, err := kairo.Run[map[string]any](context.Background(), k, "timed", nil, kairo.WithID("timed-1"))
		if err != nil {
			t.Error(err)
		}
		done <- out
	}()
	waitUntil(t, func() bool { return k.Signal(context.Background(), "timed-1", "approve", Approval{By: "alice"}) == nil })
	out := <-done
	if out["slept"].(float64) < 300 || out["by"] != "alice" || out["child"] != "KID" {
		t.Fatalf("%v", out)
	}
	kids, err := k.Children(context.Background(), "timed-1")
	if err != nil || len(kids) != 5 { // now, sleep, now, the wait, the child workflow
		t.Fatalf("children: %d %v", len(kids), err)
	}
}

func testCancel(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	started, stopped := make(chan struct{}), make(chan struct{})
	kairo.Action(k, "slow", kairo.Unprotected, func(tc *kairo.TaskContext, _ any) (any, error) {
		close(started)
		<-tc.Done()
		close(stopped)
		return nil, tc.Err()
	})
	kairo.Workflow(k, "long", func(ctx *kairo.Context, _ any) (any, error) { return kairo.Call[any](ctx, "slow", nil) })
	k.Start(context.Background())
	res := make(chan error, 1)
	go func() {
		_, err := kairo.Run[any](context.Background(), k, "long", nil, kairo.WithID("long-1"))
		res <- err
	}()
	<-started
	if err := k.Cancel(context.Background(), "long-1"); err != nil {
		t.Fatal(err)
	}
	if err := <-res; !errors.Is(err, kairo.ErrCancelled) {
		t.Fatalf("%v", err)
	}
	<-stopped
}

// A real call whose outcome is unknown stops for review; resolved, the
// workflow goes on with the output it was given, and the action ran once.
func testResolveABlockedCall(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var sends atomic.Int32
	kairo.Action(k, "send", kairo.Real, func(_ *kairo.TaskContext, to string) (string, error) {
		sends.Add(1)
		return "", kairo.Unknown(errors.New("connection lost after the request"))
	})
	kairo.Workflow(k, "notify", func(ctx *kairo.Context, to string) (string, error) { return kairo.Call[string](ctx, "send", to) })
	k.Start(context.Background())
	res := make(chan string, 1)
	go func() {
		out, err := kairo.Run[string](context.Background(), k, "notify", "alice", kairo.WithID("notify-1"))
		if err != nil {
			t.Error(err)
		}
		res <- out
	}()
	var blocked string
	waitUntil(t, func() bool {
		kids, _ := k.Children(context.Background(), "notify-1")
		for _, c := range kids {
			if c.Status == "blocked" {
				blocked = c.RunID
			}
		}
		return blocked != ""
	})
	if err := k.Resolve(context.Background(), blocked, "sent by hand"); err != nil {
		t.Fatal(err)
	}
	if out := <-res; out != "sent by hand" {
		t.Fatalf("%q", out)
	}
	if sends.Load() != 1 {
		t.Fatalf("sent %d times", sends.Load())
	}
}

// A step's live output reaches the subscribers of its workflow.
func testLiveOutput(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "stream", kairo.Unprotected, func(tc *kairo.TaskContext, n int) (int, error) {
		for i := 0; i < n; i++ {
			tc.Emit([]byte{byte('a' + i)})
		}
		return n, nil
	})
	kairo.Workflow(k, "talk", func(ctx *kairo.Context, n int) (int, error) { return kairo.Call[int](ctx, "stream", n) })
	k.Start(context.Background())
	chunks, stop := k.Subscribe("talk-1")
	defer stop()
	if _, err := kairo.Run[int](context.Background(), k, "talk", 3, kairo.WithID("talk-1")); err != nil {
		t.Fatal(err)
	}
	var got string
	for len(got) < 3 {
		select {
		case c := <-chunks:
			if !strings.HasPrefix(c.RunID, "talk-1/") {
				t.Fatalf("chunk of %s", c.RunID)
			}
			got += string(c.Data)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %q", got)
		}
	}
	if got != "abc" {
		t.Fatalf("%q", got)
	}
}

// kairo.Suspend mode: each invocation goes as far as it can and returns; Tick and
// Signal drive the workflow on; no call runs twice; Wake is told when.
func testServerlessAcrossInvocations(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	var wakes []time.Time
	start := func() *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Mode: kairo.Suspend,
			Wake: func(at time.Time) error { wakes = append(wakes, at); return nil }})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) { runs.add("llm"); return strings.ToUpper(q), nil })
		kairo.Action(k, "write", kairo.Real, func(_ *kairo.TaskContext, v string) (string, error) { runs.add("write"); return "wrote " + v, nil })
		kairo.Workflow(k, "job", func(ctx *kairo.Context, q string) (string, error) {
			a, err := kairo.Call[string](ctx, "llm", q)
			if err != nil {
				return "", err
			}
			if err := ctx.Sleep(300 * time.Millisecond); err != nil {
				return "", err
			}
			w, err := kairo.Call[string](ctx, "write", a)
			if err != nil {
				return "", err
			}
			by, err := kairo.WaitFor[string](ctx, "approve")
			return w + " for " + by, err
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k := start()
	if _, err := kairo.Run[string](context.Background(), k, "job", "hi", kairo.WithID("sl-1")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("%v", err)
	}
	if runs.get("llm") != 1 || runs.get("write") != 0 || len(wakes) != 1 {
		t.Fatalf("llm %d write %d wakes %d", runs.get("llm"), runs.get("write"), len(wakes))
	}
	k.Close()

	time.Sleep(time.Until(wakes[0]) + 20*time.Millisecond)
	k = start()
	if _, _, err := k.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runs.get("write") != 1 {
		t.Fatalf("write %d", runs.get("write"))
	}
	k.Close()

	k = start()
	defer k.Close()
	if err := k.Signal(context.Background(), "sl-1", "approve", "alice"); err != nil {
		t.Fatal(err)
	}
	out, err := kairo.Run[string](context.Background(), k, "job", "hi", kairo.WithID("sl-1"))
	if err != nil || out != "wrote HI for alice" {
		t.Fatalf("%q %v", out, err)
	}
	if runs.get("llm") != 1 || runs.get("write") != 1 {
		t.Fatalf("a call ran twice: llm %d write %d", runs.get("llm"), runs.get("write"))
	}
}

// Finished workflows go with their calls once kept long enough (ADR 0054).
func testFinishedTreesRemoved(t *testing.T, open Opener) {
	var skew atomic.Int64
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), KeepFinished: time.Hour,
		Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) { return q, nil })
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) { return kairo.Call[string](ctx, "llm", "x") })
	k.Start(context.Background())
	if _, err := kairo.Run[string](context.Background(), k, "w", nil, kairo.WithID("w-1")); err != nil {
		t.Fatal(err)
	}
	if kids, _ := k.Children(context.Background(), "w-1"); len(kids) != 1 {
		t.Fatalf("%d children", len(kids))
	}
	skew.Store(int64(2 * time.Hour))
	if _, _, err := k.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Get(context.Background(), "w-1"); !errors.Is(err, kairo.ErrUnknownRun) {
		t.Fatalf("the workflow is still there: %v", err)
	}
	if kids, _ := k.Children(context.Background(), "w-1"); len(kids) != 0 {
		t.Fatalf("its calls are still there: %d", len(kids))
	}
}

// A caller that stops waiting (its context ends) does not cancel the
// workflow: its calls stay, and it goes on when it is run again.
func testStopWaitingIsNotCancel(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Workflow(k, "inner", func(ctx *kairo.Context, _ any) (string, error) { return kairo.WaitFor[string](ctx, "go") })
	kairo.Workflow(k, "outer", func(ctx *kairo.Context, _ any) (string, error) { return kairo.Child[string](ctx, "inner", nil) })
	k.Start(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := kairo.Run[string](ctx, k, "outer", nil, kairo.WithID("stop-1")); !errors.Is(err, kairo.ErrStopped) {
		t.Fatalf("%v", err)
	}
	kids, err := k.Children(context.Background(), "stop-1")
	if err != nil || len(kids) != 1 {
		t.Fatalf("children: %v %v", kids, err)
	}
	inner := kids[0].RunID
	for _, id := range []string{"stop-1", inner} {
		if r, err := k.Get(context.Background(), id); err != nil || r.Finished() {
			t.Fatalf("%s: %+v %v", id, r, err)
		}
	}
	res := make(chan string, 1)
	go func() {
		out, err := kairo.Run[string](context.Background(), k, "outer", nil, kairo.WithID("stop-1"))
		if err != nil {
			t.Error(err)
		}
		res <- out
	}()
	if err := k.Signal(context.Background(), inner, "go", "on"); err != nil {
		t.Fatal(err)
	}
	if out := <-res; out != "on" {
		t.Fatalf("%q", out)
	}
}

// Cancelling a workflow cancels the workflows it made, and their calls.
func testCancelReachesChildren(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	started, stopped := make(chan struct{}), make(chan struct{})
	kairo.Action(k, "slow", kairo.Unprotected, func(tc *kairo.TaskContext, _ any) (any, error) {
		close(started)
		<-tc.Done()
		close(stopped)
		return nil, tc.Err()
	})
	kairo.Workflow(k, "inner", func(ctx *kairo.Context, _ any) (any, error) { return kairo.Call[any](ctx, "slow", nil) })
	kairo.Workflow(k, "outer", func(ctx *kairo.Context, _ any) (any, error) { return kairo.Child[any](ctx, "inner", nil) })
	k.Start(context.Background())
	res := make(chan error, 1)
	go func() {
		_, err := kairo.Run[any](context.Background(), k, "outer", nil, kairo.WithID("tree-1"))
		res <- err
	}()
	<-started
	if err := k.Cancel(context.Background(), "tree-1"); err != nil {
		t.Fatal(err)
	}
	if err := <-res; !errors.Is(err, kairo.ErrCancelled) {
		t.Fatalf("%v", err)
	}
	<-stopped
	kids, err := k.Children(context.Background(), "tree-1")
	if err != nil || len(kids) != 1 {
		t.Fatalf("children: %v %v", kids, err)
	}
	waitUntil(t, func() bool {
		r, err := k.Get(context.Background(), kids[0].RunID)
		return err == nil && r.Status == "cancelled"
	})
}

// A workflow that panics fails; the process goes on.
func testPanicIsAFailure(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var runs atomic.Int32
	kairo.Workflow(k, "bad", func(ctx *kairo.Context, _ any) (any, error) {
		runs.Add(1)
		var m map[string]int
		m["x"] = 1
		return nil, nil
	})
	k.Start(context.Background())
	if _, err := kairo.Run[any](context.Background(), k, "bad", nil, kairo.WithID("bad-1")); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("%v", err)
	}
	// Recorded as failed: run again, it fails at once, without running.
	if _, err := kairo.Run[any](context.Background(), k, "bad", nil, kairo.WithID("bad-1")); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("again: %v", err)
	}
	if runs.Load() != 1 {
		t.Fatalf("ran %d times", runs.Load())
	}
}

// deadStore is a store that stops answering, as the database does for a
// process that stopped without closing.
type deadStore struct {
	kairo.Store
	dead atomic.Bool
}

var errDead = errors.New("the process stopped")

func (d *deadStore) WithRun(ctx context.Context, id string, fn func(*kairo.RunRow) (*kairo.Changes, error)) error {
	if d.dead.Load() {
		return errDead
	}
	return d.Store.WithRun(ctx, id, fn)
}

func (d *deadStore) RenewLeases(ctx context.Context, owner string, until int64) error {
	if d.dead.Load() {
		return errDead
	}
	return d.Store.RenewLeases(ctx, owner, until)
}

func (d *deadStore) ExpiredLeases(ctx context.Context, now int64, limit int) ([]kairo.LeaseRow, error) {
	if d.dead.Load() {
		return nil, errDead
	}
	return d.Store.ExpiredLeases(ctx, now, limit)
}

func (d *deadStore) DueTimers(ctx context.Context, now int64, limit int) ([]kairo.TimerRow, error) {
	if d.dead.Load() {
		return nil, errDead
	}
	return d.Store.DueTimers(ctx, now, limit)
}

func (d *deadStore) EndDrive(ctx context.Context, run, owner string, token int32, now int64, resume bool) error {
	if d.dead.Load() {
		return errDead
	}
	return d.Store.EndDrive(ctx, run, owner, token, now, resume)
}

// A resident process that stops without closing: another one takes its
// workflow up when its drive lease expires, and the real call it made is
// not made again (ADR 0059).
func testStoppedDriverIsTakenUp(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	start := func(store kairo.Store, hang bool) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: store, Lease: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "write", kairo.Real, func(_ *kairo.TaskContext, p string) (string, error) {
			runs.add("write")
			return "wrote " + p, nil
		})
		kairo.Workflow(k, "w", func(ctx *kairo.Context, p string) (string, error) {
			w, err := kairo.Call[string](ctx, "write", p)
			if err != nil || hang {
				<-ctx.Done()
				return "", ctx.Err()
			}
			return w + "!", nil
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	a := &deadStore{Store: open()}
	k1 := start(a, true)
	defer k1.Close()
	if _, err := k1.Submit(context.Background(), "w", "f", kairo.WithID("taken-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return runs.get("write") == 1 })
	a.dead.Store(true) // stops: its lease is not renewed

	k2 := start(open(), false)
	defer k2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := kairo.Await[string](ctx, k2, "taken-1")
	if err != nil || out != "wrote f!" {
		t.Fatalf("%q %v", out, err)
	}
	if runs.get("write") != 1 {
		t.Fatalf("wrote %d times", runs.get("write"))
	}
}

// While a process drives a workflow, another one that runs it does not
// drive it too: it waits for the result.
func testLiveDriverIsNotDoubled(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	start := func(name string) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Lease: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
			runs.add(name)
			return kairo.WaitFor[string](ctx, "go")
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k1, k2 := start("k1"), start("k2")
	defer k1.Close()
	defer k2.Close()
	if _, err := k1.Submit(context.Background(), "w", nil, kairo.WithID("one-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return runs.get("k1") == 1 })
	res := make(chan string, 1)
	go func() {
		out, err := kairo.Run[string](context.Background(), k2, "w", nil, kairo.WithID("one-1"))
		if err != nil {
			t.Error(err)
		}
		res <- out
	}()
	time.Sleep(300 * time.Millisecond) // a lease period and more: k1 renews it
	if err := k2.Signal(context.Background(), "one-1", "go", "went"); err != nil {
		t.Fatal(err)
	}
	if out := <-res; out != "went" {
		t.Fatalf("%q", out)
	}
	if runs.get("k2") != 0 {
		t.Fatalf("k2 drove it %d times", runs.get("k2"))
	}
}

// Signals sent before the workflow reaches its waits are received there,
// in order.
func testSignalBeforeTheWait(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	gate := make(chan struct{})
	kairo.Action(k, "gate", kairo.Unprotected, func(*kairo.TaskContext, any) (any, error) {
		<-gate
		return nil, nil
	})
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) ([]string, error) {
		if _, err := kairo.Call[any](ctx, "gate", nil); err != nil {
			return nil, err
		}
		a, err := kairo.WaitFor[string](ctx, "s")
		if err != nil {
			return nil, err
		}
		b, err := kairo.WaitFor[string](ctx, "s", kairo.WaitTimeout(time.Minute))
		return []string{a, b}, err
	})
	k.Start(context.Background())
	if err := k.Signal(context.Background(), "nope", "s", "x"); err == nil {
		t.Fatal("a signal to no workflow")
	}
	if _, err := k.Submit(context.Background(), "w", nil, kairo.WithID("ahead-1")); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"first", "second"} {
		if err := k.Signal(context.Background(), "ahead-1", "s", p); err != nil {
			t.Fatal(err)
		}
	}
	close(gate)
	out, err := kairo.Await[[]string](context.Background(), k, "ahead-1")
	if err != nil || !equalJSON(out, []string{"first", "second"}) {
		t.Fatalf("%v %v", out, err)
	}
}

func testWaitTimesOut(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
		v, err := kairo.WaitFor[string](ctx, "approve", kairo.WaitTimeout(100*time.Millisecond))
		if errors.Is(err, kairo.ErrTimedOut) {
			return "timed out", nil
		}
		return v, err
	})
	k.Start(context.Background())
	out, err := kairo.Run[string](context.Background(), k, "w", nil, kairo.WithID("late-1"))
	if err != nil || out != "timed out" {
		t.Fatalf("%q %v", out, err)
	}
}

// Steps over an action's limit wait for a slot; starts keep to its rate.
func testLimits(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Concurrency: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var mu sync.Mutex
	running, most := 0, 0
	var starts []time.Time
	kairo.Action(k, "slow", kairo.Unprotected, func(_ *kairo.TaskContext, n int) (int, error) {
		mu.Lock()
		running++
		most = max(most, running)
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		return n, nil
	}, kairo.Limit(2))
	kairo.Action(k, "paced", kairo.Unprotected, func(_ *kairo.TaskContext, n int) (int, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return n, nil
	}, kairo.Rate(600)) // one each 100ms
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (int, error) {
		sum := 0
		var smu sync.Mutex
		var fns []func(*kairo.Context) error
		for i := range 6 {
			fns = append(fns, func(b *kairo.Context) error {
				v, err := kairo.Call[int](b, "slow", i)
				smu.Lock()
				sum += v
				smu.Unlock()
				return err
			})
		}
		for i := range 3 {
			fns = append(fns, func(b *kairo.Context) error {
				_, err := kairo.Call[int](b, "paced", i)
				return err
			})
		}
		return sum, kairo.Parallel(ctx, fns...)
	})
	k.Start(context.Background())
	out, err := kairo.Run[int](context.Background(), k, "w", nil, kairo.WithID("limits-1"))
	if err != nil || out != 15 {
		t.Fatalf("%d %v", out, err)
	}
	if most > 2 {
		t.Fatalf("%d steps of slow at once", most)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < 90*time.Millisecond {
			t.Fatalf("paced started %v apart", gap)
		}
	}
}

func testSubmitAwaitAndList(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Workflow(k, "double", func(ctx *kairo.Context, n int) (int, error) { return 2 * n, nil })
	kairo.Workflow(k, "other", func(ctx *kairo.Context, _ any) (any, error) { return nil, nil })
	kairo.Workflow(k, "parent", func(ctx *kairo.Context, n int) (int, error) { return kairo.Child[int](ctx, "double", n) })
	k.Start(context.Background())
	for i, id := range []string{"list-1", "list-2", "list-3"} {
		if _, err := k.Submit(context.Background(), "double", i, kairo.WithID(id), kairo.WithMeta(map[string]any{"user": id})); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := kairo.Run[any](context.Background(), k, "other", nil, kairo.WithID("other-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := kairo.Run[int](context.Background(), k, "parent", 5, kairo.WithID("parent-1")); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"list-1", "list-2", "list-3"} {
		if out, err := kairo.Await[int](context.Background(), k, id); err != nil || out != 2*i {
			t.Fatalf("%s: %d %v", id, out, err)
		}
	}
	all, err := k.List(context.Background(), kairo.Filter{Workflow: "double"})
	if err != nil || len(all) != 3 {
		t.Fatalf("%v %v", all, err) // the child workflow of parent-1 is not a root
	}
	for i, r := range all {
		want := fmt.Sprintf(`{"user":"list-%d"}`, i+1)
		if r.RunID != fmt.Sprintf("list-%d", i+1) || r.Workflow != "double" || string(r.Meta) != want || r.Created.IsZero() || r.Status != "completed" {
			t.Fatalf("%d: %+v", i, r)
		}
	}
	page, err := k.List(context.Background(), kairo.Filter{After: all[0].RunID, Limit: 1, Workflow: "double"})
	if err != nil || len(page) != 1 || page[0].RunID != "list-2" {
		t.Fatalf("%v %v", page, err)
	}
	if roots, err := k.List(context.Background(), kairo.Filter{}); err != nil || len(roots) != 5 {
		t.Fatalf("%d %v", len(roots), err)
	}
}
