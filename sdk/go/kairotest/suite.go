// Package kairotest helps test with kairo's Go SDK. New gives an
// application's tests a kairo in memory, on a clock they move (Env). Run is
// the SDK's behaviour, as a suite any store runs (ADR 0058): sdk/go runs it
// in memory, sdk/go/storetest on SQLite and PostgreSQL.
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
		"ResumedElsewhereRunsNothingTwice":   testResumedElsewhereRunsNothingTwice,
		"SleepSignalAndChild":                testSleepSignalAndChild,
		"Cancel":                             testCancel,
		"ResolveABlockedCall":                testResolveABlockedCall,
		"LiveOutput":                         testLiveOutput,
		"ServerlessAcrossInvocations":        testServerlessAcrossInvocations,
		"FinishedTreesRemoved":               testFinishedTreesRemoved,
		"ExternalParent":                     testExternalParent,
		"ExternalParentRecovery":             testExternalParentRecovery,
		"ExternalParentSettlementRace":       testExternalParentSettlementRace,
		"StopWaitingIsNotCancel":             testStopWaitingIsNotCancel,
		"CancelReachesChildren":              testCancelReachesChildren,
		"PanicIsAFailure":                    testPanicIsAFailure,
		"StoppedDriverIsTakenUp":             testStoppedDriverIsTakenUp,
		"LiveDriverIsNotDoubled":             testLiveDriverIsNotDoubled,
		"SignalBeforeTheWait":                testSignalBeforeTheWait,
		"SignalReceiptsAndOnlyIfWaiting":     testSignalReceiptsAndOnlyIfWaiting,
		"WaitTimesOut":                       testWaitTimesOut,
		"Limits":                             testLimits,
		"SubmitAwaitAndList":                 testSubmitAwaitAndList,
		"RetriesAndTypedErrors":              testRetriesAndTypedErrors,
		"TransientStoreErrorRetriesWorkflow": testTransientStoreErrorRetriesWorkflow,
		"Observe":                            testObserve,
		"VersionsArePinned":                  testVersionsArePinned,
		"SettingsChangeUnderACall":           testSettingsChangeUnderACall,
		"EffectIsNotWeakened":                testEffectIsNotWeakened,
		"ChildVersionNotHere":                testChildVersionNotHere,
		"ParallelTwice":                      testParallelTwice,
		"WaitInAParallelBranch":              testWaitInAParallelBranch,
		"DrainKeepsTheOutcome":               testDrainKeepsTheOutcome,
		"UnreachedChildIsCancelled":          testUnreachedChildIsCancelled,
	} {
		t.Run(name, func(t *testing.T) { test(t, fresh(t)) })
	}
}

// Parent settlement and an external child start have one durable order: the
// child is rejected, or it is in the tree and settled before the parent (ADR 0065).
func testExternalParentSettlementRace(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Mode: kairo.Suspend})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Workflow(k, "hold", func(ctx *kairo.Context, _ any) (string, error) {
		return kairo.WaitFor[string](ctx, "go")
	})
	kairo.Workflow(k, "quick", func(_ *kairo.Context, _ any) (string, error) { return "done", nil })
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		parent := fmt.Sprintf("settlement-parent-%d", i)
		child := fmt.Sprintf("settlement-child-%d", i)
		if _, err := k.Submit(context.Background(), "hold", nil, kairo.WithID(parent)); err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		errs := make(chan error, 2)
		go func() {
			<-ready
			errs <- k.Signal(context.Background(), parent, "go", "done")
		}()
		go func() {
			<-ready
			_, err := k.Submit(context.Background(), "quick", nil, kairo.WithID(child), kairo.Parent(parent))
			errs <- err
		}()
		close(ready)
		for range 2 {
			if err := <-errs; err != nil && !errors.Is(err, kairo.ErrParentUnavailable) {
				t.Fatal(err)
			}
		}
		if out, err := kairo.Run[string](context.Background(), k, "hold", nil, kairo.WithID(parent)); err != nil || out != "done" {
			t.Fatalf("parent %q: %v", out, err)
		}
		got, err := k.Get(context.Background(), child)
		if err == nil && !got.Finished() {
			t.Fatalf("unfinished child after parent settlement: %+v", got)
		}
		if err != nil && !errors.Is(err, kairo.ErrUnknownRun) {
			t.Fatal(err)
		}
	}
}

// An expired drive lease alone does not replay a workflow attached from the
// outside. Re-submitting its id is the new reach (ADR 0065).
func testExternalParentRecovery(t *testing.T, open Opener) {
	var replays atomic.Int32
	start := func(owner string, mode kairo.Mode) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Owner: owner, Lease: 50 * time.Millisecond, Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Workflow(k, "hold", func(ctx *kairo.Context, _ any) (string, error) {
			return kairo.WaitFor[string](ctx, "parent-go")
		})
		kairo.Workflow(k, "external-replay", func(ctx *kairo.Context, _ any) (string, error) {
			replays.Add(1)
			return kairo.WaitFor[string](ctx, "child-go")
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k := start("external-parent-recovery-1", kairo.Wait)
	if _, err := k.Submit(context.Background(), "hold", nil, kairo.WithID("recovery-parent")); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Submit(context.Background(), "external-replay", nil, kairo.WithID("recovery-child"), kairo.Parent("recovery-parent")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return replays.Load() == 1 })
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Millisecond)
	k = start("external-parent-recovery-2", kairo.Suspend)
	defer k.Close()
	if _, _, err := k.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if got := replays.Load(); got != 1 {
		t.Fatalf("expired drive replayed external child %d times", got)
	}
	if _, err := k.Submit(context.Background(), "external-replay", nil, kairo.WithID("recovery-child")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return replays.Load() == 2 })
	child, err := k.Get(context.Background(), "recovery-child")
	if err != nil || child.Parent != "recovery-parent" {
		t.Fatalf("resubmitted child %+v: %v", child, err)
	}
	if err := k.Cancel(context.Background(), "recovery-parent"); err != nil {
		t.Fatal(err)
	}
}

// A workflow started outside another workflow can join its unfinished tree;
// the relation is immutable and controls retention and cancellation (ADR 0065).
func testExternalParent(t *testing.T, open Opener) {
	var skew atomic.Int64
	k, err := kairo.Open(context.Background(), kairo.Options{
		Store: open(), KeepFinished: time.Hour,
		Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Workflow(k, "hold", func(ctx *kairo.Context, _ any) (string, error) {
		return kairo.WaitFor[string](ctx, "go")
	})
	kairo.Workflow(k, "quick", func(_ *kairo.Context, in string) (string, error) { return in, nil })
	kairo.Workflow(k, "child-wait", func(ctx *kairo.Context, _ any) (string, error) {
		return kairo.WaitFor[string](ctx, "child-go")
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitChild := func(parent, plan string) kairo.RunInfo {
		var found kairo.RunInfo
		waitUntil(t, func() bool {
			children, e := k.Children(context.Background(), parent)
			if e != nil {
				t.Fatal(e)
			}
			for _, child := range children {
				if child.Plan == plan {
					found = child
					return true
				}
			}
			return false
		})
		return found
	}

	if _, err := k.Submit(context.Background(), "hold", nil, kairo.WithID("external-parent")); err != nil {
		t.Fatal(err)
	}
	wait := waitChild("external-parent", "kairo.wait/go")
	if _, err := k.Submit(context.Background(), "quick", "kept", kairo.WithID("external-child"), kairo.Parent("external-parent")); err != nil {
		t.Fatal(err)
	}
	if out, err := kairo.Await[string](context.Background(), k, "external-child"); err != nil || out != "kept" {
		t.Fatalf("child result %q: %v", out, err)
	}
	child, err := k.Get(context.Background(), "external-child")
	if err != nil || child.Parent != "external-parent" {
		t.Fatalf("child %+v: %v", child, err)
	}
	if _, err := k.Submit(context.Background(), "quick", "ignored", kairo.WithID("external-child")); err != nil {
		t.Fatalf("resubmit without parent: %v", err)
	}
	roots, err := k.List(context.Background(), kairo.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range roots {
		if root.RunID == "external-child" {
			t.Fatal("the attached child was listed as a root")
		}
	}

	for _, tc := range []struct {
		name, parent string
	}{
		{name: "missing", parent: "no-parent"},
		{name: "not-workflow", parent: wait.RunID},
		{name: "self", parent: "bad-self"},
	} {
		id := "bad-" + tc.name
		if tc.name == "self" {
			id = tc.parent
		}
		if _, err := k.Submit(context.Background(), "quick", "x", kairo.WithID(id), kairo.Parent(tc.parent)); !errors.Is(err, kairo.ErrParentUnavailable) {
			t.Fatalf("%s parent: %v", tc.name, err)
		}
		if _, err := k.Get(context.Background(), id); !errors.Is(err, kairo.ErrUnknownRun) {
			t.Fatalf("%s child exists: %v", tc.name, err)
		}
	}
	if _, err := kairo.Run[string](context.Background(), k, "quick", "done", kairo.WithID("finished-parent")); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Submit(context.Background(), "quick", "x", kairo.WithID("bad-finished"), kairo.Parent("finished-parent")); !errors.Is(err, kairo.ErrParentUnavailable) {
		t.Fatalf("finished parent: %v", err)
	}
	if _, err := k.Submit(context.Background(), "hold", nil, kairo.WithID("other-parent")); err != nil {
		t.Fatal(err)
	}
	waitChild("other-parent", "kairo.wait/go")
	if _, err := k.Submit(context.Background(), "quick", "x", kairo.WithID("external-child"), kairo.Parent("other-parent")); !errors.Is(err, kairo.ErrParentUnavailable) {
		t.Fatalf("changed parent: %v", err)
	}

	skew.Store(int64(2 * time.Hour))
	if _, _, err := k.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Get(context.Background(), "external-child"); err != nil {
		t.Fatalf("finished child was removed before its parent: %v", err)
	}
	if err := k.Signal(context.Background(), "external-parent", "go", "done"); err != nil {
		t.Fatal(err)
	}
	if _, err := kairo.Await[string](context.Background(), k, "external-parent"); err != nil {
		t.Fatal(err)
	}
	skew.Store(int64(4 * time.Hour))
	if _, _, err := k.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"external-parent", "external-child"} {
		if _, err := k.Get(context.Background(), id); !errors.Is(err, kairo.ErrUnknownRun) {
			t.Fatalf("%s remains after its tree expired: %v", id, err)
		}
	}

	if _, err := k.Submit(context.Background(), "hold", nil, kairo.WithID("cancel-external-parent")); err != nil {
		t.Fatal(err)
	}
	waitChild("cancel-external-parent", "kairo.wait/go")
	if _, err := k.Submit(context.Background(), "child-wait", nil, kairo.WithID("cancel-external-child"), kairo.Parent("cancel-external-parent")); err != nil {
		t.Fatal(err)
	}
	waitChild("cancel-external-child", "kairo.wait/child-go")
	if err := k.Cancel(context.Background(), "cancel-external-parent"); err != nil {
		t.Fatal(err)
	}
	if got, err := k.Get(context.Background(), "cancel-external-child"); err != nil || got.Status != "cancelled" {
		t.Fatalf("cancelled external child %+v: %v", got, err)
	}
}

// A child left by a stopped process is not recovered before its parent
// replay reaches it. If the replay takes another path, the child is cancelled
// before the parent records success or failure (ADR 0062).
func testUnreachedChildIsCancelled(t *testing.T, open Opener) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			var skip atomic.Bool
			id := "unreached-" + name
			start := func() *kairo.Kairo {
				k, err := kairo.Open(context.Background(), kairo.Options{
					Store: open(), Mode: kairo.Suspend, Owner: "unreached-child-test-" + name,
				})
				if err != nil {
					t.Fatal(err)
				}
				kairo.Action(k, "old", kairo.Unprotected, func(tc *kairo.TaskContext, _ any) (string, error) {
					calls.Add(1)
					<-tc.Done()
					return "", tc.Err()
				})
				kairo.Workflow(k, "parent", func(ctx *kairo.Context, _ any) (string, error) {
					if skip.Load() {
						if fail {
							return "", errors.New("guard failed")
						}
						return "new path", nil
					}
					return kairo.Call[string](ctx, "old", nil)
				})
				if err := k.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				return k
			}

			k := start()
			stopped := make(chan error, 1)
			go func() {
				_, err := kairo.Run[string](context.Background(), k, "parent", nil, kairo.WithID(id))
				stopped <- err
			}()
			waitUntil(t, func() bool { return calls.Load() == 1 })
			if err := k.Close(); err != nil {
				t.Fatal(err)
			}
			<-stopped

			skip.Store(true)
			k = start() // the named process expires its earlier leases on open
			defer k.Close()
			if _, _, err := k.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatalf("old child ran %d times", calls.Load())
			}
			children, err := k.Children(context.Background(), id)
			if err != nil || len(children) != 1 || children[0].Status != "cancelled" {
				t.Fatalf("children %+v: %v", children, err)
			}
			parent, err := k.Get(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if !parent.Finished() {
				t.Fatalf("parent status %s", parent.Status)
			}
			out, runErr := kairo.Run[string](context.Background(), k, "parent", nil, kairo.WithID(id))
			var workflowErr *kairo.WorkflowError
			if fail && !errors.As(runErr, &workflowErr) {
				t.Fatalf("failure result: %q %v", out, runErr)
			}
			if !fail && (runErr != nil || out != "new path") {
				t.Fatalf("success result: %q %v", out, runErr)
			}
		})
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
	kairo.Workflow(k, "quick", func(_ *kairo.Context, _ any) (string, error) { return "done", nil })
	k.Start(context.Background())
	if got, err := kairo.Run[string](context.Background(), k, "quick", nil, kairo.WithID("quick-1")); err != nil || got != "done" {
		t.Fatalf("quick: %q %v", got, err)
	}
	if err := k.Cancel(context.Background(), "quick-1"); !errors.Is(err, kairo.ErrRunSettled) {
		t.Fatalf("cancel completed: %v", err)
	}
	res := make(chan error, 1)
	go func() {
		_, err := kairo.Run[any](context.Background(), k, "long", nil, kairo.WithID("long-1"))
		res <- err
	}()
	<-started
	if err := k.Cancel(context.Background(), "long-1"); err != nil {
		t.Fatal(err)
	}
	if err := k.Cancel(context.Background(), "long-1"); err != nil {
		t.Fatalf("repeated cancel: %v", err)
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
		kids, _ := k.Children(context.Background(), "sl-1")
		t.Fatalf("write %d; children %+v", runs.get("write"), kids)
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
	kids, err := k.Children(context.Background(), "tree-1")
	if err != nil || len(kids) != 1 {
		t.Fatalf("children: %v %v", kids, err)
	}
	grandkids, err := k.Children(context.Background(), kids[0].RunID)
	if err != nil || len(grandkids) != 1 {
		t.Fatalf("grandchildren: %v %v", grandkids, err)
	}
	for _, id := range []string{kids[0].RunID, grandkids[0].RunID} {
		r, err := k.Get(context.Background(), id)
		if err != nil || r.Status != "cancelled" {
			t.Fatalf("%s after Cancel: %+v %v", id, r, err)
		}
	}
	if err := <-res; !errors.Is(err, kairo.ErrCancelled) {
		t.Fatalf("%v", err)
	}
	<-stopped
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

type onceStore struct {
	kairo.Store
	fail atomic.Bool
}

var errStoreBusy = errors.New("store busy")

func (s *onceStore) WithRun(ctx context.Context, id string, fn func(*kairo.RunRow) (*kairo.Changes, error)) error {
	if s.fail.Swap(false) {
		return errStoreBusy
	}
	return s.Store.WithRun(ctx, id, fn)
}

// A transient Store failure gives up only the current drive. The lease
// expiry replays it, completed calls are reused, and no failure is recorded
// (ADR 0063). The public marker has the same meaning.
func testTransientStoreErrorRetriesWorkflow(t *testing.T, open Opener) {
	s := &onceStore{Store: open()}
	k, err := kairo.Open(context.Background(), kairo.Options{Store: s, Lease: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	var first, second, attempts atomic.Int32
	var armed atomic.Bool
	kairo.Action(k, "first", kairo.Real, func(*kairo.TaskContext, any) (string, error) {
		first.Add(1)
		return "one", nil
	})
	kairo.Action(k, "second", kairo.Unprotected, func(*kairo.TaskContext, any) (string, error) {
		second.Add(1)
		return "two", nil
	})
	kairo.Workflow(k, "store-retry", func(ctx *kairo.Context, _ any) (string, error) {
		attempts.Add(1)
		a, err := kairo.Call[string](ctx, "first", nil)
		if err != nil {
			return "", err
		}
		if armed.CompareAndSwap(false, true) {
			s.fail.Store(true)
		}
		b, err := kairo.Call[string](ctx, "second", nil)
		return a + b, err
	})
	var explicit atomic.Int32
	kairo.Workflow(k, "explicit-retry", func(*kairo.Context, any) (string, error) {
		if explicit.Add(1) == 1 {
			return "", kairo.RetryWorkflow(errors.New("try later"))
		}
		return "ok", nil
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if out, err := kairo.Run[string](context.Background(), k, "store-retry", nil, kairo.WithID("store-retry-1")); err != nil || out != "onetwo" {
		t.Fatalf("%q %v", out, err)
	}
	if first.Load() != 1 || second.Load() != 1 || attempts.Load() < 2 {
		t.Fatalf("first %d second %d attempts %d", first.Load(), second.Load(), attempts.Load())
	}
	if info, err := k.Get(context.Background(), "store-retry-1"); err != nil || info.Status != "completed" {
		t.Fatalf("run %+v: %v", info, err)
	}
	if out, err := kairo.Run[string](context.Background(), k, "explicit-retry", nil, kairo.WithID("explicit-retry-1")); err != nil || out != "ok" {
		t.Fatalf("explicit: %q %v", out, err)
	}
	if explicit.Load() != 2 {
		t.Fatalf("explicit attempts %d", explicit.Load())
	}
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

func (d *deadStore) Recoverable(ctx context.Context, run string, now int64) ([]kairo.LeaseRow, []kairo.TimerRow, error) {
	return d.Store.Recoverable(ctx, run, now)
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

func testSignalReceiptsAndOnlyIfWaiting(t *testing.T, open Opener) {
	gates := map[string]chan struct{}{
		"only":         make(chan struct{}),
		"early":        make(chan struct{}),
		"create":       make(chan struct{}),
		"create-after": make(chan struct{}),
		"legacy":       make(chan struct{}),
	}
	var gateStarted sync.Map
	start := func(store kairo.Store) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: store, Lease: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "signal-gate", kairo.Unprotected, func(_ *kairo.TaskContext, key string) (any, error) {
			gateStarted.Store(key, true)
			<-gates[key]
			return nil, nil
		})
		kairo.Workflow(k, "signals", func(ctx *kairo.Context, key string) ([]string, error) {
			a, err := kairo.WaitFor[string](ctx, "message")
			if err != nil {
				return nil, err
			}
			if _, err := kairo.Call[any](ctx, "signal-gate", key); err != nil {
				return nil, err
			}
			b, err := kairo.WaitFor[string](ctx, "message")
			return []string{a, b}, err
		})
		kairo.Workflow(k, "signal-race", func(ctx *kairo.Context, _ any) ([]string, error) {
			if _, err := kairo.Call[any](ctx, "signal-gate", "create"); err != nil {
				return nil, err
			}
			a, err := kairo.WaitFor[string](ctx, "message")
			if err != nil {
				return nil, err
			}
			if _, err := kairo.Call[any](ctx, "signal-gate", "create-after"); err != nil {
				return nil, err
			}
			b, err := kairo.WaitFor[string](ctx, "message")
			return []string{a, b}, err
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	a, b := start(open()), start(open())
	defer a.Close()
	defer b.Close()

	waits := func(id string) []kairo.RunInfo {
		children, err := a.Children(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		var out []kairo.RunInfo
		for _, child := range children {
			if child.Plan == "kairo.wait/message" {
				out = append(out, child)
			}
		}
		return out
	}
	waiting := func(id string, n int) {
		waitUntil(t, func() bool {
			ws := waits(id)
			return len(ws) == n && !ws[n-1].Finished()
		})
	}
	type answer struct {
		receipt kairo.SignalReceipt
		payload string
		err     error
	}
	sendTwo := func(id string, only bool) []answer {
		ready := make(chan struct{})
		answers := make(chan answer, 2)
		for i, k := range []*kairo.Kairo{a, b} {
			payload := fmt.Sprintf("answer-%d", i)
			go func(k *kairo.Kairo, payload string) {
				<-ready
				var r kairo.SignalReceipt
				var err error
				if only {
					r, err = k.SendSignal(context.Background(), id, "message", payload, kairo.OnlyIfWaiting())
				} else {
					r, err = k.SendSignal(context.Background(), id, "message", payload)
				}
				answers <- answer{receipt: r, payload: payload, err: err}
			}(k, payload)
		}
		close(ready)
		return []answer{<-answers, <-answers}
	}

	if _, err := a.Submit(context.Background(), "signals", "only", kairo.WithID("signal-only-1")); err != nil {
		t.Fatal(err)
	}
	waiting("signal-only-1", 1)
	only := sendTwo("signal-only-1", true)
	var winner answer
	notWaiting := 0
	for _, got := range only {
		if errors.Is(got.err, kairo.ErrNotWaiting) {
			notWaiting++
			continue
		}
		if got.err != nil {
			t.Fatal(got.err)
		}
		winner = got
		if got.receipt.Index != 0 || got.receipt.Early {
			t.Fatalf("only receipt %+v", got.receipt)
		}
	}
	if notWaiting != 1 {
		t.Fatalf("only-if-waiting losers %d: %+v", notWaiting, only)
	}
	waitUntil(t, func() bool { _, ok := gateStarted.Load("only"); return ok })
	if got := len(waits("signal-only-1")); got != 1 {
		t.Fatalf("only-if-waiting left %d waits", got)
	}
	close(gates["only"])
	waiting("signal-only-1", 2)
	second, err := a.SendSignal(context.Background(), "signal-only-1", "message", "second", kairo.OnlyIfWaiting())
	if err != nil || second.Index != 1 || second.Early {
		t.Fatalf("second %+v: %v", second, err)
	}
	if out, err := kairo.Await[[]string](context.Background(), a, "signal-only-1"); err != nil || len(out) != 2 || out[0] != winner.payload || out[1] != "second" {
		t.Fatalf("only result %#v: %v", out, err)
	}
	if _, err := a.SendSignal(context.Background(), "signal-only-1", "message", "late", kairo.OnlyIfWaiting()); !errors.Is(err, kairo.ErrNotWaiting) {
		t.Fatalf("finished workflow: %v", err)
	}
	if _, err := a.SendSignal(context.Background(), "no-such-workflow", "message", "x", kairo.OnlyIfWaiting()); err == nil || errors.Is(err, kairo.ErrNotWaiting) {
		t.Fatalf("missing workflow: %v", err)
	}

	if _, err := a.Submit(context.Background(), "signals", "early", kairo.WithID("signal-early-1")); err != nil {
		t.Fatal(err)
	}
	waiting("signal-early-1", 1)
	early := sendTwo("signal-early-1", false)
	sort.Slice(early, func(i, j int) bool { return early[i].receipt.Index < early[j].receipt.Index })
	if early[0].err != nil || early[1].err != nil || early[0].receipt.Index != 0 || early[0].receipt.Early ||
		early[1].receipt.Index != 1 || !early[1].receipt.Early || early[0].receipt.RunID == early[1].receipt.RunID {
		t.Fatalf("early receipts %+v", early)
	}
	waitUntil(t, func() bool { _, ok := gateStarted.Load("early"); return ok })
	close(gates["early"])
	if out, err := kairo.Await[[]string](context.Background(), a, "signal-early-1"); err != nil || len(out) != 2 || out[0] != early[0].payload || out[1] != early[1].payload {
		t.Fatalf("early result %#v: %v", out, err)
	}

	if _, err := a.Submit(context.Background(), "signal-race", nil, kairo.WithID("signal-create-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { _, ok := gateStarted.Load("create"); return ok })
	race := make(chan answer, 1)
	go func() {
		r, err := b.SendSignal(context.Background(), "signal-create-1", "message", "raced", kairo.OnlyIfWaiting())
		race <- answer{receipt: r, payload: "raced", err: err}
	}()
	close(gates["create"])
	raced := <-race
	first := "raced"
	if errors.Is(raced.err, kairo.ErrNotWaiting) {
		waiting("signal-create-1", 1)
		if _, err := a.SendSignal(context.Background(), "signal-create-1", "message", "manual", kairo.OnlyIfWaiting()); err != nil {
			t.Fatal(err)
		}
		first = "manual"
	} else if raced.err != nil || raced.receipt.Index != 0 || raced.receipt.Early {
		t.Fatalf("create race %+v", raced)
	}
	waitUntil(t, func() bool { _, ok := gateStarted.Load("create-after"); return ok })
	if got := len(waits("signal-create-1")); got != 1 {
		t.Fatalf("create race left %d waits", got)
	}
	close(gates["create-after"])
	waiting("signal-create-1", 2)
	if _, err := a.SendSignal(context.Background(), "signal-create-1", "message", "second", kairo.OnlyIfWaiting()); err != nil {
		t.Fatal(err)
	}
	if out, err := kairo.Await[[]string](context.Background(), a, "signal-create-1"); err != nil || !equalJSON(out, []string{first, "second"}) {
		t.Fatalf("create race result %#v: %v", out, err)
	}

	if _, err := a.Submit(context.Background(), "signals", "legacy", kairo.WithID("signal-legacy-1")); err != nil {
		t.Fatal(err)
	}
	waiting("signal-legacy-1", 1)
	ready := make(chan struct{})
	legacy := make(chan answer, 2)
	for i, k := range []*kairo.Kairo{a, b} {
		payload := fmt.Sprintf("legacy-%d", i)
		go func(k *kairo.Kairo, payload string) {
			<-ready
			legacy <- answer{payload: payload, err: k.Signal(context.Background(), "signal-legacy-1", "message", payload)}
		}(k, payload)
	}
	close(ready)
	legacyAnswers := []answer{<-legacy, <-legacy}
	for _, got := range legacyAnswers {
		if got.err != nil {
			t.Fatal(got.err)
		}
	}
	waitUntil(t, func() bool { _, ok := gateStarted.Load("legacy"); return ok })
	if got := len(waits("signal-legacy-1")); got != 2 {
		t.Fatalf("legacy signal made %d waits", got)
	}
	close(gates["legacy"])
	out, err := kairo.Await[[]string](context.Background(), a, "signal-legacy-1")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	if !equalJSON(out, []string{"legacy-0", "legacy-1"}) {
		t.Fatalf("legacy result %#v", out)
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
	began := time.Now()
	out, err := kairo.Run[int](context.Background(), k, "w", nil, kairo.WithID("limits-1"))
	if err != nil || out != 15 {
		t.Fatalf("%d %v", out, err)
	}
	if most > 2 {
		t.Fatalf("%d steps of slow at once", most)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	// The i-th start is not before its turn: i spacings after the workflow
	// began (a start that comes late on its thread does not fail this).
	for i, s := range starts {
		if after := s.Sub(began); after < time.Duration(i)*100*time.Millisecond {
			t.Fatalf("paced start %d came %v after the workflow began", i, after)
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

// An action's retryable failures are retried up to its MaxAttempts, its
// Backoff apart; a call that fails is a CallError, a workflow that fails a
// WorkflowError with the CallError in it.
func testRetriesAndTypedErrors(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	runs := &counts{m: map[string]int{}}
	kairo.Action(k, "flaky", kairo.Unprotected, func(tc *kairo.TaskContext, _ any) (int32, error) {
		runs.add("flaky")
		if tc.Attempt < 5 {
			return 0, kairo.Retryable(errors.New("busy"))
		}
		return tc.Attempt, nil
	}, kairo.MaxAttempts(5), kairo.Backoff(time.Millisecond))
	kairo.Action(k, "broken", kairo.Unprotected, func(*kairo.TaskContext, any) (any, error) {
		runs.add("broken")
		return nil, errors.New("no such file")
	}, kairo.MaxAttempts(5))
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (int32, error) {
		n, err := kairo.Call[int32](ctx, "flaky", nil)
		if err != nil {
			return 0, err
		}
		var ce *kairo.CallError
		if _, err := kairo.Call[any](ctx, "broken", nil); !errors.As(err, &ce) || ce.Action != "broken" || ce.Status != "failed" ||
			!strings.Contains(ce.Message, "no such file") {
			return 0, fmt.Errorf("not the CallError: %v", err)
		}
		_, err = kairo.Call[any](ctx, "broken", 1)
		return n, err
	})
	k.Start(context.Background())
	_, err = kairo.Run[int32](context.Background(), k, "w", nil, kairo.WithID("retry-1"))
	var we *kairo.WorkflowError
	if !errors.As(err, &we) || we.ID != "retry-1" || !strings.Contains(we.Message, "no such file") {
		t.Fatalf("%v", err)
	}
	if runs.get("flaky") != 5 {
		t.Fatalf("flaky ran %d times", runs.get("flaky"))
	}
	if runs.get("broken") != 2 { // a failure not retryable: once each
		t.Fatalf("broken ran %d times", runs.get("broken"))
	}
}

// Observe is told of runs that start and settle and of steps' attempts.
func testObserve(t *testing.T, open Opener) {
	var mu sync.Mutex
	var seen []kairo.Observation
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Observe: func(o kairo.Observation) {
		mu.Lock()
		seen = append(seen, o)
		mu.Unlock()
		if o.Kind == kairo.ObsStepStarted && o.Action == "boom" {
			panic("an observer that panics does not stop the runtime")
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "flaky", kairo.Unprotected, func(tc *kairo.TaskContext, _ any) (string, error) {
		if tc.Attempt == 1 {
			return "", kairo.Retryable(errors.New("busy"))
		}
		return "ok", nil
	}, kairo.Backoff(time.Millisecond))
	kairo.Action(k, "boom", kairo.Unprotected, func(*kairo.TaskContext, any) (string, error) { return "b", nil })
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
		a, err := kairo.Call[string](ctx, "flaky", nil)
		if err != nil {
			return "", err
		}
		b, err := kairo.Call[string](ctx, "boom", nil)
		return a + b, err
	})
	k.Start(context.Background())
	if out, err := kairo.Run[string](context.Background(), k, "w", nil, kairo.WithID("obs-1")); err != nil || out != "okb" {
		t.Fatalf("%q %v", out, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var got []string
	for _, o := range seen {
		if o.At.IsZero() {
			t.Fatalf("no time: %+v", o)
		}
		switch o.Kind {
		case kairo.ObsRunStarted:
			got = append(got, "start "+o.Plan+" "+o.Workflow)
		case kairo.ObsRunSettled:
			got = append(got, "settled "+o.Plan+" "+o.Status)
		case kairo.ObsStepStarted:
			got = append(got, fmt.Sprintf("step %s %d", o.Action, o.Attempt))
		case kairo.ObsStepFinished:
			got = append(got, fmt.Sprintf("step %s %d %s %s", o.Action, o.Attempt, o.Status, o.Error))
		}
	}
	want := []string{
		"start kairo.workflow w",
		"start kairo.call/flaky ",
		"step flaky 1", "step flaky 1 retryable busy",
		"step flaky 2", "step flaky 2 ok ",
		"settled kairo.call/flaky completed",
		"start kairo.call/boom ",
		"step boom 1", "step boom 1 ok ",
		"settled kairo.call/boom completed",
		"settled kairo.workflow completed",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("observed:\n%s", strings.Join(got, "\n"))
	}
}

// A run goes on with the version it started with; new runs start with the
// current one; a process without a run's version leaves it (ADR 0060).
func testVersionsArePinned(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	start := func(versions ...string) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Lease: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		for i, v := range versions {
			opts := []kairo.WorkflowOption{kairo.WorkflowVersion(v)}
			if i < len(versions)-1 {
				opts = append(opts, kairo.Draining())
			}
			kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
				runs.add(v)
				p, err := kairo.WaitFor[string](ctx, "go")
				return v + ":" + p, err
			}, opts...)
		}
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k1 := start("1")
	if _, err := k1.Submit(context.Background(), "w", nil, kairo.WithID("ver-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return runs.get("1") == 1 })
	k1.Close() // its drive lease is left expired

	// Only version 2 here: it does not drive ver-1.
	k2 := start("2")
	time.Sleep(300 * time.Millisecond) // sweeps of k2 see ver-1's lease
	if runs.get("2") != 0 {
		t.Fatalf("version 2 drove a version 1 run")
	}
	k2.Close()

	k3 := start("1", "2")
	defer k3.Close()
	if err := k3.Signal(context.Background(), "ver-1", "go", "x"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := kairo.Await[string](ctx, k3, "ver-1"); err != nil || out != "1:x" {
		t.Fatalf("%q %v", out, err)
	}
	if _, err := k3.Submit(context.Background(), "w", nil, kairo.WithID("ver-2")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return k3.Signal(context.Background(), "ver-2", "go", "y") == nil })
	if out, err := kairo.Await[string](ctx, k3, "ver-2"); err != nil || out != "2:y" {
		t.Fatalf("%q %v", out, err)
	}
	for id, v := range map[string]string{"ver-1": "1", "ver-2": "2"} {
		if r, err := k3.Get(context.Background(), id); err != nil || r.Version != v {
			t.Fatalf("%s: %+v %v", id, r, err)
		}
	}
	bad, _ := kairo.Open(context.Background(), kairo.Options{Store: open()})
	kairo.Workflow(bad, "w", func(*kairo.Context, any) (any, error) { return nil, nil }, kairo.WorkflowVersion("1"))
	kairo.Workflow(bad, "w", func(*kairo.Context, any) (any, error) { return nil, nil }, kairo.WorkflowVersion("2"))
	if err := bad.Start(context.Background()); err == nil {
		t.Fatal("two current versions")
	}
	bad.Close()
}

// An action's settings change while a call of it waits to retry: the call
// goes on under the new ones (it used to stop for good).
func testSettingsChangeUnderACall(t *testing.T, open Opener) {
	start := func(opts ...kairo.ActionOption) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Mode: kairo.Suspend})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) { return kairo.Call[string](ctx, "a", nil) })
		kairo.Action(k, "a", kairo.Unprotected, func(tc *kairo.TaskContext, _ any) (string, error) {
			if tc.Attempt == 1 {
				return "", kairo.Retryable(errors.New("busy"))
			}
			return "ok", nil
		}, append([]kairo.ActionOption{kairo.Backoff(50 * time.Millisecond)}, opts...)...)
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k := start()
	if _, err := kairo.Run[string](context.Background(), k, "w", nil, kairo.WithID("set-1")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("%v", err)
	}
	k.Close()
	k = start(kairo.Timeout(time.Minute)) // a deploy changed its settings
	defer k.Close()
	waitUntil(t, func() bool {
		if _, _, err := k.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		r, err := k.Get(context.Background(), "set-1")
		return err == nil && r.Finished()
	})
	if out, err := kairo.Run[string](context.Background(), k, "w", nil, kairo.WithID("set-1")); err != nil || out != "ok" {
		t.Fatalf("%q %v", out, err)
	}
}

// A real attempt is out when its action is changed to unprotected: the
// call does not go on under the new settings (it would be retried on its
// unknown outcome); Tick goes on with the others; ResolveFailed settles it.
func testEffectIsNotWeakened(t *testing.T, open Opener) {
	var mu sync.Mutex
	var stuck []string
	start := func(mode kairo.Mode, effect kairo.Effect) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Mode: mode, Lease: 100 * time.Millisecond,
			Observe: func(o kairo.Observation) {
				if o.Kind == kairo.ObsRunStuck {
					mu.Lock()
					stuck = append(stuck, o.Error)
					mu.Unlock()
				}
			}})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "pay", effect, func(tc *kairo.TaskContext, _ any) (string, error) {
			<-tc.Done() // the process stops while it runs
			return "", tc.Err()
		})
		kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
			if _, err := kairo.Call[string](ctx, "pay", nil); err != nil {
				var ce *kairo.CallError
				if errors.As(err, &ce) {
					return "resolved: " + ce.Message, nil
				}
				return "", err
			}
			return "paid", nil
		})
		kairo.Workflow(k, "nap", func(ctx *kairo.Context, _ any) (string, error) {
			return "rested", ctx.Sleep(200 * time.Millisecond)
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	// A resident process: pay's real attempt is out (its intent recorded)
	// when it stops; nap's timer is in the store.
	k := start(kairo.Wait, kairo.Real)
	for _, w := range []string{"w", "nap"} {
		if _, err := k.Submit(context.Background(), w, nil, kairo.WithID(w+"-1")); err != nil {
			t.Fatal(err)
		}
	}
	var call string
	waitUntil(t, func() bool {
		pay, _ := k.Children(context.Background(), "w-1")
		nap, _ := k.Children(context.Background(), "nap-1")
		if len(pay) == 1 {
			call = pay[0].RunID
		}
		return call != "" && len(nap) == 1
	})
	k.Close()

	k = start(kairo.Suspend, kairo.Unprotected) // a deploy made pay unprotected
	defer k.Close()
	weakened := func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range stuck {
			if strings.Contains(e, kairo.ErrEffectWeakened.Error()) {
				return true
			}
		}
		return false
	}
	// Ticks, as a scheduler's, until pay's expired lease has been seen to
	// be stuck and nap's timer has fired and nap has finished.
	waitUntil(t, func() bool {
		if _, _, err := k.Tick(context.Background()); err != nil {
			t.Fatalf("tick: %v", err)
		}
		r, err := k.Get(context.Background(), "nap-1")
		return err == nil && r.Finished() && weakened()
	})
	if out, err := kairo.Run[string](context.Background(), k, "nap", nil, kairo.WithID("nap-1")); err != nil || out != "rested" {
		t.Fatalf("the other run did not go on: %q %v", out, err)
	}
	if r, _ := k.Get(context.Background(), call); r.Finished() {
		t.Fatalf("the call went on: %+v", r)
	}
	if err := k.ResolveFailed(context.Background(), call, "not paid"); err != nil {
		t.Fatal(err)
	}
	if out, err := kairo.Run[string](context.Background(), k, "w", nil, kairo.WithID("w-1")); err != nil || !strings.Contains(out, "not paid") {
		t.Fatalf("%q %v", out, err)
	}
}

// A child workflow goes on with the version it started with: a process
// that has its parent's version but not the child's leaves both, without
// failing the parent; one with both finishes them (ADR 0060).
func testChildVersionNotHere(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	start := func(childVersions ...string) *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Lease: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Workflow(k, "parent", func(ctx *kairo.Context, _ any) (string, error) {
			runs.add("parent")
			return kairo.Child[string](ctx, "child", nil)
		})
		for i, v := range childVersions {
			opts := []kairo.WorkflowOption{kairo.WorkflowVersion(v)}
			if i < len(childVersions)-1 {
				opts = append(opts, kairo.Draining())
			}
			kairo.Workflow(k, "child", func(ctx *kairo.Context, _ any) (string, error) {
				runs.add("child " + v)
				p, err := kairo.WaitFor[string](ctx, "go")
				return v + ":" + p, err
			}, opts...)
		}
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k := start("1")
	if _, err := k.Submit(context.Background(), "parent", nil, kairo.WithID("fam-1")); err != nil {
		t.Fatal(err)
	}
	var child string
	waitUntil(t, func() bool {
		kids, _ := k.Children(context.Background(), "fam-1")
		if len(kids) == 1 {
			child = kids[0].RunID
		}
		return child != "" && runs.get("child 1") == 1
	})
	k.Close()

	// The parent's version, not the child's: it takes the parent up.
	k = start("2")
	waitUntil(t, func() bool { return runs.get("parent") >= 2 })
	if r, err := k.Get(context.Background(), "fam-1"); err != nil || r.Finished() {
		t.Fatalf("the parent ended without its child's version: %+v %v", r, err)
	}
	if runs.get("child 2") != 0 {
		t.Fatal("version 2 drove a version 1 child")
	}
	k.Close()

	k = start("1", "2")
	defer k.Close()
	waitUntil(t, func() bool { return k.Signal(context.Background(), child, "go", "x") == nil })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := kairo.Await[string](ctx, k, "fam-1"); err != nil || out != "1:x" {
		t.Fatalf("%q %v", out, err)
	}
}

// Two Parallel calls whose branches make the same call: each runs it,
// and a process that resumes the workflow gets each its own result.
func testParallelTwice(t *testing.T, open Opener) {
	var n atomic.Int32
	var hang atomic.Bool
	hang.Store(true)
	start := func() *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "next", kairo.Unprotected, func(*kairo.TaskContext, string) (int32, error) { return n.Add(1), nil })
		kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) ([]int32, error) {
			got := make([]int32, 4)
			for round := range 2 {
				err := kairo.Parallel(ctx,
					func(b *kairo.Context) (err error) { got[2*round], err = kairo.Call[int32](b, "next", "x"); return },
					func(b *kairo.Context) (err error) { got[2*round+1], err = kairo.Call[int32](b, "next", "y"); return },
				)
				if err != nil {
					return nil, err
				}
			}
			if hang.Load() {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return got, nil
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k := start()
	if _, err := k.Submit(context.Background(), "w", nil, kairo.WithID("par-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return n.Load() == 4 })
	k.Close()
	hang.Store(false)
	k = start()
	defer k.Close()
	out, err := kairo.Run[[]int32](context.Background(), k, "w", nil, kairo.WithID("par-1"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int32]bool{}
	for _, v := range out {
		seen[v] = true
	}
	if len(seen) != 4 || n.Load() != 4 {
		t.Fatalf("results %v (each call its own, none run again: next ran %d times)", out, n.Load())
	}
}

// A wait in a Parallel branch receives its signal, sent before or after
// the branch reaches it.
func testWaitInAParallelBranch(t *testing.T, open Opener) {
	k, err := kairo.Open(context.Background(), kairo.Options{Store: open()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "echo", kairo.Unprotected, func(_ *kairo.TaskContext, s string) (string, error) { return s, nil })
	kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
		var a, b, c string
		err := kairo.Parallel(ctx,
			func(br *kairo.Context) (err error) { a, err = kairo.WaitFor[string](br, "go"); return },
			func(br *kairo.Context) (err error) { b, err = kairo.Call[string](br, "echo", "b"); return },
		)
		if err != nil {
			return "", err
		}
		err = kairo.Parallel(ctx, func(br *kairo.Context) (err error) { c, err = kairo.WaitFor[string](br, "go"); return })
		return a + b + c, err
	})
	k.Start(context.Background())
	if _, err := k.Submit(context.Background(), "w", nil, kairo.WithID("pw-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return k.Signal(context.Background(), "pw-1", "go", "a") == nil })
	if err := k.Signal(context.Background(), "pw-1", "go", "c"); err != nil { // before the second branch waits, maybe
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := kairo.Await[string](ctx, k, "pw-1"); err != nil || out != "abc" {
		t.Fatalf("%q %v", out, err)
	}
}

// Drain lets a real step that runs finish and records its outcome, and
// starts no new step: the next process goes on from there, with nothing
// to review and nothing run twice (issue #1).
func testDrainKeepsTheOutcome(t *testing.T, open Opener) {
	runs := &counts{m: map[string]int{}}
	release := make(chan struct{})
	start := func() *kairo.Kairo {
		k, err := kairo.Open(context.Background(), kairo.Options{Store: open(), Lease: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		kairo.Action(k, "pay", kairo.Real, func(*kairo.TaskContext, any) (string, error) {
			runs.add("pay")
			<-release
			return "paid", nil
		})
		kairo.Action(k, "mail", kairo.Real, func(*kairo.TaskContext, any) (string, error) {
			runs.add("mail")
			return "mailed", nil
		})
		kairo.Workflow(k, "w", func(ctx *kairo.Context, _ any) (string, error) {
			p, err := kairo.Call[string](ctx, "pay", nil)
			if err != nil {
				return "", err
			}
			m, err := kairo.Call[string](ctx, "mail", nil)
			return p + ", " + m, err
		})
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		return k
	}
	k := start()
	if _, err := k.Submit(context.Background(), "w", nil, kairo.WithID("drain-1")); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return runs.get("pay") == 1 })
	// Draining from here: a Drain with its context ended has stopped the
	// new steps and the drivers (all before it returns), and waits for
	// nothing; the Drain below waits for pay.
	ended, cancelEnded := context.WithCancel(context.Background())
	cancelEnded()
	if err := k.Drain(ended); !errors.Is(err, context.Canceled) {
		t.Fatalf("a drain with its context ended: %v", err)
	}
	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drained <- k.Drain(ctx)
	}()
	select {
	case err := <-drained:
		t.Fatalf("drained while pay runs: %v", err)
	default:
	}
	close(release)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	if runs.get("mail") != 0 {
		t.Fatal("a step started while draining")
	}
	// A workflow run after Drain starts no step here either: it waits for
	// the next process.
	if _, err := k.Submit(context.Background(), "w", nil, kairo.WithID("drain-2")); err != nil {
		t.Fatal(err)
	}
	k.Close() // after its background driving has ended
	if runs.get("pay") != 1 {
		t.Fatalf("pay ran %d times: a call started after Drain", runs.get("pay"))
	}

	k = start()
	defer k.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range []string{"drain-1", "drain-2"} {
		if out, err := kairo.Run[string](ctx, k, "w", nil, kairo.WithID(id)); err != nil || out != "paid, mailed" {
			t.Fatalf("%s: %q %v", id, out, err)
		}
	}
	// Each step once for each workflow: none run again, none to review.
	if runs.get("pay") != 2 || runs.get("mail") != 2 {
		t.Fatalf("pay %d, mail %d", runs.get("pay"), runs.get("mail"))
	}
}
