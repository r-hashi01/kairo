package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/task"
)

const workflowPlan = `{"name":"kairo.workflow","root":{"kind":"wait","id":"done","signal":"done"}}`
const childWaitPlan = `{"name":"child-wait","root":{"kind":"wait","id":"hold","signal":"go"}}`
const childStepPlan = `{"name":"child-step","root":{"kind":"step","id":"work","action":"llm"}}`

func registerParentPlans(t testing.TB, e *Engine) {
	t.Helper()
	mustPlan(t, e, workflowPlan)
	mustPlan(t, e, childWaitPlan)
	mustPlan(t, e, childStepPlan)
}

func TestParentAttachAndSettlementHaveOneOrder(t *testing.T) {
	e := newEngine(t, Config{Shards: 4, DataDir: t.TempDir(), NoSync: true})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		parent := "race-parent-" + strconv.Itoa(i)
		child := "race-child-" + strconv.Itoa(i)
		if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", RunID: parent}); err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		finishErr := make(chan error, 1)
		childErr := make(chan error, 1)
		go func() {
			<-ready
			finishErr <- e.FinishWorkflow(context.Background(), parent, json.RawMessage(`{"ok":true}`))
		}()
		go func() {
			<-ready
			_, err := e.Submit(context.Background(), SubmitRequest{Plan: "child-wait", Tenant: "t", RunID: child, Parent: parent})
			childErr <- err
		}()
		close(ready)
		fe, ce := <-finishErr, <-childErr
		switch {
		case fe == nil && errors.Is(ce, ErrParentUnavailable):
		case errors.Is(fe, ErrChildrenUnsettled) && ce == nil:
			if err := e.CancelTree(context.Background(), child, "settle race child"); err != nil {
				t.Fatal(err)
			}
			wait(t, e, child)
			if err := e.FinishWorkflow(context.Background(), parent, json.RawMessage(`{"ok":true}`)); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("finish=%v child=%v", fe, ce)
		}
		wait(t, e, parent)
	}
}

func TestReusedParentIDAcceptsNewChildren(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, RecentRuns: 1})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	none := TierNone
	finish := func(id string) {
		if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &none, RunID: id}); err != nil {
			t.Fatal(err)
		}
		if err := e.FinishWorkflow(context.Background(), id, json.RawMessage(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
		wait(t, e, id)
	}
	finish("reused-parent")
	finish("evicts-parent")
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &none, RunID: "reused-parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "child-wait", Tenant: "t", Tier: &none, RunID: "new-child", Parent: "reused-parent"}); err != nil {
		t.Fatalf("new lifetime retained the old closed flag: %v", err)
	}
}

func TestParentTreeGuardsSettlement(t *testing.T) {
	e := newEngine(t, Config{Shards: 2, DataDir: t.TempDir(), NoSync: true, EvictAfter: time.Millisecond})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}

	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", RunID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "child-wait", Tenant: "t", RunID: "child", Parent: "parent"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		ri, err := e.Get(context.Background(), "child")
		return err == nil && ri.Evicted
	})
	children, err := e.Children(context.Background(), "parent")
	if err != nil || len(children) != 1 || children[0].RunID != "child" || children[0].Parent != "parent" {
		t.Fatalf("children: %+v %v", children, err)
	}
	if err := e.FinishWorkflow(context.Background(), "parent", json.RawMessage(`{"ok":true}`)); !errors.Is(err, ErrChildrenUnsettled) {
		t.Fatalf("finish with an active child: %v", err)
	}
	if err := e.CancelTree(context.Background(), "parent", "stop"); !errors.Is(err, ErrChildrenUnsettled) {
		t.Fatalf("cancel with an active child: %v", err)
	}

	e.Signal("child", "go", json.RawMessage(`{"child":true}`))
	if ri := wait(t, e, "child"); ri.Status != "completed" {
		t.Fatalf("child: %+v", ri)
	}
	children, err = e.Children(context.Background(), "parent")
	if err != nil || len(children) != 1 || children[0].Status != "completed" {
		t.Fatalf("finished child: %+v %v", children, err)
	}
	if err := e.FinishWorkflow(context.Background(), "parent", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if ri := wait(t, e, "parent"); ri.Status != "completed" {
		t.Fatalf("parent: %+v", ri)
	}
	if _, err := e.Submit(context.Background(), SubmitRequest{Plan: "child-wait", Tenant: "t", RunID: "late", Parent: "parent"}); !errors.Is(err, ErrParentUnavailable) {
		t.Fatalf("attach after parent settlement: %v", err)
	}
}

func TestFinishedChildRelationExpiresWithMarker(t *testing.T) {
	defer func(n int) { markerRetireEvery = n }(markerRetireEvery)
	markerRetireEvery = 1
	var skew atomic.Int64
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true,
		Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &ft, RunID: "expiry-parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "child-wait", Tenant: "t", Tier: &ft, RunID: "expiry-child", Parent: "expiry-parent"}); err != nil {
		t.Fatal(err)
	}
	e.Signal("expiry-child", "go", nil)
	wait(t, e, "expiry-child")
	if children, err := e.Children(context.Background(), "expiry-parent"); err != nil || len(children) != 1 {
		t.Fatalf("before expiry: %+v %v", children, err)
	}
	skew.Store(int64(25 * time.Hour))
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &ft, RunID: "expiry-trigger"}); err != nil {
		t.Fatal(err)
	}
	if err := e.FinishWorkflow(context.Background(), "expiry-trigger", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	wait(t, e, "expiry-trigger")
	waitFor(t, func() bool {
		children, err := e.Children(context.Background(), "expiry-parent")
		return err == nil && len(children) == 0
	})
}

func TestRecoveredChildWaitsUntilReached(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 2, DataDir: dir, NoSync: true})
		registerParentPlans(t, e)
		return e
	}

	e1 := start()
	if err := e1.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e1, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &ft, RunID: "parent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e1, SubmitRequest{Plan: "child-step", Tenant: "t", Tier: &ft, RunID: "child", Parent: "parent"}); err != nil {
		t.Fatal(err)
	}
	e1.Close()

	e2 := start()
	defer e2.Close()
	dispatched := make(chan string, 2)
	e2.RegisterExecutor([]string{"llm"}, 1, ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		dispatched <- tk.RunID
		return task.Result{Output: json.RawMessage(`{"ok":true}`)}
	}))
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}

	// A known task sent after recovery is the first dispatch. With one worker,
	// this also proves that recovery did not enqueue the unreached child.
	if _, err := submit(e2, SubmitRequest{Plan: "child-step", Tenant: "t", RunID: "probe"}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-dispatched:
		if id != "probe" {
			t.Fatalf("recovered child dispatched before it was reached: %s", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("probe was not dispatched")
	}
	wait(t, e2, "probe")

	children, err := e2.Children(context.Background(), "parent")
	if err != nil || len(children) != 1 || children[0].RunID != "child" || children[0].Parent != "parent" {
		t.Fatalf("children after restart: %+v %v", children, err)
	}
	res, err := e2.Submit(context.Background(), SubmitRequest{Plan: "child-step", Tenant: "t", Tier: &ft, RunID: "child", Parent: "parent"})
	if err != nil || !res.Existing {
		t.Fatalf("reach recovered child: %+v %v", res, err)
	}
	select {
	case id := <-dispatched:
		if id != "child" {
			t.Fatalf("dispatch after reach: %s", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("reached child was not dispatched")
	}
	if ri := wait(t, e2, "child"); ri.Status != "completed" {
		t.Fatalf("child after restart: %+v", ri)
	}
	if err := e2.FinishWorkflow(context.Background(), "parent", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if ri := wait(t, e2, "parent"); ri.Status != "completed" {
		t.Fatalf("parent after restart: %+v", ri)
	}
}
