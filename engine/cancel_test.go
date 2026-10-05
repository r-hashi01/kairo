package engine

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"kairo/ir"
	"kairo/task"
)

const slowPlan = `{"name":"slow","root":{"kind":"step","id":"s","action":"slow"}}`

// A step timeout cancels the running attempt (ADR 0026). With one executor
// goroutine the retry can only start once the first attempt gave up, so
// the run completes only if the cancellation reached it.
func TestStepTimeoutCancelsRunningTask(t *testing.T) {
	reg := testRegistry()
	reg.Register(ir.NodeSpec{Action: "slow", Effect: ir.EffectUnprotected, MaxAttempts: 3, Timeout: ir.Duration(50 * time.Millisecond)})
	e := newEngine(t, Config{Shards: 1, Registry: reg})
	defer e.Close()
	mustPlan(t, e, slowPlan)
	var cancelled atomic.Int32
	e.RegisterExecutor([]string{"slow"}, 1, ExecutorFunc(func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		if tk.Attempt == 1 {
			select {
			case <-ctx.Done():
				cancelled.Add(1)
				return task.Result{Err: ctx.Err().Error(), Retryable: true}
			case <-time.After(10 * time.Second):
				return task.Result{Err: "not cancelled"}
			}
		}
		return task.Result{Output: json.RawMessage(`{}`)}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	id, err := submit(e, SubmitRequest{Plan: "slow", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if ri := wait(t, e, id); ri.Status != "completed" {
		t.Fatalf("%+v", ri)
	}
	if cancelled.Load() != 1 {
		t.Fatalf("first attempt cancelled %d times", cancelled.Load())
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("took %v: the timed-out attempt kept its slot", el)
	}
	waitFor(t, func() bool { return e.disp.Tracked() == 0 })
}

// Cancelling a run cancels its running tasks.
func TestRunCancelCancelsRunningTask(t *testing.T) {
	reg := testRegistry()
	reg.Register(ir.NodeSpec{Action: "slow", Effect: ir.EffectUnprotected})
	e := newEngine(t, Config{Shards: 1, Registry: reg})
	defer e.Close()
	mustPlan(t, e, slowPlan)
	started := make(chan struct{})
	stopped := make(chan struct{})
	e.RegisterExecutor([]string{"slow"}, 1, ExecutorFunc(func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		close(started)
		<-ctx.Done()
		close(stopped)
		return task.Result{Err: "cancelled"}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	id, err := submit(e, SubmitRequest{Plan: "slow", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	e.Cancel(id, "user")
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the running task was not cancelled")
	}
	if ri := wait(t, e, id); ri.Status != "cancelled" {
		t.Fatalf("%+v", ri)
	}
	waitFor(t, func() bool { return e.disp.Tracked() == 0 })
}

// Stopping the engine cancels running executors, as before ADR 0026.
func TestCloseCancelsRunningTask(t *testing.T) {
	reg := testRegistry()
	reg.Register(ir.NodeSpec{Action: "slow", Effect: ir.EffectUnprotected})
	e := newEngine(t, Config{Shards: 1, Registry: reg})
	mustPlan(t, e, slowPlan)
	started := make(chan struct{})
	stopped := make(chan struct{})
	e.RegisterExecutor([]string{"slow"}, 1, ExecutorFunc(func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		close(started)
		<-ctx.Done()
		close(stopped)
		return task.Result{Err: "stopped"}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "slow", Tenant: "t"}); err != nil {
		t.Fatal(err)
	}
	<-started
	go e.Close()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the running task")
	}
}
