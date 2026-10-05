package engine

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"kairo/core"
	"kairo/ir"
	"kairo/task"
)

// --- ADR 0030 ---------------------------------------------------------------

// The run's time limit fails it and cancels the task that is running.
func TestRunDeadlineCancelsRunningTask(t *testing.T) {
	reg := testRegistry()
	reg.Register(ir.NodeSpec{Action: "slow", Effect: ir.EffectUnprotected})
	e := newEngine(t, Config{Shards: 1, Registry: reg, RunLimits: RunLimits{MaxDuration: 100 * time.Millisecond}})
	defer e.Close()
	mustPlan(t, e, slowPlan)
	stopped := make(chan struct{})
	e.RegisterExecutor([]string{"slow"}, 1, ExecutorFunc(func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		<-ctx.Done()
		close(stopped)
		return task.Result{Err: "cancelled"}
	}))
	e.Start()
	id, err := submit(e, SubmitRequest{Plan: "slow", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if ri := wait(t, e, id); ri.Status != "failed" || ri.Error != "max execution time exceeded" {
		t.Fatalf("%+v", ri)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the running task was not cancelled")
	}
}

// An executor's error type reaches the step's output through on_error, and
// the run reports the exception.
func TestErrTypeAndExceptions(t *testing.T) {
	e := newEngine(t, Config{Shards: 1})
	defer e.Close()
	mustPlan(t, e, `{"name":"x","root":{"kind":"step","id":"s","action":"llm","retry":{"max_attempts":1},
	  "on_error":{"strategy":"default-value","value":{"ok":false}}}}`)
	var depth int
	e.RegisterExecutor([]string{"llm"}, 1, ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		depth = tk.Depth
		return task.Result{Err: "rate limited", ErrType: "RateLimit"}
	}))
	e.Start()
	id, err := submit(e, SubmitRequest{Plan: "x", Tenant: "t", Depth: 2})
	if err != nil {
		t.Fatal(err)
	}
	ri := wait(t, e, id)
	if ri.Status != "completed" || ri.Exceptions != 1 || string(ri.Output) != `{"error_message":"rate limited","error_type":"RateLimit","ok":false}` {
		t.Fatalf("%+v", ri)
	}
	if depth != 2 {
		t.Fatalf("task depth %d, want 2", depth)
	}
}

func TestMaxDepth(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, MaxDepth: 5})
	defer e.Close()
	mustPlan(t, e, fiveNodes)
	e.Start()
	if _, err := e.Submit(context.Background(), SubmitRequest{Plan: "five", Tenant: "t", Depth: 6}); !errors.Is(err, ErrTooDeep) {
		t.Fatalf("depth 6: %v", err)
	}
}

// Events with the extension fields round-trip; events without them encode
// exactly as before.
func TestEventRecordExtension(t *testing.T) {
	plain := core.Event{Kind: core.EvStepErr, At: 5, Act: 3, Attempt: 1, Err: "x", Retryable: true}
	b := encodeEvent(nil, "r", &plain)
	if b[len(b)-1] != 1 {
		t.Fatalf("plain event changed encoding: % x", b)
	}
	for _, ev := range []core.Event{plain,
		{Kind: core.EvStepErr, Act: 3, Err: "x", ErrType: "Timeout", Unknown: true},
		{Kind: core.EvStart, Data: json.RawMessage(`{}`), Name: "web", MaxSteps: 500, Deadline: 1234567, Depth: 2},
		{Kind: core.EvStepOK, Act: 4, Data: json.RawMessage(`1`), Meta: json.RawMessage(`{"tokens":3}`)},
	} {
		r, err := decodeRecord(encodeEvent(nil, "r", &ev))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*r.ev, ev) {
			t.Fatalf("round trip: %+v != %+v", *r.ev, ev)
		}
	}
}
