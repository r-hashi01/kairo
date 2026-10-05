package engine

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"kairo/task"
	"kairo/wal"
)

// A real command held until its intent is durable is not released if its
// map element failed in the meantime (ADR 0032; found in review).
func TestHeldRealCommandOfAbortedElement(t *testing.T) {
	cfg := Config{Shards: 1, RealMinTier: TierMemory, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		if tier != TierMemory {
			return nil, nil
		}
		return &wal.MemSink{Delay: func() { time.Sleep(50 * time.Millisecond) }}, nil
	}}
	e := newEngine(t, cfg)
	defer e.Close()
	mustPlan(t, e, `{"name":"m","root":{"kind":"seq","nodes":[{"kind":"map","id":"m","over":"$input.xs","on_element_error":"null",
	  "body":{"kind":"par","id":"b","nodes":[
	    {"kind":"step","id":"work","action":"llm","retry":{"max_attempts":1}},
	    {"kind":"step","id":"send","action":"send"}]}},
	  {"kind":"wait","id":"w","signal":"go"}]}}`)
	var sends atomic.Int32
	e.RegisterExecutor([]string{"llm"}, 4, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
		return task.Result{Err: "boom"}
	}))
	e.RegisterExecutor([]string{"send"}, 4, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
		sends.Add(1)
		return task.Result{Output: json.RawMessage(`"sent"`)}
	}))
	e.Start()
	id, err := submit(e, SubmitRequest{Plan: "m", Tenant: "t", Input: json.RawMessage(`{"xs":[1]}`)})
	if err != nil {
		t.Fatal(err)
	}
	// Past the map (its element failed): now waiting, everything acknowledged.
	waitFor(t, func() bool { ri, _ := e.Get(context.Background(), id); return len(ri.Waits) == 1 })
	e.Signal(id, "go", nil)
	if ri := wait(t, e, id); ri.Status != "completed" || string(ri.Output) != `{"timed_out":false,"payload":null}` {
		t.Fatalf("%+v", ri)
	}
	if n := sends.Load(); n != 0 {
		t.Fatalf("the aborted element's real command ran %d times", n)
	}
}
