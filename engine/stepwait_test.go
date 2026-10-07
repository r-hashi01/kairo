package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"kairo/task"
)

// A step whose result is a wait ends at its deadline, also when the
// engine restarts in between: the deadline is in the log (ADR 0045).
func TestStepWaitAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"sw","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm","input":{"q":"$input.q"}},
	  {"kind":"step","id":"b","action":"llm","input":{"held":"a.held"}}]}}`
	deadline := time.Now().Add(700 * time.Millisecond)
	exec := ExecutorFunc(func(_ context.Context, t *task.Task, _ func([]byte)) task.Result {
		if t.StepID == "a" {
			return task.Result{Wait: &task.Wait{Until: deadline.UnixMilli(), Output: json.RawMessage(`{"held":"later"}`)}}
		}
		return task.Result{Output: t.Input}
	})
	e1 := newEngine(t, Config{Shards: 2, DataDir: dir, EvictAfter: time.Millisecond})
	mustPlan(t, e1, plan)
	e1.RegisterExecutor([]string{"llm"}, 2, exec)
	if err := e1.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	id, err := submit(e1, SubmitRequest{Plan: "sw", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
	if err != nil {
		t.Fatal(err)
	}
	// Waiting is quiescent: the run is evicted.
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted })
	e1.Close()
	if time.Now().After(deadline) {
		t.Skip("the machine is too slow to restart before the deadline")
	}

	e2 := newEngine(t, Config{Shards: 2, DataDir: dir})
	defer e2.Close()
	mustPlan(t, e2, plan)
	e2.RegisterExecutor([]string{"llm"}, 2, exec)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	ri := wait(t, e2, id)
	if ri.Status != "completed" || !strings.Contains(string(ri.Output), `"held":"later"`) {
		t.Fatalf("%+v", ri)
	}
	if now := time.Now(); now.Before(deadline.Truncate(time.Millisecond)) {
		t.Fatalf("finished by %v, before the deadline %v", now, deadline)
	}
}
