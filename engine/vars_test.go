package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Run variables (ADR 0033): initial values from the submission, written by
// kairo.assign, returned when the run finishes, and kept across a restart
// (they are part of the logged start event).
func TestRunVarsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"conv","vars":{"count":{"type":"integer","value":0},"topic":{"type":"string"}},
	  "root":{"kind":"seq","nodes":[
	    {"kind":"step","id":"inc","action":"kairo.assign","params":{"items":[{"var":"$var.count","op":"+=","value":1}]}},
	    {"kind":"wait","id":"w","signal":"go"},
	    {"kind":"step","id":"set","action":"kairo.assign","input":{"t":"w.payload"},"params":{"items":[{"var":"$var.topic","op":"over-write","input":"t"}]}}]}}`
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond})
		mustPlan(t, e, plan)
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	ft := TierFile
	e1 := start()
	id, err := submit(e1, SubmitRequest{Plan: "conv", Tenant: "t", Tier: &ft, Vars: json.RawMessage(`{"count":41,"topic":"old"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted })
	e1.Close()

	e2 := start()
	defer e2.Close()
	e2.Signal(id, "go", json.RawMessage(`"weather"`))
	ri := wait(t, e2, id)
	if ri.Status != "completed" || string(ri.Vars) != `{"count":42,"topic":"weather"}` {
		t.Fatalf("%+v vars=%s", ri, ri.Vars)
	}
}
