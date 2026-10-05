package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// A graph with two entries, started at one of them, survives a restart in
// the middle (ADR 0029): the entry is part of the logged start event.
func TestGraphEntryAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"g","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"web","action":"llm","input":{"q":"$input.q"}},
	    {"kind":"step","id":"cron","action":"llm"},
	    {"kind":"wait","id":"approve","signal":"go"},
	    {"kind":"step","id":"work","action":"kairo.pass","input":{"web":"web.in.q","cron":"cron"}}],
	  "edges":[{"from":"web","to":"approve"},{"from":"cron","to":"approve"},{"from":"approve","to":"work"}]}}`
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond})
		mustPlan(t, e, plan)
		e.RegisterExecutor([]string{"llm"}, 2, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	ft := TierFile
	e1 := start()
	if _, err := e1.Submit(context.Background(), SubmitRequest{Plan: "g", Tenant: "t", Tier: &ft, Entry: "nope"}); !errors.Is(err, ErrUnknownEntry) {
		t.Fatalf("unknown entry: %v", err)
	}
	id, err := submit(e1, SubmitRequest{Plan: "g", Tenant: "t", Tier: &ft, Entry: "web", Input: json.RawMessage(`{"q":"hi"}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return len(ri.Waits) == 1 && ri.Evicted })
	e1.Close()

	e2 := start()
	defer e2.Close()
	e2.Signal(id, "go", nil)
	ri := wait(t, e2, id)
	if ri.Status != "completed" || string(ri.Output) != `{"work":{"cron":null,"web":"hi"}}` {
		t.Fatalf("%+v", ri)
	}
}
