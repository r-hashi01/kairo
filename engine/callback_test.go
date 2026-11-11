package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/wal"
)

func TestCallbackWaitsForDurabilityAndIsIdempotent(t *testing.T) {
	const delay = 40 * time.Millisecond
	e := newEngine(t, Config{Shards: 1, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		if tier != TierMemory {
			return nil, nil
		}
		return &wal.MemSink{Delay: func() { time.Sleep(delay) }}, nil
	}})
	defer e.Close()
	mustPlan(t, e, `{"name":"callback","root":{"kind":"step","id":"call","action":"llm"}}`)
	poller := e.Dispatcher().NewPoller([]string{"llm"}, 1)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	tier := TierMemory
	id, err := submit(e, SubmitRequest{Plan: "callback", Tenant: "t", Tier: &tier})
	if err != nil {
		t.Fatal(err)
	}
	var tk *task.Task
	select {
	case delivery := <-poller.C:
		tk = delivery.Task
	case <-time.After(5 * time.Second):
		t.Fatal("task was not dispatched")
	}
	tk.Callback = &task.Callback{URL: "https://callbacks.example/token", Until: time.Now().Add(time.Minute).UnixMilli()}
	e.Complete(tk, task.Result{Pending: true})

	started := time.Now()
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < delay/2 {
		t.Fatalf("callback returned in %v, before durable acknowledgement", elapsed)
	}
	ri := wait(t, e, id)
	if ri.Status != "completed" || string(ri.Output) != `{"ok":true}` {
		t.Fatalf("run = %+v", ri)
	}
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`{"ok":false}`)}); err != nil {
		t.Fatalf("duplicate callback: %v", err)
	}
}

func TestADR0070EventsUseVersionedLogRecord(t *testing.T) {
	for _, kind := range []core.EventKind{core.EvStepPending, core.EvCallbackOK, core.EvCallbackErr, core.EvCallbackWait} {
		ev := core.Event{Kind: kind, Act: 2, Attempt: 3, Deadline: 4}
		encoded := encodeEvent(nil, "run", &ev)
		if encoded[0] != recEventV2 {
			t.Fatalf("kind %v record = %d, want %d", kind, encoded[0], recEventV2)
		}
		decoded, err := decodeRecord(encoded)
		if err != nil || decoded.kind != recEventV2 || decoded.ev.Kind != kind {
			t.Fatalf("kind %v decoded = %+v, %v", kind, decoded, err)
		}
	}
	legacy := core.Event{Kind: core.EvStepOK, Act: 1, Attempt: 1, Data: json.RawMessage(`1`)}
	encoded := encodeEvent(nil, "run", &legacy)
	if encoded[0] != recEvent {
		t.Fatalf("legacy record = %d", encoded[0])
	}
}

func TestPendingCallbackSurvivesRestartWithoutRedispatch(t *testing.T) {
	dir := t.TempDir()
	config := Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond}
	e1 := newEngine(t, config)
	mustPlan(t, e1, `{"name":"callback","root":{"kind":"step","id":"call","action":"llm"}}`)
	p1 := e1.Dispatcher().NewPoller([]string{"llm"}, 1)
	if err := e1.Start(); err != nil {
		t.Fatal(err)
	}
	tier := TierFile
	id, err := submit(e1, SubmitRequest{Plan: "callback", Tenant: "t", Tier: &tier})
	if err != nil {
		t.Fatal(err)
	}
	var tk *task.Task
	select {
	case delivery := <-p1.C:
		tk = delivery.Task
	case <-time.After(5 * time.Second):
		t.Fatal("task was not dispatched")
	}
	tk.Callback = &task.Callback{URL: "https://callbacks.example/token", Until: time.Now().Add(time.Minute).UnixMilli()}
	e1.Complete(tk, task.Result{Pending: true})
	waitFor(t, func() bool {
		ri, err := e1.Get(context.Background(), id)
		return err == nil && ri.Evicted
	})
	e1.Close()

	e2 := newEngine(t, config)
	defer e2.Close()
	mustPlan(t, e2, `{"name":"callback","root":{"kind":"step","id":"call","action":"llm"}}`)
	p2 := e2.Dispatcher().NewPoller([]string{"llm"}, 1)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case delivery := <-p2.C:
		t.Fatalf("pending attempt was redispatched after restart: %+v", delivery.Task)
	case <-time.After(20 * time.Millisecond):
	}
	if err := e2.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`"done"`)}); err != nil {
		t.Fatal(err)
	}
	ri := wait(t, e2, id)
	if ri.Status != "completed" || string(ri.Output) != `"done"` {
		t.Fatalf("run = %+v", ri)
	}
}
