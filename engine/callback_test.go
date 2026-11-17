package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/sched"
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

func TestFinishedCallbackIsIdempotentAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Shards: 1, DataDir: dir, NoSync: true, RecentRuns: 1}
	open := func() (*Engine, *sched.Poller) {
		e := newEngine(t, cfg)
		mustPlan(t, e, `{"name":"callback","root":{"kind":"step","id":"call","action":"llm"}}`)
		p := e.Dispatcher().NewPoller([]string{"llm"}, 1)
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e, p
	}
	e1, p1 := open()
	tk := pendingCallbackTask(t, e1, p1, "callback-restart")
	if err := e1.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`{"first":true}`)}); err != nil {
		t.Fatal(err)
	}
	if ri := wait(t, e1, tk.RunID); ri.Status != "completed" {
		t.Fatalf("run = %+v", ri)
	}
	e1.Close()
	before := walRecordCount(t, dir, "shard-000")

	e2, _ := open()
	if _, ok := e2.marker(tk.RunID); !ok {
		t.Fatal("finished marker was not recovered")
	}
	if err := e2.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`{"second":true}`)}); err != nil {
		t.Fatalf("duplicate callback after restart: %v", err)
	}
	e2.Close()
	if after := walRecordCount(t, dir, "shard-000"); after != before {
		t.Fatalf("duplicate callback changed the run log: records %d -> %d", before, after)
	}
}

func TestFinishedCallbackUsesMarkerUntilReuseWindowEnds(t *testing.T) {
	var skew atomic.Int64
	dir := t.TempDir()
	cfg := Config{Shards: 1, DataDir: dir, NoSync: true, RecentRuns: 1, IdempotencyTTL: time.Hour,
		Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }}
	e := newEngine(t, cfg)
	defer e.Close()
	mustPlan(t, e, `{"name":"callback","root":{"kind":"step","id":"call","action":"llm"}}`)
	mustPlan(t, e, `{"name":"hold","root":{"kind":"wait","id":"w","signal":"go"}}`)
	p := e.Dispatcher().NewPoller([]string{"llm"}, 2)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	missing := &task.Task{RunID: "missing", Act: 1, Attempt: 1, Action: "llm", StepID: "call"}
	if err := e.Callback(context.Background(), missing, task.Result{Output: json.RawMessage(`0`)}); !errors.Is(err, ErrUnknownRun) {
		t.Fatalf("callback for missing run = %v, want ErrUnknownRun", err)
	}
	tk := pendingCallbackTask(t, e, p, "callback-window")
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`1`)}); err != nil {
		t.Fatal(err)
	}
	wait(t, e, tk.RunID)

	// Finish another run to evict the callback run from RecentRuns. Its
	// durable marker must still make the callback retry idempotent.
	other, err := submit(e, SubmitRequest{Plan: "callback", RunID: "other", Tenant: "t", Tier: tierPtr(TierFile)})
	if err != nil {
		t.Fatal(err)
	}
	dl := <-p.C
	if dl.Task.RunID != other {
		t.Fatalf("task = %s, want %s", dl.Task.RunID, other)
	}
	e.Complete(dl.Task, task.Result{Output: json.RawMessage(`2`)})
	e.Dispatcher().Poll(p, 1)
	wait(t, e, other)
	e.waitMu.Lock()
	_, cached := e.finished[tk.RunID]
	e.waitMu.Unlock()
	if cached {
		t.Fatal("callback run was not evicted from RecentRuns")
	}
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`3`)}); err != nil {
		t.Fatalf("duplicate callback after cache eviction: %v", err)
	}

	// Once the same id may be reused, its old marker no longer authorizes a
	// callback retry. The capability is bounded by the same external expiry.
	skew.Store(int64(2 * time.Hour))
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`4`)}); !errors.Is(err, ErrUnknownRun) {
		t.Fatalf("callback after idempotency window = %v, want ErrUnknownRun", err)
	}
	r, err := e.Submit(context.Background(), SubmitRequest{Plan: "hold", RunID: tk.RunID, Tenant: "t", Tier: tierPtr(TierFile)})
	if err != nil || r.Existing {
		t.Fatalf("RunID was not reusable after the window: %+v, %v", r, err)
	}
}

func pendingCallbackTask(t *testing.T, e *Engine, p *sched.Poller, runID string) *task.Task {
	t.Helper()
	if _, err := e.Submit(context.Background(), SubmitRequest{Plan: "callback", RunID: runID, Tenant: "t", Tier: tierPtr(TierFile)}); err != nil {
		t.Fatal(err)
	}
	var tk *task.Task
	select {
	case dl := <-p.C:
		tk = dl.Task
	case <-time.After(5 * time.Second):
		t.Fatal("task was not dispatched")
	}
	tk.Callback = &task.Callback{URL: "https://callbacks.example/token", Until: time.Now().Add(time.Minute).UnixMilli()}
	e.Complete(tk, task.Result{Pending: true})
	e.Dispatcher().Poll(p, 1)
	return tk
}

func tierPtr(t Tier) *Tier { return &t }

func walRecordCount(t *testing.T, dir, prefix string) int {
	t.Helper()
	s, err := wal.OpenFile(dir+"/wal", prefix, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n := 0
	if err := s.ReadAll(func(uint64, []byte) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	return n
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
