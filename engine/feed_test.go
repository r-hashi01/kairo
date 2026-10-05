package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"kairo/core"
	"kairo/task"
)

// --- ADR 0034: the execution event feed -------------------------------------

func kinds(es []FeedEntry) string {
	var b []string
	for _, e := range es {
		switch {
		case e.Trace == nil:
			b = append(b, "chunk:"+string(e.Chunk))
		case e.Trace.Kind == core.TrNodeStart:
			b = append(b, "+"+e.Trace.StepID)
		case e.Trace.Kind == core.TrNodeEnd:
			b = append(b, "-"+e.Trace.StepID)
		case e.Trace.Kind == core.TrRunStart:
			b = append(b, "run+")
		case e.Trace.Kind == core.TrRunEnd:
			b = append(b, "run-")
		default:
			b = append(b, fmt.Sprint(e.Trace.Kind))
		}
	}
	return strings.Join(b, " ")
}

// readUntil reads entries until one satisfies stop.
func readUntil(t *testing.T, f *Feed, stop func(FeedEntry) bool) []FeedEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var all []FeedEntry
	for {
		es, err := f.Next(ctx, 0)
		if err != nil {
			t.Fatalf("after %s: %v", kinds(all), err)
		}
		for _, e := range es {
			all = append(all, e)
			if stop(e) {
				return all
			}
		}
	}
}

func isRunEnd(e FeedEntry) bool { return e.Trace != nil && e.Trace.Kind == core.TrRunEnd }

const chunked = `{"name":"c","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"a","action":"llm"},{"kind":"step","id":"b","action":"llm"}]}}`

func streamingExec() Executor {
	return ExecutorFunc(func(_ context.Context, tk *task.Task, emit func([]byte)) task.Result {
		emit([]byte(tk.StepID + "1"))
		emit([]byte(tk.StepID + "2"))
		return task.Result{Output: json.RawMessage(`"` + tk.StepID + `"`), Meta: json.RawMessage(`{"step":"` + tk.StepID + `"}`)}
	})
}

func TestFeedDeliversTracesAndChunksInOrder(t *testing.T) {
	e := newEngine(t, Config{Shards: 2, DataDir: t.TempDir(), NoSync: true, Feeds: []string{"dify"}})
	defer e.Close()
	mustPlan(t, e, chunked)
	e.RegisterExecutor([]string{"llm"}, 2, streamingExec())
	e.Start()
	if _, err := e.Subscribe("nope"); !errors.Is(err, ErrUnknownFeed) {
		t.Fatal(err)
	}
	f, err := e.Subscribe("dify")
	if err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	id, _ := submit(e, SubmitRequest{Plan: "c", Tenant: "t", Tier: &ft})
	got := readUntil(t, f, isRunEnd)
	if s := kinds(got); s != "run+ +a chunk:a1 chunk:a2 -a +b chunk:b1 chunk:b2 -b run-" {
		t.Fatalf("feed: %s", s)
	}
	for _, en := range got {
		if en.RunID != id {
			t.Fatalf("entry of run %s", en.RunID)
		}
		if en.Trace != nil && en.Cursor.LSN == 0 {
			t.Fatalf("durable trace without a position: %+v", en)
		}
		if en.Trace != nil && en.Trace.Kind == core.TrNodeEnd && string(en.Trace.Meta) != `{"step":"`+en.Trace.StepID+`"}` {
			t.Fatalf("executor metadata not passed through: %s", en.Trace.Meta)
		}
	}
	if err := f.Ack(got[len(got)-1].Cursor); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return e.Stats().FeedBacklog == 0 })
}

// Until its traces are acknowledged, a waiting run is not snapshotted; a
// restart produces the unacknowledged traces again, and nothing before
// the acknowledged position.
func TestFeedHoldsStateAndReplaysAfterRestart(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"w","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},{"kind":"wait","id":"w","signal":"go"},{"kind":"step","id":"b","action":"llm"}]}}`
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond, Feeds: []string{"dify"}})
		mustPlan(t, e, plan)
		e.RegisterExecutor([]string{"llm"}, 2, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	ft := TierFile
	e1 := start()
	f1, _ := e1.Subscribe("dify")
	id, _ := submit(e1, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft})
	got := readUntil(t, f1, func(en FeedEntry) bool {
		return en.Trace != nil && en.Trace.Kind == core.TrNodeStart && en.Trace.StepID == "w"
	})
	// Waiting, but not evicted: its traces are unacknowledged.
	time.Sleep(20 * time.Millisecond)
	if ri, _ := e1.Get(context.Background(), id); ri.Evicted {
		t.Fatal("evicted before the feed acknowledged its traces")
	}
	// Acknowledge up to the end of step a; then the run may be evicted.
	var ackAt Cursor
	for _, en := range got {
		if en.Trace != nil && en.Trace.Kind == core.TrNodeEnd && en.Trace.StepID == "a" {
			ackAt = en.Cursor
		}
	}
	f1.Ack(ackAt)
	f1.Ack(got[len(got)-1].Cursor)
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted })
	// Signal, read the rest, acknowledge nothing more, and stop.
	e1.Signal(id, "go", nil)
	rest := readUntil(t, f1, isRunEnd)
	if s := kinds(rest); s != "-w +b -b run-" {
		t.Fatalf("after the signal: %s", s)
	}
	e1.Close()

	e2 := start()
	defer e2.Close()
	f2, _ := e2.Subscribe("dify")
	again := readUntil(t, f2, isRunEnd)
	if s := kinds(again); s != "-w +b -b run-" {
		t.Fatalf("after restart: %s, want the unacknowledged traces again", s)
	}
	for i := range again {
		if !sameTrace(again[i].Trace, rest[i].Trace) {
			t.Fatalf("trace %d differs:\n%+v\n%+v", i, *again[i].Trace, *rest[i].Trace)
		}
	}
}

func sameTrace(a, b *core.Trace) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// A subscription that falls too far behind is cut and stops holding the
// engine back; subscribing again starts from the present.
func TestFeedLag(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, EvictAfter: time.Millisecond,
		Feeds: []string{"dify"}, FeedLimit: 20})
	defer e.Close()
	mustPlan(t, e, fiveNodes)
	mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`)
	e.RegisterExecutor([]string{"llm"}, 4, echoExec())
	e.Start()
	f, _ := e.Subscribe("dify")
	ft := TierFile
	waiting, _ := submit(e, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft})
	for i := 0; i < 10; i++ {
		id, _ := submit(e, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		wait(t, e, id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitFor(t, func() bool {
		_, err := f.Next(ctx, 1)
		return errors.Is(err, ErrLagged)
	})
	// Cut: the waiting run can be evicted now.
	waitFor(t, func() bool { ri, _ := e.Get(context.Background(), waiting); return ri.Evicted })
	f2, err := e.Subscribe("dify")
	if err != nil {
		t.Fatal(err)
	}
	e.Signal(waiting, "go", nil)
	got := readUntil(t, f2, isRunEnd)
	if s := kinds(got); !strings.HasSuffix(s, "run-") || strings.Contains(s, "+n1") {
		t.Fatalf("after resubscribing: %s", s)
	}
}

// Records the feed has not acknowledged are not retired; once they are,
// compaction goes on.
func TestFeedHoldsLogRetirement(t *testing.T) {
	dir := t.TempDir()
	e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, SegmentSize: 4 << 10, CompactEvery: 64, Feeds: []string{"dify"}})
	defer e.Close()
	mustPlan(t, e, fiveNodes)
	e.RegisterExecutor([]string{"llm"}, 8, echoExec())
	e.Start()
	f, _ := e.Subscribe("dify")
	ft := TierFile
	first := func() string { return walSegments(t, dir)[0] }
	seg0 := first()
	var last Cursor
	for i := 0; i < 150; i++ {
		id, _ := submit(e, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		wait(t, e, id)
	}
	if first() != seg0 {
		t.Fatal("retired records the feed had not acknowledged")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for e.Stats().FeedBacklog > 0 {
		es, err := f.Next(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		last = es[len(es)-1].Cursor
		f.Ack(last)
	}
	waitFor(t, func() bool {
		id, _ := submit(e, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"y"}`), Tenant: "t", Tier: &ft})
		wait(t, e, id)
		if es, err := f.Next(ctx, 0); err == nil {
			f.Ack(es[len(es)-1].Cursor)
		}
		return first() != seg0
	})
}

// --- review findings ----------------------------------------------------------

// A run that finished while its traces were unacknowledged keeps them
// through any number of restarts.
func TestFeedFinishedRunAcrossTwoRestarts(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"w","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},{"kind":"wait","id":"w","signal":"go"},{"kind":"step","id":"b","action":"llm"}]}}`
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond,
			SegmentSize: 4 << 10, CompactEvery: 64, Feeds: []string{"dify"}})
		mustPlan(t, e, plan)
		mustPlan(t, e, fiveNodes)
		e.RegisterExecutor([]string{"llm"}, 4, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	ft := TierFile
	e1 := start()
	f1, _ := e1.Subscribe("dify")
	id, _ := submit(e1, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft})
	got := readUntil(t, f1, func(en FeedEntry) bool { return en.Trace != nil && en.Trace.StepID == "w" })
	f1.Ack(got[len(got)-1].Cursor)
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted })
	// Traffic, acknowledged, so compaction retires the run's start record.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 60; i++ {
		x, _ := submit(e1, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		wait(t, e1, x)
		if es, err := f1.Next(ctx, 0); err == nil {
			f1.Ack(es[len(es)-1].Cursor)
		}
	}
	e1.Signal(id, "go", nil)
	readUntil(t, f1, func(en FeedEntry) bool { return isRunEnd(en) && en.RunID == id })
	e1.Close() // not acknowledged

	for restart := 1; restart <= 2; restart++ {
		e := start()
		f, _ := e.Subscribe("dify")
		var mine []FeedEntry
		for _, en := range readUntil(t, f, func(en FeedEntry) bool { return isRunEnd(en) && en.RunID == id }) {
			if en.RunID == id {
				mine = append(mine, en)
			}
		}
		if s := kinds(mine); s != "-w +b -b run-" {
			t.Fatalf("restart %d: %s", restart, s)
		}
		e.Close()
	}
}

// A disconnected subscription is not cut by live chunks it cannot get,
// and does not get them on reconnecting.
func TestFeedChunksWhileDisconnected(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, Feeds: []string{"dify"}, FeedLimit: 50})
	defer e.Close()
	mustPlan(t, e, chunked)
	e.RegisterExecutor([]string{"llm"}, 2, ExecutorFunc(func(_ context.Context, tk *task.Task, emit func([]byte)) task.Result {
		for i := 0; i < 100; i++ {
			emit([]byte("x"))
		}
		return task.Result{Output: json.RawMessage(`1`)}
	}))
	e.Start()
	f, _ := e.Subscribe("dify")
	f.Close()
	ft := TierFile
	id, _ := submit(e, SubmitRequest{Plan: "c", Tenant: "t", Tier: &ft})
	wait(t, e, id)
	f2, _ := e.Subscribe("dify")
	got := readUntil(t, f2, isRunEnd)
	if strings.Contains(kinds(got), "chunk") {
		t.Fatalf("chunks from before the connection: %s", kinds(got))
	}
}

// A run whose last logged events produced no traces (here the recovery of
// a waiting run) can be evicted once its traces are acknowledged.
func TestFeedTracelessEventsDoNotHoldRuns(t *testing.T) {
	dir := t.TempDir()
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond, Feeds: []string{"dify"}})
		mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`)
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	ft := TierFile
	e1 := start()
	f1, _ := e1.Subscribe("dify")
	id, _ := submit(e1, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft})
	got := readUntil(t, f1, func(en FeedEntry) bool { return en.Trace != nil && en.Trace.StepID == "w" })
	f1.Ack(got[len(got)-1].Cursor)
	e1.Close()
	e2 := start()
	defer e2.Close()
	waitFor(t, func() bool { ri, _ := e2.Get(context.Background(), id); return ri.Evicted })
}
