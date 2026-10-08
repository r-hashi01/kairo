package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/wal"
)

// keptBigExec answers every step with an output larger than the blob
// threshold of these tests, so the run's output refers to a blob.
func keptBigExec() Executor {
	return ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		out, _ := json.Marshal(map[string]string{"text": strings.Repeat("x", 300)})
		return task.Result{Output: out}
	})
}

const oneStep = `{"name":"one","root":{"kind":"step","id":"s","action":"llm"}}`

// ADR 0050: a run submitted with KeepOutput returns its output after a
// restart, from Get, Wait and Submit; one without it is trimmed as before.
func TestKeptOutputAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, BlobThreshold: 64})
		mustPlan(t, e, oneStep)
		e.RegisterExecutor([]string{"llm"}, 4, keptBigExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	e1 := start()
	for _, req := range []SubmitRequest{
		{Plan: "one", Tenant: "t", Tier: &ft, RunID: "kept", KeepOutput: true},
		{Plan: "one", Tenant: "t", Tier: &ft, RunID: "plain"},
	} {
		if _, err := submit(e1, req); err != nil {
			t.Fatal(err)
		}
		wait(t, e1, req.RunID)
	}
	want := wait(t, e1, "kept").Output
	if !strings.Contains(string(want), "xxxx") || strings.Contains(string(want), "$blob") {
		t.Fatalf("output before the restart: %s", want)
	}
	// The kept output is there once its marker is.
	waitFor(t, func() bool { en, ok := e1.marker("kept"); return ok && en.kept })
	e1.Close()

	e2 := start()
	defer e2.Close()
	ri, err := e2.Get(context.Background(), "kept")
	if err != nil || ri.Trimmed || ri.Status != "completed" || string(ri.Output) != string(want) {
		t.Fatalf("Get after the restart: %+v %v", ri, err)
	}
	if wr := wait(t, e2, "kept"); wr.Trimmed || string(wr.Output) != string(want) {
		t.Fatalf("Wait after the restart: %+v", wr)
	}
	if r, _ := e2.Submit(context.Background(), SubmitRequest{Plan: "one", Tenant: "t", Tier: &ft, RunID: "kept", KeepOutput: true}); !r.Existing {
		t.Fatal("the finished run started again")
	}
	if ri, err := e2.Get(context.Background(), "plain"); err != nil || !ri.Trimmed || ri.Output != nil {
		t.Fatalf("a run without KeepOutput: %+v %v", ri, err)
	}
}

// The kept output goes with its marker: when it expires while running, and
// when it expired while the engine was down.
func TestKeptOutputExpires(t *testing.T) {
	defer func(n int) { markerRetireEvery = n }(markerRetireEvery)
	markerRetireEvery = 1
	dir := t.TempDir()
	var skew atomic.Int64
	ft := TierFile
	start := func() *Engine {
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, RecentRuns: 1,
			Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }})
		mustPlan(t, e, oneStep)
		e.RegisterExecutor([]string{"llm"}, 4, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	run := func(e *Engine, id string, keep bool) {
		if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &ft, RunID: id, KeepOutput: keep}); err != nil {
			t.Fatal(err)
		}
		wait(t, e, id)
	}
	keptGen := func(e *Engine, id string) uint64 {
		var gen uint64
		waitFor(t, func() bool { en, ok := e.marker(id); gen = en.gen; return ok && en.kept })
		return gen
	}
	gone := func(e *Engine, id string, gen uint64) bool {
		_, err := e.blobs.Get(keptKey(id, gen))
		return errors.Is(err, blob.ErrNotFound)
	}

	e1 := start()
	run(e1, "a", true)
	genA := keptGen(e1, "a")
	if gone(e1, "a", genA) {
		t.Fatal("no kept output")
	}
	skew.Store(int64(25 * time.Hour))
	run(e1, "b", false) // a finish expires the old markers
	waitFor(t, func() bool { return gone(e1, "a", genA) })

	// Expired while down: deleted when the engine starts.
	skew.Store(0)
	run(e1, "c", true)
	genC := keptGen(e1, "c")
	e1.Close()
	skew.Store(int64(25 * time.Hour))
	e2 := start()
	defer e2.Close()
	if !gone(e2, "c", genC) {
		t.Fatal("an output whose marker expired while down was left behind")
	}
}

// The same id run again right after its marker expired: deleting the
// earlier output (on I/O workers, in any order) never deletes the new one,
// which has its own generation (ADR 0050).
func TestKeptOutputOfARunAgain(t *testing.T) {
	var skew atomic.Int64
	ft := TierFile
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, RecentRuns: 1,
		Now: func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }})
	defer e.Close()
	mustPlan(t, e, oneStep)
	e.RegisterExecutor([]string{"llm"}, 4, echoExec())
	e.Start()
	for i := 0; i < 20; i++ {
		if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &ft, RunID: "again", KeepOutput: true}); err != nil {
			t.Fatal(err)
		}
		wait(t, e, "again")
		var en doneEntry
		waitFor(t, func() bool { var ok bool; en, ok = e.marker("again"); return ok && en.kept })
		if _, err := e.blobs.Get(keptKey("again", en.gen)); err != nil {
			t.Fatalf("round %d: the marker says kept, the output is not there: %v", i, err)
		}
		skew.Add(int64(25 * time.Hour)) // expired: the id may run again
		// Another run pushes it out of the recent runs kept in memory.
		if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &ft, RunID: "filler" + strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
		wait(t, e, "filler"+strconv.Itoa(i))
	}
}

// failingDeletes is a blob store whose deletes of kept outputs fail while
// fail is set.
type failingDeletes struct {
	blob.Store
	fail   atomic.Bool
	failed atomic.Int64
}

func (f *failingDeletes) Delete(key string) error {
	if f.fail.Load() && strings.HasPrefix(key, "kept/") {
		f.failed.Add(1)
		return errors.New("disk on fire")
	}
	return f.Store.Delete(key)
}

// A kept output whose delete fails keeps its marker in the done log, so the
// next start deletes it: it is never left with nothing pointing at it.
func TestKeptOutputDeleteRetried(t *testing.T) {
	defer func(n int) { markerRetireEvery = n }(markerRetireEvery)
	markerRetireEvery = 1
	dir := t.TempDir()
	blobs := &failingDeletes{Store: blob.NewMem()}
	doneLog := &wal.MemSink{}
	var skew atomic.Int64
	ft := TierFile
	start := func() *Engine {
		// A done log that retires record by record, kept across restarts.
		e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, RecentRuns: 1, Blobs: blobs,
			DoneLogs: func(int) (wal.Sink, error) { return doneLog, nil },
			Now:      func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }})
		mustPlan(t, e, oneStep)
		e.RegisterExecutor([]string{"llm"}, 4, echoExec())
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	run := func(e *Engine, id string, keep bool) {
		if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &ft, RunID: id, KeepOutput: keep}); err != nil {
			t.Fatal(err)
		}
		wait(t, e, id)
	}
	e1 := start()
	run(e1, "k", true)
	var gen uint64
	waitFor(t, func() bool { en, ok := e1.marker("k"); gen = en.gen; return ok && en.kept })
	blobs.fail.Store(true)
	skew.Store(int64(25 * time.Hour))
	for i := 0; i < 40; i++ { // finishes expire markers, and retire the log behind them
		run(e1, "other"+strconv.Itoa(i), false)
	}
	waitFor(t, func() bool { return blobs.failed.Load() > 0 })
	if _, err := blobs.Get(keptKey("k", gen)); err != nil {
		t.Fatalf("the delete was to fail: %v", err)
	}
	e1.Close()

	blobs.fail.Store(false)
	e2 := start()
	defer e2.Close()
	if _, err := blobs.Get(keptKey("k", gen)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the output whose delete failed was not deleted at the next start: %v", err)
	}
}

// A crash after the run finished but before its marker: recovery keeps the
// output, then writes the marker.
func TestKeptOutputWrittenOnRecovery(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	plan := `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`
	failing := failingSink{&wal.MemSink{}}
	e1 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true,
		DoneLogs: func(int) (wal.Sink, error) { return failing, nil }})
	mustPlan(t, e1, plan)
	e1.Start()
	if _, err := submit(e1, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft, RunID: "x", KeepOutput: true}); err != nil {
		t.Fatal(err)
	}
	e1.Signal("x", "go", json.RawMessage(`{"by":"alice"}`))
	want := wait(t, e1, "x").Output
	waitFor(t, func() bool { return e1.Stats().FailedLogs == 1 })
	e1.Close()

	e2 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true})
	defer e2.Close()
	mustPlan(t, e2, plan)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	ri, err := e2.Get(context.Background(), "x")
	if err != nil || ri.Trimmed || string(ri.Output) != string(want) || !strings.Contains(string(want), "alice") {
		t.Fatalf("after recovery: %+v %v (want %s)", ri, err, want)
	}
}

// Markers of version 1 (before ADR 0050) read as not kept.
func TestMarkerVersion1(t *testing.T) {
	m := marker{runID: "r", at: 42, status: "completed"}
	v2 := encodeMarker(nil, m)
	v1 := append([]byte{1}, v2[1:len(v2)-1]...) // no kept byte
	got, err := decodeMarker(v1)
	if err != nil || got != m {
		t.Fatalf("v1: %+v %v", got, err)
	}
	m.kept = true
	if got, err := decodeMarker(encodeMarker(nil, m)); err != nil || got != m {
		t.Fatalf("v2: %+v %v", got, err)
	}
}

// The keep flag rides in the tier byte; old records read as not kept.
func TestStartMetaKeep(t *testing.T) {
	for _, keep := range []bool{false, true} {
		m := &startMeta{RunID: "r", Plan: "p", PlanHash: "h", Tenant: "t", Tier: TierFile, Keep: keep}
		rec, err := decodeRecord(encodeStart(nil, m))
		if err != nil || *rec.meta != *m {
			t.Fatalf("keep=%v: %+v %v", keep, rec.meta, err)
		}
	}
}
