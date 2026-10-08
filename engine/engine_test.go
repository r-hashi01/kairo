package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kairo/core"
	"kairo/ir"
	"kairo/sched"
	"kairo/task"
	"kairo/wal"
)

func testRegistry() *ir.Registry {
	r := ir.NewRegistry()
	r.Register(ir.NodeSpec{Action: "llm", Effect: ir.EffectUnprotected, Outputs: map[string]ir.FieldType{
		"label": {Type: ir.FieldEnum, Values: []string{"big", "small"}},
	}})
	r.Register(ir.NodeSpec{Action: "send", Effect: ir.EffectReal})
	return r
}

func mustPlan(t testing.TB, e *Engine, js string) *ir.Plan {
	t.Helper()
	d, err := ir.ParseDefinition([]byte(js))
	if err != nil {
		t.Fatal(err)
	}
	p, err := e.RegisterPlan(d)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newEngine(t testing.TB, cfg Config) *Engine {
	t.Helper()
	if cfg.Registry == nil {
		cfg.Registry = testRegistry()
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func echoExec() Executor {
	return ExecutorFunc(func(_ context.Context, t *task.Task, _ func([]byte)) task.Result {
		return task.Result{Output: json.RawMessage(fmt.Sprintf(`{"step":%q,"in":%s}`, t.StepID, t.Input))}
	})
}

const fiveNodes = `{"name":"five","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"n1","action":"llm","input":{"q":"$input.q"}},
  {"kind":"step","id":"n2","action":"llm","input":{"p":"n1.in.q"}},
  {"kind":"step","id":"n3","action":"llm","input":{"p":"n2.in.p"}},
  {"kind":"step","id":"n4","action":"llm","input":{"p":"n3.in.p"}},
  {"kind":"step","id":"n5","action":"llm","input":{"p":"n4.in.p"}}]}}`

func wait(t testing.TB, e *Engine, id string) RunInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ri, err := e.Wait(ctx, id)
	if err != nil {
		t.Fatalf("waiting for %s: %v", id, err)
	}
	return ri
}

func TestFiveNodeRun(t *testing.T) {
	for _, tier := range []Tier{TierNone, TierMemory, TierFile} {
		t.Run(tier.String(), func(t *testing.T) {
			e := newEngine(t, Config{Shards: 4, DataDir: t.TempDir(), NoSync: true})
			defer e.Close()
			mustPlan(t, e, fiveNodes)
			e.RegisterExecutor([]string{"llm"}, 8, echoExec())
			if err := e.Start(); err != nil {
				t.Fatal(err)
			}
			tr := tier
			id, err := submit(e, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"hi"}`), Tenant: "t1", Tier: &tr})
			if err != nil {
				t.Fatal(err)
			}
			ri := wait(t, e, id)
			if ri.Status != "completed" || !strings.Contains(string(ri.Output), `"p":"hi"`) {
				t.Fatalf("%+v", ri)
			}
		})
	}
}

// Real commands must never be released before their intent (and everything
// before it) is durable. The sink here acknowledges slowly; the executor
// checks the durable log contents at the moment it receives the command.
func TestRealCommandWaitsForDurableIntent(t *testing.T) {
	var sinks [64]*wal.MemSink
	var durableMu sync.Mutex
	durable := map[int][]byte{}
	cfg := Config{Shards: 2, RealMinTier: TierMemory, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		if tier != TierMemory {
			return nil, nil
		}
		m := &wal.MemSink{}
		m.Delay = func() { time.Sleep(15 * time.Millisecond) }
		sinks[shard] = m
		return durableSink{m, shard, &durableMu, durable}, nil
	}}
	e := newEngine(t, cfg)
	defer e.Close()
	mustPlan(t, e, `{"name":"r","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"prep","action":"llm"},
	  {"kind":"step","id":"send","action":"send","input":{"x":"prep.step"}}]}}`)
	e.RegisterExecutor([]string{"llm"}, 4, echoExec())
	var violations, checked atomic.Int32
	e.RegisterExecutor([]string{"send"}, 4, ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		sh := e.shardFor(tk.RunID).id
		durableMu.Lock()
		data := append([]byte(nil), durable[sh]...)
		durableMu.Unlock()
		found := false
		wal.Scan(data, func(rec []byte) error {
			r, _ := decodeRecord(rec)
			if r.kind == recEvent && r.runID == tk.RunID && r.ev.Kind == core.EvIntent && r.ev.Act == tk.Act && r.ev.Attempt == tk.Attempt {
				found = true
			}
			return nil
		})
		if !found {
			violations.Add(1)
		}
		checked.Add(1)
		return task.Result{Output: json.RawMessage(`"sent"`)}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 50; i++ {
		id, err := submit(e, SubmitRequest{Plan: "r", Tenant: "t"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if ri := wait(t, e, id); ri.Status != "completed" {
			t.Fatalf("%+v", ri)
		}
	}
	if checked.Load() != 50 || violations.Load() != 0 {
		t.Fatalf("checked %d, released before durable: %d", checked.Load(), violations.Load())
	}
}

// durableSink records what has been acknowledged.
type durableSink struct {
	*wal.MemSink
	shard int
	mu    *sync.Mutex
	acked map[int][]byte
}

func (d durableSink) Append(b []byte) error {
	if err := d.MemSink.Append(b); err != nil {
		return err
	}
	d.mu.Lock()
	d.acked[d.shard] = append(d.acked[d.shard], b...)
	d.mu.Unlock()
	return nil
}

func TestRecoveryAfterRestart(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"rec","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm","input":{"q":"$input.q"}},
	  {"kind":"wait","id":"approve","signal":"ok"},
	  {"kind":"step","id":"b","action":"llm","input":{"by":"approve.payload"}}]}}`

	e1 := newEngine(t, Config{Shards: 2, DataDir: dir, EvictAfter: time.Millisecond})
	mustPlan(t, e1, plan)
	e1.RegisterExecutor([]string{"llm"}, 2, echoExec())
	if err := e1.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	id, err := submit(e1, SubmitRequest{Plan: "rec", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted })
	e1.Close()

	e2 := newEngine(t, Config{Shards: 2, DataDir: dir})
	defer e2.Close()
	mustPlan(t, e2, plan)
	e2.RegisterExecutor([]string{"llm"}, 2, echoExec())
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	e2.Signal(id, "ok", json.RawMessage(`"alice"`))
	ri := wait(t, e2, id)
	if ri.Status != "completed" || !strings.Contains(string(ri.Output), `"by":"alice"`) {
		t.Fatalf("%+v", ri)
	}
}

// A crash while a real command may have been executed must not re-execute
// it: the run stops for review. An unprotected step is simply re-issued.
func TestRecoveryRealStepNeedsReview(t *testing.T) {
	dir := t.TempDir()
	plan := `{"name":"crash","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"read","action":"llm"},
	  {"kind":"step","id":"send","action":"send"}]}}`
	block := make(chan struct{})
	var sendCalls atomic.Int32
	hang := ExecutorFunc(func(ctx context.Context, t *task.Task, _ func([]byte)) task.Result {
		if t.Action == "send" {
			sendCalls.Add(1)
		}
		select {
		case <-block:
		case <-ctx.Done():
		}
		return task.Result{Unknown: true, Err: "shutdown"}
	})
	e1 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true})
	mustPlan(t, e1, plan)
	e1.RegisterExecutor([]string{"llm", "send"}, 4, hang)
	e1.Start()
	id, _ := submit(e1, SubmitRequest{Plan: "crash", Tenant: "t"})
	waitFor(t, func() bool { return sendCalls.Load() == 1 })
	// Simulate a crash: stop without letting results reach the shard.
	for _, s := range e1.shards {
		s.inbox.Push(msg{kind: mStop})
	}
	e1.wg.Wait()
	close(block)
	e1.Close()

	var reads atomic.Int32
	e2 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true})
	defer e2.Close()
	mustPlan(t, e2, plan)
	e2.RegisterExecutor([]string{"llm"}, 2, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
		reads.Add(1)
		return task.Result{Output: json.RawMessage(`"r"`)}
	}))
	e2.RegisterExecutor([]string{"send"}, 2, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
		sendCalls.Add(1)
		return task.Result{Output: json.RawMessage(`"sent"`)}
	}))
	e2.Start()
	var ri RunInfo
	waitFor(t, func() bool {
		ri, _ = e2.Get(context.Background(), id)
		return ri.Status == "blocked" && reads.Load() == 1
	})
	if sendCalls.Load() != 1 || len(ri.Reviews) != 1 {
		t.Fatalf("send executed %d times, reviews %+v", sendCalls.Load(), ri.Reviews)
	}
	// The operator confirms the message was sent.
	e2.Resolve(id, ri.Reviews[0].Act, json.RawMessage(`"confirmed"`), "")
	if ri := wait(t, e2, id); ri.Status != "completed" || ri.Output == nil {
		t.Fatalf("%+v", ri)
	}
}

func TestEvictionAndTimers(t *testing.T) {
	e := newEngine(t, Config{Shards: 2, EvictAfter: 50 * time.Millisecond})
	defer e.Close()
	mustPlan(t, e, `{"name":"sleep","root":{"kind":"seq","nodes":[
	  {"kind":"wait","id":"w","duration":"300ms"},{"kind":"step","id":"a","action":"llm"}]}}`)
	e.RegisterExecutor([]string{"llm"}, 2, echoExec())
	e.Start()
	mem := TierMemory
	var ids []string
	for i := 0; i < 100; i++ {
		id, _ := submit(e, SubmitRequest{Plan: "sleep", Tenant: "t", Tier: &mem})
		ids = append(ids, id)
	}
	waitFor(t, func() bool { return e.Stats().Evicted == 100 })
	for _, id := range ids {
		if ri := wait(t, e, id); ri.Status != "completed" {
			t.Fatalf("%+v", ri)
		}
	}
}

func TestLargeOutputsBecomeBlobs(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, BlobThreshold: 1024})
	defer e.Close()
	mustPlan(t, e, `{"name":"blob","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"gen","action":"llm"},
	  {"kind":"cond","if":{"field":"gen.label","op":"eq","value":"big"},
	   "then":{"kind":"step","id":"use","action":"llm","input":{"doc":"gen.doc","label":"gen.label"}}}]}}`)
	big := strings.Repeat("x", 100_000)
	var got atomic.Value
	e.RegisterExecutor([]string{"llm"}, 1, ExecutorFunc(func(_ context.Context, t *task.Task, _ func([]byte)) task.Result {
		if t.StepID == "gen" {
			return task.Result{Output: json.RawMessage(`{"label":"big","doc":"` + big + `"}`)}
		}
		got.Store(string(t.Input))
		return task.Result{Output: json.RawMessage(`"ok"`)}
	}))
	e.Start()
	id, _ := submit(e, SubmitRequest{Plan: "blob", Tenant: "t"})
	if ri := wait(t, e, id); ri.Status != "completed" {
		t.Fatalf("%+v", ri)
	}
	in, _ := got.Load().(string)
	if in != `{"doc":"`+big+`","label":"big"}` {
		t.Fatalf("resolved input wrong (len %d)", len(in))
	}
}

func TestLiveStream(t *testing.T) {
	e := newEngine(t, Config{Shards: 1})
	defer e.Close()
	mustPlan(t, e, `{"name":"s","root":{"kind":"seq","nodes":[{"kind":"wait","id":"go","signal":"go"},{"kind":"step","id":"a","action":"llm"}]}}`)
	e.RegisterExecutor([]string{"llm"}, 1, ExecutorFunc(func(_ context.Context, t *task.Task, emit func([]byte)) task.Result {
		for i := 0; i < 5; i++ {
			emit([]byte(fmt.Sprint(i)))
		}
		return task.Result{Output: json.RawMessage(`"done"`)}
	}))
	e.Start()
	id, _ := submit(e, SubmitRequest{Plan: "s", Tenant: "t"})
	sub := e.Live().Subscribe(id, 64)
	e.Signal(id, "go", nil)
	var got []string
	for {
		b, ok := sub.Next(nil)
		for _, c := range b {
			if !c.Control {
				got = append(got, string(c.Data))
			}
		}
		if !ok {
			break
		}
	}
	if strings.Join(got, "") != "01234" {
		t.Fatalf("chunks %v", got)
	}
}

func waitFor(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- invariants -----------------------------------------------------------

// No goroutine per run: 20k waiting runs must not grow the goroutine count.
func TestNoGoroutinePerRun(t *testing.T) {
	e := newEngine(t, Config{Shards: 4, EvictAfter: -1})
	defer e.Close()
	mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"never"}}`)
	e.Start()
	before := runtime.NumGoroutine()
	for i := 0; i < 20000; i++ {
		if _, err := submit(e, SubmitRequest{Plan: "w", Tenant: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { return e.Stats().InMemory == 20000 })
	if after := runtime.NumGoroutine(); after > before+10 {
		t.Fatalf("goroutines grew from %d to %d for 20000 runs", before, after)
	}
}

// No polling: an engine holding waiting runs and timers far in the future
// does not wake up.
func TestIdleEngineDoesNotWake(t *testing.T) {
	e := newEngine(t, Config{Shards: 4, EvictAfter: -1})
	defer e.Close()
	mustPlan(t, e, `{"name":"w","root":{"kind":"par","nodes":[
	  {"kind":"wait","id":"a","signal":"never"},{"kind":"wait","id":"b","duration":"1h"}]}}`)
	e.Start()
	for i := 0; i < 1000; i++ {
		submit(e, SubmitRequest{Plan: "w", Tenant: "t"})
	}
	waitFor(t, func() bool { return e.Stats().InMemory == 1000 })
	time.Sleep(20 * time.Millisecond)
	count := func() (n uint64) {
		for _, s := range e.shards {
			n += s.wakeups.Load()
		}
		return
	}
	w0 := count()
	time.Sleep(300 * time.Millisecond)
	if w1 := count(); w1 != w0 {
		t.Fatalf("idle shards woke %d times", w1-w0)
	}
}

// Memory of a waiting run. Eviction is disabled here: this is the in-memory
// cost; evicted runs cost only their snapshot bytes.
func TestWaitingRunMemory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	for _, evict := range []time.Duration{-1, time.Millisecond} {
		t.Run(fmt.Sprint("evict=", evict > 0), func(t *testing.T) {
			e := newEngine(t, Config{Shards: 4, EvictAfter: evict})
			defer e.Close()
			mustPlan(t, e, `{"name":"w","root":{"kind":"seq","nodes":[
			  {"kind":"step","id":"p","action":"kairo.pass","input":{"q":"$input.q"}},
			  {"kind":"wait","id":"approve","signal":"approve","timeout":"72h"},
			  {"kind":"step","id":"a","action":"llm"}]}}`)
			e.Start()
			const n = 100_000
			runtime.GC()
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			for i := 0; i < n; i++ {
				submit(e, SubmitRequest{Plan: "w", Tenant: "t", Input: json.RawMessage(`{"q":"hello"}`)})
			}
			waitFor(t, func() bool {
				st := e.Stats()
				if evict > 0 {
					return st.Evicted == n
				}
				return st.InMemory == n
			})
			runtime.GC()
			runtime.ReadMemStats(&m1)
			per := float64(m1.HeapAlloc-m0.HeapAlloc) / n
			t.Logf("heap per waiting run: %.0f bytes", per)
			if per > 20*1024 {
				t.Fatalf("waiting run costs %.0f bytes (> 20 KB)", per)
			}
		})
	}
}

func BenchmarkFiveNodeRun(b *testing.B) {
	for _, tier := range []Tier{TierNone, TierMemory} {
		b.Run(tier.String(), func(b *testing.B) {
			e := newEngine(b, Config{})
			defer e.Close()
			mustPlan(b, e, fiveNodes)
			e.RegisterExecutor([]string{"llm"}, 256, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
				return task.Result{Output: json.RawMessage(`{"in":{"q":"x","p":"x"}}`)}
			}))
			e.Start()
			tr := tier
			b.ReportAllocs()
			b.ResetTimer()
			var ru0, ru1 runtime.MemStats
			_ = ru0
			_ = ru1
			start := time.Now()
			cpu0 := cpuTime()
			const inflight = 2048
			sem := make(chan struct{}, inflight)
			var wg sync.WaitGroup
			for i := 0; i < b.N; i++ {
				sem <- struct{}{}
				id, err := submit(e, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: fmt.Sprint(i % 8), Tier: &tr})
				if err != nil {
					b.Fatal(err)
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					e.Wait(context.Background(), id)
					<-sem
				}()
			}
			wg.Wait()
			el := time.Since(start)
			b.ReportMetric(float64(b.N)/el.Seconds(), "runs/s")
			b.ReportMetric(float64((cpuTime()-cpu0).Microseconds())/float64(b.N), "cpu-µs/run")
		})
	}
}

// Hand-off latency between nodes: from an executor returning the result of
// step k to the executor receiving step k+1 of the same run. A measurement,
// not a check (ADR 0056): it depends on the machine, so it runs only for
// scripts/bench.sh (KAIRO_MEASURE=1), which reports it against its budget.
func TestHandoffLatency(t *testing.T) {
	if os.Getenv("KAIRO_MEASURE") == "" {
		t.Skip("a measurement: scripts/bench.sh runs it (KAIRO_MEASURE=1)")
	}
	e := newEngine(t, Config{Shards: 8})
	defer e.Close()
	mustPlan(t, e, fiveNodes)
	var mu sync.Mutex
	last := map[string]time.Time{}
	var lats []time.Duration
	e.RegisterExecutor([]string{"llm"}, 512, ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		now := time.Now()
		mu.Lock()
		if t0, ok := last[tk.RunID]; ok {
			lats = append(lats, now.Sub(t0))
		}
		mu.Unlock()
		time.Sleep(2 * time.Millisecond) // pretend to wait on a provider
		mu.Lock()
		last[tk.RunID] = time.Now()
		mu.Unlock()
		return task.Result{Output: json.RawMessage(`{"in":{"q":"x","p":"x"}}`)}
	}))
	e.Start()
	mem := TierMemory
	var ids []string
	for i := 0; i < 2000; i++ {
		id, _ := submit(e, SubmitRequest{Plan: "five", Tenant: fmt.Sprint(i % 4), Input: json.RawMessage(`{"q":"x"}`), Tier: &mem})
		ids = append(ids, id)
		if i%100 == 99 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	for _, id := range ids {
		wait(t, e, id)
	}
	slices.Sort(lats)
	p50, p99 := lats[len(lats)/2], lats[len(lats)*99/100]
	t.Logf("node hand-off latency over %d hand-offs: p50=%v p99=%v max=%v", len(lats), p50, p99, lats[len(lats)-1])
	// The distribution: a slow machine shifts it all; a fixed delay on the
	// path would show as a heap of its own.
	bounds := []time.Duration{250 * time.Microsecond, 500 * time.Microsecond, 750 * time.Microsecond, time.Millisecond,
		1250 * time.Microsecond, 1500 * time.Microsecond, 2 * time.Millisecond}
	counts := make([]int, len(bounds)+1)
	for _, l := range lats {
		i, _ := slices.BinarySearch(bounds, l)
		counts[i]++
	}
	t.Logf("hand-off latency histogram (GOMAXPROCS=%d): <=250µs %d, <=500µs %d, <=750µs %d, <=1ms %d, <=1.25ms %d, <=1.5ms %d, <=2ms %d, >2ms %d",
		runtime.GOMAXPROCS(0), counts[0], counts[1], counts[2], counts[3], counts[4], counts[5], counts[6], counts[7])
}

// A burst of evictions to the file tier (thousands of snapshot writes at
// once) must not start goroutines: I/O runs on a fixed pool.
func TestEvictionBurstUsesFixedIOPool(t *testing.T) {
	e := newEngine(t, Config{Shards: 4, DataDir: t.TempDir(), NoSync: true, EvictAfter: time.Millisecond})
	defer e.Close()
	mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"never"}}`)
	e.Start()
	before := runtime.NumGoroutine()
	file := TierFile
	peak := before
	for i := 0; i < 10000; i++ {
		submit(e, SubmitRequest{Plan: "w", Tenant: "t", Tier: &file})
		if i%500 == 0 {
			peak = max(peak, runtime.NumGoroutine())
		}
	}
	waitFor(t, func() bool {
		peak = max(peak, runtime.NumGoroutine())
		return e.Stats().Evicted == 10000
	})
	if peak > before+10 {
		t.Fatalf("goroutines peaked at %d (from %d) during an eviction burst", peak, before)
	}
}

func walSegments(t testing.TB, dir string) []string {
	t.Helper()
	segs, _ := filepath.Glob(filepath.Join(dir, "wal", "shard-*.wal"))
	return segs
}

// ADR 0016: finished runs' records are retired a whole segment at a time,
// runs that live on are checkpointed so they stop pinning old segments, and
// everything still recovers after a restart from the compacted log.
func TestLogCompaction(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Shards: 1, DataDir: dir, NoSync: true, SegmentSize: 4 << 10, CompactEvery: 64, EvictAfter: 50 * time.Millisecond}
	plans := []string{fiveNodes,
		`{"name":"approve","root":{"kind":"seq","nodes":[{"kind":"wait","id":"w","signal":"go"},{"kind":"step","id":"a","action":"llm","input":{"by":"w.payload"}}]}}`,
		`{"name":"slow","root":{"kind":"step","id":"s","action":"blocker"}}`,
	}
	ft := TierFile
	release := make(chan struct{})
	blocking := ExecutorFunc(func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return task.Result{Output: json.RawMessage(`"slow done"`)}
	})
	reg := func() *ir.Registry {
		r := testRegistry()
		r.Register(ir.NodeSpec{Action: "blocker", Effect: ir.EffectUnprotected})
		return r
	}

	e1 := newEngine(t, func() Config { c := cfg; c.Registry = reg(); return c }())
	for _, p := range plans {
		mustPlan(t, e1, p)
	}
	e1.RegisterExecutor([]string{"llm"}, 16, echoExec())
	e1.RegisterExecutor([]string{"blocker"}, 1, blocking)
	e1.Start()
	// Started first, so their start records sit in the oldest segment.
	waiting, _ := submit(e1, SubmitRequest{Plan: "approve", Tenant: "t", Tier: &ft}) // evicted to a snapshot
	running, _ := submit(e1, SubmitRequest{Plan: "slow", Tenant: "t", Tier: &ft})    // stays in memory, in flight
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), waiting); return ri.Evicted })
	var ids []string
	for i := 0; i < 400; i++ {
		id, err := submit(e1, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if ri := wait(t, e1, id); ri.Status != "completed" {
			t.Fatalf("%+v", ri)
		}
	}
	// Compaction is checked only as the log grows (no timer), so keep a
	// trickle of work going while checkpoints and snapshot deletes finish.
	var segs []string
	waitFor(t, func() bool {
		id, _ := submit(e1, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"y"}`), Tenant: "t", Tier: &ft})
		wait(t, e1, id)
		segs = walSegments(t, dir)
		return !strings.HasSuffix(filepath.Base(segs[0]), ".L00000000000000000001.wal")
	})
	t.Logf("%d segments left after ~%d records", len(segs), 450*13)
	if len(segs) > 30 {
		t.Fatalf("log not compacted: %d segments", len(segs))
	}
	// Crash without draining the blocked task.
	for _, s := range e1.shards {
		s.inbox.Push(msg{kind: mStop})
	}
	e1.wg.Wait()
	e1.Close()
	close(release)

	e2 := newEngine(t, func() Config { c := cfg; c.Registry = reg(); return c }())
	defer e2.Close()
	for _, p := range plans {
		mustPlan(t, e2, p)
	}
	e2.RegisterExecutor([]string{"llm"}, 4, echoExec())
	e2.RegisterExecutor([]string{"blocker"}, 1, blocking)
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	if st := e2.Stats(); st.Active != 2 {
		t.Fatalf("recovered %d active runs, want 2 (finished runs must stay finished)", st.Active)
	}
	if ri := wait(t, e2, running); ri.Status != "completed" || string(ri.Output) != `"slow done"` {
		t.Fatalf("in-flight run after restart: %+v", ri)
	}
	e2.Signal(waiting, "go", json.RawMessage(`"bob"`))
	if ri := wait(t, e2, waiting); ri.Status != "completed" || !strings.Contains(string(ri.Output), `"by":"bob"`) {
		t.Fatalf("waiting run after restart: %+v", ri)
	}
}

// Compaction must not feed itself: on an idle engine with many waiting
// runs (more than 2*CompactEvery), the checkpoint records it writes do not
// trigger further checks, so the shards go quiet.
func TestCompactionDoesNotWakeIdleEngine(t *testing.T) {
	for _, evict := range []time.Duration{20 * time.Millisecond, -1} {
		t.Run(fmt.Sprint("evict=", evict > 0), func(t *testing.T) {
			e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, CompactEvery: 8, EvictAfter: evict})
			defer e.Close()
			mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"never"}}`)
			e.Start()
			ft := TierFile
			for i := 0; i < 40; i++ {
				submit(e, SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft})
			}
			waitFor(t, func() bool { st := e.Stats(); return st.InMemory+st.Evicted == 40 })
			wakes := func() uint64 { return e.shards[0].wakeups.Load() }
			// Let in-flight snapshot writes and checkpoints settle.
			var w0 uint64
			waitFor(t, func() bool {
				a := wakes()
				time.Sleep(100 * time.Millisecond)
				w0 = wakes()
				return a == w0
			})
			time.Sleep(300 * time.Millisecond)
			if w1 := wakes(); w1 != w0 {
				t.Fatalf("idle shard woke %d times in 300ms", w1-w0)
			}
		})
	}
}

// Reusing the id of a finished run: the new run must survive a restart
// (recovery must not mix in the old run's records), and the old run's
// snapshot delete must not race the new run.
func TestRunIDReuseSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	plans := []string{fiveNodes, `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`}
	ft := TierFile
	// RecentRuns: 1, so finishing another run moves "x" out of the
	// in-memory window (ADR 0023), and the clock then passes
	// IdempotencyTTL, so its marker expires too (ADR 0027).
	var skew atomic.Int64
	now := func() time.Time { return time.Now().Add(time.Duration(skew.Load())) }
	e1 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond, RecentRuns: 1, Now: now})
	for _, p := range plans {
		mustPlan(t, e1, p)
	}
	e1.RegisterExecutor([]string{"llm"}, 4, echoExec())
	e1.Start()
	if _, err := submit(e1, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft, RunID: "x"}); err != nil {
		t.Fatal(err)
	}
	wait(t, e1, "x")
	other, _ := submit(e1, SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"y"}`), Tenant: "t", Tier: &ft})
	wait(t, e1, other)
	if r, _ := e1.Submit(context.Background(), SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft, RunID: "x"}); !r.Existing {
		t.Fatal("reused within IdempotencyTTL")
	}
	skew.Store(int64(25 * time.Hour))
	if r, err := e1.Submit(context.Background(), SubmitRequest{Plan: "w", Tenant: "t", Tier: &ft, RunID: "x"}); err != nil || r.Existing {
		t.Fatalf("reuse after the window: %+v %v", r, err)
	}
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), "x"); return ri.Plan == "w" && ri.Evicted })
	e1.Close()

	e2 := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, Now: now})
	defer e2.Close()
	for _, p := range plans {
		mustPlan(t, e2, p)
	}
	e2.Start()
	ri, err := e2.Get(context.Background(), "x")
	if err != nil || ri.Plan != "w" || ri.Status != "running" {
		t.Fatalf("reused run after restart: %+v %v", ri, err)
	}
	e2.Signal("x", "go", nil)
	if ri := wait(t, e2, "x"); ri.Status != "completed" {
		t.Fatalf("%+v", ri)
	}
}

// submit is Submit for tests that only need the run id.
func submit(e *Engine, req SubmitRequest) (string, error) {
	r, err := e.Submit(context.Background(), req)
	return r.RunID, err
}

// --- ADR 0023: durable, idempotent Submit ---------------------------------

// Submit returns only once the start record is durable: here the sink
// acknowledges after 100ms, and the start is in the acknowledged bytes by
// the time Submit returns.
func TestSubmitWaitsForDurableStart(t *testing.T) {
	var mu sync.Mutex
	acked := map[int][]byte{}
	cfg := Config{Shards: 1, DefaultTier: TierMemory, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		if tier != TierMemory {
			return nil, nil
		}
		m := &wal.MemSink{Delay: func() { time.Sleep(100 * time.Millisecond) }}
		return durableSink{m, shard, &mu, acked}, nil
	}}
	e := newEngine(t, cfg)
	defer e.Close()
	mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"never"}}`)
	e.Start()
	t0 := time.Now()
	r, err := e.Submit(context.Background(), SubmitRequest{Plan: "w", Tenant: "t", RunID: "durable-1"})
	if err != nil || r.Existing {
		t.Fatalf("%+v %v", r, err)
	}
	if el := time.Since(t0); el < 90*time.Millisecond {
		t.Fatalf("Submit returned after %v, before the start could be durable", el)
	}
	mu.Lock()
	data := append([]byte(nil), acked[0]...)
	mu.Unlock()
	found := false
	wal.Scan(data, func(rec []byte) error {
		if r, _ := decodeRecord(rec); r.kind == recStart && r.runID == "durable-1" {
			found = true
		}
		return nil
	})
	if !found {
		t.Fatal("Submit returned but the start record was not acknowledged")
	}
	// A context shorter than the acknowledgement: started, not confirmed.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := e.Submit(ctx, SubmitRequest{Plan: "w", Tenant: "t", RunID: "durable-2"}); !errors.Is(err, ErrUnconfirmed) {
		t.Fatalf("got %v, want ErrUnconfirmed", err)
	}
	// Retrying with the same id answers for the same run once durable.
	if r, err := e.Submit(context.Background(), SubmitRequest{Plan: "w", Tenant: "t", RunID: "durable-2"}); err != nil || !r.Existing {
		t.Fatalf("retry: %+v %v", r, err)
	}
	if st := e.Stats(); st.Active != 2 {
		t.Fatalf("%d active runs, want 2 (no duplicate)", st.Active)
	}
}

func TestSubmitIsIdempotent(t *testing.T) {
	e := newEngine(t, Config{Shards: 2})
	defer e.Close()
	mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`)
	e.Start()
	ctx := context.Background()
	r1, err := e.Submit(ctx, SubmitRequest{Plan: "w", Tenant: "t", RunID: "same"})
	if err != nil || r1.Existing {
		t.Fatalf("%+v %v", r1, err)
	}
	r2, err := e.Submit(ctx, SubmitRequest{Plan: "w", Tenant: "t", RunID: "same"})
	if err != nil || !r2.Existing {
		t.Fatalf("duplicate while running: %+v %v", r2, err)
	}
	e.Signal("same", "go", nil)
	wait(t, e, "same")
	r3, err := e.Submit(ctx, SubmitRequest{Plan: "w", Tenant: "t", RunID: "same"})
	if err != nil || !r3.Existing {
		t.Fatalf("duplicate after finishing: %+v %v", r3, err)
	}
	if st := e.Stats(); st.Active != 0 {
		t.Fatalf("a duplicate started: %d active", st.Active)
	}
}

// A run still waiting for admission when ctx ends is withdrawn: it never
// starts.
func TestSubmitNotAcceptedWhileQueued(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, Admission: sched.AdmissionConfig{MaxActive: 1}})
	defer e.Close()
	mustPlan(t, e, `{"name":"w","root":{"kind":"wait","id":"w","signal":"go"}}`)
	e.Start()
	if _, err := submit(e, SubmitRequest{Plan: "w", Tenant: "t", RunID: "first"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := e.Submit(ctx, SubmitRequest{Plan: "w", Tenant: "t", RunID: "queued"}); !errors.Is(err, ErrNotAccepted) {
		t.Fatalf("got %v, want ErrNotAccepted", err)
	}
	e.Signal("first", "go", nil)
	wait(t, e, "first")
	time.Sleep(50 * time.Millisecond)
	if _, err := e.Get(context.Background(), "queued"); !errors.Is(err, ErrUnknownRun) {
		t.Fatalf("withdrawn run exists: %v", err)
	}
	if active, queued := e.adm.Stats(); active != 0 || queued != 0 {
		t.Fatalf("admission leaked: active %d queued %d", active, queued)
	}
}
