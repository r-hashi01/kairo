package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kairo/core"
	"kairo/ir"
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
			id, err := e.Submit(SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"hi"}`), Tenant: "t1", Tier: &tr})
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
			kind, _, id, ev, _ := decodeRecord(rec)
			if kind == recEvent && id == tk.RunID && ev.Kind == core.EvIntent && ev.Act == tk.Act && ev.Attempt == tk.Attempt {
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
		id, err := e.Submit(SubmitRequest{Plan: "r", Tenant: "t"})
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
	id, err := e1.Submit(SubmitRequest{Plan: "rec", Input: json.RawMessage(`{"q":"x"}`), Tenant: "t", Tier: &ft})
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
	id, _ := e1.Submit(SubmitRequest{Plan: "crash", Tenant: "t"})
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
		id, _ := e.Submit(SubmitRequest{Plan: "sleep", Tenant: "t", Tier: &mem})
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
	id, _ := e.Submit(SubmitRequest{Plan: "blob", Tenant: "t"})
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
	id, _ := e.Submit(SubmitRequest{Plan: "s", Tenant: "t"})
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
		if _, err := e.Submit(SubmitRequest{Plan: "w", Tenant: "t"}); err != nil {
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
		e.Submit(SubmitRequest{Plan: "w", Tenant: "t"})
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
				e.Submit(SubmitRequest{Plan: "w", Tenant: "t", Input: json.RawMessage(`{"q":"hello"}`)})
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
				id, err := e.Submit(SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: fmt.Sprint(i % 8), Tier: &tr})
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
// step k to the executor receiving step k+1 of the same run.
func TestHandoffLatency(t *testing.T) {
	if testing.Short() {
		t.Skip()
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
		id, _ := e.Submit(SubmitRequest{Plan: "five", Tenant: fmt.Sprint(i % 4), Input: json.RawMessage(`{"q":"x"}`), Tier: &mem})
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
	if p99 > time.Millisecond && !raceEnabled {
		t.Errorf("p99 hand-off latency %v > 1ms", p99)
	}
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
		e.Submit(SubmitRequest{Plan: "w", Tenant: "t", Tier: &file})
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
