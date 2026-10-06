package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"kairo/compat/dify"
	"kairo/engine"
	"kairo/ir"
	"kairo/sched"
	"kairo/task"
)

// A workflow with a worker step that streams, and a human input whose
// actions branch.
const workflow = `{"graph":{"nodes":[
  {"id":"start","data":{"type":"start","title":"Start","variables":[{"variable":"name","type":"text-input"}]}},
  {"id":"tpl","data":{"type":"template-transform","title":"Greet","template":"Hello {{ name }}",
    "variables":[{"variable":"name","value_selector":["start","name"]}]}},
  {"id":"review","data":{"type":"human-input","title":"Review","user_actions":[{"id":"approve"},{"id":"reject"}],"timeout":1,"timeout_unit":"day"}},
  {"id":"ok","data":{"type":"end","title":"OK","outputs":[{"variable":"greeting","value_selector":["tpl","output"]},{"variable":"comment","value_selector":["review","comment"]}]}},
  {"id":"no","data":{"type":"end","title":"No","outputs":[]}}],
  "edges":[{"source":"start","target":"tpl"},{"source":"tpl","target":"review"},
    {"source":"review","target":"ok","sourceHandle":"approve"},{"source":"review","target":"no","sourceHandle":"reject"}]}}`

type fixture struct {
	t   *testing.T
	e   *engine.Engine
	d   *Daemon
	srv *httptest.Server
}

func setup(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	e, err := engine.New(engine.Config{Shards: 4, Registry: ir.NewRegistry(), DataDir: dir, DefaultTier: engine.TierFile,
		NoSync: true, Feeds: []string{FeedName}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	e.RegisterExecutor([]string{"dify.template-transform"}, 4, engine.ExecutorFunc(
		func(ctx context.Context, tk *task.Task, emit func([]byte)) task.Result {
			var in map[string]any
			json.Unmarshal(tk.Input, &in)
			emit([]byte("Hello "))
			emit([]byte(in["start.name"].(string)))
			ctxIn, _ := json.Marshal(in["__dify"])
			return task.Result{Output: json.RawMessage(`{"output":"Hello ` + in["start.name"].(string) + `","ctx":` + string(ctxIn) + `}`)}
		}))
	d := &Daemon{E: e, Key: "k", Dir: dir}
	if err := d.Load(); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go d.Consume(ctx)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)
	return &fixture{t: t, e: e, d: d, srv: srv}
}

func (f *fixture) do(method, path, body string, out any) int {
	f.t.Helper()
	req, _ := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer k")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer res.Body.Close()
	if out != nil {
		json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode
}

// worker is a feed connection.
type worker struct {
	f      *fixture
	conn   uint64
	events chan Event
	cancel func()
}

func (f *fixture) connect(want int) *worker {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", f.srv.URL+"/v1/feed?worker=test&want="+strconv.Itoa(want), nil)
	req.Header.Set("Authorization", "Bearer k")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	if res.StatusCode != 200 {
		f.t.Fatalf("feed: %d", res.StatusCode)
	}
	w := &worker{f: f, events: make(chan Event, 1024), cancel: func() { cancel(); res.Body.Close() }}
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(nil, 1<<20)
	if !sc.Scan() {
		f.t.Fatal("no hello")
	}
	var hello Event
	json.Unmarshal(sc.Bytes(), &hello)
	w.conn = hello.Conn
	go func() {
		defer close(w.events)
		for sc.Scan() {
			if len(bytes.TrimSpace(sc.Bytes())) == 0 {
				continue
			}
			var ev Event
			if json.Unmarshal(sc.Bytes(), &ev) == nil {
				w.events <- ev
			}
		}
	}()
	f.t.Cleanup(w.cancel)
	return w
}

// until reads events of run until stop holds for one (notices and other
// runs' events are skipped).
func (w *worker) until(run string, stop func(Event) bool) []Event {
	w.f.t.Helper()
	var out []Event
	timeout := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-w.events:
			if !ok {
				w.f.t.Fatal("feed closed")
			}
			if ev.Run != run {
				continue
			}
			out = append(out, ev)
			if stop(ev) {
				return out
			}
		case <-timeout:
			w.f.t.Fatalf("timed out; got %s", kindsOf(out))
		}
	}
}

func (w *worker) ack(evs []Event) {
	w.f.t.Helper()
	type group struct {
		Gen uint64   `json:"gen"`
		IDs []string `json:"ids"`
	}
	byGen := map[uint64]*group{}
	for _, ev := range evs {
		if ev.ID == "" {
			continue
		}
		g := byGen[ev.Gen]
		if g == nil {
			g = &group{Gen: ev.Gen}
			byGen[ev.Gen] = g
		}
		g.IDs = append(g.IDs, ev.ID)
	}
	var acks []*group
	for _, g := range byGen {
		acks = append(acks, g)
	}
	body, _ := json.Marshal(map[string]any{"conn": w.conn, "acks": acks})
	if code := w.f.do("POST", "/v1/feed/ack", string(body), nil); code != 200 {
		w.f.t.Fatalf("ack: %d", code)
	}
}

func kindsOf(evs []Event) string {
	var s []string
	for _, ev := range evs {
		k := ev.Kind
		if ev.Node != "" {
			k += ":" + ev.Node
		}
		if ev.Kind == "chunk" {
			k += ":" + ev.Chunk
		}
		s = append(s, k)
	}
	return strings.Join(s, " ")
}

func isEnd(ev Event) bool { return ev.Kind == "run_end" }

func TestAuth(t *testing.T) {
	f := setup(t)
	for _, h := range []string{"", "Bearer wrong", "k"} {
		req, _ := http.NewRequest("GET", f.srv.URL+"/v1/plans/x", nil)
		if h != "" {
			req.Header.Set("Authorization", h)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 401 {
			t.Fatalf("%q: %d", h, res.StatusCode)
		}
	}
	if code := f.do("GET", "/v1/plans/x", "", nil); code != 404 {
		t.Fatalf("with the key: %d", code)
	}
}

// A run's events, through the human input, on the feed of a worker that
// holds every partition.
func TestRunEventsAndHumanInput(t *testing.T) {
	f := setup(t)
	w := f.connect(0)
	var pi planInfo
	if code := f.do("PUT", "/v1/plans/wf@1", workflow, &pi); code != 200 || pi.Hash == "" {
		t.Fatalf("put plan: %d %+v", code, pi)
	}
	var sr submitReply
	body := `{"plan":"wf@1","run_id":"r1","tenant":"t","input":{"name":"kairo","sys":{"user_id":"u"},"__dify":{"tenant_id":"t1"}}}`
	if code := f.do("POST", "/v1/runs", body, &sr); code != 201 || sr.RunID != "r1" {
		t.Fatalf("submit: %d %+v", code, sr)
	}
	first := w.until("r1", func(ev Event) bool { return ev.Kind == "node_start" && ev.Node == "review" })
	want := "run_start node_start:start node_end:start node_start:tpl chunk:tpl:Hello  chunk:tpl:kairo node_end:tpl node_start:review"
	if got := kindsOf(first); got != want {
		t.Fatalf("first part:\n got %s\nwant %s", got, want)
	}
	for _, ev := range first {
		if ev.Kind == "node_end" && ev.Node == "tpl" && !strings.Contains(string(ev.Output), `"ctx":{"tenant_id":"t1"}`) {
			t.Fatalf("the run context did not reach the worker: %s", ev.Output)
		}
		if ev.Kind != "chunk" && ev.ID == "" {
			t.Fatalf("a durable entry without an id: %+v", ev)
		}
	}
	if code := f.do("POST", "/v1/runs/r1/signal", `{"node_id":"review","handle":"approve","outputs":{"comment":"lgtm"}}`, nil); code != 202 {
		t.Fatalf("signal: %d", code)
	}
	rest := w.until("r1", isEnd)
	want = "node_end:review node_start:review__route node_end:review__route node_start:ok node_end:ok node_skip:no run_end"
	if got := kindsOf(rest); got != want {
		t.Fatalf("second part:\n got %s\nwant %s", got, want)
	}
	end := rest[len(rest)-1]
	var out struct {
		OK struct {
			Greeting string `json:"greeting"`
			Comment  string `json:"comment"`
		} `json:"ok"`
	}
	json.Unmarshal(end.Output, &out)
	if end.Status != "completed" || out.OK.Greeting != "Hello kairo" || out.OK.Comment != "lgtm" {
		t.Fatalf("%+v %s", end, end.Output)
	}
	w.ack(append(first, rest...))
}

// Entries a worker did not acknowledge come again to the next holder of
// the partition, from the first one; acknowledged ones do not.
func TestUnacknowledgedEntriesAreDeliveredAgain(t *testing.T) {
	f := setup(t)
	f.do("PUT", "/v1/plans/wf", workflow, nil)
	w1 := f.connect(0)
	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"r2","tenant":"t","input":{"name":"x"}}`, nil)
	part1 := w1.until("r2", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	var durable []Event
	for _, ev := range part1 {
		if ev.ID != "" {
			durable = append(durable, ev)
		}
	}
	w1.ack(durable[:3])
	w1.cancel()

	w2 := f.connect(0)
	again := w2.until("r2", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	var ids, wantIDs []string
	for _, ev := range again {
		if ev.ID != "" {
			ids = append(ids, ev.ID)
		}
	}
	for _, ev := range durable[3:] {
		wantIDs = append(wantIDs, ev.ID)
	}
	if strings.Join(ids, " ") != strings.Join(wantIDs, " ") {
		t.Fatalf("delivered again:\n got %v\nwant %v", ids, wantIDs)
	}
}

// Partitions are shared between connections: each is held by one at a
// time, a new connection gets its share from the others, and a closed
// connection's partitions go to the rest.
func TestPartitionsAreShared(t *testing.T) {
	f := setup(t)
	type tracker struct {
		w    *worker
		held map[int]bool
	}
	wait := func(tr *tracker, n int) {
		t.Helper()
		timeout := time.After(5 * time.Second)
		for len(tr.held) != n {
			select {
			case ev := <-tr.w.events:
				switch ev.Kind {
				case "assigned":
					tr.held[ev.Partition] = true
				case "revoked":
					delete(tr.held, ev.Partition)
				}
			case <-timeout:
				t.Fatalf("holds %v, want %d partitions", tr.held, n)
			}
		}
	}
	t1 := &tracker{w: f.connect(0), held: map[int]bool{}}
	wait(t1, 4)
	t2 := &tracker{w: f.connect(0), held: map[int]bool{}}
	wait(t2, 2)
	wait(t1, 2)
	for p := range t2.held {
		if t1.held[p] {
			t.Fatalf("partition %d held twice: %v %v", p, t1.held, t2.held)
		}
	}
	t2.w.cancel()
	wait(t1, 4)
}

func TestCancel(t *testing.T) {
	f := setup(t)
	w := f.connect(0)
	f.do("PUT", "/v1/plans/wf", workflow, nil)
	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"r3","tenant":"t","input":{"name":"x"}}`, nil)
	w.until("r3", func(ev Event) bool { return ev.Node == "review" })
	if code := f.do("POST", "/v1/runs/r3/cancel", `{}`, nil); code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	evs := w.until("r3", isEnd)
	if end := evs[len(evs)-1]; end.Status != "cancelled" {
		t.Fatalf("%+v", end)
	}
}

// A cancellation answers once it is applied (ADR 0041): no node starts
// after it, and a run that had ended answers how it ended.
func TestCancelAnswersOnceApplied(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.New(engine.Config{Shards: 1, Registry: ir.NewRegistry(), DataDir: dir, DefaultTier: engine.TierFile,
		NoSync: true, Feeds: []string{FeedName}, EvictAfter: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	running := make(chan struct{}, 1)
	e.RegisterExecutor([]string{"dify.template-transform"}, 1, engine.ExecutorFunc(
		func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
			if strings.Contains(string(tk.Input), "slow") {
				running <- struct{}{}
				<-ctx.Done() // runs until the cancellation reaches it
				return task.Result{Err: ctx.Err().Error()}
			}
			return task.Result{Output: json.RawMessage(`{"output":"x"}`)}
		}))
	d := &Daemon{E: e, Key: "k", Dir: dir}
	if err := d.Load(); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go d.Consume(ctx)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)
	f := &fixture{t: t, e: e, d: d, srv: srv}
	w := f.connect(0)
	f.do("PUT", "/v1/plans/wf", workflow, nil)

	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"stopped","tenant":"t","input":{"name":"slow"}}`, nil)
	<-running
	var res struct{ Status string }
	if code := f.do("POST", "/v1/runs/stopped/cancel", `{"reason":"stop"}`, &res); code != 200 || res.Status != "cancelled" {
		t.Fatalf("cancel: %d %+v", code, res)
	}
	stopped := w.until("stopped", isEnd)
	w.ack(stopped)
	for _, ev := range stopped {
		if ev.Node == "review" {
			t.Fatalf("a node started after the cancellation: %+v", ev)
		}
		if ev.Kind == "run_end" && ev.Status != "cancelled" {
			t.Fatalf("%+v", ev)
		}
	}

	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"ended","tenant":"t","input":{"name":"x"}}`, nil)
	w.ack(w.until("ended", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" }))
	f.do("POST", "/v1/runs/ended/signal", `{"node_id":"review","handle":"reject"}`, nil)
	w.ack(w.until("ended", isEnd))
	if code := f.do("POST", "/v1/runs/ended/cancel", `{}`, &res); code != 200 || res.Status != "completed" {
		t.Fatalf("cancel of an ended run: %d %+v", code, res)
	}

	// A run waiting for a human is evicted; its cancellation is applied
	// once it is loaded, and the answer waits for that.
	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"evicted","tenant":"t","input":{"name":"x"}}`, nil)
	// Its traces acknowledged, a waiting run can be evicted (ADR 0034).
	w.ack(w.until("evicted", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" }))
	deadline := time.Now().Add(5 * time.Second)
	for {
		ri, err := e.Get(context.Background(), "evicted")
		if err == nil && ri.Evicted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the run was not evicted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if code := f.do("POST", "/v1/runs/evicted/cancel", `{}`, &res); code != 200 || res.Status != "cancelled" {
		t.Fatalf("cancel of an evicted run: %d %+v", code, res)
	}
}

// A run still waiting for admission is withdrawn by its cancellation: its
// submission is refused and it never starts.
func TestCancelWithdrawsAQueuedRun(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.New(engine.Config{Shards: 1, Registry: ir.NewRegistry(), DataDir: dir, DefaultTier: engine.TierFile,
		NoSync: true, Feeds: []string{FeedName}, Admission: sched.AdmissionConfig{MaxActive: 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	e.RegisterExecutor([]string{"dify.template-transform"}, 1, engine.ExecutorFunc(
		func(context.Context, *task.Task, func([]byte)) task.Result {
			return task.Result{Output: json.RawMessage(`{"output":"x"}`)}
		}))
	d := &Daemon{E: e, Key: "k", Dir: dir}
	if err := d.Load(); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go d.Consume(ctx)
	srv := httptest.NewServer(d.Handler())
	t.Cleanup(srv.Close)
	f := &fixture{t: t, e: e, d: d, srv: srv}
	w := f.connect(0)
	f.do("PUT", "/v1/plans/wf", workflow, nil)

	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"first","tenant":"t","input":{"name":"x"}}`, nil)
	w.until("first", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	submitted := make(chan int, 1)
	go func() {
		submitted <- f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"queued","tenant":"t","input":{"name":"y"}}`, nil)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().AdmissionQueued == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the second run did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	var res struct{ Status string }
	if code := f.do("POST", "/v1/runs/queued/cancel", `{}`, &res); code != 200 || res.Status != "cancelled" {
		t.Fatalf("cancel of a queued run: %d %+v", code, res)
	}
	if code := <-submitted; code != 409 {
		t.Fatalf("submission of the withdrawn run: %d", code)
	}
	f.do("POST", "/v1/runs/first/signal", `{"node_id":"review","handle":"reject"}`, nil)
	w.until("first", isEnd)
	if e.Stats().AdmissionQueued != 0 {
		t.Fatal("the withdrawn run is still queued")
	}
	if _, err := e.Get(context.Background(), "queued"); err == nil {
		t.Fatal("the withdrawn run started")
	}
}

// Plans survive a restart of the daemon.
func TestPlansPersist(t *testing.T) {
	f := setup(t)
	f.do("PUT", "/v1/plans/wf", workflow, nil)
	d2 := &Daemon{E: f.e, Dir: f.d.Dir}
	if err := d2.Load(); err != nil {
		t.Fatal(err)
	}
	if p := d2.plans[planName("wf")]; p == nil || p.Hash != f.d.plans[planName("wf")].Hash {
		t.Fatal("plan not restored")
	}
}

// What workers acknowledged is acknowledged to the engine: a restarted
// daemon delivers only what was not.
func TestAcknowledgementsReachTheEngine(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.New(engine.Config{Shards: 1, Registry: ir.NewRegistry(), DataDir: dir, DefaultTier: engine.TierFile,
		NoSync: true, Feeds: []string{FeedName}})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	e.RegisterExecutor([]string{"dify.template-transform"}, 1, engine.ExecutorFunc(
		func(context.Context, *task.Task, func([]byte)) task.Result {
			return task.Result{Output: json.RawMessage(`{"output":"x"}`)}
		}))
	start := func() (*fixture, context.CancelFunc) {
		d := &Daemon{E: e, Key: "k", Dir: dir}
		if err := d.Load(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go d.Consume(ctx)
		srv := httptest.NewServer(d.Handler())
		t.Cleanup(srv.Close)
		return &fixture{t: t, e: e, d: d, srv: srv}, cancel
	}
	f, stop := start()
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	w := f.connect(0)
	f.do("PUT", "/v1/plans/wf", workflow, nil)
	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"done","tenant":"t","input":{"name":"x"}}`, nil)
	first := w.until("done", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	f.do("POST", "/v1/runs/done/signal", `{"node_id":"review","handle":"reject"}`, nil)
	evs := append(first, w.until("done", isEnd)...)
	w.ack(evs)
	// The acknowledgement is persisted asynchronously: wait until the
	// engine holds nothing more of it.
	deadline := time.Now().Add(5 * time.Second)
	for e.Stats().FeedBacklog > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("backlog %d", e.Stats().FeedBacklog)
		}
		time.Sleep(time.Millisecond)
	}
	w.cancel()
	stop()

	f2, stop2 := start()
	defer stop2()
	w2 := f2.connect(0)
	f2.do("POST", "/v1/runs", `{"plan":"wf","run_id":"next","tenant":"t","input":{"name":"y"}}`, nil)
	timeout := time.After(10 * time.Second)
	for {
		select {
		case ev := <-w2.events:
			if ev.Run == "done" {
				t.Fatalf("an acknowledged entry came again: %+v", ev)
			}
			if ev.Run == "next" {
				return
			}
		case <-timeout:
			t.Fatal("no events of the next run")
		}
	}
}

// A prepared run waits for its variables: the host submits only its own
// parameters, the worker sends the input and the conversation variables.
func TestPreparedRun(t *testing.T) {
	f := setup(t)
	w := f.connect(0)
	wf := `{"graph":{"nodes":[
	  {"id":"start","data":{"type":"start","title":"Start","variables":[{"variable":"name","type":"text-input"}]}},
	  {"id":"fin","data":{"type":"end","title":"End","outputs":[
	    {"variable":"name","value_selector":["start","name"]},
	    {"variable":"user","value_selector":["sys","user_id"]},
	    {"variable":"memo","value_selector":["conversation","memo"]}]}}],
	  "edges":[{"source":"start","target":"fin"}]},
	  "conversation_variables":[{"name":"memo","value_type":"string","value":""}]}`
	if code := f.do("PUT", "/v1/plans/p1?prepare=1", wf, nil); code != 200 {
		t.Fatalf("put: %d", code)
	}
	if code := f.do("GET", "/v1/plans/p1?prepare=1", "", nil); code != 200 {
		t.Fatalf("get: %d", code)
	}
	if code := f.do("GET", "/v1/plans/p1", "", nil); code != 404 {
		t.Fatalf("the plain plan should not exist: %d", code)
	}
	if code := f.do("POST", "/v1/runs?prepare=1", `{"plan":"p1","run_id":"pr","tenant":"t","input":{"__host":{"app":"a"}}}`, nil); code != 201 {
		t.Fatalf("submit: %d", code)
	}
	head := w.until("pr", func(ev Event) bool { return ev.Kind == "node_start" && ev.Node == dify.PrepareNode })
	if !strings.Contains(string(head[0].Input), `"__host":{"app":"a"}`) {
		t.Fatalf("run start input %s", head[0].Input)
	}
	if code := f.do("POST", "/v1/runs/pr/prepare", `{"input":{"name":"kairo","sys":{"user_id":"u1"}},"vars":{"memo":"hi"}}`, nil); code != 202 {
		t.Fatalf("prepare: %d", code)
	}
	evs := w.until("pr", isEnd)
	end := evs[len(evs)-1]
	if end.Status != "completed" || !strings.Contains(string(end.Output), `"fin":{"memo":"hi","name":"kairo","user":"u1"}`) {
		t.Fatalf("%s %s %s", end.Status, end.Error, end.Output)
	}
}

// A partition with entries not acknowledged stays with its holder when
// another connection wants its share: idle ones move instead.
func TestBusyPartitionsDoNotMove(t *testing.T) {
	f := setup(t)
	f.do("PUT", "/v1/plans/wf", workflow, nil)
	w1 := f.connect(0)
	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"busy","tenant":"t","input":{"name":"x"}}`, nil)
	evs := w1.until("busy", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	busy := evs[0].Partition

	f.connect(0)
	revoked := map[int]bool{}
	timeout := time.After(3 * time.Second)
	for len(revoked) < 2 {
		select {
		case ev := <-w1.events:
			if ev.Kind == "revoked" {
				revoked[ev.Partition] = true
			}
		case <-timeout:
			t.Fatalf("revoked %v, want 2 of 4 partitions moved", revoked)
		}
	}
	if revoked[busy] {
		t.Fatalf("the busy partition %d was taken while its run was in progress (revoked %v)", busy, revoked)
	}
}

// Live chunks go once, to the holder of the time: a later holder of the
// partition gets the entries not acknowledged, without the chunks.
func TestChunksAreNotSentAgain(t *testing.T) {
	f := setup(t)
	f.do("PUT", "/v1/plans/wf", workflow, nil)
	w1 := f.connect(0)
	f.do("POST", "/v1/runs", `{"plan":"wf","run_id":"ch","tenant":"t","input":{"name":"x"}}`, nil)
	first := w1.until("ch", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	if !strings.Contains(kindsOf(first), "chunk") {
		t.Fatalf("no chunks to begin with: %s", kindsOf(first))
	}
	w1.cancel()
	w2 := f.connect(0)
	again := w2.until("ch", func(ev Event) bool { return ev.Node == "review" && ev.Kind == "node_start" })
	if strings.Contains(kindsOf(again), "chunk") {
		t.Fatalf("chunks sent again: %s", kindsOf(again))
	}
}
