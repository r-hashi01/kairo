package n8n

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/engine"
	"kairo/ir"
	"kairo/store/postgres"
	"kairo/task"
)

// The daemon against a real PostgreSQL: KAIRO_N8N_PG_DSN, e.g.
// postgres://postgres:kairo@127.0.0.1:55432/kairo_n8n?sslmode=disable
// (scripts: docker run -p 127.0.0.1:55432:5432 -e POSTGRES_PASSWORD=kairo
// -e POSTGRES_DB=kairo_n8n postgres:16-alpine).

var secret = []byte("0123456789abcdef0123456789abcdef")

type fixture struct {
	t      *testing.T
	d      *Daemon
	srv    *httptest.Server
	mu     sync.Mutex
	events []LifecycleEvent
	frames []string
	// What node-m's step read of the steps before it.
	stepData string
}

func newFixture(t *testing.T) *fixture {
	dsn := os.Getenv("KAIRO_N8N_PG_DSN")
	if dsn == "" {
		t.Skip("KAIRO_N8N_PG_DSN not set")
	}
	db, err := postgres.Open(dsn, postgres.Options{AllowInsecureTransport: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	prefix := "t" + strconv.FormatInt(time.Now().UnixNano()%1e9, 10) + "_"
	views, err := OpenViews(db, prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DROP TABLE ` + prefix + `execution, ` + prefix + `step`) })
	f := &fixture{t: t}

	// The control plane: lifecycle events.
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Events []LifecycleEvent }
		json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.events = append(f.events, body.Events...)
		f.mu.Unlock()
		w.WriteHeader(204)
	}))
	t.Cleanup(cp.Close)
	// Redis: PUBLISH frames.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serveRedis(c)
		}
	}()

	reg := ir.NewRegistry()
	Spec(reg)
	dir := t.TempDir()
	e, err := engine.New(engine.Config{Shards: 2, Registry: reg, DataDir: dir, DefaultTier: engine.TierFile,
		NoSync: true, Feeds: []string{FeedName}, BlobThreshold: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	e.RegisterExecutor([]string{ActionNode}, 8, engine.ExecutorFunc(
		func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
			var node Node
			json.Unmarshal(tk.Params, &node)
			slots := inputSlots(tk.Input)
			switch node.ID {
			case "node-if":
				return task.Result{Output: json.RawMessage(`[[{"json":{"taken":true}}],null]`)}
			case "node-fail":
				return task.Result{Err: "boom"}
			case "node-slow":
				time.Sleep(600 * time.Millisecond)
			case "node-wait":
				// As n8n's Wait node: wait, then emit its input (ADR 0045).
				return task.Result{Wait: &task.Wait{Until: time.Now().Add(400 * time.Millisecond).UnixMilli(),
					Output: json.RawMessage(`[[{"json":{"waited":true}}]]`)}}
			case "body":
				return task.Result{Output: json.RawMessage("[" + string(slots[0]) + "]")}
			case "node-m":
				// As n8n's worker: read the outputs before this step.
				req, _ := http.NewRequest("POST", f.srv.URL+"/internal/step-data",
					strings.NewReader(`{"executionId":"`+tk.RunID+`","nodeId":"node-m","iteration":0}`))
				req.Header.Set("Authorization", "Bearer worker-token")
				if res, err := http.DefaultClient.Do(req); err == nil {
					b, _ := io.ReadAll(res.Body)
					res.Body.Close()
					f.mu.Lock()
					f.stepData = strconv.Itoa(res.StatusCode) + " " + string(b)
					f.mu.Unlock()
				}
			}
			return task.Result{Output: json.RawMessage(`[[{"json":{"ran":"` + node.ID + `"}}]]`)}
		}))
	events := &Events{URL: cp.URL, Secret: secret}
	f.d = &Daemon{E: e, Views: views, Secret: secret, Events: events, Dir: dir, WorkerToken: "worker-token",
		Responses: &Responses{Addr: ln.Addr().String()}}
	if err := f.d.Load(); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go events.Run(ctx)
	go f.d.Consume(ctx)
	f.srv = httptest.NewServer(f.d.Handler())
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fixture) serveRedis(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		args := make([]string, n)
		for i := range args {
			l, _ := rd.ReadString('\n')
			k, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
			b := make([]byte, k+2)
			io.ReadFull(rd, b)
			args[i] = string(b[:k])
		}
		if len(args) == 3 && args[0] == "PUBLISH" {
			f.mu.Lock()
			f.frames = append(f.frames, args[1]+" "+args[2])
			f.mu.Unlock()
		}
		c.Write([]byte(":1\r\n"))
	}
}

func (f *fixture) do(method, path, body string, out any) int {
	f.t.Helper()
	tok, _ := cpToken(secret, "cp-1", time.Now())
	req, _ := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
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

func (f *fixture) waitFor(cond func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			f.t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

const diamondGraph = `{"nodes":[
  {"id":"trigger","name":"Webhook","type":"trigger"},
  {"id":"node-if","name":"If","type":"v1-node"},
  {"id":"node-a","name":"A","type":"v1-node"},
  {"id":"node-b","name":"B","type":"v1-node"},
  {"id":"node-m","name":"M","type":"v1-node"}],
 "edges":[
  {"from":"trigger","to":"node-if","outputIndex":0,"inputIndex":0},
  {"from":"node-if","to":"node-a","outputIndex":0,"inputIndex":0},
  {"from":"node-if","to":"node-b","outputIndex":1,"inputIndex":0},
  {"from":"node-a","to":"node-m","outputIndex":0,"inputIndex":0},
  {"from":"node-b","to":"node-m","outputIndex":0,"inputIndex":1}]}`

func startBody(id, graph string) string {
	return `{"executionId":"` + id + `","workflowId":"wf-1","graph":` + graph + `,"workflow":{"name":"x"},
	  "triggerOutputs":[[{"json":{"q":1}}]],"mode":"manual","callerContext":{"hostMode":"manual","userId":"u1"}}`
}

func TestDaemonRunsAndRecords(t *testing.T) {
	f := newFixture(t)
	id := "01890a5d-ac96-774b-bcce-b302099a8057"
	var res map[string]string
	if code := f.do("POST", "/api/workflow-executions", startBody(id, diamondGraph), &res); code != 201 || res["executionId"] != id {
		t.Fatalf("start: %d %v", code, res)
	}
	var snap struct {
		Status, Mode, HostMode string
		FinishedAt             *string
		Steps                  []Step
	}
	f.waitFor(func() bool {
		f.do("GET", "/api/workflow-executions/"+id+"?includeSteps=true", "", &snap)
		return snap.Status == "completed"
	})
	if snap.Mode != "manual" || snap.HostMode != "manual" || snap.FinishedAt == nil {
		t.Fatalf("%+v", snap)
	}
	got := map[string]string{}
	for _, s := range snap.Steps {
		got[s.NodeID] = s.Status
	}
	want := map[string]string{"trigger": "completed", "node-if": "completed", "node-a": "completed", "node-b": "skipped", "node-m": "completed"}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("steps %v, want %v", got, want)
		}
	}
	// Lifecycle events: execution started and completed, a start and an
	// end per step that ran, in order per step; none for the trigger or a
	// skipped step.
	f.waitFor(func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.events) > 0 && f.events[len(f.events)-1].Type == "execution:completed"
	})
	f.mu.Lock()
	var types []string
	for _, ev := range f.events {
		types = append(types, ev.Type+":"+ev.NodeID)
	}
	frames := append([]string(nil), f.frames...)
	f.mu.Unlock()
	if types[0] != "execution:started:" || strings.Count(strings.Join(types, " "), "step:completed") != 3 ||
		strings.Contains(strings.Join(types, " "), "node-b") || strings.Contains(strings.Join(types, " "), ":trigger") {
		t.Fatalf("events %v", types)
	}
	// The "ended" response for a waiting webhook.
	f.waitFor(func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.frames) > 0 })
	f.mu.Lock()
	frames = append([]string(nil), f.frames...)
	f.mu.Unlock()
	if !strings.HasPrefix(frames[0], "n8n:engine-v2-responses:"+id+" ") || !strings.Contains(frames[0], `"type":"ended"`) ||
		!strings.Contains(frames[0], `"status":"completed"`) {
		t.Fatalf("frame %s", frames[0])
	}
	// node-m's step read everything completed before it: the trigger,
	// the If and A (not B, which was skipped).
	f.mu.Lock()
	sd := f.stepData
	f.mu.Unlock()
	var data struct {
		OutputsByNode map[string]map[string]json.RawMessage
	}
	json.Unmarshal([]byte(strings.TrimPrefix(sd, "200 ")), &data)
	if !strings.HasPrefix(sd, "200 ") || string(data.OutputsByNode["node-a"]["0"]) != `[[{"json":{"ran":"node-a"}}]]` ||
		data.OutputsByNode["node-if"] == nil || data.OutputsByNode["trigger"] == nil || data.OutputsByNode["node-b"] != nil {
		t.Fatalf("step data %s", sd)
	}
	// The search finds it.
	var page struct {
		Items      []Execution
		NextCursor *Cursor
		Total      *int
	}
	if code := f.do("POST", "/api/workflow-executions/search", `{"workflowIds":["wf-1"],"status":["completed"],"includeTotal":true}`, &page); code != 200 ||
		len(page.Items) != 1 || page.Items[0].ID != id || page.Total == nil || *page.Total != 1 {
		t.Fatalf("search: %d %+v", code, page)
	}
}

func TestDaemonFailureAndErrors(t *testing.T) {
	f := newFixture(t)
	failing := `{"nodes":[{"id":"trigger","type":"trigger","name":"T"},{"id":"node-fail","type":"v1-node","name":"F"},
	  {"id":"node-after","type":"v1-node","name":"After"}],
	  "edges":[{"from":"trigger","to":"node-fail","outputIndex":0,"inputIndex":0},{"from":"node-fail","to":"node-after","outputIndex":0,"inputIndex":0}]}`
	id := "01890a5d-ac96-774b-bcce-b302099a8058"
	if code := f.do("POST", "/api/workflow-executions", startBody(id, failing), nil); code != 201 {
		t.Fatal(code)
	}
	var snap struct {
		Status string
		Steps  []Step
	}
	f.waitFor(func() bool {
		f.do("GET", "/api/workflow-executions/"+id+"?includeSteps=true", "", &snap)
		return snap.Status == "failed"
	})
	for _, s := range snap.Steps {
		if s.NodeID == "node-fail" && (s.Status != "failed" || !bytes.Contains(s.Error, []byte("boom"))) {
			t.Fatalf("%+v", s)
		}
		if s.NodeID == "node-after" && s.Status == "completed" {
			t.Fatal("ran after a failure")
		}
	}
	// Errors as engine v2 answers them.
	for _, tc := range []struct {
		method, path, body string
		code               int
	}{
		{"POST", "/api/workflow-executions", startBody("not-a-uuid", failing), 400},
		{"POST", "/api/workflow-executions", startBody("01890a5d-ac96-774b-bcce-b302099a8059", `{"nodes":[{"id":"w","type":"wait"}],"edges":[]}`), 501},
		{"POST", "/api/workflow-executions", startBody(id, failing), 409},
		{"GET", "/api/workflow-executions/01890a5d-ac96-774b-bcce-b302099a8000", "", 404},
		{"GET", "/api/workflow-executions/" + id + "?nope=1", "", 400},
		{"POST", "/api/workflow-executions/search", `{"workflowIds":"all","limit":101}`, 400},
	} {
		if code := f.do(tc.method, tc.path, tc.body, nil); code != tc.code {
			t.Fatalf("%s %s: %d, want %d", tc.method, tc.path, code, tc.code)
		}
	}
	// Without a valid token: 401.
	res, _ := http.Get(f.srv.URL + "/api/workflow-executions/" + id)
	if res.StatusCode != 401 {
		t.Fatalf("no token: %d", res.StatusCode)
	}
}

const batchGraph = `{"nodes":[
  {"id":"trigger","name":"Webhook","type":"trigger"},
  {"id":"loop","name":"Loop","type":"batch","config":{"batchSize":2}},
  {"id":"body","name":"Body","type":"v1-node"},
  {"id":"node-a","name":"Done","type":"v1-node"}],
 "edges":[
  {"from":"trigger","to":"loop","outputIndex":0,"inputIndex":0},
  {"from":"loop","to":"node-a","outputIndex":0,"inputIndex":0},
  {"from":"loop","to":"body","outputIndex":1,"inputIndex":0},
  {"from":"body","to":"loop","outputIndex":0,"inputIndex":0,"isBackEdge":true}]}`

// A batch loop records a Loop step per round and a Body step per running
// round only: as in engine v2, the terminal round has no body steps, not
// even skipped ones. Items keep their keys in order.
func TestDaemonBatchLoopSteps(t *testing.T) {
	f := newFixture(t)
	id := "01890a5d-ac96-774b-bcce-b302099a8058"
	body := strings.Replace(startBody(id, batchGraph), `[[{"json":{"q":1}}]]`,
		`[[{"json":{"v":1,"a":0}},{"json":{"v":2,"a":0}},{"json":{"v":3,"a":0}}]]`, 1)
	if code := f.do("POST", "/api/workflow-executions", body, nil); code != 201 {
		t.Fatalf("start: %d", code)
	}
	var snap struct {
		Status string
		Steps  []Step
	}
	f.waitFor(func() bool {
		f.do("GET", "/api/workflow-executions/"+id+"?includeSteps=true", "", &snap)
		return snap.Status == "completed"
	})
	var got []string
	for _, s := range snap.Steps {
		got = append(got, s.NodeID+"["+strconv.Itoa(s.Iteration)+"]:"+s.Status)
	}
	slices.Sort(got)
	want := "body[0]:completed body[1]:completed loop[0]:completed loop[1]:completed loop[2]:completed node-a[0]:completed trigger[0]:completed"
	if strings.Join(got, " ") != want {
		t.Fatalf("steps\n %s\nwant\n %s", strings.Join(got, " "), want)
	}
	// The records are jsonb (as engine v2's), which sorts keys; the
	// lifecycle events carry the outputs as they were.
	f.waitFor(func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, ev := range f.events {
			if ev.Type == "step:completed" && ev.NodeID == "loop" && *ev.Iteration == 2 {
				if !strings.Contains(string(ev.Outputs), `{"json":{"v":1,"a":0}}`) {
					t.Errorf("loop's done slot %s", ev.Outputs)
				}
				return true
			}
		}
		return false
	})
}

const waitGraph = `{"nodes":[
  {"id":"trigger","name":"Webhook","type":"trigger"},
  {"id":"node-wait","name":"Wait","type":"v1-node"},
  {"id":"node-a","name":"A","type":"v1-node"}],
 "edges":[
  {"from":"trigger","to":"node-wait","outputIndex":0,"inputIndex":0},
  {"from":"node-wait","to":"node-a","outputIndex":0,"inputIndex":0}]}`

// A step that waits: its row and the execution are waiting until the
// deadline, with no lifecycle event; then the step completes with the
// output it gave and what follows runs (ADR 0045).
func TestDaemonStepWaits(t *testing.T) {
	f := newFixture(t)
	id := "01890a5d-ac96-774b-bcce-b302099a8059"
	if code := f.do("POST", "/api/workflow-executions", startBody(id, waitGraph), nil); code != 201 {
		t.Fatalf("start: %d", code)
	}
	var snap struct {
		Status string
		Steps  []Step
	}
	status := func(node string) string {
		for _, s := range snap.Steps {
			if s.NodeID == node {
				return s.Status
			}
		}
		return ""
	}
	f.waitFor(func() bool {
		f.do("GET", "/api/workflow-executions/"+id+"?includeSteps=true", "", &snap)
		return snap.Status == "waiting"
	})
	if status("node-wait") != "waiting" || status("node-a") != "" {
		t.Fatalf("while waiting: %+v", snap)
	}
	f.mu.Lock()
	for _, ev := range f.events {
		if ev.NodeID == "node-wait" && ev.Type != "step:started" {
			t.Errorf("event %s while waiting", ev.Type)
		}
	}
	f.mu.Unlock()
	f.waitFor(func() bool {
		f.do("GET", "/api/workflow-executions/"+id+"?includeSteps=true", "", &snap)
		return snap.Status == "completed"
	})
	if status("node-wait") != "completed" || status("node-a") != "completed" {
		t.Fatalf("after the deadline: %+v", snap)
	}
	for _, s := range snap.Steps {
		if s.NodeID == "node-wait" && string(s.Outputs) != `[[{"json":{"waited":true}}]]` {
			t.Fatalf("wait step outputs %s", s.Outputs)
		}
	}
}

const waitSlowGraph = `{"nodes":[
  {"id":"trigger","name":"Webhook","type":"trigger"},
  {"id":"node-wait","name":"Wait","type":"v1-node"},
  {"id":"node-slow","name":"Slow","type":"v1-node"}],
 "edges":[
  {"from":"trigger","to":"node-wait","outputIndex":0,"inputIndex":0},
  {"from":"node-wait","to":"node-slow","outputIndex":0,"inputIndex":0}]}`

// What the daemon keeps of an execution in memory is rebuilt from the
// records after a restart: a step still waits, so when it ends the
// execution is running again (ADR 0045).
func TestDaemonRestoresWaitingSteps(t *testing.T) {
	f := newFixture(t)
	id := "01890a5d-ac96-774b-bcce-b302099a805a"
	if code := f.do("POST", "/api/workflow-executions", startBody(id, waitSlowGraph), nil); code != 201 {
		t.Fatalf("start: %d", code)
	}
	var snap struct {
		Status string
		Steps  []Step
	}
	get := func() { f.do("GET", "/api/workflow-executions/"+id+"?includeSteps=true", "", &snap) }
	f.waitFor(func() bool { get(); return snap.Status == "waiting" })
	// As a restart: the daemon forgets the execution.
	f.d.mu.Lock()
	delete(f.d.runs, id)
	f.d.mu.Unlock()
	// The wait ends and Slow runs: the execution is running again.
	f.waitFor(func() bool {
		get()
		for _, s := range snap.Steps {
			if s.NodeID == "node-slow" && s.Status == "running" {
				return true
			}
		}
		return snap.Status == "completed"
	})
	if snap.Status != "running" {
		t.Fatalf("while Slow runs after the wait: %s", snap.Status)
	}
	f.waitFor(func() bool { get(); return snap.Status == "completed" })
}

// The terminal round of a batch loop and the step that settled last come
// back from the step rows.
func TestRunInfoRestore(t *testing.T) {
	var g Graph
	if err := json.Unmarshal([]byte(batchGraph), &g); err != nil {
		t.Fatal(err)
	}
	def, err := Convert("wf", &g)
	if err != nil {
		t.Fatal(err)
	}
	reg := ir.NewRegistry()
	Spec(reg)
	plan, err := ir.Compile(def, reg)
	if err != nil {
		t.Fatal(err)
	}
	ri := &runInfo{plan: plan, names: map[string]string{"loop": "Loop", "body": "Body"}}
	at := time.Unix(100, 0)
	ri.restore([]Step{
		{ID: "a", NodeID: "loop", Iteration: 0, Status: "completed", Outputs: json.RawMessage(`[null, [{"json": {}}]]`), UpdatedAt: at},
		{ID: "b", NodeID: "body", Iteration: 0, Status: "completed", Outputs: json.RawMessage(`[[{"json": {}}]]`), UpdatedAt: at.Add(time.Second)},
		{ID: "c", NodeID: "loop", Iteration: 1, Status: "completed", Outputs: json.RawMessage(`[[{"json": {}}], null]`), UpdatedAt: at.Add(2 * time.Second)},
		{ID: "d", NodeID: "node-a", Iteration: 0, Status: "running", UpdatedAt: at.Add(3 * time.Second)},
	})
	var loop int32 = -1
	for i := range plan.Nodes {
		if plan.Nodes[i].Kind == ir.KLoop {
			loop = int32(i)
		}
	}
	if r, ok := ri.ended[loop]; !ok || r != 1 {
		t.Fatalf("terminal rounds %v, want loop %d at 1", ri.ended, loop)
	}
	if ri.last == nil || ri.last.ID != "c" || ri.lastName != "Loop" {
		t.Fatalf("last %+v %q", ri.last, ri.lastName)
	}
	if len(ri.waiting) != 0 {
		t.Fatalf("waiting %v", ri.waiting)
	}
}
