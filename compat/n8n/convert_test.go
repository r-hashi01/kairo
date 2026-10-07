package n8n

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"kairo/engine"
	"kairo/ir"
	"kairo/task"
)

// request is what a step executor was asked: the node and its input slots,
// as engine v2's StepExecutionRequest.
type request struct {
	Node   string
	Inputs []json.RawMessage
}

// execFunc answers a request with output slots (JSON), as an engine v2
// IStepExecutor.
type execFunc func(r request) string

// run runs graph g on kairo with trigger outputs trigger, executing its
// v1 nodes with exec; it returns the requests in the order they came and
// the run's status.
func run(t *testing.T, g *Graph, trigger string, exec execFunc) ([]request, string) {
	t.Helper()
	def, err := Convert("wf", g)
	if err != nil {
		t.Fatal(err)
	}
	reg := ir.NewRegistry()
	Spec(reg)
	e, err := engine.New(engine.Config{Shards: 1, Registry: reg, DataDir: t.TempDir(), DefaultTier: engine.TierFile, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var mu sync.Mutex
	var reqs []request
	e.RegisterExecutor([]string{ActionNode, ActionPureNode}, 8, engine.ExecutorFunc(
		func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
			var node Node
			json.Unmarshal(tk.Params, &node)
			r := request{Node: node.ID, Inputs: inputSlots(tk.Input)}
			mu.Lock()
			reqs = append(reqs, r)
			mu.Unlock()
			return task.Result{Output: json.RawMessage(exec(r))}
		}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterPlan(def); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	in := `{"` + TriggerInput + `":` + trigger + `}`
	res, err := e.Submit(ctx, engine.SubmitRequest{Plan: "wf", Tenant: "t", Input: json.RawMessage(in)})
	if err != nil {
		t.Fatal(err)
	}
	ri, err := e.Wait(ctx, res.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if ri.Status != "completed" {
		t.Logf("run: %s %s", ri.Status, ri.Error)
	}
	return reqs, ri.Status
}

// inputSlots turns a step input {"in0": ..., "in1": ...} into engine v2's
// input slots (null for a dead input).
func inputSlots(in json.RawMessage) []json.RawMessage {
	var obj map[string]json.RawMessage
	json.Unmarshal(in, &obj)
	n := 0
	for k := range obj {
		if i, err := strconv.Atoi(strings.TrimPrefix(k, "in")); err == nil && i+1 > n {
			n = i + 1
		}
	}
	slots := make([]json.RawMessage, n)
	for i := range slots {
		slots[i] = json.RawMessage("null")
		if v, ok := obj["in"+strconv.Itoa(i)]; ok {
			slots[i] = v
		}
	}
	return slots
}

func slotsJSON(s []json.RawMessage) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func ran(r request) string { return `[[{"json":{"ran":"` + r.Node + `"}}]]` }

// The cases of engine v2's step-execution integration tests
// (packages/@n8n/engine/src/execution/__tests__), with the same executors.

func TestChain(t *testing.T) {
	g := &Graph{
		Nodes: []Node{{ID: "trigger", Name: "Webhook", Type: "trigger"}, {ID: "node-a", Name: "A", Type: "v1-node"}, {ID: "node-b", Name: "B", Type: "v1-node"}},
		Edges: []Edge{{From: "trigger", To: "node-a"}, {From: "node-a", To: "node-b"}},
	}
	reqs, status := run(t, g, `[{"body":{"name":"ada"}}]`, ran)
	if status != "completed" || len(reqs) != 2 || reqs[0].Node != "node-a" || reqs[1].Node != "node-b" {
		t.Fatalf("%s %+v", status, reqs)
	}
	if got := slotsJSON(reqs[0].Inputs); got != `[{"body":{"name":"ada"}}]` {
		t.Fatalf("node-a inputs %s", got)
	}
	if got := slotsJSON(reqs[1].Inputs); got != `[[{"json":{"ran":"node-a"}}]]` {
		t.Fatalf("node-b inputs %s", got)
	}
}

func TestFanIn(t *testing.T) {
	g := &Graph{
		Nodes: []Node{{ID: "trigger", Type: "trigger"}, {ID: "node-a", Type: "v1-node"}, {ID: "node-b", Type: "v1-node"}, {ID: "node-m", Type: "v1-node"}},
		Edges: []Edge{{From: "trigger", To: "node-a"}, {From: "trigger", To: "node-b"},
			{From: "node-a", To: "node-m"}, {From: "node-b", To: "node-m", InputIndex: 1}},
	}
	reqs, status := run(t, g, `[{"body":{"name":"ada"}}]`, ran)
	var merge []request
	for _, r := range reqs {
		if r.Node == "node-m" {
			merge = append(merge, r)
		}
	}
	if status != "completed" || len(merge) != 1 {
		t.Fatalf("%s %+v", status, reqs)
	}
	if got := slotsJSON(merge[0].Inputs); got != `[[{"json":{"ran":"node-a"}}],[{"json":{"ran":"node-b"}}]]` {
		t.Fatalf("merge inputs %s", got)
	}
}

func TestConditionalDiamond(t *testing.T) {
	g := &Graph{
		Nodes: []Node{{ID: "trigger", Type: "trigger"}, {ID: "node-if", Type: "v1-node"}, {ID: "node-a", Type: "v1-node"},
			{ID: "node-b", Type: "v1-node"}, {ID: "node-c", Type: "v1-node"}, {ID: "node-m", Type: "v1-node"}},
		Edges: []Edge{{From: "trigger", To: "node-if"}, {From: "node-if", To: "node-a"}, {From: "node-if", To: "node-b", OutputIndex: 1},
			{From: "node-b", To: "node-c"}, {From: "node-a", To: "node-m"}, {From: "node-c", To: "node-m", InputIndex: 1}},
	}
	reqs, status := run(t, g, `[{}]`, func(r request) string {
		if r.Node == "node-if" {
			return `[[{"json":{"taken":true}}],null]`
		}
		return ran(r)
	})
	var ids []string
	for _, r := range reqs {
		ids = append(ids, r.Node)
	}
	sort.Strings(ids)
	if status != "completed" || !slices.Equal(ids, []string{"node-a", "node-if", "node-m"}) {
		t.Fatalf("%s %v", status, ids)
	}
	for _, r := range reqs {
		if r.Node == "node-m" {
			if got := slotsJSON(r.Inputs); got != `[[{"json":{"ran":"node-a"}}],null]` {
				t.Fatalf("merge inputs %s", got)
			}
		}
	}
}

// A SplitInBatches loop (engine v2's m1 acceptance cases, with an executor
// that sends each part back as it came): the body runs once per part, with
// $runIndex rising; the done slot carries everything gathered.
func loopGraph(size int) *Graph {
	cfg, _ := json.Marshal(map[string]int{"batchSize": size})
	return &Graph{
		Nodes: []Node{{ID: "trigger", Type: "trigger"}, {ID: "loop", Name: "Loop", Type: "batch", Config: cfg},
			{ID: "body", Type: "v1-node"}, {ID: "done", Type: "v1-node"}},
		Edges: []Edge{{From: "trigger", To: "loop"}, {From: "loop", To: "done"}, {From: "loop", To: "body", OutputIndex: 1},
			{From: "body", To: "loop", IsBackEdge: true}},
	}
}

func TestBatchLoop(t *testing.T) {
	echo := func(r request) string { return "[" + string(r.Inputs[0]) + "]" }
	reqs, status := run(t, loopGraph(1), `[[{"json":{"n":1}},{"json":{"n":2}},{"json":{"n":3}}]]`, echo)
	if status != "completed" {
		t.Fatal(status)
	}
	var body, done []request
	for _, r := range reqs {
		switch r.Node {
		case "body":
			body = append(body, r)
		case "done":
			done = append(done, r)
		}
	}
	if len(body) != 3 || len(done) != 1 {
		t.Fatalf("body ran %d times, done %d", len(body), len(done))
	}
	for i, r := range body {
		if got, want := slotsJSON(r.Inputs), `[[{"json":{"n":`+strconv.Itoa(i+1)+`}}]]`; got != want {
			t.Fatalf("round %d: %s, want %s", i, got, want)
		}
	}
	if got := slotsJSON(done[0].Inputs); got != `[[{"json":{"n":1}},{"json":{"n":2}},{"json":{"n":3}}]]` {
		t.Fatalf("done inputs %s", got)
	}
}

// The body sends nothing back: the loop ends and what follows is skipped.
func TestBatchLoopBodySendsNothing(t *testing.T) {
	reqs, status := run(t, loopGraph(1), `[[{"json":{"n":1}},{"json":{"n":2}}]]`, func(r request) string { return `[null]` })
	if status != "completed" {
		t.Fatal(status)
	}
	for _, r := range reqs {
		if r.Node == "done" {
			t.Fatal("done ran")
		}
	}
	if len(reqs) != 1 {
		t.Fatalf("body ran %d times", len(reqs))
	}
}

func TestUnsupported(t *testing.T) {
	for _, g := range []*Graph{
		{Nodes: []Node{{ID: "a", Type: "v1-node"}}},                          // no trigger
		{Nodes: []Node{{ID: "t", Type: "trigger"}, {ID: "w", Type: "wait"}}}, // wait steps
		{Nodes: []Node{{ID: "t", Type: "trigger"}, {ID: "a", Type: "v1-node"}, {ID: "b", Type: "v1-node"}}, // two edges into one input
			Edges: []Edge{{From: "t", To: "b"}, {From: "a", To: "b"}, {From: "t", To: "a"}}},
	} {
		if _, err := Convert("x", g); err == nil {
			t.Fatalf("converted %+v", g)
		}
	}
}

// Nodes of a type that acts on nothing outside are unprotected steps; any
// other node is real (ADR 0047).
func TestPureNodeTypes(t *testing.T) {
	cfg := func(typ string) json.RawMessage { return json.RawMessage(`{"nodeType":"` + typ + `","typeVersion":1}`) }
	g := &Graph{Nodes: []Node{
		{ID: "trigger", Type: "trigger"},
		{ID: "set", Type: "v1-node", Config: cfg("n8n-nodes-base.set")},
		{ID: "if", Type: "v1-node", Config: cfg("n8n-nodes-base.if")},
		{ID: "http", Type: "v1-node", Config: cfg("n8n-nodes-base.httpRequest")},
		{ID: "code", Type: "v1-node", Config: cfg("n8n-nodes-base.code")},
		{ID: "dedupe", Type: "v1-node", Config: cfg("n8n-nodes-base.removeDuplicates")},
		{ID: "community", Type: "v1-node", Config: cfg("n8n-nodes-acme.set")},
		{ID: "bare", Type: "v1-node"},
	}, Edges: []Edge{{From: "trigger", To: "set"}, {From: "set", To: "if"}, {From: "if", To: "http"},
		{From: "http", To: "code"}, {From: "code", To: "dedupe"}, {From: "dedupe", To: "community"}, {From: "community", To: "bare"}}}
	def, err := Convert("wf", g)
	if err != nil {
		t.Fatal(err)
	}
	reg := ir.NewRegistry()
	Spec(reg)
	p, err := ir.Compile(def, reg)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]ir.Effect{"set": ir.EffectUnprotected, "if": ir.EffectUnprotected, "http": ir.EffectReal,
		"code": ir.EffectReal, "dedupe": ir.EffectReal, "community": ir.EffectReal, "bare": ir.EffectReal}
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if w, ok := want[n.ID]; ok {
			if n.Effect() != w {
				t.Errorf("%s: effect %v, want %v", n.ID, n.Effect(), w)
			}
			delete(want, n.ID)
		}
	}
	if len(want) != 0 {
		t.Fatalf("not in the plan: %v", want)
	}
}
