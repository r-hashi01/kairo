package core

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/ir"
)

// --- ADR 0029: graphs ------------------------------------------------------

func graphRegistry() *ir.Registry {
	r := registry()
	r.Register(ir.NodeSpec{Action: "classify", Effect: ir.EffectUnprotected, Branch: "label", Outputs: map[string]ir.FieldType{
		"label": {Type: ir.FieldEnum, Values: []string{"refund", "other", "spam"}},
		"done":  {Type: ir.FieldBool},
	}})
	return r
}

func compileGraph(t testing.TB, js string) *ir.Plan {
	t.Helper()
	d, err := ir.ParseDefinition([]byte(js))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ir.Compile(d, graphRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// label answers classify with the given label and every other step with
// its own id.
func label(l string) func(Command, *ir.Node) Event {
	return func(c Command, n *ir.Node) Event {
		if n.Spec.Action == "classify" {
			return ok(`{"label":"` + l + `","done":true}`)
		}
		return ok(`"` + n.ID + `"`)
	}
}

func dispatched(x *sim) []string {
	var ids []string
	for _, ev := range x.log {
		if ev.Kind == EvStepOK {
			ids = append(ids, StepIDOf(x, ev.Act))
		}
	}
	return ids
}

// StepIDOf finds the node id an event's activation was dispatched for.
func StepIDOf(x *sim, act uint32) string {
	// Replay until the activation exists, then read it.
	r := NewState(x.s.RunID)
	for i := range x.log {
		if a := r.Acts[act]; a != nil {
			return x.p.Nodes[a.Node].ID
		}
		Apply(x.p, r, &x.log[i], nil)
	}
	return "?"
}

// A branch, its two arms and a merge: the arm not taken is skipped, the
// merge runs once the taken arm is done (OR join), and the skipped arm's
// output reads null.
const diamond = `{"name":"diamond","root":{"kind":"graph","id":"g",
  "nodes":[
    {"kind":"step","id":"start","action":"llm"},
    {"kind":"step","id":"cls","action":"classify"},
    {"kind":"step","id":"r","action":"llm"},
    {"kind":"step","id":"o","action":"llm"},
    {"kind":"step","id":"o2","action":"llm"},
    {"kind":"step","id":"merge","action":"kairo.pass","input":{"r":"r","o":"o2"}}],
  "edges":[
    {"from":"start","to":"cls"},
    {"from":"cls","handle":"refund","to":"r"},
    {"from":"cls","handle":"other","to":"o"},
    {"from":"o","to":"o2"},
    {"from":"r","to":"merge"},
    {"from":"o2","to":"merge"}]}}`

func TestGraphBranchAndMerge(t *testing.T) {
	p := compileGraph(t, diamond)
	for _, tc := range []struct {
		label, out string
		ran        []string
	}{
		{"refund", `{"merge":{"o":null,"r":"r"}}`, []string{"start", "cls", "r"}},
		{"other", `{"merge":{"o":"o2","r":null}}`, []string{"start", "cls", "o", "o2"}},
		// No edge for spam: everything after the branch is skipped.
		{"spam", `{}`, []string{"start", "cls"}},
	} {
		x := newSim(t, p)
		x.handler = label(tc.label)
		x.run(`{}`)
		if x.s.Status != StatusCompleted || string(x.s.Output) != tc.out {
			t.Fatalf("%s: %v %s %s", tc.label, x.s.Status, x.s.Error, x.s.Output)
		}
		if got := dispatched(x); !slices.Equal(got, tc.ran) {
			t.Fatalf("%s: ran %v, want %v", tc.label, got, tc.ran)
		}
		x.checkReplay()
	}
}

// Fan-out and joins finish with the same state whatever order the
// results arrive in.
func TestGraphAnyOrder(t *testing.T) {
	p := compileGraph(t, `{"name":"fan","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"a","action":"llm"},
	    {"kind":"step","id":"b","action":"llm"},
	    {"kind":"step","id":"c","action":"llm"},
	    {"kind":"step","id":"d","action":"llm"},
	    {"kind":"step","id":"e","action":"kairo.pass","input":{"b":"b","c":"c","d":"d"}},
	    {"kind":"step","id":"f","action":"llm"}],
	  "edges":[
	    {"from":"a","to":"b"},{"from":"a","to":"c"},{"from":"a","to":"d"},
	    {"from":"b","to":"e"},{"from":"c","to":"e"},{"from":"d","to":"e"},
	    {"from":"c","to":"f"}],
	  "output":{"e":"e","f":"f"}}}`)
	var want []byte
	for seed := int64(0); seed < 20; seed++ {
		x := newSim(t, p)
		x.rng = rand.New(rand.NewSource(seed))
		x.handler = label("")
		x.run(`{}`)
		if x.s.Status != StatusCompleted {
			t.Fatal(x.s.Error)
		}
		if want == nil {
			want = x.s.Output
		}
		if !bytes.Equal(want, x.s.Output) {
			t.Fatalf("order-dependent output %s vs %s", want, x.s.Output)
		}
		x.checkReplay()
	}
	if string(want) != `{"e":{"b":"b","c":"c","d":"d"},"f":"f"}` {
		t.Fatalf("output %s", want)
	}
}

// A run can start at one entry of its root graph; the other entries'
// paths are skipped.
func TestGraphEntries(t *testing.T) {
	p := compileGraph(t, `{"name":"entries","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"web","action":"llm"},
	    {"kind":"step","id":"cron","action":"llm"},
	    {"kind":"step","id":"work","action":"kairo.pass","input":{"web":"web","cron":"cron"}}],
	  "edges":[{"from":"web","to":"work"},{"from":"cron","to":"work"}]}}`)
	for _, tc := range []struct{ entry, out string }{
		{"", `{"work":{"cron":"cron","web":"web"}}`},
		{"cron", `{"work":{"cron":"cron","web":null}}`},
	} {
		x := newSim(t, p)
		x.handler = label("")
		x.apply(Event{Kind: EvStart, Name: tc.entry, Data: json.RawMessage(`{}`)})
		x.run2()
		if x.s.Status != StatusCompleted || string(x.s.Output) != tc.out {
			t.Fatalf("entry %q: %v %s %s", tc.entry, x.s.Status, x.s.Error, x.s.Output)
		}
		x.checkReplay()
	}
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart, Name: "nope"})
	if x.s.Status != StatusFailed || !strings.Contains(x.s.Error, "unknown entry") {
		t.Fatalf("%v %s", x.s.Status, x.s.Error)
	}
}

// A branch value outside the declared handles fails the run.
func TestGraphBadBranchValue(t *testing.T) {
	x := newSim(t, compileGraph(t, diamond))
	x.handler = label("bogus")
	x.run(`{}`)
	if x.s.Status != StatusFailed || !strings.Contains(x.s.Error, "branch field") {
		t.Fatalf("%v %s", x.s.Status, x.s.Error)
	}
	x.checkReplay()
}

// In a loop, a node skipped in this iteration reads null, not the value it
// had in an earlier one.
func TestGraphSkippedValuesAreCleared(t *testing.T) {
	p := compileGraph(t, `{"name":"loop","root":{"kind":"loop","id":"l","max_iter":2,
	  "while":{"field":"cls.done","op":"eq","value":false},
	  "body":{"kind":"graph","id":"g",
	    "nodes":[
	      {"kind":"step","id":"cls","action":"classify"},
	      {"kind":"step","id":"r","action":"llm"},
	      {"kind":"step","id":"o","action":"llm"},
	      {"kind":"step","id":"z","action":"kairo.pass","input":{"r":"r","o":"o"}}],
	    "edges":[
	      {"from":"cls","handle":"refund","to":"r"},{"from":"cls","handle":"other","to":"o"},
	      {"from":"r","to":"z"},{"from":"o","to":"z"}]}}}`)
	x := newSim(t, p)
	x.handler = func(c Command, n *ir.Node) Event {
		if n.ID == "cls" {
			if strings.HasSuffix(c.StepID, "[0]") {
				return ok(`{"label":"refund","done":false}`)
			}
			return ok(`{"label":"other","done":true}`)
		}
		return ok(`"` + c.StepID + `"`)
	}
	x.run(`{}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"z":{"o":"o[1]","r":null}}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
}

func TestGraphCompileErrors(t *testing.T) {
	step := func(id string) string { return `{"kind":"step","id":"` + id + `","action":"llm"}` }
	graph := func(nodes []string, edges string, extra string) string {
		return `{"name":"x","root":{"kind":"graph","nodes":[` + strings.Join(nodes, ",") + `],"edges":[` + edges + `]` + extra + `}}`
	}
	for _, tc := range []struct{ js, want string }{
		{graph([]string{step("a"), step("b")}, `{"from":"a","to":"b"},{"from":"b","to":"a"}`, ""), "no entry"},
		{graph([]string{step("s"), step("a"), step("b")}, `{"from":"s","to":"a"},{"from":"a","to":"b"},{"from":"b","to":"a"}`, ""), "cycle"},
		{graph([]string{step("a")}, `{"from":"a","to":"zz"}`, ""), "both ends"},
		{graph([]string{step("a"), step("b")}, `{"from":"a","handle":"yes","to":"b"}`, ""), `no handle "yes"`},
		{graph([]string{step("a"), step("b")}, `{"from":"a","to":"b"}`, `,"entry":["b"]`), "incoming edges"},
		{graph([]string{step("a"), step("b")}, `{"from":"a","to":"b"}`, `,"entry":["a","a"]`), "duplicate entry"},
		{graph([]string{step("a"), step("b"), step("c")}, `{"from":"a","to":"b"},{"from":"a","to":"c"}`, `,"entry":["a"]`), ""},
		// c reads b, but b is not upstream of c.
		{graph([]string{step("a"), step("b"), `{"kind":"step","id":"c","action":"llm","input":{"x":"b"}}`},
			`{"from":"a","to":"b"},{"from":"a","to":"c"}`, ""), "must be upstream"},
		// Reading an upstream node, also inside a nested construct, is fine.
		{graph([]string{step("a"), `{"kind":"seq","id":"s","nodes":[{"kind":"step","id":"c","action":"llm","input":{"x":"a"}}]}`},
			`{"from":"a","to":"s"}`, ""), ""},
		{graph([]string{step("a"), step("b")}, `{"from":"a","to":"b"},{"from":"a","to":"b"}`, ""), "duplicate edge"},
	} {
		d, err := ir.ParseDefinition([]byte(tc.js))
		if err != nil {
			t.Fatal(err)
		}
		_, err = ir.Compile(d, graphRegistry())
		if tc.want == "" {
			if err != nil {
				t.Errorf("%s: %v", tc.js, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.js, err, tc.want)
		}
	}
	reg := graphRegistry()
	reg.Register(ir.NodeSpec{Action: "bad", Effect: ir.EffectUnprotected, Branch: "text", Outputs: map[string]ir.FieldType{"text": {Type: ir.FieldText}}})
	d, _ := ir.ParseDefinition([]byte(`{"name":"x","root":{"kind":"step","id":"a","action":"bad"}}`))
	if _, err := ir.Compile(d, reg); err == nil || !strings.Contains(err.Error(), "not a declared enum") {
		t.Errorf("branch on a text field: %v", err)
	}
}

// --- version 1 snapshots ----------------------------------------------------

// A run snapshotted by the tree core (seq, par and cond progress in Pos,
// Results and the running child) continues correctly after decoding.
func TestVersion1SnapshotMigrates(t *testing.T) {
	p := compile(t, `{"name":"legacy","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"par","id":"p","nodes":[{"kind":"step","id":"b","action":"llm"},{"kind":"step","id":"c","action":"llm"}]},
	  {"kind":"cond","id":"k","if":{"field":"a.done","op":"eq","value":true},
	   "then":{"kind":"step","id":"d","action":"llm"},"else":{"kind":"step","id":"e","action":"llm"}},
	  {"kind":"step","id":"f","action":"kairo.pass","input":{"p":"p","k":"k"}}]}}`)
	answer := func(n *ir.Node) Event {
		if n.ID == "a" {
			return ok(`{"done":true}`)
		}
		return ok(`"` + n.ID + `"`)
	}
	// The reference: straight through.
	ref := newSim(t, p)
	ref.handler = func(c Command, n *ir.Node) Event { return answer(n) }
	ref.run(`{}`)

	for _, stopAt := range []string{"c", "d"} { // inside par, inside cond
		x := newSim(t, p)
		x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{}`)})
		// Answer until the step stopAt is the only one outstanding.
		for len(x.pending) > 0 {
			i := slices.IndexFunc(x.pending, func(c Command) bool { return p.Nodes[c.Node].ID != stopAt })
			if i < 0 {
				break
			}
			c := x.pending[i]
			x.pending = slices.Delete(x.pending, i, i+1)
			ev := answer(&p.Nodes[c.Node])
			ev.Act, ev.Attempt = c.Act, c.Attempt
			x.apply(ev)
		}
		legacy := toVersion1(t, p, x.s)
		s, err := DecodeState(legacy)
		if err != nil {
			t.Fatal(err)
		}
		x.s = s
		x.handler = func(c Command, n *ir.Node) Event { return answer(n) }
		x.run2()
		if x.s.Status != StatusCompleted || !bytes.Equal(x.s.Output, ref.s.Output) {
			t.Fatalf("stopped at %s: %v %s %s, want %s", stopAt, x.s.Status, x.s.Error, x.s.Output, ref.s.Output)
		}
	}
}

// toVersion1 encodes s as the tree core did: graph progress moves back to
// Pos (seq) and Results (par); a cond kept nothing but its running child.
func toVersion1(t *testing.T, p *ir.Plan, s *State) []byte {
	t.Helper()
	e := enc{}
	e.u(1)
	e.str(s.RunID)
	e.u(uint64(s.Status))
	e.raw(s.Input)
	e.raw(s.Output)
	e.str(s.Error)
	e.u(uint64(s.NextAct))
	e.u(uint64(s.NextScope))
	e.u(uint64(s.NextTimer))
	e.i(int64(s.Inflight))
	ids := sortedActs(s)
	e.u(uint64(len(ids)))
	for _, id := range ids {
		a := *s.Acts[id]
		if n := &p.Nodes[a.Node]; n.Kind == ir.KGraph {
			switch n.Sugar {
			case ir.SugarSeq:
				a.Pos = int32(slices.Index(a.G.Members, mRunning))
				a.Pending = 0 // only par counted its children
			case ir.SugarCond:
				a.Pending = 0
			case ir.SugarPar:
				a.Results = make([]json.RawMessage, len(n.Children))
				for mi, c := range n.Children {
					if a.G.Members[mi] == mDone {
						a.Results[mi] = s.Scopes[a.Scope].Vals[c]
					}
				}
			}
		}
		e.u(uint64(id))
		e.i(int64(a.Node))
		e.u(uint64(a.Parent))
		e.u(uint64(a.Scope))
		e.i(int64(a.Idx))
		e.i(int64(a.Pos))
		e.i(int64(a.Pending))
		e.i(int64(a.Attempt))
		e.u(uint64(a.Flags))
		e.u(uint64(a.Timer))
		e.i(a.TimerAt)
		e.u(uint64(len(a.Results)))
		for _, r := range a.Results {
			e.raw(r)
		}
		e.u(uint64(len(a.Items)))
		for _, r := range a.Items {
			e.raw(r)
		}
	}
	// Scopes and mailbox are unchanged between the versions.
	v2 := s.Encode(nil)
	d := &dec{b: v2}
	d.u()
	d.str()
	d.u()
	d.bytes()
	d.bytes()
	d.str()
	d.u()
	d.u()
	d.u()
	d.i()
	// Version 3: limits and counters.
	d.i()
	d.i()
	d.i()
	d.i()
	d.u()
	d.i()
	for n := d.count(); n > 0; n-- {
		d.u()
		d.i()
		d.u()
		d.u()
		d.i()
		d.i()
		d.i()
		d.i()
		d.u()
		d.u()
		d.i()
		for k := d.count(); k > 0; k-- {
			d.bytes()
		}
		for k := d.count(); k > 0; k-- {
			d.bytes()
		}
		if d.u() == 1 {
			d.bytes()
			d.bytes()
		}
	}
	if d.err != nil {
		t.Fatal(d.err)
	}
	return append(e.b, d.b...)
}

// Settling scans a graph's members once per completion: this measures the
// cost per transition as a graph gets wide (fan) or long (chain).
func BenchmarkGraphWide(b *testing.B) {
	for _, shape := range []string{"fan", "chain"} {
		for _, n := range []int{10, 100, 500} {
			benchGraph(b, shape, n)
		}
	}
}

func benchGraph(b *testing.B, shape string, n int) {
	{
		b.Run(shape+"/"+strconv.Itoa(n), func(b *testing.B) {
			var nodes, edges []string
			nodes = append(nodes, `{"kind":"step","id":"s","action":"llm"}`)
			for i := 0; i < n; i++ {
				nodes = append(nodes, `{"kind":"step","id":"n`+strconv.Itoa(i)+`","action":"llm"}`)
				from := "s"
				if shape == "chain" && i > 0 {
					from = "n" + strconv.Itoa(i-1)
				}
				edges = append(edges, `{"from":"`+from+`","to":"n`+strconv.Itoa(i)+`"}`)
			}
			p := compileGraph(b, `{"name":"wide","root":{"kind":"graph","nodes":[`+strings.Join(nodes, ",")+`],"edges":[`+strings.Join(edges, ",")+`]}}`)
			b.ResetTimer()
			transitions := 0
			for i := 0; i < b.N; i++ {
				s := NewState("r")
				out, _ := Apply(p, s, &Event{Kind: EvStart, Data: json.RawMessage(`{}`)}, nil)
				transitions++
				for len(out) > 0 && !s.Status.Done() {
					var next []Command
					for _, c := range out {
						if c.Kind == CmdDispatch {
							next, _ = Apply(p, s, &Event{Kind: EvStepOK, Act: c.Act, Attempt: c.Attempt, Data: json.RawMessage(`1`)}, next)
							transitions++
						}
					}
					out = next
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(transitions), "ns/transition")
		})
	}
}

// A branching action inside seq, par or cond does not branch: every member
// runs, as in the tree core.
func TestBranchingActionInSugarRunsEverything(t *testing.T) {
	p := compileGraph(t, `{"name":"s","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"c","action":"classify"},
	  {"kind":"step","id":"x","action":"llm"}]}}`)
	x := newSim(t, p)
	x.handler = label("bogus") // not even a declared handle
	x.run(`{}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `"x"` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
	// Entries are only for hand-written graphs.
	y := newSim(t, p)
	y.apply(Event{Kind: EvStart, Name: "c"})
	if y.s.Status != StatusFailed {
		t.Fatalf("entry on a seq: %v", y.s.Status)
	}
}
