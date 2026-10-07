package core

import (
	"encoding/json"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"kairo/ir"
)

// --- ADR 0043: output ports and kairo.slice --------------------------------

func portsRegistry() *ir.Registry {
	r := graphRegistry()
	r.Register(ir.NodeSpec{Action: "route", Effect: ir.EffectUnprotected, Ports: true})
	return r
}

func compilePorts(t testing.TB, js string) *ir.Plan {
	t.Helper()
	d, err := ir.ParseDefinition([]byte(js))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ir.Compile(d, portsRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// A step with ports: every live port's edges are taken (several at once),
// a null port's are skipped, and a reference to a port reads it (null
// for a dead one). A merge runs once with null for its dead input.
const routed = `{"name":"routed","root":{"kind":"graph","id":"g",
  "nodes":[
    {"kind":"step","id":"sw","action":"route"},
    {"kind":"step","id":"a","action":"llm","input":{"x":"sw.0"}},
    {"kind":"step","id":"b","action":"llm","input":{"x":"sw.1"}},
    {"kind":"step","id":"c","action":"llm","input":{"x":"sw.2"}},
    {"kind":"step","id":"m","action":"kairo.pass","input":{"in0":"a","in1":"b"}}],
  "edges":[
    {"from":"sw","port":0,"to":"a"},
    {"from":"sw","port":1,"to":"b"},
    {"from":"sw","port":2,"to":"c"},
    {"from":"a","to":"m"},
    {"from":"b","to":"m"}],
  "output":{"m":"m","c":"c","dead":"sw.1"}}}`

func TestPortsTakeEveryLivePort(t *testing.T) {
	p := compilePorts(t, routed)
	for seed := int64(0); seed < 10; seed++ {
		x := newSim(t, p)
		x.rng = rand.New(rand.NewSource(seed))
		x.handler = func(c Command, n *ir.Node) Event {
			if n.ID == "sw" {
				return ok(`[[1],null,[]]`) // an empty port is live
			}
			return ok(`"` + n.ID + `"`)
		}
		x.run(`{}`)
		if x.s.Status != StatusCompleted {
			t.Fatal(x.s.Error)
		}
		if got, want := string(x.s.Output), `{"c":"c","dead":null,"m":{"in0":"a","in1":null}}`; got != want {
			t.Fatalf("output %s, want %s", got, want)
		}
		ran := dispatched(x)
		slices.Sort(ran)
		if !slices.Equal(ran, []string{"a", "c", "sw"}) {
			t.Fatalf("ran %v", ran)
		}
		x.checkReplay()
	}
}

// A port beyond the output, and an output that is not a list, are dead.
func TestPortsOutOfRangeAreDead(t *testing.T) {
	p := compilePorts(t, routed)
	for _, out := range []string{`[[1]]`, `"text"`, `null`} {
		x := newSim(t, p)
		x.handler = func(c Command, n *ir.Node) Event {
			if n.ID == "sw" {
				return ok(out)
			}
			return ok(`"` + n.ID + `"`)
		}
		x.run(`{}`)
		if x.s.Status != StatusCompleted {
			t.Fatal(x.s.Error)
		}
		for _, id := range dispatched(x) {
			if id == "b" || id == "c" {
				t.Fatalf("%s: %s ran on a dead port", out, id)
			}
		}
		x.checkReplay()
	}
}

func TestPortsCompileErrors(t *testing.T) {
	for _, tc := range []struct{ edges, want string }{
		{`{"from":"sw","to":"a"}`, "needs a port"},
		{`{"from":"sw","handle":"x","port":0,"to":"a"}`, "needs a port and no handle"},
		{`{"from":"sw","port":101,"to":"a"}`, "out of range"},
		{`{"from":"a","port":0,"to":"b"}`, "has no ports"},
	} {
		js := `{"name":"x","root":{"kind":"graph","nodes":[
		  {"kind":"step","id":"sw","action":"route"},
		  {"kind":"step","id":"a","action":"llm"},
		  {"kind":"step","id":"b","action":"llm"}],
		  "edges":[` + tc.edges + `]}}`
		d, err := ir.ParseDefinition([]byte(js))
		if err == nil {
			_, err = ir.Compile(d, portsRegistry())
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: %v, want %q", tc.edges, err, tc.want)
		}
	}
	d, _ := ir.ParseDefinition([]byte(`{"name":"x","root":{"kind":"graph","nodes":[
	  {"kind":"step","id":"c","action":"classify","ports":true}]}}`))
	if _, err := ir.Compile(d, portsRegistry()); err == nil || !strings.Contains(err.Error(), "handle or by port") {
		t.Fatalf("branch and ports: %v", err)
	}
}

// n8n's SplitInBatches as a loop (ADR 0043): kairo.slice takes the next
// part of the items each round; the body's results are gathered; when
// nothing is left, the gathered results leave by the loop's done port. If
// the body sends nothing back, the loop ends there and its done port is
// dead.
const batches = `{"name":"batches","root":{"kind":"graph","id":"g","nodes":[
  {"kind":"loop","id":"l","max_iter":100,"ports":true,"loop_output":"vars",
   "vars":{"rest":{"type":"array[any]","ref":"$input.items"},
           "arrivals":{"type":"array[any]","value":[]},
           "go":{"type":"integer","value":1},
           "0":{"type":"array[any]","value":null}},
   "break":{"logical_operator":"and","conditions":[{"var":"go","operator":"=","value":"0"}]},
   "break_input":{"go":"l.go"},
   "body":{"kind":"graph","id":"body","nodes":[
     {"kind":"step","id":"s","action":"kairo.slice","input":{"items":"l.rest","done":"l.arrivals"},"params":{"size":2}},
     {"kind":"step","id":"next","action":"kairo.assign","input":{"rest":"s.2"},
      "params":{"items":[{"var":"l.rest","op":"over-write","input":"rest"},{"var":"l.go","op":"over-write","value":0}]}},
     {"kind":"step","id":"work","action":"route","input":{"part":"s.1"}},
     {"kind":"step","id":"keep","action":"kairo.assign","input":{"out":"work.0"},
      "params":{"items":[{"var":"l.arrivals","op":"extend","input":"out"},{"var":"l.go","op":"over-write","value":1}]}},
     {"kind":"step","id":"fin","action":"kairo.assign","input":{"done":"s.0"},
      "params":{"items":[{"var":"l.0","op":"over-write","input":"done"},{"var":"l.go","op":"over-write","value":0}]}}],
   "edges":[
     {"from":"s","port":1,"to":"next"},
     {"from":"s","port":0,"to":"fin"},
     {"from":"next","to":"work"},
     {"from":"work","port":0,"to":"keep"}]}},
  {"kind":"step","id":"after","action":"kairo.pass","input":{"x":"l.0"}}],
  "edges":[{"from":"l","port":0,"to":"after"}]}}`

func TestSliceLoop(t *testing.T) {
	p := compilePorts(t, batches)
	echo := func(c Command, n *ir.Node) Event {
		// work returns its part on port 0: the loop gathers the items back.
		v := extract(c.Input, []string{"part"})
		return ok("[" + string(v) + "]")
	}
	for _, tc := range []struct{ in, out string }{
		{`{"items":[1,2,3,4,5]}`, `{"after":{"x":[1,2,3,4,5]}}`},
		{`{"items":[1,2]}`, `{"after":{"x":[1,2]}}`},
		{`{"items":[]}`, `{"after":{"x":[]}}`},
	} {
		x := newSim(t, p)
		x.handler = echo
		x.run(tc.in)
		if x.s.Status != StatusCompleted || string(x.s.Output) != tc.out {
			t.Fatalf("%s: %v %s %s", tc.in, x.s.Status, x.s.Error, x.s.Output)
		}
		x.checkReplay()
	}
	// The body sends nothing back (work's port is null): the loop ends
	// after that round and what follows it is skipped.
	x := newSim(t, p)
	x.handler = func(c Command, n *ir.Node) Event { return ok(`[null]`) }
	x.run(`{"items":[1,2,3]}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{}` {
		t.Fatalf("nothing back: %v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
}

// An output the engine kept as a blob: the ports it recorded decide the
// edges (live unless recorded dead, dead beyond the list), a blob it did
// not record as a list of ports has none live, and a reference to a live
// port reads as a reference into the blob.
func TestPortsOfABlob(t *testing.T) {
	p := compilePorts(t, routed)
	for _, tc := range []struct{ out, ran, dead string }{
		{`{"$blob":"k","size":99999,"fields":{"$ports":2,"1":null}}`, "a sw", "null"},
		{`{"$blob":"k","size":99999,"fields":{"$ports":3}}`, "a b c sw", `{"$blob":"k","$path":"1"}`},
		{`{"$blob":"k","size":99999}`, "sw", "null"}, // not a list of ports
	} {
		x := newSim(t, p)
		x.handler = func(c Command, n *ir.Node) Event {
			if n.ID == "sw" {
				return ok(tc.out)
			}
			return ok(`"` + n.ID + `"`)
		}
		x.run(`{}`)
		if x.s.Status != StatusCompleted {
			t.Fatal(x.s.Error)
		}
		ran := dispatched(x)
		slices.Sort(ran)
		if got := strings.Join(ran, " "); got != tc.ran {
			t.Fatalf("%s: ran %s, want %s", tc.out, got, tc.ran)
		}
		if got := string(extract(x.s.Output, []string{"dead"})); got != tc.dead {
			t.Fatalf("%s: sw.1 reads %s, want %s", tc.out, got, tc.dead)
		}
		x.checkReplay()
	}
}

// The loop's variables gather items as they came: objects keep their keys
// in order through kairo.assign (n8n and Python keep insertion order).
func TestSliceLoopKeepsKeyOrder(t *testing.T) {
	p := compilePorts(t, batches)
	x := newSim(t, p)
	x.handler = func(c Command, n *ir.Node) Event {
		return ok("[" + string(extract(c.Input, []string{"part"})) + "]")
	}
	x.run(`{"items":[{"v":1,"a":{"z":1,"b":2}},{"v":2, "a":[]},{"v":3}]}`)
	want := `{"after":{"x":[{"v":1,"a":{"z":1,"b":2}},{"v":2,"a":[]},{"v":3}]}}`
	if x.s.Status != StatusCompleted || string(x.s.Output) != want {
		t.Fatalf("%v %s %s, want %s", x.s.Status, x.s.Error, x.s.Output, want)
	}
	x.checkReplay()
}

// Values moved by kairo.assign keep their keys in order but are written
// as pyEncode writes them: Python's number forms, no spaces; a number with
// no finite value is refused (the item then fails as before).
func TestSpliceAssignWritesAsPython(t *testing.T) {
	for _, tc := range []struct{ op, cur, in, want string }{
		{"append", `[1.50, 1e2]`, `2.50`, `[1.5,100.0,2.5]`},
		{"append", `[]`, `-0`, `[0]`},
		{"extend", `[{"z":1,"b":"é"}]`, `[{"y":1.0,"a":[1E2]}]`, `[{"z":1,"b":"é"},{"y":1.0,"a":[100.0]}]`},
		{"over-write", `[]`, `[{"b":1, "a":2}]`, `[{"b":1,"a":2}]`},
		{"remove-first", `[{"b":1},{"d":2,"c":3}]`, ``, `[{"d":2,"c":3}]`},
		{"append", `[1]`, `1e400`, ``},
		{"extend", `[1]`, `[1e400]`, ``},
	} {
		cur, _ := pyDecode(json.RawMessage(tc.cur))
		var in, v any
		if tc.in != "" {
			in, _ = pyDecode(json.RawMessage(tc.in))
		}
		v, _ = applyAssign(ir.AssignItem{Op: tc.op}, cur, in)
		var raw json.RawMessage
		if tc.in != "" {
			raw = json.RawMessage(tc.in)
		}
		got, ok := spliceAssign(tc.op, json.RawMessage(tc.cur), raw, v)
		if tc.want == "" {
			if ok {
				t.Fatalf("%s %s %s: spliced %s, want refused", tc.op, tc.cur, tc.in, got)
			}
			continue
		}
		if !ok || string(got) != tc.want {
			t.Fatalf("%s %s %s: %s %v, want %s", tc.op, tc.cur, tc.in, got, ok, tc.want)
		}
	}
}
