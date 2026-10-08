package core

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/ir"
)

// --- ADR 0032: map element errors, element output, flatten -----------------

// mapPlan maps over $input.xs with a body graph: a step that fails for the
// item "bad", and a sibling step that runs alongside it.
func mapPlan(mode string, extra string) string {
	return `{"name":"m","root":{"kind":"map","id":"m","over":"$input.xs","on_element_error":"` + mode + `"` + extra + `,
	  "body":{"kind":"par","id":"b","nodes":[
	    {"kind":"step","id":"work","action":"llm","input":{"x":"$item"},"retry":{"max_attempts":1}},
	    {"kind":"step","id":"side","action":"llm","input":{"x":"$item"}}]}}}`
}

func mapHandler(c Command, n *ir.Node) Event {
	var in struct{ X json.RawMessage }
	json.Unmarshal(c.Input, &in)
	if n.ID == "work" && string(in.X) == `"bad"` {
		return Event{Kind: EvStepErr, Err: "boom"}
	}
	return ok(`{"v":` + string(in.X) + `}`)
}

func TestMapElementErrorModes(t *testing.T) {
	for _, tc := range []struct{ mode, out, status string }{
		{"null", `[{"work":{"v":"a"},"side":{"v":"a"}},null,{"work":{"v":"c"},"side":{"v":"c"}}]`, "completed"},
		{"omit", `[{"work":{"v":"a"},"side":{"v":"a"}},{"work":{"v":"c"},"side":{"v":"c"}}]`, "completed"},
		{"fail", ``, "failed"},
	} {
		var want []byte
		for seed := int64(0); seed < 10; seed++ {
			x := newSim(t, compile(t, mapPlan(tc.mode, "")))
			x.rng = rand.New(rand.NewSource(seed))
			x.handler = mapHandler
			x.run(`{"xs":["a","bad","c"]}`)
			if x.s.Status.String() != tc.status {
				t.Fatalf("%s: %v %s", tc.mode, x.s.Status, x.s.Error)
			}
			if tc.status == "completed" && string(x.s.Output) != tc.out {
				t.Fatalf("%s: output %s", tc.mode, x.s.Output)
			}
			if x.s.Inflight != 0 || len(x.pending) != 0 {
				t.Fatalf("%s: %d tasks left in flight", tc.mode, x.s.Inflight)
			}
			x.checkReplay()
			enc := x.s.Encode(nil)
			if want == nil {
				want = enc
			}
			if tc.status == "completed" && !bytes.Equal(want, enc) {
				t.Fatalf("%s: state depends on the order results arrive in", tc.mode)
			}
		}
	}
}

// The failing element's running sibling is aborted.
func TestMapElementAbortsItsTasks(t *testing.T) {
	x := newSim(t, compile(t, mapPlan("null", "")))
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{"xs":["bad"]}`)})
	var work Command
	for _, c := range x.pending {
		if x.p.Nodes[c.Node].ID == "work" {
			work = c
		}
	}
	ev := Event{Kind: EvStepErr, Act: work.Act, Attempt: work.Attempt, Err: "boom", At: x.now}
	out, err := Apply(x.p, x.s, &ev, nil)
	if err != nil {
		t.Fatal(err)
	}
	aborted := 0
	for _, c := range out {
		if c.Kind == CmdAbort {
			aborted++
		}
	}
	if aborted != 1 || x.s.Status != StatusCompleted || string(x.s.Output) != `[null]` || len(x.s.Acts) != 0 {
		t.Fatalf("aborted=%d %v %s acts=%d", aborted, x.s.Status, x.s.Output, len(x.s.Acts))
	}
}

func TestMapElementOutputAndFlatten(t *testing.T) {
	p := compile(t, `{"name":"m","root":{"kind":"map","id":"m","over":"$input.xs","element_output":"two","flatten":true,
	  "body":{"kind":"seq","nodes":[
	    {"kind":"step","id":"one","action":"llm"},
	    {"kind":"step","id":"two","action":"kairo.pass","input":{"l":"$item"}}]}}}`)
	x := newSim(t, p)
	x.handler = func(Command, *ir.Node) Event { return ok(`1`) }
	// two outputs {"l":[...]}: not lists, so no flattening.
	x.run(`{"xs":[[1],[2,3]]}`)
	if string(x.s.Output) != `[{"l":[1]},{"l":[2,3]}]` {
		t.Fatalf("%s", x.s.Output)
	}
	p = compile(t, `{"name":"m","root":{"kind":"map","id":"m","over":"$input.xs","flatten":true,
	  "body":{"kind":"step","id":"two","action":"kairo.append","input":{"l":"$item"}}}}`)
	x = newSim(t, p)
	x.run(`{"xs":[[1],[2,3],[]]}`)
	if string(x.s.Output) != `[1,2,3]` {
		t.Fatalf("%s", x.s.Output)
	}
	x.checkReplay()
}

// --- ADR 0032/0033: Dify loops ----------------------------------------------

// A Dify loop: a counter variable incremented by kairo.assign each round,
// a break condition on it, checked before the first round too.
const counterLoop = `{"name":"l","root":{"kind":"seq","nodes":[
  {"kind":"loop","id":"l","max_iter":10,"check":"before","loop_output":"vars",
   "vars":{"n":{"type":"integer","value":0},"log":{"type":"array[string]","value":[]}},
   "break":{"logical_operator":"and","conditions":[{"var":"n","operator":"≥","value":"{{#in.limit#}}"}]},
   "break_input":{"n":"l.n","in.limit":"$input.limit"},
   "body":{"kind":"seq","nodes":[
     {"kind":"step","id":"inc","action":"kairo.assign","input":{"tag":"$input.tag"},
      "params":{"items":[{"var":"l.n","op":"+=","value":1},{"var":"l.log","op":"append","input":"tag"}]}}]}},
  {"kind":"step","id":"after","action":"kairo.pass","input":{"n":"l.n","round":"l.loop_round"}}]}}`

func TestDifyLoop(t *testing.T) {
	p := compile(t, counterLoop)
	for _, tc := range []struct{ in, out string }{
		{`{"limit":"3","tag":"x"}`, `{"n":3,"round":3}`},
		{`{"limit":"0","tag":"x"}`, `{"n":0,"round":null}`}, // broken before the first round
	} {
		x := newSim(t, p)
		x.run(tc.in)
		if x.s.Status != StatusCompleted || string(x.s.Output) != tc.out {
			t.Fatalf("%s: %v %s %s", tc.in, x.s.Status, x.s.Error, x.s.Output)
		}
		x.checkReplay()
	}
	x := newSim(t, p)
	x.run(`{"limit":"2","tag":"y"}`)
	// The loop's own value: its variables and loop_round.
	if v := x.s.Scopes[0].Vals[p.ByID["l"]]; string(v) != `{"log":["y","y"],"loop_round":2,"n":2}` {
		t.Fatalf("loop value %s", v)
	}
}

// break_on ends the loop after a round in which the node ran (Dify's
// loop-end).
func TestLoopBreakOn(t *testing.T) {
	p := compileGraph(t, `{"name":"l","root":{"kind":"loop","id":"l","max_iter":10,"loop_output":"vars","break_on":["end"],
	  "vars":{"i":{"type":"number","value":0}},
	  "body":{"kind":"graph","id":"g","nodes":[
	    {"kind":"step","id":"inc","action":"kairo.assign","params":{"items":[{"var":"l.i","op":"+=","value":1}]}},
	    {"kind":"step","id":"sw","action":"kairo.switch","input":{"i":"l.i"},
	     "params":{"cases":[{"id":"done","logical_operator":"and","conditions":[{"var":"i","operator":"≥","value":"4"}]}]}},
	    {"kind":"step","id":"end","action":"kairo.pass"}],
	  "edges":[{"from":"inc","to":"sw"},{"from":"sw","handle":"done","to":"end"}]}}}`)
	x := newSim(t, p)
	x.run(`{}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"i":4,"loop_round":4}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
}

// --- ADR 0033: run variables and kairo.assign -------------------------------

func assignPlan(typ, init string, items string) string {
	return `{"name":"a","vars":{"v":{"type":"` + typ + `","value":` + init + `}},"root":{"kind":"step","id":"a","action":"kairo.assign",
	  "input":{"x":"$input.x"},"params":{"items":[` + items + `]}}}`
}

func TestAssignOperations(t *testing.T) {
	for _, tc := range []struct{ typ, init, items, input, want, err string }{
		{"string", `"a"`, `{"var":"$var.v","op":"over-write","input":"x"}`, `"b"`, `"b"`, ""},
		{"string", `"a"`, `{"var":"$var.v","op":"clear"}`, `null`, `""`, ""},
		{"integer", `1`, `{"var":"$var.v","op":"+=","value":2}`, `null`, `3`, ""},
		{"integer", `1`, `{"var":"$var.v","op":"+=","value":0.5}`, `null`, `1.5`, ""},
		{"integer", `3`, `{"var":"$var.v","op":"/=","value":2}`, `null`, `1.5`, ""},
		{"integer", `4`, `{"var":"$var.v","op":"/=","value":2}`, `null`, `2.0`, ""}, // Python's true division
		{"integer", `4`, `{"var":"$var.v","op":"/=","value":0}`, `null`, ``, "InvalidInputValueError"},
		{"integer", `4`, `{"var":"$var.v","op":"*=","input":"x"}`, `2`, ``, "InputTypeNotSupportedError"},
		{"float", `1.0`, `{"var":"$var.v","op":"-=","value":3}`, `null`, `-2.0`, ""},
		{"array[string]", `["a"]`, `{"var":"$var.v","op":"append","input":"x"}`, `"b"`, `["a","b"]`, ""},
		{"array[string]", `["a"]`, `{"var":"$var.v","op":"append","input":"x"}`, `1`, ``, "InvalidInputValueError"},
		{"array[string]", `["a"]`, `{"var":"$var.v","op":"extend","input":"x"}`, `["b","c"]`, `["a","b","c"]`, ""},
		{"array[number]", `[1,2,3]`, `{"var":"$var.v","op":"remove-first"},{"var":"$var.v","op":"remove-last"}`, `null`, `[2]`, ""},
		{"array[string]", `[]`, `{"var":"$var.v","op":"remove-first"}`, `null`, `[]`, ""},
		{"object", `{}`, `{"var":"$var.v","op":"set","value":"{\"k\":1}"}`, `null`, `{"k":1}`, ""},
		{"string", `"a"`, `{"var":"$var.v","op":"+=","value":1}`, `null`, ``, "OperationNotSupportedError"},
		// A null variable input leaves the variable as it was.
		{"string", `"a"`, `{"var":"$var.v","op":"over-write","input":"x"}`, `null`, `"a"`, ""},
		// Items apply in order, reading what earlier ones wrote; a failure
		// writes nothing.
		{"integer", `1`, `{"var":"$var.v","op":"+=","value":1},{"var":"$var.v","op":"*=","value":10}`, `null`, `20`, ""},
		{"integer", `1`, `{"var":"$var.v","op":"+=","value":1},{"var":"$var.v","op":"/=","value":0}`, `null`, ``, "InvalidInputValueError"},
	} {
		p := compile(t, assignPlan(tc.typ, tc.init, tc.items))
		x := newSim(t, p)
		x.run(`{"x":` + tc.input + `}`)
		if tc.err != "" {
			if x.s.Status != StatusFailed || !strings.Contains(x.s.Error, "invalid") && !strings.Contains(x.s.Error, "not supported") {
				t.Errorf("%s: %v %s (want %s)", tc.items, x.s.Status, x.s.Error, tc.err)
			}
			if got := RunVarsValue(x.s); got != tc.init {
				t.Errorf("%s: failed assign wrote %s", tc.items, got)
			}
			continue
		}
		if x.s.Status != StatusCompleted || RunVarsValue(x.s) != tc.want {
			t.Errorf("%s on %s: %v %s v=%s, want %s", tc.items, tc.input, x.s.Status, x.s.Error, RunVarsValue(x.s), tc.want)
		}
		x.checkReplay()
	}
}

// RunVarsValue is the run variable v.
func RunVarsValue(s *State) string {
	var m map[string]json.RawMessage
	json.Unmarshal(RunVars(s), &m)
	return string(m["v"])
}

// Initial values come from the start event, else the declaration, else
// the type's zero value; $var reads them.
func TestRunVarsInit(t *testing.T) {
	p := compile(t, `{"name":"v","vars":{"a":{"type":"string","value":"dflt"},"b":{"type":"integer"},"c":{"type":"string"}},
	  "root":{"kind":"step","id":"s","action":"kairo.pass","input":{"a":"$var.a","b":"$var.b","c":"$var.c"}}}`)
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{}`), Vars: json.RawMessage(`{"c":"given"}`)})
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"a":"dflt","b":0,"c":"given"}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
}

func TestVarsCompileErrors(t *testing.T) {
	for _, tc := range []struct{ js, want string }{
		{`{"name":"x","root":{"kind":"step","id":"s","action":"kairo.pass","input":{"a":"$var.nope"}}}`, "undeclared run variable"},
		{`{"name":"x","vars":{"v":{"type":"strng"}},"root":{"kind":"step","id":"s","action":"kairo.pass"}}`, "unknown type"},
		{`{"name":"x","root":{"kind":"step","id":"s","action":"kairo.assign","params":{"items":[{"var":"elsewhere.v","op":"clear"}]}}}`, "enclosing loop"},
		{`{"name":"x","root":{"kind":"loop","id":"l","max_iter":1,"vars":{"v":{"type":"string"}},"body":{"kind":"step","id":"s","action":"kairo.pass"}}}`, "loop_output vars"},
		{`{"name":"x","root":{"kind":"loop","id":"l","max_iter":1,"break_on":["zz"],"body":{"kind":"step","id":"s","action":"kairo.pass"}}}`, "break_on"},
		{`{"name":"x","root":{"kind":"map","over":"$input.xs","on_element_error":"skip","body":{"kind":"step","id":"s","action":"kairo.pass"}}}`, "on_element_error"},
	} {
		d, err := ir.ParseDefinition([]byte(tc.js))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ir.Compile(d, graphRegistry()); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.js, err, tc.want)
		}
	}
}

func TestPyFloatRepr(t *testing.T) {
	for f, want := range map[float64]string{2: "2.0", 1.5: "1.5", 1e16: "1e+16", 123456789.5: "123456789.5", 0.0001: "0.0001", 0.00001: "1e-05", -2: "-2.0"} {
		if got := pyFloatRepr(f); got != want {
			t.Errorf("%v: %s, want %s", f, got, want)
		}
	}
}

// A failing assignment reports Dify's exception class as error_type.
func TestAssignErrorType(t *testing.T) {
	p := compile(t, `{"name":"a","vars":{"v":{"type":"integer","value":1}},"root":{"kind":"step","id":"a","action":"kairo.assign",
	  "on_error":{"strategy":"default-value"},"params":{"items":[{"var":"$var.v","op":"/=","value":0}]}}}`)
	x := newSim(t, p)
	x.run(`{}`)
	if x.s.Status != StatusCompleted || !strings.Contains(string(x.s.Output), `"error_type":"InvalidInputValueError"`) || RunVarsValue(x.s) != "1" {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
}

// --- review findings (ADR 0030–0033) ----------------------------------------

// Aborting an element removes activations at any depth; a late result for
// one of them is ignored.
func TestAbortElementRemovesDeepActivations(t *testing.T) {
	p := compile(t, `{"name":"m","root":{"kind":"seq","nodes":[
	  {"kind":"map","id":"m","over":"$input.xs","on_element_error":"null",
	   "body":{"kind":"par","nodes":[{"kind":"par","nodes":[{"kind":"step","id":"slow","action":"llm"}]},
	                                  {"kind":"step","id":"work","action":"llm","retry":{"max_attempts":1}}]}},
	  {"kind":"wait","id":"w","signal":"go"}]}}`)
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{"xs":[1]}`)})
	var slow, work Command
	for _, c := range x.pending {
		switch x.p.Nodes[c.Node].ID {
		case "slow":
			slow = c
		case "work":
			work = c
		}
	}
	x.pending = nil
	x.apply(Event{Kind: EvStepErr, Act: work.Act, Attempt: work.Attempt, Err: "boom"})
	if x.s.Inflight != 0 || x.s.Acts[slow.Act] != nil {
		t.Fatalf("inflight=%d, slow left behind: %+v", x.s.Inflight, x.s.Acts[slow.Act])
	}
	ev := Event{Kind: EvStepOK, Act: slow.Act, Attempt: slow.Attempt, Data: json.RawMessage(`1`)}
	if _, err := Apply(x.p, x.s, &ev, nil); err != ErrIgnored {
		t.Fatalf("late result: %v", err)
	}
	x.checkReplay()
}

// A step needing review is never contained by an element failure.
func TestElementFailureWithPendingReviewFailsRun(t *testing.T) {
	p := compile(t, `{"name":"m","root":{"kind":"map","id":"m","over":"$input.xs","on_element_error":"null",
	  "body":{"kind":"par","nodes":[{"kind":"step","id":"send","action":"send"},{"kind":"step","id":"work","action":"llm","retry":{"max_attempts":1}}]}}}`)
	x := newSim(t, p)
	x.handler = func(c Command, n *ir.Node) Event {
		if n.ID == "send" {
			return Event{Kind: EvStepErr, Err: "timeout", Unknown: true}
		}
		return Event{Kind: EvStepErr, Err: "boom"}
	}
	x.run(`{"xs":[1]}`)
	if x.s.Status != StatusFailed || !strings.Contains(x.s.Error, "needed review") {
		t.Fatalf("%v %s", x.s.Status, x.s.Error)
	}
	x.checkReplay()
}

// A bad branch value inside a containing map ends only its element.
func TestBadBranchValueIsContained(t *testing.T) {
	p := compileGraph(t, `{"name":"m","root":{"kind":"map","id":"m","over":"$input.xs","on_element_error":"null",
	  "body":{"kind":"graph","nodes":[{"kind":"step","id":"cls","action":"classify"},{"kind":"step","id":"r","action":"llm"}],
	    "edges":[{"from":"cls","handle":"refund","to":"r"}]}}}`)
	x := newSim(t, p)
	x.handler = label("bogus")
	x.run(`{"xs":[1]}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `[null]` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
}

// break_on is only for members of a hand-written body graph, and a value
// from an earlier activation of the loop does not end a later one.
func TestBreakOnAcrossActivations(t *testing.T) {
	p := compileGraph(t, `{"name":"o","root":{"kind":"loop","id":"o","max_iter":2,"body":
	  {"kind":"loop","id":"l","max_iter":3,"break_on":["t"],"body":{"kind":"graph","id":"g",
	    "nodes":[{"kind":"step","id":"q","action":"classify"},{"kind":"step","id":"t","action":"kairo.pass"},{"kind":"step","id":"f","action":"kairo.pass"}],
	    "edges":[{"from":"q","handle":"refund","to":"t"},{"from":"q","handle":"other","to":"f"}]}}}}`)
	x := newSim(t, p)
	rounds := map[string]int{}
	x.handler = func(c Command, n *ir.Node) Event {
		outer := c.StepID[strings.Index(c.StepID, "[")+1 : strings.Index(c.StepID, ",")]
		rounds[outer]++
		if outer == "0" {
			return ok(`{"label":"refund"}`)
		}
		return ok(`{"label":"other"}`)
	}
	x.run(`{}`)
	if x.s.Status != StatusCompleted || rounds["0"] != 1 || rounds["1"] != 3 {
		t.Fatalf("%v %s rounds=%v", x.s.Status, x.s.Error, rounds)
	}
	x.checkReplay()
	d, _ := ir.ParseDefinition([]byte(`{"name":"x","root":{"kind":"loop","id":"l","max_iter":2,"break_on":["t"],
	  "body":{"kind":"seq","nodes":[{"kind":"step","id":"t","action":"kairo.pass"}]}}}`))
	if _, err := ir.Compile(d, graphRegistry()); err == nil || !strings.Contains(err.Error(), "body graph") {
		t.Fatalf("break_on in a seq: %v", err)
	}
}

// An overflow fails the assignment instead of corrupting the variables.
func TestAssignOverflow(t *testing.T) {
	p := compile(t, `{"name":"a","vars":{"v":{"type":"float","value":1e308},"w":{"type":"string","value":"keep"}},
	  "root":{"kind":"step","id":"a","action":"kairo.assign","params":{"items":[{"var":"$var.v","op":"*=","value":10}]}}}`)
	x := newSim(t, p)
	x.run(`{}`)
	if x.s.Status != StatusFailed || string(RunVars(x.s)) != `{"v":1e308,"w":"keep"}` {
		t.Fatalf("%v %s vars=%s", x.s.Status, x.s.Error, RunVars(x.s))
	}
}

func TestReviewCompileErrors(t *testing.T) {
	for _, tc := range []struct{ js, want string }{
		{`{"name":"x","root":{"kind":"step","id":"c","action":"classify","on_error":{"strategy":"default-value"}}}`, "cannot use on_error default-value"},
		{`{"name":"x","root":{"kind":"step","id":"s","action":"kairo.switch","input":{"n":"$input.n"},"params":{"cases":[{"id":"fail-branch","logical_operator":"and","conditions":[]}]}}}`, "case ids"},
	} {
		d, _ := ir.ParseDefinition([]byte(tc.js))
		if _, err := ir.Compile(d, graphRegistry()); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.js, err)
		}
	}
}

// A map element's writes to run variables stay in that element, as
// graphon copies the variables for each iteration element.
func TestRunVarsWrittenInsideMap(t *testing.T) {
	p := compile(t, `{"name":"m","vars":{"v":{"type":"string","value":"outer"}},"root":{"kind":"seq","nodes":[
	  {"kind":"map","id":"m","over":"$input.xs","body":{"kind":"seq","nodes":[
	    {"kind":"step","id":"set","action":"kairo.assign","input":{"x":"$item"},"params":{"items":[{"var":"$var.v","op":"over-write","input":"x"}]}},
	    {"kind":"step","id":"read","action":"kairo.pass","input":{"v":"$var.v"}}]}},
	  {"kind":"step","id":"after","action":"kairo.pass","input":{"v":"$var.v","m":"m"}}]}}`)
	x := newSim(t, p)
	x.run(`{"xs":["a","b"]}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"m":[{"v":"a"},{"v":"b"}],"v":"outer"}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	x.checkReplay()
}
