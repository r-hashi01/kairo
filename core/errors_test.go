package core

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/ir"
)

// --- ADR 0030: per-node error handling and run limits ----------------------

// failing answers step "bad" with a definite, retryable failure and every
// other step with its id.
func failing(c Command, n *ir.Node) Event {
	if n.ID == "bad" {
		return Event{Kind: EvStepErr, Err: "boom", ErrType: "HTTPError", Retryable: true}
	}
	return ok(`"` + n.ID + `"`)
}

func TestFailBranch(t *testing.T) {
	p := compileGraph(t, `{"name":"fb","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"bad","action":"llm","retry":{"max_attempts":2,"interval":"10ms"},"on_error":{"strategy":"fail-branch"}},
	    {"kind":"step","id":"next","action":"llm"},
	    {"kind":"step","id":"recover","action":"kairo.pass","input":{"why":"bad.error_message","kind":"bad.error_type"}}],
	  "edges":[{"from":"bad","to":"next"},{"from":"bad","handle":"fail-branch","to":"recover"}]}}`)
	x := newSim(t, p)
	x.handler = failing
	x.run(`{}`)
	if x.s.Status != StatusCompleted || x.s.Exceptions != 1 {
		t.Fatalf("%v %s exceptions=%d", x.s.Status, x.s.Error, x.s.Exceptions)
	}
	if got := string(x.s.Output); got != `{"recover":{"kind":"HTTPError","why":"boom"}}` {
		t.Fatalf("output %s", got)
	}
	attempts := 0
	for _, ev := range x.log {
		if ev.Kind == EvStepErr {
			attempts++
		}
	}
	if attempts != 2 {
		t.Fatalf("%d attempts, want 2 (the node's retry overrides the spec's 3)", attempts)
	}
	// The interval is fixed: the retry ran 10ms after the failure.
	if x.now != 1010 {
		t.Fatalf("retry at %d, want 1010", x.now)
	}
	x.checkReplay()
}

func TestDefaultValue(t *testing.T) {
	p := compile(t, `{"name":"dv","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"bad","action":"llm","retry":{"max_attempts":1},
	   "on_error":{"strategy":"default-value","value":{"text":"n/a","score":0}}},
	  {"kind":"step","id":"use","action":"kairo.pass","input":{"t":"bad.text","e":"bad.error_message"}}]}}`)
	x := newSim(t, p)
	x.handler = failing
	x.run(`{}`)
	if x.s.Status != StatusCompleted || x.s.Exceptions != 1 || string(x.s.Output) != `{"e":"boom","t":"n/a"}` {
		t.Fatalf("%v %s %d %s", x.s.Status, x.s.Error, x.s.Exceptions, x.s.Output)
	}
	x.checkReplay()
}

// An unknown outcome of a real step still needs review, whatever its
// on_error says (invariant 5; ADR 0035 adds the explicit exception).
func TestOnErrorDoesNotCoverUnknownRealOutcome(t *testing.T) {
	p := compile(t, `{"name":"u","root":{"kind":"step","id":"s","action":"send",
	  "on_error":{"strategy":"default-value","value":{}}}}`)
	x := newSim(t, p)
	x.handler = func(Command, *ir.Node) Event { return Event{Kind: EvStepErr, Err: "timeout", Unknown: true} }
	x.run(`{}`)
	if x.s.Status != StatusBlocked || len(x.reviews) != 1 {
		t.Fatalf("%v reviews=%d", x.s.Status, len(x.reviews))
	}
	x.checkReplay()
}

func TestMaxSteps(t *testing.T) {
	p := compile(t, seqPlan) // three steps
	x := newSim(t, p)
	x.handler = echo(nil)
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{"q":"x"}`), MaxSteps: 2})
	x.run2()
	if x.s.Status != StatusFailed || x.s.Error != "max steps exceeded" || x.s.Steps != 3 {
		t.Fatalf("%v %q steps=%d", x.s.Status, x.s.Error, x.s.Steps)
	}
	x.checkReplay()
}

// The run's deadline fails it and aborts what is running.
func TestDeadline(t *testing.T) {
	p := compile(t, `{"name":"d","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"wait","id":"w","signal":"never"}]}}`)
	x := newSim(t, p)
	var aborted []Command
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{}`), Deadline: x.now + 5000})
	ev := Event{Kind: EvTimer, Timer: x.s.DeadlineTimer}
	x.now += 5000
	ev.At = x.now
	out, tr, err := ApplyTraced(x.p, x.s, &ev, nil, x.traces)
	if err != nil {
		t.Fatal(err)
	}
	x.traces = tr
	x.log = append(x.log, ev)
	for _, c := range out {
		if c.Kind == CmdAbort {
			aborted = append(aborted, c)
		}
	}
	if x.s.Status != StatusFailed || x.s.Error != "max execution time exceeded" || len(aborted) != 1 {
		t.Fatalf("%v %q aborted=%d", x.s.Status, x.s.Error, len(aborted))
	}
	x.checkReplay()
	// Finishing in time disarms it.
	y := newSim(t, compile(t, seqPlan))
	y.handler = echo(nil)
	y.apply(Event{Kind: EvStart, Data: json.RawMessage(`{"q":"x"}`), Deadline: y.now + 5000})
	y.run2()
	if y.s.Status != StatusCompleted || len(y.timers) != 0 || y.s.DeadlineTimer != 0 {
		t.Fatalf("%v timers=%d", y.s.Status, len(y.timers))
	}
}

func TestErrorHandlingCompileErrors(t *testing.T) {
	for _, tc := range []struct{ js, want string }{
		{`{"name":"x","root":{"kind":"seq","nodes":[{"kind":"step","id":"a","action":"llm","on_error":{"strategy":"fail-branch"}}]}}`, "node of a graph"},
		{`{"name":"x","root":{"kind":"step","id":"a","action":"llm","on_error":{"strategy":"fail-branch"}}}`, "node of a graph"},
		{`{"name":"x","root":{"kind":"graph","nodes":[{"kind":"step","id":"a","action":"llm","on_error":{"strategy":"fail-branch"}},{"kind":"step","id":"b","action":"llm"}],
		  "edges":[{"from":"a","to":"b"}]}}`, "no fail-branch edge"},
		{`{"name":"x","root":{"kind":"step","id":"a","action":"llm","on_error":{"strategy":"default-value","value":[1]}}}`, "must be an object"},
		{`{"name":"x","root":{"kind":"step","id":"a","action":"llm","on_error":{"strategy":"retry-forever"}}}`, "unknown on_error"},
		{`{"name":"x","root":{"kind":"step","id":"a","action":"llm","retry":{"max_attempts":0}}}`, "at least 1"},
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

// --- ADR 0035 ---------------------------------------------------------------

// on_unknown: fail turns an unknown outcome of a real step into a definite
// failure: no retry, no review, on to on_error.
func TestOnUnknownFail(t *testing.T) {
	p := compileGraph(t, `{"name":"u","root":{"kind":"graph","nodes":[
	  {"kind":"step","id":"s","action":"send","on_unknown":"fail","retry":{"max_attempts":3},"on_error":{"strategy":"fail-branch"}},
	  {"kind":"step","id":"comp","action":"kairo.pass","input":{"t":"s.error_type"}}],
	  "edges":[{"from":"s","handle":"fail-branch","to":"comp"}]}}`)
	x := newSim(t, p)
	attempts := 0
	x.handler = func(Command, *ir.Node) Event {
		attempts++
		return Event{Kind: EvStepErr, Err: "timeout", Unknown: true}
	}
	x.run(`{}`)
	if x.s.Status != StatusCompleted || attempts != 1 || len(x.reviews) != 0 || string(x.s.Output) != `{"comp":{"t":"outcome_unknown"}}` {
		t.Fatalf("%v %s attempts=%d reviews=%d %s", x.s.Status, x.s.Error, attempts, len(x.reviews), x.s.Output)
	}
	x.checkReplay()
	// A definite failure is still retried.
	y := newSim(t, p)
	attempts = 0
	y.handler = func(Command, *ir.Node) Event {
		attempts++
		return Event{Kind: EvStepErr, Err: "500", Retryable: true}
	}
	y.run(`{}`)
	if attempts != 3 {
		t.Fatalf("definite failure: %d attempts", attempts)
	}
}

// After a restart, a released real step of on_unknown: fail fails too.
func TestOnUnknownFailAfterRestart(t *testing.T) {
	p := compile(t, `{"name":"u","root":{"kind":"step","id":"s","action":"send","on_unknown":"fail"}}`)
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(`{}`)})
	c := x.pending[0]
	x.apply(Event{Kind: EvIntent, Act: c.Act, Attempt: c.Attempt})
	x.apply(Event{Kind: EvRecover})
	if x.s.Status != StatusFailed || len(x.reviews) != 0 || !strings.Contains(x.s.Error, "unknown") {
		t.Fatalf("%v %s reviews=%d", x.s.Status, x.s.Error, len(x.reviews))
	}
	x.checkReplay()
}
