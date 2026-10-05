package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"kairo/ir"
)

// --- ADR 0034: traces -------------------------------------------------------

func summarize(x *sim) string {
	var parts []string
	for _, t := range x.traces {
		s := ""
		switch t.Kind {
		case TrRunStart:
			s = "run+"
		case TrNodeStart:
			s = "+" + t.StepID
		case TrNodeEnd:
			s = "-" + t.StepID + ":" + t.Status
			if t.Handle != ir.HandleSource {
				s += "/" + t.Handle
			}
		case TrNodeSkip:
			s = "skip " + t.StepID
		case TrNodeRetry:
			s = fmt.Sprintf("retry %s#%d", t.StepID, t.Attempt)
		case TrRoundStart:
			s = fmt.Sprintf("round+ %s[%d]", t.StepID, t.Index)
		case TrRoundEnd:
			s = fmt.Sprintf("round- %s[%d]", t.StepID, t.Index)
		case TrVarUpdate:
			s = "var " + t.Var + "=" + string(t.Output)
		case TrRunEnd:
			s = "run-:" + t.Status
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " ")
}

func TestTraceOfGraph(t *testing.T) {
	x := newSim(t, compileGraph(t, diamond))
	x.handler = label("refund")
	x.run(`{}`)
	want := "run+ +start -start:succeeded +cls -cls:succeeded/refund +r skip o skip o2 -r:succeeded +merge -merge:succeeded run-:completed"
	if got := summarize(x); got != want {
		t.Fatalf("traces\n got: %s\nwant: %s", got, want)
	}
	x.checkReplay()
}

func TestTraceOfRetryExceptionAndMeta(t *testing.T) {
	p := compileGraph(t, `{"name":"fb","root":{"kind":"graph",
	  "nodes":[
	    {"kind":"step","id":"bad","action":"llm","retry":{"max_attempts":2,"interval":"10ms"},"on_error":{"strategy":"fail-branch"}},
	    {"kind":"step","id":"next","action":"llm"},
	    {"kind":"step","id":"recover","action":"llm"}],
	  "edges":[{"from":"bad","to":"next"},{"from":"bad","handle":"fail-branch","to":"recover"}]}}`)
	x := newSim(t, p)
	x.handler = func(c Command, n *ir.Node) Event {
		if n.ID == "bad" {
			return Event{Kind: EvStepErr, Err: "boom", ErrType: "HTTPError", Retryable: true, Meta: json.RawMessage(`{"usage":1}`)}
		}
		return Event{Kind: EvStepOK, Data: json.RawMessage(`1`), Meta: json.RawMessage(`{"tokens":7}`)}
	}
	x.run(`{}`)
	want := "run+ +bad retry bad#1 -bad:exception/fail-branch skip next +recover -recover:succeeded run-:completed"
	if got := summarize(x); got != want {
		t.Fatalf("traces\n got: %s\nwant: %s", got, want)
	}
	for _, tr := range x.traces {
		if tr.Kind == TrNodeEnd && tr.StepID == "recover" && string(tr.Meta) != `{"tokens":7}` {
			t.Fatalf("meta %s", tr.Meta)
		}
		if tr.Kind == TrNodeEnd && tr.StepID == "bad" && (tr.ErrType != "HTTPError" || string(tr.Meta) != `{"usage":1}`) {
			t.Fatalf("exception trace %+v", tr)
		}
	}
	x.checkReplay()
}

func TestTraceOfMapLoopAndVars(t *testing.T) {
	x := newSim(t, compile(t, `{"name":"m","vars":{"n":{"type":"integer","value":0}},"root":{"kind":"seq","nodes":[
	  {"kind":"map","id":"m","over":"$input.xs","body":{"kind":"step","id":"w","action":"llm"}},
	  {"kind":"step","id":"inc","action":"kairo.assign","params":{"items":[{"var":"$var.n","op":"+=","value":2}]}}]}}`))
	x.handler = func(Command, *ir.Node) Event { return ok(`1`) }
	x.run(`{"xs":[1,2]}`)
	want := "run+ +m round+ m[0] +w[0] round+ m[1] +w[1] -w[0]:succeeded round- m[0] -w[1]:succeeded round- m[1] -m:succeeded +inc var n=2 -inc:succeeded run-:completed"
	if got := summarize(x); got != want {
		t.Fatalf("traces\n got: %s\nwant: %s", got, want)
	}
	x.checkReplay()
}
