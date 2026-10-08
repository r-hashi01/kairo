package core

import (
	"encoding/json"
	"testing"

	"github.com/r-hashi01/kairo/ir"
)

// waitAt returns a's result as a wait until at, then held (ADR 0045).
func waitAt(at func() int64, held string) func(Command, *ir.Node) Event {
	return func(c Command, n *ir.Node) Event {
		if n.ID == "a" {
			return Event{Kind: EvStepWait, Deadline: at(), Data: json.RawMessage(held), Meta: json.RawMessage(`{"m":1}`)}
		}
		return echo(nil)(c, n)
	}
}

// A step whose result is a wait ends at the deadline with the output it
// gave; what follows it runs then, with that output.
func TestStepWaitsForItsDeadline(t *testing.T) {
	x := newSim(t, compile(t, seqPlan))
	x.handler = waitAt(func() int64 { return x.now + 5000 }, `{"in":{"q":"held"}}`)
	start := x.now
	x.run(`{"q":"hello"}`)
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"q":"hello","x":"held"}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
	if x.now != start+5000 {
		t.Fatalf("ended at %d, want the deadline %d", x.now, start+5000)
	}
	var wait, end *Trace
	for i := range x.traces {
		tr := &x.traces[i]
		if tr.StepID == "a" && tr.Kind == TrNodeWait {
			wait = tr
		}
		if tr.StepID == "a" && tr.Kind == TrNodeEnd {
			end = tr
		}
	}
	if wait == nil || wait.Until != start+5000 || wait.At != start || string(wait.Meta) != `{"m":1}` {
		t.Fatalf("wait trace %+v", wait)
	}
	if end == nil || end.Status != "succeeded" || end.At != start+5000 || string(end.Output) != `{"in":{"q":"held"}}` {
		t.Fatalf("end trace %+v", end)
	}
	x.checkReplay()
}

// A deadline already past: the step ends at once, with no timer.
func TestStepWaitPastDeadline(t *testing.T) {
	x := newSim(t, compile(t, seqPlan))
	x.handler = waitAt(func() int64 { return x.now - 1 }, `{"in":{"q":"held"}}`)
	start := x.now
	x.run(`{"q":"hello"}`)
	if x.s.Status != StatusCompleted || x.now != start || string(x.s.Output) != `{"q":"hello","x":"held"}` {
		t.Fatalf("%v %d %s", x.s.Status, x.now-start, x.s.Output)
	}
	for _, tr := range x.traces {
		if tr.Kind == TrNodeWait {
			t.Fatal("a wait trace for a past deadline")
		}
	}
	x.checkReplay()
}

// A waiting run is quiescent; a cancel ends it and disarms the timer.
func TestStepWaitCancelled(t *testing.T) {
	x := newSim(t, compile(t, seqPlan))
	x.handler = waitAt(func() int64 { return x.now + 5000 }, `null`)
	x.limit = x.now + 1000
	x.run(`{"q":"hello"}`)
	if x.s.Status.Done() || !x.s.Quiescent() || len(x.timers) != 1 {
		t.Fatalf("%v quiescent=%v timers=%d", x.s.Status, x.s.Quiescent(), len(x.timers))
	}
	x.apply(Event{Kind: EvCancel, Err: "user"})
	if x.s.Status != StatusCancelled || len(x.timers) != 0 {
		t.Fatalf("%v timers=%d", x.s.Status, len(x.timers))
	}
	x.checkReplay()
}

// A waiting run survives a snapshot and a restart: the timer is armed
// again for the same deadline, and the step ends with its output then.
func TestStepWaitAcrossRestart(t *testing.T) {
	x := newSim(t, compile(t, seqPlan))
	deadline := x.now + 5000
	x.handler = waitAt(func() int64 { return deadline }, `{"in":{"q":"held"}}`)
	x.limit = x.now + 1000
	x.run(`{"q":"hello"}`)
	s, err := DecodeState(x.s.Encode(nil))
	if err != nil {
		t.Fatal(err)
	}
	x.s = s
	x.timers = map[uint32]Command{}
	x.apply(Event{Kind: EvRecover})
	if len(x.timers) != 1 {
		t.Fatalf("%d timers after recover", len(x.timers))
	}
	for _, c := range x.timers {
		if c.At != deadline {
			t.Fatalf("timer at %d, want %d", c.At, deadline)
		}
	}
	x.limit = 0
	x.handler = echo(nil)
	x.run2()
	if x.s.Status != StatusCompleted || string(x.s.Output) != `{"q":"hello","x":"held"}` {
		t.Fatalf("%v %s %s", x.s.Status, x.s.Error, x.s.Output)
	}
}
