package core

import (
	"encoding/json"
	"testing"

	"github.com/r-hashi01/kairo/ir"
)

func pendingPlan(t testing.TB, action string) *ir.Plan {
	t.Helper()
	return compile(t, `{"name":"pending","root":{"kind":"step","id":"call","action":"`+action+`"}}`)
}

func TestStepPendingIsQuiescentAndRecoversWithoutDispatch(t *testing.T) {
	p := pendingPlan(t, "llm")
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart})
	c := x.pending[0]
	x.pending = nil
	deadline := x.now + 5000
	x.apply(Event{Kind: EvStepPending, Act: c.Act, Attempt: c.Attempt, Deadline: deadline})
	if !x.s.Quiescent() || x.s.Inflight != 0 || len(x.timers) != 1 {
		t.Fatalf("quiescent=%v inflight=%d timers=%d", x.s.Quiescent(), x.s.Inflight, len(x.timers))
	}

	state, err := DecodeState(x.s.Encode(nil))
	if err != nil {
		t.Fatal(err)
	}
	out, err := Apply(p, state, &Event{Kind: EvRecover, At: x.now + 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var timers, dispatches int
	for _, cmd := range out {
		switch cmd.Kind {
		case CmdTimer:
			timers++
			if cmd.At != deadline {
				t.Fatalf("recovered timer at %d, want %d", cmd.At, deadline)
			}
		case CmdDispatch:
			dispatches++
		}
	}
	if timers != 1 || dispatches != 0 {
		t.Fatalf("recovery timers=%d dispatches=%d", timers, dispatches)
	}
	x.checkReplay()
}

func TestCallbackSettlesBeforeOrAfterPending(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(map[bool]string{false: "after", true: "before"}[before], func(t *testing.T) {
			x := newSim(t, pendingPlan(t, "llm"))
			x.apply(Event{Kind: EvStart})
			c := x.pending[0]
			x.pending = nil
			pending := Event{Kind: EvStepPending, Act: c.Act, Attempt: c.Attempt, Deadline: x.now + 5000}
			callback := Event{Kind: EvCallbackOK, Act: c.Act, Attempt: c.Attempt, Data: json.RawMessage(`{"ok":true}`)}
			if before {
				x.apply(callback)
				x.apply(pending) // ignored: the attempt is already settled
			} else {
				x.apply(pending)
				x.apply(callback)
			}
			if x.s.Status != StatusCompleted || string(x.s.Output) != `{"ok":true}` || len(x.timers) != 0 {
				t.Fatalf("status=%v output=%s timers=%d", x.s.Status, x.s.Output, len(x.timers))
			}
			x.checkReplay()
		})
	}
}

func TestCallbackSettlesRealAttemptAfterRestartReview(t *testing.T) {
	p := pendingPlan(t, "send")
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart})
	c := x.pending[0]
	x.pending = nil
	x.apply(Event{Kind: EvIntent, Act: c.Act, Attempt: c.Attempt})
	x.apply(Event{Kind: EvRecover})
	if x.s.Status != StatusBlocked || !x.s.Acts[c.Act].NeedsReview() {
		t.Fatalf("status=%v review=%v", x.s.Status, x.s.Acts[c.Act].NeedsReview())
	}
	x.apply(Event{Kind: EvCallbackOK, Act: c.Act, Attempt: c.Attempt, Data: json.RawMessage(`"sent"`)})
	if x.s.Status != StatusCompleted || string(x.s.Output) != `"sent"` {
		t.Fatalf("status=%v output=%s", x.s.Status, x.s.Output)
	}
	x.checkReplay()
}

func TestPendingDeadlineBecomesUnknown(t *testing.T) {
	x := newSim(t, pendingPlan(t, "send"))
	x.apply(Event{Kind: EvStart})
	c := x.pending[0]
	x.pending = nil
	x.apply(Event{Kind: EvStepPending, Act: c.Act, Attempt: c.Attempt, Deadline: x.now + 1})
	for timer := range x.timers {
		x.now++
		x.apply(Event{Kind: EvTimer, Act: c.Act, Timer: timer})
	}
	if x.s.Status != StatusBlocked || !x.s.Acts[c.Act].NeedsReview() {
		t.Fatalf("status=%v review=%v", x.s.Status, x.s.Acts[c.Act].NeedsReview())
	}
	x.checkReplay()
}
