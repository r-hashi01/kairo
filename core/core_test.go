package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"kairo/ir"
)

func registry() *ir.Registry {
	r := ir.NewRegistry()
	r.Register(ir.NodeSpec{Action: "llm", Effect: ir.EffectUnprotected, Outputs: map[string]ir.FieldType{
		"label": {Type: ir.FieldEnum, Values: []string{"refund", "other"}},
		"score": {Type: ir.FieldNumber},
		"done":  {Type: ir.FieldBool},
		"text":  {Type: ir.FieldText},
	}})
	r.Register(ir.NodeSpec{Action: "send", Effect: ir.EffectReal})
	r.Register(ir.NodeSpec{Action: "pay", Effect: ir.EffectReal, IdempotentRetry: true})
	return r
}

func compile(t testing.TB, js string) *ir.Plan {
	t.Helper()
	d, err := ir.ParseDefinition([]byte(js))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ir.Compile(d, registry())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// sim drives a run: it answers dispatches through handler and fires timers
// when nothing else is pending. Every applied event is recorded.
type sim struct {
	t       testing.TB
	p       *ir.Plan
	s       *State
	now     int64
	log     []Event
	pending []Command // outstanding dispatches
	timers  map[uint32]Command
	reviews []Command
	done    bool
	rng     *rand.Rand
	limit   int64 // if set, timers after this tick are not fired
	handler func(c Command, n *ir.Node) Event
}

func newSim(t testing.TB, p *ir.Plan) *sim {
	return &sim{t: t, p: p, s: NewState("run1"), now: 1000, timers: map[uint32]Command{}}
}

func (x *sim) apply(ev Event) {
	ev.At = x.now
	out, err := Apply(x.p, x.s, &ev, nil)
	if err == ErrIgnored {
		return
	}
	if err != nil {
		x.t.Fatal(err)
	}
	x.log = append(x.log, ev)
	for _, c := range out {
		switch c.Kind {
		case CmdDispatch:
			x.pending = append(x.pending, c)
		case CmdTimer:
			x.timers[c.Timer] = c
		case CmdCancelTimer:
			delete(x.timers, c.Timer)
		case CmdReview:
			x.reviews = append(x.reviews, c)
		case CmdDone:
			x.done = true
		case CmdAbort:
			for i, p := range x.pending {
				if p.Act == c.Act {
					x.pending = append(x.pending[:i], x.pending[i+1:]...)
					break
				}
			}
		}
	}
}

func (x *sim) run(input string) {
	x.apply(Event{Kind: EvStart, Data: json.RawMessage(input)})
	for steps := 0; !x.done && steps < 100000; steps++ {
		if len(x.pending) > 0 {
			i := 0
			if x.rng != nil {
				i = x.rng.Intn(len(x.pending))
			}
			c := x.pending[i]
			x.pending = append(x.pending[:i], x.pending[i+1:]...)
			ev := x.handler(c, &x.p.Nodes[c.Node])
			ev.Act, ev.Attempt = c.Act, c.Attempt
			x.apply(ev)
			continue
		}
		if len(x.timers) > 0 {
			var first Command
			for _, c := range x.timers {
				if first.Timer == 0 || c.At < first.At || (c.At == first.At && c.Timer < first.Timer) {
					first = c
				}
			}
			if x.limit != 0 && first.At > x.limit {
				return
			}
			delete(x.timers, first.Timer)
			if first.At > x.now {
				x.now = first.At
			}
			x.apply(Event{Kind: EvTimer, Act: first.Act, Timer: first.Timer})
			continue
		}
		return // waiting for a signal or review
	}
}

func ok(data string) Event { return Event{Kind: EvStepOK, Data: json.RawMessage(data)} }

// echo returns {"node":<id>,"in":<input>} plus the typed fields in extra.
func echo(extra map[string]string) func(Command, *ir.Node) Event {
	return func(c Command, n *ir.Node) Event {
		f := ""
		if e, ok := extra[n.ID]; ok {
			f = "," + e
		}
		return ok(fmt.Sprintf(`{"node":%q,"step":%q,"in":%s%s}`, n.ID, c.StepID, c.Input, f))
	}
}

func (x *sim) checkReplay() {
	x.t.Helper()
	r := NewState(x.s.RunID)
	for i := range x.log {
		if _, err := Apply(x.p, r, &x.log[i], nil); err != nil {
			x.t.Fatalf("replay event %d: %v", i, err)
		}
	}
	a, b := x.s.Encode(nil), r.Encode(nil)
	if !bytes.Equal(a, b) {
		x.t.Fatalf("replayed state differs\nlive:   %x\nreplay: %x", a, b)
	}
	d, err := DecodeState(a)
	if err != nil {
		x.t.Fatal(err)
	}
	if !bytes.Equal(d.Encode(nil), a) {
		x.t.Fatal("decode/encode round trip differs")
	}
}

const seqPlan = `{"name":"seq","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"a","action":"llm","input":{"q":"$input.q"}},
  {"kind":"step","id":"b","action":"llm","input":{"prev":"a.in.q"}},
  {"kind":"step","id":"c","action":"kairo.pass","input":{"x":"b.in.prev","q":"$input.q"}}
]}}`

func TestSequence(t *testing.T) {
	x := newSim(t, compile(t, seqPlan))
	x.handler = echo(nil)
	x.run(`{"q":"hello"}`)
	if x.s.Status != StatusCompleted {
		t.Fatalf("status %v %s", x.s.Status, x.s.Error)
	}
	if got := string(x.s.Output); got != `{"q":"hello","x":"hello"}` {
		t.Fatalf("output %s", got)
	}
	x.checkReplay()
}

func TestParallelJoinAnyOrder(t *testing.T) {
	p := compile(t, `{"name":"par","root":{"kind":"par","id":"all","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"step","id":"b","action":"llm"},
	  {"kind":"seq","id":"s","nodes":[{"kind":"step","id":"c","action":"llm"},{"kind":"step","id":"d","action":"llm"}]}
	]}}`)
	var want []byte
	for seed := int64(0); seed < 20; seed++ {
		x := newSim(t, p)
		x.rng = rand.New(rand.NewSource(seed))
		x.handler = func(c Command, n *ir.Node) Event { return ok(`"` + n.ID + `"`) }
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
	if string(want) != `{"a":"a","b":"b","s":"d"}` {
		t.Fatalf("output %s", want)
	}
}

func TestCondOnTypedField(t *testing.T) {
	p := compile(t, `{"name":"cond","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"classify","action":"llm"},
	  {"kind":"cond","id":"route","if":{"field":"classify.label","op":"eq","value":"refund"},
	   "then":{"kind":"step","id":"refund","action":"llm"},
	   "else":{"kind":"step","id":"other","action":"llm"}}
	]}}`)
	for _, label := range []string{"refund", "other"} {
		x := newSim(t, p)
		x.handler = echo(map[string]string{"classify": `"label":"` + label + `"`})
		x.run(`{}`)
		if !strings.Contains(string(x.s.Output), `"node":"`+label+`"`) {
			t.Fatalf("label %s took wrong branch: %s", label, x.s.Output)
		}
		x.checkReplay()
	}
}

func TestCondRejectsFreeText(t *testing.T) {
	d, _ := ir.ParseDefinition([]byte(`{"name":"bad","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"cond","if":{"field":"a.text","op":"eq","value":"yes"},"then":{"kind":"step","id":"b","action":"llm"}}]}}`))
	if _, err := ir.Compile(d, registry()); err == nil || !strings.Contains(err.Error(), "text") {
		t.Fatalf("expected free-text condition to be rejected, got %v", err)
	}
	d, _ = ir.ParseDefinition([]byte(`{"name":"bad","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"cond","if":{"field":"a.undeclared","op":"eq","value":1},"then":{"kind":"step","id":"b","action":"llm"}}]}}`))
	if _, err := ir.Compile(d, registry()); err == nil {
		t.Fatal("expected untyped field to be rejected")
	}
}

func TestMapWithConcurrencyLimit(t *testing.T) {
	p := compile(t, `{"name":"map","root":{"kind":"map","id":"m","over":"$input.docs","max_concurrency":2,
	  "body":{"kind":"seq","nodes":[
	    {"kind":"step","id":"sum","action":"llm","input":{"doc":"$item","i":"$index"}},
	    {"kind":"step","id":"fmt","action":"kairo.pass","input":{"s":"sum.in.doc"}}]}}}`)
	x := newSim(t, p)
	x.rng = rand.New(rand.NewSource(7))
	maxOut := 0
	x.handler = func(c Command, n *ir.Node) Event {
		if l := len(x.pending) + 1; l > maxOut {
			maxOut = l
		}
		return ok(fmt.Sprintf(`{"in":%s,"step":%q}`, c.Input, c.StepID))
	}
	x.run(`{"docs":["a","b","c","d","e"]}`)
	if x.s.Status != StatusCompleted {
		t.Fatal(x.s.Error)
	}
	if maxOut > 2 {
		t.Fatalf("concurrency %d exceeded limit 2", maxOut)
	}
	if got := string(x.s.Output); got != `[{"s":"a"},{"s":"b"},{"s":"c"},{"s":"d"},{"s":"e"}]` {
		t.Fatalf("output %s", got)
	}
	if len(x.s.Scopes) != 1 {
		t.Fatalf("element scopes leaked: %d", len(x.s.Scopes))
	}
	x.checkReplay()
}

func TestLoopUntilDone(t *testing.T) {
	p := compile(t, `{"name":"loop","root":{"kind":"loop","id":"l","max_iter":10,
	  "while":{"field":"check.done","op":"eq","value":false},
	  "body":{"kind":"step","id":"check","action":"llm"}}}`)
	x := newSim(t, p)
	n := 0
	var steps []string
	x.handler = func(c Command, _ *ir.Node) Event {
		n++
		steps = append(steps, c.StepID)
		return ok(fmt.Sprintf(`{"done":%v}`, n == 3))
	}
	x.run(`{}`)
	if n != 3 || x.s.Status != StatusCompleted {
		t.Fatalf("iterations %d status %v", n, x.s.Status)
	}
	if strings.Join(steps, " ") != "check[0] check[1] check[2]" {
		t.Fatalf("step ids %v", steps)
	}
	x.checkReplay()
}

func TestWaitTimerAndSignal(t *testing.T) {
	p := compile(t, `{"name":"wait","root":{"kind":"seq","nodes":[
	  {"kind":"wait","id":"sleep","duration":"1h"},
	  {"kind":"wait","id":"approve","signal":"approval","timeout":"24h"},
	  {"kind":"cond","if":{"field":"approve.timed_out","op":"eq","value":false},
	   "then":{"kind":"step","id":"go","action":"llm","input":{"who":"approve.payload.by"}}}
	]}}`)
	x := newSim(t, p)
	x.handler = echo(nil)
	x.limit = 1000 + 3600_000
	x.run(`{}`)
	if x.done || x.now != 1000+3600_000 {
		t.Fatalf("expected to be waiting for the signal after 1h, now=%d done=%v", x.now, x.done)
	}
	if !x.s.Quiescent() {
		t.Fatal("waiting run should be quiescent")
	}
	x.apply(Event{Kind: EvSignal, Name: "approval", Data: json.RawMessage(`{"by":"alice"}`)})
	x.run2()
	if !strings.Contains(string(x.s.Output), `"who":"alice"`) {
		t.Fatalf("output %s", x.s.Output)
	}
	if len(x.timers) != 0 {
		t.Fatal("timeout timer not cancelled")
	}
	x.checkReplay()

	// Nobody approves: the timeout fires and the condition skips "go".
	y := newSim(t, p)
	y.handler = echo(nil)
	y.run(`{}`)
	if !y.done || string(y.s.Output) != "null" || y.now != 1000+3600_000+24*3600_000 {
		t.Fatalf("timeout path: done=%v output=%s now=%d", y.done, y.s.Output, y.now)
	}
}

// run2 continues a run without a start event.
func (x *sim) run2() {
	for !x.done && (len(x.pending) > 0 || len(x.timers) > 0) {
		c := x.pending
		if len(c) > 0 {
			cmd := c[0]
			x.pending = c[1:]
			ev := x.handler(cmd, &x.p.Nodes[cmd.Node])
			ev.Act, ev.Attempt = cmd.Act, cmd.Attempt
			x.apply(ev)
			continue
		}
		var first Command
		for _, t := range x.timers {
			if first.Timer == 0 || t.At < first.At {
				first = t
			}
		}
		delete(x.timers, first.Timer)
		x.now = max(x.now, first.At)
		x.apply(Event{Kind: EvTimer, Act: first.Act, Timer: first.Timer})
	}
}

func TestSignalBeforeWaitIsBuffered(t *testing.T) {
	p := compile(t, `{"name":"w","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"llm"},
	  {"kind":"wait","id":"hook","signal":"cb"}]}}`)
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart})
	x.apply(Event{Kind: EvSignal, Name: "cb", Data: json.RawMessage(`1`)})
	c := x.pending[0]
	x.pending = nil
	x.apply(Event{Kind: EvStepOK, Act: c.Act, Attempt: c.Attempt, Data: json.RawMessage(`{}`)})
	if !x.done || string(x.s.Output) != `{"timed_out":false,"payload":1}` {
		t.Fatalf("done=%v output %s", x.done, x.s.Output)
	}
	x.checkReplay()
}

func TestRetryUnprotected(t *testing.T) {
	x := newSim(t, compile(t, `{"name":"r","root":{"kind":"step","id":"a","action":"llm"}}`))
	n := 0
	keys := map[string]bool{}
	x.handler = func(c Command, _ *ir.Node) Event {
		n++
		keys[c.IdemKey] = true
		if n < 3 {
			return Event{Kind: EvStepErr, Err: "429", Retryable: true}
		}
		return ok(`1`)
	}
	x.run(`{}`)
	if x.s.Status != StatusCompleted || n != 3 {
		t.Fatalf("status %v attempts %d", x.s.Status, n)
	}
	if len(keys) != 1 {
		t.Fatal("idempotency key changed across attempts")
	}
	x.checkReplay()
}

func TestRealUnknownOutcomeNeedsReview(t *testing.T) {
	x := newSim(t, compile(t, `{"name":"r","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"send","action":"send"},{"kind":"step","id":"after","action":"llm"}]}}`))
	x.handler = func(c Command, n *ir.Node) Event {
		if n.ID == "send" {
			return Event{Kind: EvStepErr, Err: "connection reset", Unknown: true}
		}
		return ok(`"ok"`)
	}
	x.run(`{}`)
	if x.s.Status != StatusBlocked || len(x.reviews) != 1 || x.done {
		t.Fatalf("status %v reviews %d", x.s.Status, len(x.reviews))
	}
	x.apply(Event{Kind: EvResolve, Act: x.reviews[0].Act, Data: json.RawMessage(`{"sent":true}`)})
	x.run2()
	if x.s.Status != StatusCompleted {
		t.Fatalf("status after resolve %v", x.s.Status)
	}
	x.checkReplay()
}

func TestRealIdempotentRetriesOnUnknown(t *testing.T) {
	x := newSim(t, compile(t, `{"name":"r","root":{"kind":"step","id":"pay","action":"pay"}}`))
	n := 0
	x.handler = func(c Command, n2 *ir.Node) Event {
		n++
		if n == 1 {
			return Event{Kind: EvStepErr, Err: "timeout", Unknown: true}
		}
		return ok(`"paid"`)
	}
	x.run(`{}`)
	if x.s.Status != StatusCompleted || n != 2 {
		t.Fatalf("status %v attempts %d", x.s.Status, n)
	}
}

func TestRecoverReissuesInFlight(t *testing.T) {
	p := compile(t, `{"name":"rec","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"read","action":"llm"},
	  {"kind":"step","id":"send","action":"send"},
	  {"kind":"step","id":"send2","action":"send"},
	  {"kind":"wait","id":"t","duration":"10m"}]}}`)
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart})
	// send's intent became durable (it may have run); send2's did not.
	var sendCmd Command
	for _, c := range x.pending {
		if p.Nodes[c.Node].ID == "send" {
			sendCmd = c
		}
	}
	x.apply(Event{Kind: EvIntent, Act: sendCmd.Act, Attempt: sendCmd.Attempt})

	// Crash: rebuild from the log and recover.
	r := NewState("run1")
	for i := range x.log {
		Apply(p, r, &x.log[i], nil)
	}
	out, _ := Apply(p, r, &Event{Kind: EvRecover, At: 5000}, nil)
	var redispatched []string
	var timers, reviews int
	for _, c := range out {
		switch c.Kind {
		case CmdDispatch:
			redispatched = append(redispatched, p.Nodes[c.Node].ID)
		case CmdTimer:
			timers++
		case CmdReview:
			reviews++
		}
	}
	if strings.Join(redispatched, ",") != "read,send2" {
		t.Fatalf("redispatched %v", redispatched)
	}
	if timers != 1 || reviews != 1 || r.Status != StatusBlocked {
		t.Fatalf("timers %d reviews %d status %v", timers, reviews, r.Status)
	}
}

func TestStepTimeoutIsUnknown(t *testing.T) {
	reg := registry()
	reg.Register(ir.NodeSpec{Action: "slow", Effect: ir.EffectUnprotected, Timeout: ir.Duration(30e9), MaxAttempts: 2})
	d, _ := ir.ParseDefinition([]byte(`{"name":"t","root":{"kind":"step","id":"a","action":"slow"}}`))
	p, err := ir.Compile(d, reg)
	if err != nil {
		t.Fatal(err)
	}
	x := newSim(t, p)
	x.apply(Event{Kind: EvStart})
	if len(x.timers) != 1 {
		t.Fatal("no timeout timer")
	}
	x.pending = nil
	x.handler = func(Command, *ir.Node) Event { return ok(`1`) }
	x.run2() // timeout fires -> retry timer -> re-dispatch -> ok
	if x.s.Status != StatusCompleted {
		t.Fatalf("status %v %s", x.s.Status, x.s.Error)
	}
	x.checkReplay()
}

func TestFailureAbortsSiblings(t *testing.T) {
	x := newSim(t, compile(t, `{"name":"f","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"a","action":"llm"},{"kind":"step","id":"b","action":"llm"}]}}`))
	x.apply(Event{Kind: EvStart})
	c := x.pending[0]
	x.pending = x.pending[1:]
	x.apply(Event{Kind: EvStepErr, Act: c.Act, Attempt: c.Attempt, Err: "bad request"})
	if x.s.Status != StatusFailed || len(x.pending) != 0 || !x.done {
		t.Fatalf("status %v pending %d", x.s.Status, len(x.pending))
	}
}

func TestStaleResultsIgnored(t *testing.T) {
	x := newSim(t, compile(t, `{"name":"r","root":{"kind":"step","id":"a","action":"llm"}}`))
	x.apply(Event{Kind: EvStart})
	c := x.pending[0]
	if _, err := Apply(x.p, x.s, &Event{Kind: EvStepOK, Act: c.Act, Attempt: c.Attempt + 1}, nil); err != ErrIgnored {
		t.Fatal("result for another attempt was accepted")
	}
}

func BenchmarkTransition(b *testing.B) {
	p := compile(b, seqPlan)
	in := json.RawMessage(`{"q":"hello"}`)
	outData := json.RawMessage(`{"in":{"q":"hello"},"text":"some answer"}`)
	var out []Command
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := NewState("r")
		out, _ = Apply(p, s, &Event{Kind: EvStart, Data: in}, out[:0])
		for !s.Status.Done() {
			c := out[0]
			out, _ = Apply(p, s, &Event{Kind: EvStepOK, Act: c.Act, Attempt: c.Attempt, Data: outData}, out[:0])
		}
	}
	// 3 events per iteration
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*3), "ns/transition")
}
