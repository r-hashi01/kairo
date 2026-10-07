package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"kairo/ir"
)

// ErrIgnored is returned for events that do not apply to the current state
// (stale attempts, timers already cancelled, runs already finished). They
// are harmless and are not logged.
var ErrIgnored = errors.New("core: event ignored")

var null = json.RawMessage("null")

type machine struct {
	p     *ir.Plan
	s     *State
	at    int64
	out   []Command
	entry string // EvStart: the entry chosen for the root graph

	// Tracing (ADR 0034): traces are appended to tr. meta belongs to the
	// step metaAct the event reports on. endStatus etc. describe how the
	// activation complete is about to finish failed (exception).
	tracing   bool
	tr        []Trace
	meta      json.RawMessage
	metaAct   uint32
	endStatus string
	endErr    string
	endType   string
}

// Apply applies ev to s, appending the resulting commands to out.
func Apply(p *ir.Plan, s *State, ev *Event, out []Command) ([]Command, error) {
	if s.Status.Done() {
		return out, ErrIgnored
	}
	m := machine{p: p, s: s, at: ev.At, out: out}
	err := m.apply(ev)
	return m.out, err
}

func (m *machine) apply(ev *Event) error {
	s := m.s
	switch ev.Kind {
	case EvStart:
		if s.NextAct != 0 {
			return ErrIgnored
		}
		s.Input = ev.Data
		if len(s.Input) == 0 {
			s.Input = json.RawMessage("{}")
		}
		m.entry = ev.Name
		s.MaxSteps, s.Depth = ev.MaxSteps, ev.Depth
		m.initRunVars(ev.Vars)
		m.trace(Trace{Kind: TrRunStart, Input: s.Input})
		if ev.Deadline > 0 {
			// One timer for the whole run (ADR 0030).
			s.NextTimer++
			s.Deadline, s.DeadlineTimer = ev.Deadline, s.NextTimer
			m.out = append(m.out, Command{Kind: CmdTimer, Timer: s.DeadlineTimer, At: s.Deadline})
		}
		m.start(0, 0, 0, 0)
		return nil

	case EvStepOK, EvStepErr:
		a := s.Acts[ev.Act]
		if a == nil || m.p.Nodes[a.Node].Kind != ir.KStep || a.Flags&fDispatched == 0 || a.Attempt != ev.Attempt {
			return ErrIgnored
		}
		m.undispatch(a)
		m.cancelTimer(a)
		if ev.Kind == EvStepOK {
			out := ev.Data
			if len(out) == 0 {
				out = null
			}
			m.complete(ev.Act, a, out)
			return nil
		}
		m.stepFailed(ev.Act, a, ev.Err, ev.ErrType, ev.Retryable, ev.Unknown)
		return nil

	case EvStepWait:
		// The step's result: wait until the deadline, then end with this
		// output (ADR 0045). A definite result, also for real steps.
		a := s.Acts[ev.Act]
		if a == nil || m.p.Nodes[a.Node].Kind != ir.KStep || a.Flags&fDispatched == 0 || a.Attempt != ev.Attempt {
			return ErrIgnored
		}
		m.undispatch(a)
		m.cancelTimer(a)
		out := ev.Data
		if len(out) == 0 {
			out = null
		}
		if ev.Deadline <= m.at {
			m.complete(ev.Act, a, out)
			return nil
		}
		a.Flags |= fStepWait
		a.Results = []json.RawMessage{out}
		m.armTimer(ev.Act, a, ev.Deadline)
		// The executor's metadata goes with the wait: the end at the deadline
		// comes from a timer, which has none.
		m.trace(Trace{Kind: TrNodeWait, Act: ev.Act, Node: a.Node, StepID: m.stepID(ev.Act, a), Attempt: a.Attempt, Until: ev.Deadline, Meta: m.meta})
		return nil

	case EvIntent:
		a := s.Acts[ev.Act]
		if a == nil || a.Flags&fDispatched == 0 || a.Attempt != ev.Attempt {
			return ErrIgnored
		}
		a.Flags |= fIntent
		return nil

	case EvTimer:
		if ev.Act == 0 && ev.Timer != 0 && ev.Timer == s.DeadlineTimer {
			s.DeadlineTimer = 0
			m.fail(StatusFailed, "max execution time exceeded")
			return nil
		}
		a := s.Acts[ev.Act]
		if a == nil || a.Timer == 0 || a.Timer != ev.Timer {
			return ErrIgnored
		}
		a.Timer, a.TimerAt = 0, 0
		n := &m.p.Nodes[a.Node]
		switch {
		case n.Kind == ir.KWait && a.Flags&fSignalWait != 0:
			a.Flags &^= fSignalWait
			m.complete(ev.Act, a, json.RawMessage(`{"timed_out":true,"payload":null}`))
		case n.Kind == ir.KWait:
			m.complete(ev.Act, a, null)
		case a.Flags&fStepWait != 0:
			a.Flags &^= fStepWait
			out := a.Results[0]
			a.Results = nil
			m.complete(ev.Act, a, out)
		case a.Flags&fRetryWait != 0:
			a.Flags &^= fRetryWait
			m.dispatch(ev.Act, a)
		case a.Flags&fDispatched != 0:
			// Step timeout: the outcome is unknown.
			m.out = append(m.out, Command{Kind: CmdAbort, Act: ev.Act, Node: a.Node, Attempt: a.Attempt})
			m.undispatch(a)
			m.stepFailed(ev.Act, a, "timeout", "timeout", true, true)
		default:
			return ErrIgnored
		}
		return nil

	case EvSignal:
		if ev.Act != 0 {
			// Addressed to one waiting step (e.g. one of several parallel
			// approvals). Not buffered: the step must be waiting now.
			a := s.Acts[ev.Act]
			if a == nil || a.Flags&fSignalWait == 0 || m.p.Nodes[a.Node].Signal != ev.Name {
				return ErrIgnored
			}
			a.Flags &^= fSignalWait
			m.cancelTimer(a)
			m.complete(ev.Act, a, signalOutput(ev.Data))
			return nil
		}
		for _, id := range sortedActs(s) {
			a := s.Acts[id]
			if a.Flags&fSignalWait != 0 && m.p.Nodes[a.Node].Signal == ev.Name {
				a.Flags &^= fSignalWait
				m.cancelTimer(a)
				m.complete(id, a, signalOutput(ev.Data))
				return nil
			}
		}
		if s.Mailbox == nil {
			s.Mailbox = map[string][]json.RawMessage{}
		}
		d := ev.Data
		if len(d) == 0 {
			d = null
		}
		s.Mailbox[ev.Name] = append(s.Mailbox[ev.Name], d)
		return nil

	case EvResolve:
		a := s.Acts[ev.Act]
		if a == nil || a.Flags&fReview == 0 {
			return ErrIgnored
		}
		a.Flags &^= fReview
		m.refreshBlocked()
		if ev.Err != "" {
			m.fail(StatusFailed, "step "+m.p.Nodes[a.Node].ID+" resolved as failed: "+ev.Err)
			return nil
		}
		out := ev.Data
		if len(out) == 0 {
			out = null
		}
		m.complete(ev.Act, a, out)
		return nil

	case EvCancel:
		reason := ev.Err
		if reason == "" {
			reason = "cancelled"
		}
		m.fail(StatusCancelled, reason)
		return nil

	case EvRecover:
		m.recover()
		return nil
	}
	return ErrIgnored
}

func signalOutput(payload json.RawMessage) json.RawMessage {
	if len(payload) == 0 {
		payload = null
	}
	b := make([]byte, 0, len(payload)+32)
	b = append(b, `{"timed_out":false,"payload":`...)
	b = append(b, payload...)
	b = append(b, '}')
	return b
}

func sortedActs(s *State) []uint32 {
	ids := make([]uint32, 0, len(s.Acts))
	for id := range s.Acts {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// start creates an activation of node under parent in scope.
func (m *machine) start(node int32, parent, scope uint32, idx int32) {
	s := m.s
	s.NextAct++
	id := s.NextAct
	a := &Act{Node: node, Parent: parent, Scope: scope, Idx: idx}
	s.Acts[id] = a
	n := &m.p.Nodes[node]
	if n.Kind != ir.KGraph {
		// Graphs and cond tests are structure, not steps (ADR 0030).
		s.Steps++
		if s.MaxSteps > 0 && s.Steps > s.MaxSteps {
			m.fail(StatusFailed, "max steps exceeded")
			return
		}
	}
	if m.tracing && n.Kind != ir.KStep && n.Kind != ir.KMap && n.Kind != ir.KLoop {
		m.traceStart(id, a, n, nil) // maps and loops trace their input below
	}
	switch n.Kind {
	case ir.KStep:
		if n.Spec.Effect == ir.EffectProtected {
			if m.tracing {
				m.traceStart(id, a, n, m.buildInput(n, scope))
			}
			// Evaluated in the core; never leaves the process.
			out, err := m.protected(n, scope)
			if err != nil {
				if n.OnError != ir.OnErrorFail {
					typ := "error"
					var ce *condError
					if errors.As(err, &ce) {
						typ = ce.typ
					}
					m.exception(id, a, n, err.Error(), typ)
					return
				}
				typ := "error"
				var ce *condError
				if errors.As(err, &ce) {
					typ = ce.typ
				}
				m.traceEnd(id, a, nil, "failed", err.Error(), typ)
				m.failAt(id, "step "+m.stepID(id, a)+": "+err.Error())
				return
			}
			m.complete(id, a, out)
			return
		}
		m.dispatch(id, a)

	case ir.KGraph:
		m.startGraph(id, a, n)

	case ir.KMap:
		list := m.resolve(n.Over, scope)
		m.traceStart(id, a, n, list)
		var items []json.RawMessage
		if err := json.Unmarshal(list, &items); err != nil || bytes.Equal(bytes.TrimSpace(list), null) {
			m.failAt(id, "map "+n.ID+": value to map over is not a list")
			return
		}
		if len(items) == 0 {
			m.complete(id, a, json.RawMessage("[]"))
			return
		}
		a.Items = items
		a.Results = make([]json.RawMessage, len(items))
		conc := int(n.MaxConc)
		if conc <= 0 || conc > len(items) {
			conc = len(items)
		}
		for s.Acts[id] != nil && int(a.Pending) < conc && int(a.Pos) < len(a.Items) {
			m.launchElement(id, a, n)
		}

	case ir.KLoop:
		m.startLoop(id, a, n, node)

	case ir.KWait:
		if n.Signal != "" {
			if q := s.Mailbox[n.Signal]; len(q) > 0 {
				p := q[0]
				if len(q) == 1 {
					delete(s.Mailbox, n.Signal)
				} else {
					s.Mailbox[n.Signal] = q[1:]
				}
				m.complete(id, a, signalOutput(p))
				return
			}
			a.Flags |= fSignalWait
			if n.Timeout > 0 {
				m.armTimer(id, a, m.at+n.Timeout.Milliseconds())
			}
			return
		}
		m.armTimer(id, a, m.at+n.Wait.Milliseconds())
	}
}

func (m *machine) launchElement(id uint32, a *Act, n *ir.Node) {
	s := m.s
	i := a.Pos
	a.Pos++
	a.Pending++
	s.NextScope++
	sc := s.NextScope
	s.Scopes[sc] = &Scope{Parent: a.Scope, Map: m.nodeIndex(n), Index: i, Item: a.Items[i]}
	m.trace(Trace{Kind: TrRoundStart, Act: id, Node: a.Node, StepID: m.stepID(id, a), Index: i})
	m.start(n.Children[0], id, sc, i)
}

func (m *machine) nodeIndex(n *ir.Node) int32 {
	// Children[0]'s parent is n.
	return m.p.Nodes[n.Children[0]].Parent
}

func (m *machine) dispatch(id uint32, a *Act) {
	n := &m.p.Nodes[a.Node]
	a.Attempt++
	a.Flags |= fDispatched
	a.Flags &^= fIntent
	m.s.Inflight++
	stepID := m.stepID(id, a)
	in := m.buildInput(n, a.Scope)
	m.out = append(m.out, Command{
		Kind:    CmdDispatch,
		Act:     id,
		Node:    a.Node,
		Attempt: a.Attempt,
		StepID:  stepID,
		IdemKey: m.s.RunID + "/" + stepID,
		Input:   in,
	})
	if a.Attempt == 1 {
		m.traceStart(id, a, n, in) // retries are TrNodeRetry
	}
	if n.Spec.Timeout > 0 {
		m.armTimer(id, a, m.at+time.Duration(n.Spec.Timeout).Milliseconds())
	}
}

func (m *machine) undispatch(a *Act) {
	if a.Flags&fDispatched != 0 {
		a.Flags &^= fDispatched
		m.s.Inflight--
	}
}

// stepID is the node id plus the map indices and loop iterations of every
// enclosing construct, e.g. "summarize[3,1]". It is stable across attempts
// and across replays, so it makes a good idempotency key.
func (m *machine) stepID(id uint32, a *Act) string {
	n := &m.p.Nodes[a.Node]
	var path []int32
	child := a
	for p := a.Parent; p != 0; {
		pa := m.s.Acts[p]
		switch m.p.Nodes[pa.Node].Kind {
		case ir.KMap:
			path = append(path, child.Idx)
		case ir.KLoop:
			path = append(path, pa.Pos)
		}
		child = pa
		p = pa.Parent
	}
	if len(path) == 0 {
		return n.ID
	}
	b := make([]byte, 0, len(n.ID)+2+4*len(path))
	b = append(b, n.ID...)
	b = append(b, '[')
	for i := len(path) - 1; i >= 0; i-- {
		b = strconv.AppendInt(b, int64(path[i]), 10)
		if i > 0 {
			b = append(b, ',')
		}
	}
	b = append(b, ']')
	return string(b)
}

func (m *machine) armTimer(id uint32, a *Act, at int64) {
	m.s.NextTimer++
	a.Timer = m.s.NextTimer
	a.TimerAt = at
	m.out = append(m.out, Command{Kind: CmdTimer, Act: id, Node: a.Node, Timer: a.Timer, At: at})
}

func (m *machine) cancelTimer(a *Act) {
	if a.Timer != 0 {
		m.out = append(m.out, Command{Kind: CmdCancelTimer, Node: a.Node, Timer: a.Timer})
		a.Timer, a.TimerAt = 0, 0
	}
}

func (m *machine) stepFailed(id uint32, a *Act, msg, errType string, retryable, unknown bool) {
	n := &m.p.Nodes[a.Node]
	spec := n.Spec
	if unknown {
		if spec.Effect == ir.EffectReal && !spec.IdempotentRetry {
			// Never treat an unknown outcome of a real command as success,
			// and never retry it blindly: stop and ask, or, where the
			// definition says so, fail without retrying (ADR 0035).
			if n.UnknownFails {
				m.giveUp(id, a, n, msg, "outcome_unknown")
				return
			}
			m.trace(Trace{Kind: TrNodeReview, Act: id, Node: a.Node, StepID: m.stepID(id, a), Attempt: a.Attempt, Err: msg})
			a.Flags |= fReview
			m.s.Status = StatusBlocked
			m.out = append(m.out, Command{Kind: CmdReview, Act: id, Node: a.Node, Attempt: a.Attempt, StepID: m.stepID(id, a)})
			return
		}
		retryable = true
	}
	if retryable && a.Attempt < n.MaxAttempts {
		backoff := n.RetryInterval
		if backoff == 0 {
			backoff = time.Duration(spec.Backoff) << (a.Attempt - 1)
			if backoff > time.Minute {
				backoff = time.Minute
			}
		}
		a.Flags |= fRetryWait
		m.trace(Trace{Kind: TrNodeRetry, Act: id, Node: a.Node, StepID: m.stepID(id, a), Attempt: a.Attempt, Err: msg, ErrType: errType})
		m.armTimer(id, a, m.at+backoff.Milliseconds())
		return
	}
	m.giveUp(id, a, n, msg, errType)
}

// giveUp ends a step that failed for good: through its on_error strategy,
// or by failing its map element or the run.
func (m *machine) giveUp(id uint32, a *Act, n *ir.Node, msg, errType string) {
	if n.OnError != ir.OnErrorFail {
		m.exception(id, a, n, msg, errType)
		return
	}
	m.traceEnd(id, a, nil, "failed", msg, errType)
	m.failAt(id, "step "+m.stepID(id, a)+": "+msg)
}

// exception finishes a failed step through its on_error strategy
// (ADR 0030): its output describes the error (plus the default value), and
// with fail-branch it takes its fail-branch edges.
func (m *machine) exception(id uint32, a *Act, n *ir.Node, msg, errType string) {
	m.s.Exceptions++
	if errType == "" {
		errType = "error"
	}
	var obj map[string]json.RawMessage
	if n.OnError == ir.OnErrorDefault {
		json.Unmarshal(n.ErrValue, &obj) // validated at compile time
	} else {
		a.Flags |= fFailBranch
	}
	if obj == nil {
		obj = map[string]json.RawMessage{}
	}
	obj["error_message"], _ = json.Marshal(msg)
	obj["error_type"], _ = json.Marshal(errType)
	out, _ := json.Marshal(obj) // sorted keys: deterministic
	m.endStatus, m.endErr, m.endType = "exception", msg, errType
	m.complete(id, a, out)
}

func (m *machine) refreshBlocked() {
	if m.s.Status != StatusBlocked {
		return
	}
	for _, a := range m.s.Acts {
		if a.Flags&fReview != 0 {
			return
		}
	}
	m.s.Status = StatusRunning
}

// complete records the output of activation id and advances its parent.
func (m *machine) complete(id uint32, a *Act, out json.RawMessage) {
	s := m.s
	if sc := s.Scopes[a.Scope]; sc != nil {
		if sc.Vals == nil {
			sc.Vals = map[int32]json.RawMessage{}
		}
		sc.Vals[a.Node] = out
	}
	if m.tracing {
		status := "succeeded"
		if m.endStatus != "" {
			status = m.endStatus
		}
		m.traceEnd(id, a, out, status, m.endErr, m.endType)
		m.endStatus, m.endErr, m.endType = "", "", ""
	}
	if a.Parent != 0 {
		if pa := s.Acts[a.Parent]; pa.G == nil && m.p.Nodes[pa.Node].Kind == ir.KGraph {
			m.migrateGraph(a.Parent, pa, &m.p.Nodes[pa.Node])
		}
	}
	delete(s.Acts, id)
	if a.Parent == 0 {
		m.cancelDeadline()
		s.Status = StatusCompleted
		s.Output = out
		m.trace(Trace{Kind: TrRunEnd, Status: s.Status.String(), Output: out})
		m.out = append(m.out, Command{Kind: CmdDone})
		return
	}
	pid := a.Parent
	pa := s.Acts[pid]
	pn := &m.p.Nodes[pa.Node]
	switch pn.Kind {
	case ir.KGraph:
		m.memberDone(pid, pa, a, out)

	case ir.KMap:
		if pn.ElemOut != nil {
			out = m.resolve(*pn.ElemOut, a.Scope) // the element's chosen node
		}
		delete(s.Scopes, a.Scope)
		m.elementDone(pid, pa, pn, a.Idx, out, false)

	case ir.KLoop:
		m.roundDone(pid, pa, pn, out)
	}
}

func joinArray(parts []json.RawMessage) json.RawMessage {
	n := 2
	for _, p := range parts {
		n += len(p) + 1
	}
	b := make([]byte, 0, n)
	b = append(b, '[')
	for i, p := range parts {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, p...)
	}
	b = append(b, ']')
	return b
}

// fail terminates the run, disarming timers and aborting outstanding steps.
func (m *machine) fail(status RunStatus, msg string) {
	s := m.s
	for _, id := range sortedActs(s) {
		a := s.Acts[id]
		if a.Flags&fDispatched != 0 {
			m.out = append(m.out, Command{Kind: CmdAbort, Act: id, Node: a.Node, Attempt: a.Attempt})
		}
		if a.Timer != 0 {
			m.out = append(m.out, Command{Kind: CmdCancelTimer, Node: a.Node, Timer: a.Timer})
		}
	}
	clear(s.Acts)
	m.cancelDeadline()
	s.Inflight = 0
	s.Status = status
	s.Error = msg
	m.trace(Trace{Kind: TrRunEnd, Status: status.String(), Err: msg})
	m.out = append(m.out, Command{Kind: CmdDone})
}

func (m *machine) cancelDeadline() {
	if m.s.DeadlineTimer != 0 {
		m.out = append(m.out, Command{Kind: CmdCancelTimer, Timer: m.s.DeadlineTimer})
		m.s.DeadlineTimer = 0
	}
}

// recover re-issues everything that was in flight when the process died.
func (m *machine) recover() {
	s := m.s
	if s.DeadlineTimer != 0 {
		m.out = append(m.out, Command{Kind: CmdTimer, Timer: s.DeadlineTimer, At: s.Deadline})
	}
	for _, id := range sortedActs(s) {
		a := s.Acts[id]
		if s.Acts[id] == nil {
			continue
		}
		n := &m.p.Nodes[a.Node]
		switch {
		case a.Flags&fReview != 0:
			m.out = append(m.out, Command{Kind: CmdReview, Act: id, Node: a.Node, Attempt: a.Attempt, StepID: m.stepID(id, a)})
		case a.Flags&fDispatched != 0:
			if a.Timer != 0 {
				// A step timeout timer; the step is re-issued below.
				m.cancelTimer(a)
			}
			if n.Spec.Effect == ir.EffectReal && a.Flags&fIntent != 0 {
				// It may have been executed: the outcome is unknown.
				m.undispatch(a)
				m.stepFailed(id, a, "outcome unknown after restart", "outcome_unknown", true, true)
				continue
			}
			// Unprotected, or real but provably never released.
			m.undispatch(a)
			m.dispatch(id, a)
		case a.Timer != 0:
			m.out = append(m.out, Command{Kind: CmdTimer, Act: id, Node: a.Node, Timer: a.Timer, At: a.TimerAt})
		}
		if s.Status.Done() {
			return
		}
	}
}

// StepID returns the step id (node id plus iteration path) of activation
// id, or "" if it does not exist.
func StepID(p *ir.Plan, s *State, id uint32) string {
	a := s.Acts[id]
	if a == nil {
		return ""
	}
	m := machine{p: p, s: s}
	return m.stepID(id, a)
}

// Wait describes a step waiting for a signal.
type Wait struct {
	Act    uint32 `json:"act"`
	StepID string `json:"step_id"`
	Signal string `json:"signal"`
	// Item is the map element the waiting step belongs to, if any (for an
	// approval gate inside a map: the element being approved).
	Item json.RawMessage `json:"item,omitempty"`
}

// Waits lists the steps currently waiting for a signal, in activation order.
func Waits(p *ir.Plan, s *State) []Wait {
	var out []Wait
	for _, id := range sortedActs(s) {
		a := s.Acts[id]
		if a.Flags&fSignalWait != 0 {
			w := Wait{Act: id, StepID: StepID(p, s, id), Signal: p.Nodes[a.Node].Signal}
			if sc := s.Scopes[a.Scope]; sc != nil && sc.Map >= 0 {
				w.Item = sc.Item
			}
			out = append(out, w)
		}
	}
	return out
}
