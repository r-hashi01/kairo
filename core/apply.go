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
	p   *ir.Plan
	s   *State
	at  int64
	out []Command
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
		m.stepFailed(ev.Act, a, ev.Err, ev.Retryable, ev.Unknown)
		return nil

	case EvIntent:
		a := s.Acts[ev.Act]
		if a == nil || a.Flags&fDispatched == 0 || a.Attempt != ev.Attempt {
			return ErrIgnored
		}
		a.Flags |= fIntent
		return nil

	case EvTimer:
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
		case a.Flags&fRetryWait != 0:
			a.Flags &^= fRetryWait
			m.dispatch(ev.Act, a)
		case a.Flags&fDispatched != 0:
			// Step timeout: the outcome is unknown.
			m.out = append(m.out, Command{Kind: CmdAbort, Act: ev.Act, Node: a.Node, Attempt: a.Attempt})
			m.undispatch(a)
			m.stepFailed(ev.Act, a, "timeout", true, true)
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
	switch n.Kind {
	case ir.KStep:
		if n.Spec.Effect == ir.EffectProtected {
			// Evaluated in the core; never leaves the process.
			out, err := m.protected(n, scope)
			if err != nil {
				m.fail(StatusFailed, "step "+m.stepID(id, a)+": "+err.Error())
				return
			}
			m.complete(id, a, out)
			return
		}
		m.dispatch(id, a)

	case ir.KSeq:
		if len(n.Children) == 0 {
			m.complete(id, a, null)
			return
		}
		m.start(n.Children[0], id, scope, 0)

	case ir.KPar:
		if len(n.Children) == 0 {
			m.complete(id, a, json.RawMessage("{}"))
			return
		}
		a.Results = make([]json.RawMessage, len(n.Children))
		a.Pending = int32(len(n.Children))
		for i, c := range n.Children {
			if s.Acts[id] == nil { // failed while starting siblings
				return
			}
			m.start(c, id, scope, int32(i))
		}

	case ir.KCond:
		if m.evalPred(n.Pred, scope) {
			m.start(n.Children[0], id, scope, 0)
		} else if len(n.Children) > 1 {
			m.start(n.Children[1], id, scope, 1)
		} else {
			m.complete(id, a, null)
		}

	case ir.KMap:
		list := m.resolve(n.Over, scope)
		var items []json.RawMessage
		if err := json.Unmarshal(list, &items); err != nil || bytes.Equal(bytes.TrimSpace(list), null) {
			m.fail(StatusFailed, "map "+n.ID+": value to map over is not a list")
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
		m.start(n.Children[0], id, scope, 0)

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
	m.out = append(m.out, Command{
		Kind:    CmdDispatch,
		Act:     id,
		Node:    a.Node,
		Attempt: a.Attempt,
		StepID:  stepID,
		IdemKey: m.s.RunID + "/" + stepID,
		Input:   m.buildInput(n, a.Scope),
	})
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

func (m *machine) stepFailed(id uint32, a *Act, msg string, retryable, unknown bool) {
	n := &m.p.Nodes[a.Node]
	spec := n.Spec
	if unknown {
		if spec.Effect == ir.EffectReal && !spec.IdempotentRetry {
			// Never treat an unknown outcome of a real command as success,
			// and never retry it blindly: stop and ask.
			a.Flags |= fReview
			m.s.Status = StatusBlocked
			m.out = append(m.out, Command{Kind: CmdReview, Act: id, Node: a.Node, Attempt: a.Attempt, StepID: m.stepID(id, a)})
			return
		}
		retryable = true
	}
	if retryable && int(a.Attempt) < spec.MaxAttempts {
		backoff := time.Duration(spec.Backoff) << (a.Attempt - 1)
		if backoff > time.Minute {
			backoff = time.Minute
		}
		a.Flags |= fRetryWait
		m.armTimer(id, a, m.at+backoff.Milliseconds())
		return
	}
	m.fail(StatusFailed, "step "+m.stepID(id, a)+": "+msg)
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
	delete(s.Acts, id)
	if a.Parent == 0 {
		s.Status = StatusCompleted
		s.Output = out
		m.out = append(m.out, Command{Kind: CmdDone})
		return
	}
	pid := a.Parent
	pa := s.Acts[pid]
	pn := &m.p.Nodes[pa.Node]
	switch pn.Kind {
	case ir.KSeq:
		pa.Pos++
		if int(pa.Pos) < len(pn.Children) {
			m.start(pn.Children[pa.Pos], pid, pa.Scope, pa.Pos)
			return
		}
		m.complete(pid, pa, out)

	case ir.KPar:
		pa.Results[a.Idx] = out
		pa.Pending--
		if pa.Pending == 0 {
			m.complete(pid, pa, m.parOutput(pn, pa))
		}

	case ir.KCond:
		m.complete(pid, pa, out)

	case ir.KMap:
		pa.Results[a.Idx] = out
		pa.Pending--
		delete(s.Scopes, a.Scope)
		if int(pa.Pos) < len(pa.Items) {
			m.launchElement(pid, pa, pn)
			return
		}
		if pa.Pending == 0 {
			res := joinArray(pa.Results)
			pa.Results, pa.Items = nil, nil
			m.complete(pid, pa, res)
		}

	case ir.KLoop:
		pa.Pos++
		if int(pa.Pos) < int(pn.MaxIter) && m.evalPred(pn.Pred, pa.Scope) {
			m.start(pn.Children[0], pid, pa.Scope, 0)
			return
		}
		m.complete(pid, pa, out)
	}
}

func (m *machine) parOutput(pn *ir.Node, pa *Act) json.RawMessage {
	var b []byte
	b = append(b, '{')
	for i, c := range pn.Children {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendQuote(b, m.p.Nodes[c].ID)
		b = append(b, ':')
		b = append(b, pa.Results[i]...)
	}
	b = append(b, '}')
	return b
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
	s.Inflight = 0
	s.Status = status
	s.Error = msg
	m.out = append(m.out, Command{Kind: CmdDone})
}

// recover re-issues everything that was in flight when the process died.
func (m *machine) recover() {
	s := m.s
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
				m.stepFailed(id, a, "outcome unknown after restart", true, true)
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
