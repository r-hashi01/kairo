package core

import (
	"encoding/json"

	"kairo/ir"
)

// Traces (ADR 0034): what happened while an event was applied, for the
// execution event feed. They depend only on the applied events, so
// replaying the log yields the same traces in the same order; they are
// therefore not logged themselves.

type TraceKind uint8

const (
	TrRunStart   TraceKind = iota + 1 // Input
	TrNodeStart                       // Act, Node, StepID, Attempt, Input (steps, waits, maps, loops)
	TrNodeEnd                         // Status succeeded|exception|failed, Output, Handle, Err, ErrType, Meta
	TrNodeSkip                        // Node, StepID: a graph member that will not run
	TrNodeRetry                       // Attempt (the one that failed), Err, ErrType
	TrNodeReview                      // a real step with an unknown outcome waits for an operator
	TrRoundStart                      // Node (map or loop), Index
	TrRoundEnd                        // Node, Index
	TrVarUpdate                       // Var, Loop (-1: a run variable), Output (the new value)
	TrRunEnd                          // Status, Output, Err
)

// Trace is one record of the feed. At is the time of the event applied.
type Trace struct {
	Kind    TraceKind
	At      int64
	Act     uint32
	Node    int32
	StepID  string
	Attempt int32
	Index   int32
	Status  string
	Input   json.RawMessage
	Output  json.RawMessage
	Handle  string
	Err     string
	ErrType string
	Meta    json.RawMessage
	Var     string
	Loop    int32
}

// ApplyTraced is Apply that also appends the traces of the transition to
// tr.
func ApplyTraced(p *ir.Plan, s *State, ev *Event, out []Command, tr []Trace) ([]Command, []Trace, error) {
	if s.Status.Done() {
		return out, tr, ErrIgnored
	}
	m := machine{p: p, s: s, at: ev.At, out: out, tracing: true, tr: tr, meta: ev.Meta, metaAct: ev.Act}
	n := len(tr)
	err := m.apply(ev)
	if err != nil {
		// Ignored events change nothing, and leave no traces.
		return m.out, m.tr[:n], err
	}
	return m.out, m.tr, nil
}

func (m *machine) trace(t Trace) {
	if !m.tracing {
		return
	}
	t.At = m.at
	m.tr = append(m.tr, t)
}

// traceStart records that activation id of node n started, unless it is
// structure (a graph).
func (m *machine) traceStart(id uint32, a *Act, n *ir.Node, input json.RawMessage) {
	if !m.tracing || n.Kind == ir.KGraph {
		return
	}
	m.trace(Trace{Kind: TrNodeStart, Act: id, Node: a.Node, StepID: m.stepID(id, a), Attempt: a.Attempt, Input: input})
}

// traceEnd records that activation id finished with out.
func (m *machine) traceEnd(id uint32, a *Act, out json.RawMessage, status, errMsg, errType string) {
	if !m.tracing {
		return
	}
	n := &m.p.Nodes[a.Node]
	if n.Kind == ir.KGraph {
		return
	}
	t := Trace{Kind: TrNodeEnd, Act: id, Node: a.Node, StepID: m.stepID(id, a), Attempt: a.Attempt,
		Status: status, Output: out, Err: errMsg, ErrType: errType, Handle: ir.HandleSource}
	switch {
	case a.Flags&fFailBranch != 0:
		t.Handle = ir.HandleFailBranch
	case n.Kind == ir.KStep && len(n.Handles) > 0:
		var h string
		json.Unmarshal(extract(out, []string{n.Spec.Branch}), &h)
		t.Handle = h
	}
	if id == m.metaAct && (m.p.Nodes[a.Node].Kind == ir.KStep) {
		t.Meta = m.meta // the executor's process_data and metadata
	}
	m.trace(t)
}
