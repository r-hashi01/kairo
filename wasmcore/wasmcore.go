// Package wasmcore is the pure core of kairo as a host embeds it (ADR
// 0051): node specs and plans compiled once, then events applied to a
// run's state given as bytes. It does no I/O; the host reads and writes
// the state, records the events and carries out the commands.
//
// Events and commands cross as JSON with names of their own (not the Go
// field names), so that the interface holds when the core's types change.
// The state crosses as the core's encoding (core.State.Encode).
//
// cmd/kairo-wasm exports it from a WASM module; Go hosts may use it
// directly.
package wasmcore

import (
	"encoding/json"
	"errors"
	"fmt"

	"kairo/core"
	"kairo/ir"
)

// ABIVersion changes when the JSON or the exports change incompatibly.
const ABIVersion = 1

// Core holds the node specs and the compiled plans of one host.
type Core struct {
	reg   *ir.Registry
	plans []*ir.Plan
}

func New() *Core { return &Core{reg: ir.NewRegistry()} }

// Register registers node specs (a JSON array of ir.NodeSpec).
func (c *Core) Register(specsJSON []byte) error {
	var specs []ir.NodeSpec
	if err := json.Unmarshal(specsJSON, &specs); err != nil {
		return err
	}
	for _, s := range specs {
		if s.Action == "" {
			return errors.New("wasmcore: node spec without action")
		}
		c.reg.Register(s)
	}
	return nil
}

// Compiled is what Compile answers besides the plan's handle.
type Compiled struct {
	Plan    int               `json:"plan"`
	Name    string            `json:"name"`
	Hash    string            `json:"hash"`
	HasReal bool              `json:"has_real"`
	Effects map[string]string `json:"effects"`
}

// Compile compiles a workflow definition (ir.Definition as JSON).
func (c *Core) Compile(defJSON []byte) (Compiled, error) {
	def, err := ir.ParseDefinition(defJSON)
	if err != nil {
		return Compiled{}, err
	}
	p, err := ir.Compile(def, c.reg)
	if err != nil {
		return Compiled{}, err
	}
	c.plans = append(c.plans, p)
	effects := map[string]string{}
	for _, s := range p.Specs() {
		effects[s.Action] = s.Effect.String()
	}
	return Compiled{Plan: len(c.plans) - 1, Name: p.Name, Hash: p.Hash, HasReal: p.HasReal, Effects: effects}, nil
}

// Event is an input to a run (core.Event).
type Event struct {
	Kind      string          `json:"kind"` // see eventKinds
	At        int64           `json:"at"`   // unix ms
	Act       uint32          `json:"act,omitempty"`
	Attempt   int32           `json:"attempt,omitempty"`
	Timer     uint32          `json:"timer,omitempty"`
	Name      string          `json:"name,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	Err       string          `json:"error,omitempty"`
	ErrType   string          `json:"error_type,omitempty"`
	Retryable bool            `json:"retryable,omitempty"`
	Unknown   bool            `json:"unknown,omitempty"`
	MaxSteps  int32           `json:"max_steps,omitempty"`
	Deadline  int64           `json:"deadline,omitempty"`
	Depth     int32           `json:"depth,omitempty"`
	Vars      json.RawMessage `json:"vars,omitempty"`
	Meta      json.RawMessage `json:"meta,omitempty"`
}

var eventKinds = map[string]core.EventKind{
	"start":     core.EvStart,
	"step_ok":   core.EvStepOK,
	"step_err":  core.EvStepErr,
	"timer":     core.EvTimer,
	"signal":    core.EvSignal,
	"intent":    core.EvIntent,
	"recover":   core.EvRecover,
	"resolve":   core.EvResolve,
	"cancel":    core.EvCancel,
	"step_wait": core.EvStepWait,
}

var commandKinds = map[core.CmdKind]string{
	core.CmdDispatch:    "dispatch",
	core.CmdTimer:       "timer",
	core.CmdCancelTimer: "cancel_timer",
	core.CmdAbort:       "abort",
	core.CmdReview:      "review",
	core.CmdDone:        "done",
}

// Command is an output of a run (core.Command), with the effect of its
// step so that the host knows whether to hold it for a durable intent.
type Command struct {
	Kind    string          `json:"kind"` // see commandKinds
	Act     uint32          `json:"act,omitempty"`
	Node    string          `json:"node,omitempty"` // the plan node's id
	Action  string          `json:"action,omitempty"`
	Effect  string          `json:"effect,omitempty"`
	Attempt int32           `json:"attempt,omitempty"`
	Timer   uint32          `json:"timer,omitempty"`
	At      int64           `json:"at,omitempty"`
	StepID  string          `json:"step_id,omitempty"`
	IdemKey string          `json:"idempotency_key,omitempty"`
	Input   json.RawMessage `json:"input,omitempty"`
}

var traceKinds = map[core.TraceKind]string{
	core.TrRunStart:   "run_start",
	core.TrNodeStart:  "node_start",
	core.TrNodeEnd:    "node_end",
	core.TrNodeSkip:   "node_skip",
	core.TrNodeRetry:  "node_retry",
	core.TrNodeReview: "node_review",
	core.TrRoundStart: "round_start",
	core.TrRoundEnd:   "round_end",
	core.TrVarUpdate:  "var_update",
	core.TrRunEnd:     "run_end",
	core.TrNodeWait:   "node_wait",
}

// Trace is one record of what a transition did (core.Trace, ADR 0034).
type Trace struct {
	Kind    string          `json:"kind"` // see traceKinds
	At      int64           `json:"at"`
	Act     uint32          `json:"act,omitempty"`
	Node    string          `json:"node,omitempty"` // the plan node's id
	StepID  string          `json:"step_id,omitempty"`
	Attempt int32           `json:"attempt,omitempty"`
	Index   int32           `json:"index,omitempty"`
	Status  string          `json:"status,omitempty"`
	Input   json.RawMessage `json:"input,omitempty"`
	Output  json.RawMessage `json:"output,omitempty"`
	Handle  string          `json:"handle,omitempty"`
	Err     string          `json:"error,omitempty"`
	ErrType string          `json:"error_type,omitempty"`
	Meta    json.RawMessage `json:"meta,omitempty"`
	Var     string          `json:"var,omitempty"`
	Loop    string          `json:"loop,omitempty"` // var_update of a loop variable: the loop's id
	Until   int64           `json:"until,omitempty"`
}

// Result is what applying an event answers besides the new state.
type Result struct {
	// Ignored: the event did not apply (a stale or duplicate one). The
	// state is unchanged and the event must not be recorded (invariant 3).
	Ignored  bool            `json:"ignored,omitempty"`
	Commands []Command       `json:"commands"`
	Traces   []Trace         `json:"traces,omitempty"`
	Status   string          `json:"status"`
	Output   json.RawMessage `json:"output,omitempty"`
	Error    string          `json:"error,omitempty"`
	// Quiescent: nothing is in flight; the run only waits (timers,
	// signals, reviews), and the host need not keep it in memory.
	Quiescent bool `json:"quiescent"`
}

// Apply applies an event (Event as JSON) to a run's state (empty for a new
// run, whose id is runID) and returns the new state and the result. With
// traced, the result carries the transition's traces (ADR 0034).
func (c *Core) Apply(plan int, runID string, state, eventJSON []byte, traced bool) ([]byte, Result, error) {
	if plan < 0 || plan >= len(c.plans) {
		return nil, Result{}, fmt.Errorf("wasmcore: no plan %d", plan)
	}
	p := c.plans[plan]
	var s *core.State
	if len(state) == 0 {
		s = core.NewState(runID)
	} else {
		var err error
		if s, err = core.DecodeState(state); err != nil {
			return nil, Result{}, err
		}
	}
	var e Event
	if err := json.Unmarshal(eventJSON, &e); err != nil {
		return nil, Result{}, err
	}
	kind, ok := eventKinds[e.Kind]
	if !ok {
		return nil, Result{}, fmt.Errorf("wasmcore: unknown event kind %q", e.Kind)
	}
	ev := core.Event{Kind: kind, At: e.At, Act: e.Act, Attempt: e.Attempt, Timer: e.Timer, Name: e.Name, Data: e.Data,
		Err: e.Err, ErrType: e.ErrType, Retryable: e.Retryable, Unknown: e.Unknown, MaxSteps: e.MaxSteps,
		Deadline: e.Deadline, Depth: e.Depth, Vars: e.Vars, Meta: e.Meta}
	var cmds []core.Command
	var traces []core.Trace
	var err error
	if traced {
		cmds, traces, err = core.ApplyTraced(p, s, &ev, nil, nil)
	} else {
		cmds, err = core.Apply(p, s, &ev, nil)
	}
	if errors.Is(err, core.ErrIgnored) {
		return state, Result{Ignored: true, Commands: []Command{}, Status: s.Status.String(), Quiescent: s.Quiescent()}, nil
	}
	if err != nil {
		return nil, Result{}, err
	}
	r := Result{Commands: make([]Command, 0, len(cmds)), Status: s.Status.String(), Output: s.Output,
		Error: s.Error, Quiescent: s.Quiescent()}
	for _, cm := range cmds {
		out := Command{Kind: commandKinds[cm.Kind], Act: cm.Act, Attempt: cm.Attempt, Timer: cm.Timer, At: cm.At,
			StepID: cm.StepID, IdemKey: cm.IdemKey, Input: cm.Input}
		if cm.Node >= 0 && int(cm.Node) < len(p.Nodes) {
			n := &p.Nodes[cm.Node]
			out.Node = n.ID
			if cm.Kind == core.CmdDispatch {
				out.Action, out.Effect = n.Spec.Action, n.Effect().String()
			}
		}
		r.Commands = append(r.Commands, out)
	}
	nodeID := func(i int32) string {
		if i >= 0 && int(i) < len(p.Nodes) {
			return p.Nodes[i].ID
		}
		return ""
	}
	for _, t := range traces {
		out := Trace{Kind: traceKinds[t.Kind], At: t.At, Act: t.Act, StepID: t.StepID, Attempt: t.Attempt,
			Index: t.Index, Status: t.Status, Input: t.Input, Output: t.Output, Handle: t.Handle, Err: t.Err,
			ErrType: t.ErrType, Meta: t.Meta, Var: t.Var, Until: t.Until}
		if t.Kind != core.TrRunStart && t.Kind != core.TrRunEnd && t.Kind != core.TrVarUpdate {
			out.Node = nodeID(t.Node)
		}
		if t.Kind == core.TrVarUpdate {
			out.Loop = nodeID(t.Loop)
		}
		r.Traces = append(r.Traces, out)
	}
	return s.Encode(nil), r, nil
}
