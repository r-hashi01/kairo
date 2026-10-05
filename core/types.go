// Package core is the state-transition core:
//
//	(state, event) -> (new state, commands)
//
// Apply is deterministic and performs no I/O: it does not read the clock
// (time arrives inside events), touch the network or the filesystem, start
// goroutines, or iterate Go maps in an order-dependent way. Replaying the
// same event sequence from an empty state always yields the same state.
//
// Parallelism inside a run is not threads: it is simply several commands
// being outstanding at once.
package core

import (
	"encoding/json"

	"kairo/ir"
)

type RunStatus uint8

const (
	StatusRunning RunStatus = iota
	// StatusBlocked: at least one real step has an unknown outcome and is
	// waiting for an operator to resolve it ("needs review").
	StatusBlocked
	StatusCompleted
	StatusFailed
	StatusCancelled
)

var statusNames = []string{"running", "blocked", "completed", "failed", "cancelled"}

func (s RunStatus) String() string { return statusNames[s] }
func (s RunStatus) Done() bool     { return s >= StatusCompleted }

func (s RunStatus) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

type EventKind uint8

const (
	EvStart   EventKind = iota + 1 // Data = run input; Name = entry node of a root graph ("" = all)
	EvStepOK                       // Act, Attempt, Data = output
	EvStepErr                      // Act, Attempt, Err, Retryable, Unknown
	EvTimer                        // Act, Timer
	EvSignal                       // Name, Data = payload
	EvIntent                       // Act, Attempt: intent to execute a real command is durable
	EvRecover                      // the process restarted; re-issue what was in flight
	EvResolve                      // Act, Data or Err: operator resolution of a needs-review step
	EvCancel                       // Err = reason
)

// Event is an input to the core. Everything the core needs, including the
// current time, is carried in the event, so the log of events is a complete
// description of the run.
type Event struct {
	Kind      EventKind
	At        int64 // unix milliseconds, assigned by the shard
	Act       uint32
	Attempt   int32
	Timer     uint32
	Name      string
	Data      json.RawMessage
	Err       string
	Retryable bool
	Unknown   bool   // outcome unknown (timeout, disconnect)
	ErrType   string // EvStepErr: the executor's error class (ADR 0030)

	// EvStart: limits of the run (ADR 0030). Deadline is absolute (unix
	// ms), so replaying the log reproduces it; 0 means none.
	MaxSteps int32
	Deadline int64
	Depth    int32 // nesting depth of the run (a workflow called as a tool)
	// EvStart: initial values of the run variables (an object, ADR 0033).
	Vars json.RawMessage
	// EvStepOK / EvStepErr: the executor's process data and metadata,
	// passed through to the step's trace (ADR 0034).
	Meta json.RawMessage
}

type CmdKind uint8

const (
	CmdDispatch    CmdKind = iota + 1 // run a step on an executor
	CmdTimer                          // arm timer Timer at At
	CmdCancelTimer                    // disarm timer Timer
	CmdAbort                          // best-effort cancellation of an outstanding step
	CmdReview                         // a real step's outcome is unknown; needs an operator
	CmdDone                           // the run reached a terminal status
)

// Command is an output of the core. The effect type of a dispatch is
// Plan.Nodes[Node].Effect().
type Command struct {
	Kind    CmdKind
	Act     uint32
	Node    int32
	Attempt int32
	Timer   uint32
	At      int64
	StepID  string // node id + iteration path, e.g. "send[2]"
	IdemKey string // stable across attempts
	Input   json.RawMessage
}

const (
	fDispatched uint8 = 1 << iota
	fIntent           // intent for the current attempt is durable (real steps)
	fRetryWait
	fReview
	fSignalWait
	fFailBranch // finished as an exception that takes the fail-branch edges
)

// Act is one activation of a plan node.
type Act struct {
	Node    int32
	Parent  uint32 // activation id of the parent, 0 for the root
	Scope   uint32
	Idx     int32 // child position in parent, or map element index
	Pos     int32 // map: next element; loop: iterations done
	Pending int32 // graph/map: children outstanding
	Attempt int32
	Flags   uint8
	Timer   uint32
	TimerAt int64
	Results []json.RawMessage // map
	Items   []json.RawMessage // map elements
	G       *Graph            // graph: member and edge states
}

// Member states of a graph activation.
const (
	mUnrun uint8 = iota
	mRunning
	mDone
	mSkipped
)

// Edge states of a graph activation.
const (
	eUnknown uint8 = iota
	eTaken
	eSkipped
)

// Graph is the state of a graph activation (ADR 0029): one byte per
// member and per edge of the plan's graph node.
type Graph struct {
	Members []uint8
	Edges   []uint8
}

// Scope holds node values. Scope 0 is the run's root scope; each map element
// gets its own scope, dropped when the element finishes.
type Scope struct {
	Parent uint32
	Map    int32 // map node that created it, -1 for root
	Index  int32
	Item   json.RawMessage
	Vals   map[int32]json.RawMessage
}

// State is the whole state of one run. Its cost is its byte size.
type State struct {
	RunID     string
	Status    RunStatus
	Input     json.RawMessage
	Output    json.RawMessage
	Error     string
	NextAct   uint32
	NextScope uint32
	NextTimer uint32
	Inflight  int32 // dispatched steps
	// Limits and counters (ADR 0030).
	Steps         int32 // activations started (steps, waits, containers)
	MaxSteps      int32
	Exceptions    int32 // steps that failed into on_error
	Deadline      int64
	DeadlineTimer uint32
	Depth         int32
	Acts          map[uint32]*Act
	Scopes        map[uint32]*Scope
	// Signals that arrived before anything waited for them.
	Mailbox map[string][]json.RawMessage
}

func NewState(runID string) *State {
	return &State{
		RunID:  runID,
		Acts:   map[uint32]*Act{},
		Scopes: map[uint32]*Scope{0: {Map: -1}},
	}
}

// Quiescent reports whether the run has nothing outstanding on executors:
// it is only waiting for timers, signals or an operator. Such a run can be
// snapshotted and evicted from memory.
func (s *State) Quiescent() bool { return s.Inflight == 0 && !s.Status.Done() }

// NodeEffect is a helper for callers holding a command.
func NodeEffect(p *ir.Plan, c *Command) ir.Effect { return p.Nodes[c.Node].Effect() }

// Dispatched reports whether activation act is outstanding on an executor
// with attempt attempt (not aborted, finished or superseded since).
func Dispatched(s *State, act uint32, attempt int32) bool {
	a := s.Acts[act]
	return a != nil && a.Flags&fDispatched != 0 && a.Attempt == attempt
}

// NeedsReview reports whether the activation is a real step stopped with an
// unknown outcome.
func (a *Act) NeedsReview() bool { return a.Flags&fReview != 0 }
