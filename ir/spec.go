// Package ir defines the runtime-independent execution plan: the definition
// format, node specifications (effect types declared by node
// implementations) and the compiler that turns a definition into an
// immutable Plan shared by every run.
package ir

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Effect is the type of an instruction (Jim Gray's three classes plus Wait).
//
// The zero value is EffectReal on purpose: anything whose effect is not
// declared is treated as an irreversible external side effect.
type Effect uint8

const (
	// EffectReal: irreversible external side effect. Requires an
	// idempotency key and follows the outbox ordering rules.
	EffectReal Effect = iota
	// EffectUnprotected: no undo needed, safe to re-execute (LLM call, read,
	// search). Losing the result only costs a re-execution.
	EffectUnprotected
	// EffectProtected: a transition of the runtime's own state. Evaluated
	// inside the core; never leaves the process.
	EffectProtected
	// EffectWait: waiting for time or an external event.
	EffectWait
)

func (e Effect) String() string {
	switch e {
	case EffectReal:
		return "real"
	case EffectUnprotected:
		return "unprotected"
	case EffectProtected:
		return "protected"
	case EffectWait:
		return "wait"
	}
	return fmt.Sprintf("effect(%d)", e)
}

func (e Effect) MarshalText() ([]byte, error) { return []byte(e.String()), nil }

func (e *Effect) UnmarshalText(b []byte) error {
	switch string(b) {
	case "real", "":
		*e = EffectReal
	case "unprotected":
		*e = EffectUnprotected
	case "protected":
		*e = EffectProtected
	case "wait":
		*e = EffectWait
	default:
		return fmt.Errorf("unknown effect %q", b)
	}
	return nil
}

// FieldKind is the type of a typed output/input field. Conditions may only
// branch on Bool, Int, Number and Enum fields; Text is free-form and is
// rejected by the compiler as a branch condition.
type FieldKind uint8

const (
	FieldAny FieldKind = iota
	FieldText
	FieldBool
	FieldInt
	FieldNumber
	FieldEnum
	// FieldList: a list the plan maps over. Like every declared field it
	// stays inline when the payload moves to a blob (regardless of size),
	// so a map can always read it without I/O. Not branchable.
	FieldList
)

var fieldKindNames = map[FieldKind]string{
	FieldAny: "any", FieldText: "text", FieldBool: "bool",
	FieldInt: "int", FieldNumber: "number", FieldEnum: "enum", FieldList: "list",
}

func (k FieldKind) String() string { return fieldKindNames[k] }

func (k FieldKind) MarshalText() ([]byte, error) { return []byte(k.String()), nil }

func (k *FieldKind) UnmarshalText(b []byte) error {
	for kk, name := range fieldKindNames {
		if name == string(b) {
			*k = kk
			return nil
		}
	}
	if string(b) == "string" {
		*k = FieldText
		return nil
	}
	return fmt.Errorf("unknown field type %q", b)
}

// Branchable reports whether a condition may test a field of this kind.
func (k FieldKind) Branchable() bool {
	return k == FieldBool || k == FieldInt || k == FieldNumber || k == FieldEnum
}

type FieldType struct {
	Type   FieldKind `json:"type"`
	Values []string  `json:"values,omitempty"` // for enum
}

// NodeSpec is what a node implementation declares about itself. It is
// resolved when a plan is compiled and never changes during execution.
type NodeSpec struct {
	Action string `json:"action"`
	Effect Effect `json:"effect"`
	// IdempotentRetry declares that a real node may be retried with the same
	// idempotency key when its outcome is unknown (timeout, disconnect).
	// Without it an unknown outcome stops the step in "needs review".
	IdempotentRetry bool `json:"idempotent_retry,omitempty"`
	// MaxAttempts bounds retries on definite, retryable failures.
	// Defaults: 3 for unprotected, 1 for real.
	MaxAttempts int `json:"max_attempts,omitempty"`
	// Backoff is the base retry delay (doubled per attempt).
	Backoff Duration `json:"backoff,omitempty"`
	// Timeout is how long a dispatched command may be outstanding before
	// its outcome is considered unknown. Zero means no timeout.
	Timeout Duration `json:"timeout,omitempty"`
	// Destination is the rate-limit key (e.g. "openai/gpt-4o").
	// Defaults to Action.
	Destination string `json:"destination,omitempty"`
	// Outputs declares typed fields of the node's output object. Typed
	// fields stay inline in the run state even when the payload is moved
	// out to a blob, so conditions can always be evaluated.
	Outputs map[string]FieldType `json:"outputs,omitempty"`
	// Branch names an enum field of Outputs whose value chooses the node's
	// outgoing edges in a graph (ADR 0029): the edges whose handle equals
	// the value are taken, the others skipped. The enum's values are the
	// node's handles.
	Branch string `json:"branch,omitempty"`
	// Resource is what a task of the node mostly uses (ADR 0039):
	// ResourceIO (the default: it waits for the outside, as an LLM or an
	// HTTP call) or ResourceCPU (it computes). Executors size how many
	// tasks they take by it.
	Resource string `json:"resource,omitempty"`
}

// Resources of NodeSpec.Resource.
const (
	ResourceIO  = "io"
	ResourceCPU = "cpu"
)

func (s *NodeSpec) normalize() {
	if s.MaxAttempts <= 0 {
		if s.Effect == EffectReal && !s.IdempotentRetry {
			s.MaxAttempts = 1
		} else {
			s.MaxAttempts = 3
		}
	}
	if s.Backoff == 0 {
		s.Backoff = Duration(200 * time.Millisecond)
	}
	if s.Destination == "" {
		s.Destination = s.Action
	}
}

// Duration is a time.Duration that marshals as a Go duration string.
type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) { return []byte(time.Duration(d).String()), nil }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Built-in protected actions. They are evaluated inside the core and never
// leave the process.
//
// ActionPass (and any other action declared protected) outputs its step
// params merged with its resolved input object (input wins). Declaring a
// protected action with typed Outputs turns a pass-through into a typed
// value that conditions may branch on.
//
// ActionAppend outputs one list: its inputs in name order, arrays spliced
// in, nulls skipped, other values appended as single elements.
//
// ActionSwitch evaluates Dify-style conditions (ADR 0031) and branches on
// the first case that holds ("false" if none).
const (
	ActionPass   = "kairo.pass"
	ActionAppend = "kairo.append"
	ActionSwitch = "kairo.switch"
	// ActionTemplate renders a template of {{#input#}} references, as
	// Dify's answer node does; ActionCoalesce picks the first input that
	// exists, as its variable-aggregator does (ADR 0028).
	ActionTemplate = "kairo.template"
	ActionCoalesce = "kairo.coalesce"
	// ActionList is Dify's list-operator.
	ActionList = "kairo.list"
)

// Registry holds node specs by action name.
type Registry struct {
	mu    sync.RWMutex
	specs map[string]*NodeSpec
}

func NewRegistry() *Registry {
	r := &Registry{specs: map[string]*NodeSpec{}}
	r.Register(NodeSpec{Action: ActionPass, Effect: EffectProtected})
	r.Register(NodeSpec{Action: ActionAppend, Effect: EffectProtected})
	r.Register(NodeSpec{Action: ActionAssign, Effect: EffectProtected})
	r.Register(NodeSpec{Action: ActionTemplate, Effect: EffectProtected})
	r.Register(NodeSpec{Action: ActionCoalesce, Effect: EffectProtected})
	r.Register(NodeSpec{Action: ActionList, Effect: EffectProtected})
	// The output is Dify's if-else's: {"result", "selected_case_id"}; the
	// case id (or "false") is the branch.
	r.Register(NodeSpec{Action: ActionSwitch, Effect: EffectProtected, Branch: "selected_case_id", Outputs: map[string]FieldType{
		"selected_case_id": {Type: FieldEnum},
		"result":           {Type: FieldBool},
	}})
	return r
}

func (r *Registry) Register(s NodeSpec) {
	if s.Action == "" {
		panic("ir: NodeSpec without action")
	}
	s.normalize()
	r.mu.Lock()
	r.specs[s.Action] = &s
	r.mu.Unlock()
}

// Lookup returns the spec for action. Undeclared actions get a spec with
// EffectReal: the safe default.
func (r *Registry) Lookup(action string) *NodeSpec {
	r.mu.RLock()
	s := r.specs[action]
	r.mu.RUnlock()
	if s != nil {
		return s
	}
	d := NodeSpec{Action: action, Effect: EffectReal}
	d.normalize()
	return &d
}

// LoadJSON registers a JSON array of specs.
func (r *Registry) LoadJSON(b []byte) error {
	var specs []NodeSpec
	if err := json.Unmarshal(b, &specs); err != nil {
		return err
	}
	for _, s := range specs {
		r.Register(s)
	}
	return nil
}

// Specs returns all registered specs.
func (r *Registry) Specs() []NodeSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeSpec, 0, len(r.specs))
	for _, s := range r.specs {
		out = append(out, *s)
	}
	return out
}
