package ir

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Definition is the serialized form of a workflow (JSON in v0).
type Definition struct {
	Name   string               `json:"name"`
	Inputs map[string]FieldType `json:"inputs,omitempty"`
	// Vars are the run's variables (ADR 0033): read as $var.<name>,
	// written by kairo.assign, given initial values by the submission.
	Vars map[string]VarDef `json:"vars,omitempty"`
	Root *Def              `json:"root"`
}

// VarDef declares a variable: its Dify type (string, number, integer,
// float, boolean, object, array[string], array[number], array[object],
// array[boolean], array[any], file, array[file], secret) and its initial
// value, a constant (Value) or, for loop variables, a reference (Ref).
type VarDef struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value,omitempty"`
	Ref   string          `json:"ref,omitempty"`
}

// Def is one node of the definition. Kind is one of
// graph | step | map | loop | wait, or seq | par | cond, which compile to
// graphs (ADR 0029).
type Def struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`

	// step
	Action string            `json:"action,omitempty"`
	Input  map[string]string `json:"input,omitempty"` // name -> ref
	Params json.RawMessage   `json:"params,omitempty"`
	// Per-node retry and error handling (ADR 0030), overriding the
	// action's spec.
	Retry   *RetryDef   `json:"retry,omitempty"`
	OnError *OnErrorDef `json:"on_error,omitempty"`
	// Handles of a branching step at this place (ADR 0031): the values its
	// branch field may take, instead of the spec's enum values.
	Handles []string `json:"handles,omitempty"`
	// OnUnknown "fail" treats an unknown outcome of a real step as a
	// definite failure, without retrying it (ADR 0035); the default,
	// "review", stops the run for an operator.
	OnUnknown string `json:"on_unknown,omitempty"`

	// graph, seq, par
	Nodes []*Def `json:"nodes,omitempty"`

	// graph: edges between Nodes (by id), the entry nodes (default: those
	// without incoming edges) and the output object (name -> ref; default:
	// the outputs of the nodes without outgoing edges that ran, by id).
	Edges  []EdgeDef         `json:"edges,omitempty"`
	Entry  []string          `json:"entry,omitempty"`
	Output map[string]string `json:"output,omitempty"`

	// cond
	If   *PredDef `json:"if,omitempty"`
	Then *Def     `json:"then,omitempty"`
	Else *Def     `json:"else,omitempty"`

	// map (over a list), loop (do-while)
	Over           string   `json:"over,omitempty"`
	Body           *Def     `json:"body,omitempty"`
	MaxConcurrency int      `json:"max_concurrency,omitempty"`
	While          *PredDef `json:"while,omitempty"`
	MaxIter        int      `json:"max_iter,omitempty"`

	// map (ADR 0032): what a failing element does (fail | null | omit),
	// the node inside the body whose output is the element's result, and
	// whether results that are all lists are concatenated.
	OnElementError string `json:"on_element_error,omitempty"`
	ElementOutput  string `json:"element_output,omitempty"`
	Flatten        bool   `json:"flatten,omitempty"`
	// loop (ADR 0032, 0033): check "before" also tests the condition
	// before the first round; Break is a Dify condition over BreakInput
	// (instead of While); BreakOn ends the loop after a round in which one
	// of these nodes ran; Vars are loop variables, read as <loop>.<name>.
	Check      string            `json:"check,omitempty"`
	Break      *SwitchCase       `json:"break,omitempty"`
	BreakInput map[string]string `json:"break_input,omitempty"`
	BreakOn    []string          `json:"break_on,omitempty"`
	Vars       map[string]VarDef `json:"vars,omitempty"`
	// LoopOutput "vars" makes the loop's output its variables plus
	// loop_round, as a Dify loop's; the default is the body's last output.
	LoopOutput string `json:"loop_output,omitempty"`

	// wait: a duration, or a named signal (optionally with timeout)
	Duration Duration `json:"duration,omitempty"`
	Signal   string   `json:"signal,omitempty"`
	Timeout  Duration `json:"timeout,omitempty"`
}

// RetryDef overrides a step's retries: at most MaxAttempts attempts,
// Interval apart (0: the spec's exponential backoff).
type RetryDef struct {
	MaxAttempts int      `json:"max_attempts"`
	Interval    Duration `json:"interval,omitempty"`
}

// OnErrorDef says what a step's definite failure (retries exhausted) does
// instead of failing the run: "fail-branch" takes the node's fail-branch
// edges with {error_message, error_type} as output; "default-value"
// outputs Value plus those fields and goes on.
type OnErrorDef struct {
	Strategy string          `json:"strategy"`
	Value    json.RawMessage `json:"value,omitempty"`
}

// Error strategies (ADR 0030).
const (
	OnErrorFail uint8 = iota
	OnErrorBranch
	OnErrorDefault
)

// HandleFailBranch is the handle taken by a step that failed with
// on_error fail-branch.
const HandleFailBranch = "fail-branch"

// EdgeDef connects two nodes of a graph. Handle selects the edge by the
// source's branch value; "" means "source", the handle of nodes that do
// not branch.
type EdgeDef struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Handle string `json:"handle,omitempty"`
}

// PredDef is a condition on a typed field: {"field":"classify.label",
// "op":"eq","value":"refund"}.
type PredDef struct {
	Field string          `json:"field"`
	Op    string          `json:"op"`
	Value json.RawMessage `json:"value"`
}

type Kind uint8

const (
	KStep Kind = iota
	KGraph
	KMap
	KLoop
	KWait
	// KTest is the condition of a compiled cond: a graph member that is
	// evaluated in place (no activation) and takes its "true" or "false"
	// edge.
	KTest
)

var kindNames = []string{"step", "graph", "map", "loop", "wait", "test"}

// Sugar records which construct a graph was compiled from. It decides the
// graph's output: seq the last node's, par an object of all nodes', cond
// the branch that ran, a graph its Output (ADR 0029).
type Sugar uint8

const (
	SugarGraph Sugar = iota
	SugarSeq
	SugarPar
	SugarCond
)

// HandleSource is the handle of the edges of a node that does not branch.
const HandleSource = "source"

// Edge is a compiled graph edge between members (indices into the graph
// node's Children).
type Edge struct {
	From, To int32
	Handle   string
}

func (k Kind) String() string { return kindNames[k] }

type RefKind uint8

const (
	RefInput RefKind = iota // $input
	RefItem                 // $item (innermost map element)
	RefIndex                // $index (innermost map index)
	RefNode                 // <node id>
	RefVar                  // $var.<name> (Path[0] is the name)
)

type Ref struct {
	Kind RefKind
	Node int32 // for RefNode
	Path []string
}

type Input struct {
	Name string
	Ref  Ref
}

type Op uint8

const (
	OpEq Op = iota
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIn
)

var opNames = map[string]Op{"eq": OpEq, "ne": OpNe, "lt": OpLt, "le": OpLe, "gt": OpGt, "ge": OpGe, "in": OpIn}

// Pred is a compiled condition. Field is the typed field name (Ref.Path has
// exactly one element).
type Pred struct {
	Ref   Ref
	Op    Op
	Type  FieldKind
	Bool  bool
	Num   float64
	Str   string
	Set   []string
	Field string
}

// Node is one compiled node. Nodes live in Plan.Nodes; index 0 is the root.
type Node struct {
	Kind     Kind
	ID       string
	Parent   int32
	Idx      int32 // position in parent's children
	Children []int32

	// step
	Spec   *NodeSpec
	Params json.RawMessage
	Inputs []Input
	// Retries and error handling, resolved from the spec and the node's
	// overrides: MaxAttempts attempts, RetryInterval apart (0: the spec's
	// exponential backoff); OnError is one of OnErrorFail/Branch/Default,
	// ErrValue the default-value object.
	MaxAttempts   int32
	RetryInterval time.Duration
	OnError       uint8
	ErrValue      json.RawMessage
	UnknownFails  bool // on_unknown: fail
	// Handles of a branching step (its spec has Branch); Switch is the
	// compiled condition of a kairo.switch step.
	Handles []string
	Switch  *Switch

	// graph: members are Children. In and Out list edge indices per
	// member; Topo is the order in which ready members are started; Entry
	// lists the entry members.
	Sugar   Sugar
	Edges   []Edge
	In, Out [][]int32
	Topo    []int32
	Entry   []int32
	Output  []Input // SugarGraph with an explicit output
	// reach[i] has bit j set if member j reaches member i (SugarGraph).
	reach [][]uint64

	// test / loop
	Pred *Pred

	// map
	Over    Ref
	MaxConc int32
	ElemErr uint8 // ElemFail, ElemNull or ElemOmit
	ElemOut *Ref
	Flatten bool
	// loop
	MaxIter int32
	Before  bool        // test the condition before the first round too
	Break   *SwitchCase // Dify break conditions over BreakIn
	BreakIn []Input
	BreakOn []int32
	Vars    []Var // loop variables, by name
	VarsOut bool  // output the variables and loop_round
	// kairo.assign
	Assign []AssignItem

	// wait
	Wait    time.Duration
	Signal  string
	Timeout time.Duration

	// End is one past the last node of this node's subtree (nodes are
	// numbered depth first; cond tests come after all of them).
	End int32

	// Innermost enclosing map node (-1 if none). Values of this node live in
	// the scope created by that map for each element.
	MapScope int32
}

// Effect returns the node's effect type.
func (n *Node) Effect() Effect {
	switch n.Kind {
	case KStep:
		return n.Spec.Effect
	case KWait:
		return EffectWait
	}
	return EffectProtected
}

// Element error modes of a map (ADR 0032).
const (
	ElemFail uint8 = iota
	ElemNull
	ElemOmit
)

// Var is a compiled variable declaration: its type and initial value
// (Init, or the value of Ref).
type Var struct {
	Name string
	Type string
	Init json.RawMessage
	Ref  *Ref
}

// AssignItem is one operation of a kairo.assign step (ADR 0033): Op on
// variable Name of loop Loop (-1: a run variable), with the step input
// named Input or the constant Value.
type AssignItem struct {
	Loop  int32
	Name  string
	Type  string
	Op    string
	Input string
	Value json.RawMessage
}

// Plan is an immutable compiled definition shared by all runs.
type Plan struct {
	Name    string
	Hash    string
	Nodes   []Node
	ByID    map[string]int32
	HasReal bool
	Inputs  map[string]FieldType
	Vars    []Var // run variables, by name
	Def     *Definition
}

// Compile validates def against the registry and produces a Plan.
func Compile(def *Definition, reg *Registry) (*Plan, error) {
	if def == nil || def.Root == nil {
		return nil, errors.New("ir: empty definition")
	}
	c := &compiler{reg: reg, plan: &Plan{Name: def.Name, ByID: map[string]int32{}, Inputs: def.Inputs, Def: def}}
	vars, err := compileVars(def.Vars, "run", false)
	if err != nil {
		return nil, err
	}
	c.plan.Vars = vars
	if _, err := c.add(def.Root, -1, 0, -1); err != nil {
		return nil, err
	}
	// The conditions of conds are appended after every other node, so
	// node indices (kept in states and snapshots) are those of the tree.
	if err := c.addTests(); err != nil {
		return nil, err
	}
	// Only a node of a hand-written graph has fail-branch edges to take.
	for _, n := range c.plan.Nodes {
		if n.Kind == KStep && n.OnError == OnErrorBranch {
			if n.Parent < 0 || c.plan.Nodes[n.Parent].Kind != KGraph || c.plan.Nodes[n.Parent].Sugar != SugarGraph {
				return nil, fmt.Errorf("ir: step %q: on_error fail-branch needs the step to be a node of a graph", n.ID)
			}
		}
	}
	// Second pass: resolve references now that all IDs are known.
	for i := range c.plan.Nodes {
		if err := c.resolve(int32(i)); err != nil {
			return nil, err
		}
	}
	h := sha256.New()
	enc, _ := json.Marshal(def)
	h.Write(enc)
	for _, n := range c.plan.Nodes {
		if n.Kind == KStep {
			spec, _ := json.Marshal(n.Spec)
			h.Write(spec)
		}
	}
	c.plan.Hash = hex.EncodeToString(h.Sum(nil))[:16]
	return c.plan, nil
}

type compiler struct {
	reg   *Registry
	plan  *Plan
	defs  []*Def
	conds []int32 // cond graphs waiting for their test member
}

func (c *compiler) add(d *Def, parent int32, idx int32, mapScope int32) (int32, error) {
	if d == nil {
		return -1, errors.New("ir: nil node")
	}
	i := int32(len(c.plan.Nodes))
	n := Node{ID: d.ID, Parent: parent, Idx: idx, MapScope: mapScope}
	switch d.Kind {
	case "step":
		n.Kind = KStep
	case "graph":
		n.Kind, n.Sugar = KGraph, SugarGraph
	case "seq":
		n.Kind, n.Sugar = KGraph, SugarSeq
	case "par":
		n.Kind, n.Sugar = KGraph, SugarPar
	case "cond":
		n.Kind, n.Sugar = KGraph, SugarCond
	case "map":
		n.Kind = KMap
	case "loop":
		n.Kind = KLoop
	case "wait":
		n.Kind = KWait
	default:
		return -1, fmt.Errorf("ir: unknown node kind %q", d.Kind)
	}
	if n.ID == "" {
		if n.Kind == KStep || n.Kind == KWait {
			return -1, fmt.Errorf("ir: %s node needs an id", d.Kind)
		}
		n.ID = fmt.Sprintf("_%s%d", d.Kind, i)
	}
	if strings.ContainsAny(n.ID, ".$[]/:") {
		return -1, fmt.Errorf("ir: invalid node id %q", n.ID)
	}
	if _, dup := c.plan.ByID[n.ID]; dup {
		return -1, fmt.Errorf("ir: duplicate node id %q", n.ID)
	}
	c.plan.ByID[n.ID] = i
	c.plan.Nodes = append(c.plan.Nodes, n)
	c.defs = append(c.defs, d)

	var kids []*Def
	childScope := mapScope
	switch n.Kind {
	case KStep:
		if d.Action == "" {
			return -1, fmt.Errorf("ir: step %q has no action", n.ID)
		}
		spec := c.reg.Lookup(d.Action)
		c.plan.Nodes[i].Spec = spec
		c.plan.Nodes[i].Params = d.Params
		if spec.Effect == EffectReal {
			c.plan.HasReal = true
		}
		if spec.Effect == EffectWait {
			return -1, fmt.Errorf("ir: step %q: action %q declares effect wait; use a wait node", n.ID, d.Action)
		}
		if err := c.branchHandles(i, d); err != nil {
			return -1, err
		}
		if spec.Action == ActionAssign {
			if err := c.compileAssign(i, d); err != nil {
				return -1, err
			}
		}
		if err := c.errorHandling(i, d); err != nil {
			return -1, err
		}
	case KGraph:
		switch n.Sugar {
		case SugarCond:
			if d.If == nil || d.Then == nil {
				return -1, fmt.Errorf("ir: cond %q needs if and then", n.ID)
			}
			kids = []*Def{d.Then}
			if d.Else != nil {
				kids = append(kids, d.Else)
			}
			c.conds = append(c.conds, i)
		default:
			kids = d.Nodes
		}
	case KMap:
		if d.Body == nil || d.Over == "" {
			return -1, fmt.Errorf("ir: map %q needs over and body", n.ID)
		}
		kids = []*Def{d.Body}
		childScope = i
		c.plan.Nodes[i].MaxConc = int32(d.MaxConcurrency)
		switch d.OnElementError {
		case "", "fail":
		case "null":
			c.plan.Nodes[i].ElemErr = ElemNull
		case "omit":
			c.plan.Nodes[i].ElemErr = ElemOmit
		default:
			return -1, fmt.Errorf("ir: map %q: on_element_error must be fail, null or omit", n.ID)
		}
		c.plan.Nodes[i].Flatten = d.Flatten
	case KLoop:
		if d.Body == nil || (d.While != nil && d.Break != nil) {
			return -1, fmt.Errorf("ir: loop %q needs a body and at most one of while and break", n.ID)
		}
		switch d.Check {
		case "", "after":
		case "before":
			c.plan.Nodes[i].Before = true
		default:
			return -1, fmt.Errorf("ir: loop %q: check must be before or after", n.ID)
		}
		if d.MaxIter <= 0 {
			return -1, fmt.Errorf("ir: loop %q needs max_iter > 0", n.ID)
		}
		kids = []*Def{d.Body}
		c.plan.Nodes[i].MaxIter = int32(d.MaxIter)
		vars, err := compileVars(d.Vars, "loop "+n.ID, true)
		if err != nil {
			return -1, err
		}
		c.plan.Nodes[i].Vars = vars
		switch d.LoopOutput {
		case "", "body":
			if len(vars) > 0 {
				// The loop's value holds its variables.
				return -1, fmt.Errorf("ir: loop %q: loop variables need loop_output vars", n.ID)
			}
		case "vars":
			c.plan.Nodes[i].VarsOut = true
		default:
			return -1, fmt.Errorf("ir: loop %q: loop_output must be body or vars", n.ID)
		}
	case KWait:
		if (d.Duration == 0) == (d.Signal == "") {
			return -1, fmt.Errorf("ir: wait %q needs exactly one of duration or signal", n.ID)
		}
		c.plan.Nodes[i].Wait = time.Duration(d.Duration)
		c.plan.Nodes[i].Signal = d.Signal
		c.plan.Nodes[i].Timeout = time.Duration(d.Timeout)
	}
	for k, kd := range kids {
		ci, err := c.add(kd, i, int32(k), childScope)
		if err != nil {
			return -1, err
		}
		c.plan.Nodes[i].Children = append(c.plan.Nodes[i].Children, ci)
	}
	c.plan.Nodes[i].End = int32(len(c.plan.Nodes))
	if n.Kind == KGraph && n.Sugar != SugarCond {
		if err := c.edges(i, d); err != nil {
			return -1, err
		}
	}
	return i, nil
}

func (c *compiler) resolve(i int32) error {
	n := &c.plan.Nodes[i]
	d := c.defs[i]
	switch n.Kind {
	case KStep:
		names := make([]string, 0, len(d.Input))
		for k := range d.Input {
			names = append(names, k)
		}
		slices.Sort(names)
		for _, name := range names {
			r, err := c.parseRef(i, d.Input[name])
			if err != nil {
				return fmt.Errorf("ir: step %q input %q: %w", n.ID, name, err)
			}
			n.Inputs = append(n.Inputs, Input{Name: name, Ref: r})
		}
	case KTest:
		// Evaluated in the cond's scope; references are checked from the
		// cond node, as before conds became graphs.
		p, err := c.parsePred(n.Parent, c.defs[n.Parent].If)
		if err != nil {
			return fmt.Errorf("ir: cond %q: %w", c.plan.Nodes[n.Parent].ID, err)
		}
		n.Pred = p
	case KGraph:
		if n.Sugar == SugarGraph && len(d.Output) > 0 {
			names := make([]string, 0, len(d.Output))
			for k := range d.Output {
				names = append(names, k)
			}
			slices.Sort(names)
			for _, name := range names {
				r, err := c.parseRef(i, d.Output[name])
				if err != nil {
					return fmt.Errorf("ir: graph %q output %q: %w", n.ID, name, err)
				}
				n.Output = append(n.Output, Input{Name: name, Ref: r})
			}
		}
	case KLoop:
		// The conditions are evaluated in the loop's own scope, after the
		// body has run, so they may reference nodes inside the body.
		if d.While != nil {
			p, err := c.parsePred(n.Children[0], d.While)
			if err != nil {
				return fmt.Errorf("ir: loop %q: %w", n.ID, err)
			}
			n.Pred = p
		}
		if err := c.loopExtras(i, d); err != nil {
			return err
		}
	case KMap:
		r, err := c.parseRef(i, d.Over)
		if err != nil {
			return fmt.Errorf("ir: map %q over: %w", n.ID, err)
		}
		n.Over = r
		if d.ElementOutput != "" {
			r, err := c.parseRef(n.Children[0], d.ElementOutput)
			if err != nil {
				return fmt.Errorf("ir: map %q element_output: %w", n.ID, err)
			}
			if r.Kind != RefNode || !isAncestor(c.plan, i, r.Node) {
				return fmt.Errorf("ir: map %q: element_output must be a node inside the body", n.ID)
			}
			n.ElemOut = &r
		}
	}
	return nil
}

// visible reports whether node target's value can be referenced from node
// from: target's map scope must enclose from.
func (c *compiler) visible(from, target int32) bool {
	ts := c.plan.Nodes[target].MapScope
	if ts < 0 {
		return true
	}
	for s := c.plan.Nodes[from].MapScope; s >= 0; s = c.plan.Nodes[s].MapScope {
		if s == ts {
			return true
		}
	}
	return false
}

func (c *compiler) parseRef(from int32, s string) (Ref, error) {
	parts := strings.Split(s, ".")
	head, path := parts[0], parts[1:]
	for _, p := range path {
		if p == "" {
			return Ref{}, fmt.Errorf("bad reference %q", s)
		}
	}
	switch head {
	case "$input":
		return Ref{Kind: RefInput, Path: path}, nil
	case "$var":
		if len(path) == 0 || !slices.ContainsFunc(c.plan.Vars, func(v Var) bool { return v.Name == path[0] }) {
			return Ref{}, fmt.Errorf("undeclared run variable %q", s)
		}
		return Ref{Kind: RefVar, Path: path}, nil
	case "$item", "$index":
		// Only inside a map body. (A map node itself lives in the enclosing
		// scope, so its own "over" sees the outer map's item, if any.)
		if c.plan.Nodes[from].MapScope < 0 {
			return Ref{}, fmt.Errorf("%s used outside a map body", head)
		}
		if head == "$index" {
			if len(path) > 0 {
				return Ref{}, errors.New("$index has no fields")
			}
			return Ref{Kind: RefIndex}, nil
		}
		return Ref{Kind: RefItem, Path: path}, nil
	}
	if strings.HasPrefix(head, "$") {
		return Ref{}, fmt.Errorf("unknown reference %q", head)
	}
	t, ok := c.plan.ByID[head]
	if !ok {
		return Ref{}, fmt.Errorf("unknown node %q", head)
	}
	if !c.visible(from, t) {
		return Ref{}, fmt.Errorf("node %q is inside a map body that does not enclose the reference", head)
	}
	if isAncestor(c.plan, t, from) && !c.loopVarRef(t, path) {
		return Ref{}, fmt.Errorf("node %q references its own ancestor", head)
	}
	if err := c.graphOrder(from, t); err != nil {
		return Ref{}, err
	}
	return Ref{Kind: RefNode, Node: t, Path: path}, nil
}

func isAncestor(p *Plan, a, n int32) bool {
	for x := p.Nodes[n].Parent; x >= 0; x = p.Nodes[x].Parent {
		if x == a {
			return true
		}
	}
	return false
}

func (c *compiler) parsePred(from int32, d *PredDef) (*Pred, error) {
	if d == nil {
		return nil, errors.New("missing condition")
	}
	op, ok := opNames[d.Op]
	if !ok {
		return nil, fmt.Errorf("unknown operator %q", d.Op)
	}
	r, err := c.parseRef(from, d.Field)
	if err != nil {
		return nil, err
	}
	if len(r.Path) != 1 {
		return nil, fmt.Errorf("condition must test exactly one typed field, got %q", d.Field)
	}
	field := r.Path[0]
	var ft FieldType
	switch r.Kind {
	case RefInput:
		t, ok := c.plan.Inputs[field]
		if !ok {
			return nil, fmt.Errorf("input field %q has no declared type", field)
		}
		ft = t
	case RefNode:
		n := &c.plan.Nodes[r.Node]
		if n.Kind == KWait && n.Signal != "" && field == "timed_out" {
			ft = FieldType{Type: FieldBool}
			break
		}
		if n.Kind != KStep {
			return nil, fmt.Errorf("condition on %q: only step outputs have typed fields", n.ID)
		}
		t, ok := n.Spec.Outputs[field]
		if !ok {
			return nil, fmt.Errorf("field %q of %q (action %s) has no declared type", field, n.ID, n.Spec.Action)
		}
		ft = t
	default:
		return nil, fmt.Errorf("conditions must reference $input or a step output, got %q", d.Field)
	}
	if !ft.Type.Branchable() {
		return nil, fmt.Errorf("field %q has type %s; conditions may only test bool, int, number or enum fields", d.Field, ft.Type)
	}
	p := &Pred{Ref: r, Op: op, Type: ft.Type, Field: field}
	switch ft.Type {
	case FieldBool:
		if op != OpEq && op != OpNe {
			return nil, fmt.Errorf("operator %s not valid for bool", d.Op)
		}
		if err := json.Unmarshal(d.Value, &p.Bool); err != nil {
			return nil, fmt.Errorf("value for bool field: %w", err)
		}
	case FieldInt, FieldNumber:
		if op == OpIn {
			return nil, fmt.Errorf("operator in not valid for numbers")
		}
		if err := json.Unmarshal(d.Value, &p.Num); err != nil {
			return nil, fmt.Errorf("value for number field: %w", err)
		}
	case FieldEnum:
		switch op {
		case OpEq, OpNe:
			if err := json.Unmarshal(d.Value, &p.Str); err != nil {
				return nil, fmt.Errorf("value for enum field: %w", err)
			}
			if !slices.Contains(ft.Values, p.Str) {
				return nil, fmt.Errorf("%q is not a value of enum %q %v", p.Str, d.Field, ft.Values)
			}
		case OpIn:
			if err := json.Unmarshal(d.Value, &p.Set); err != nil {
				return nil, fmt.Errorf("value for in: %w", err)
			}
			for _, v := range p.Set {
				if !slices.Contains(ft.Values, v) {
					return nil, fmt.Errorf("%q is not a value of enum %q %v", v, d.Field, ft.Values)
				}
			}
		default:
			return nil, fmt.Errorf("operator %s not valid for enum", d.Op)
		}
	}
	return p, nil
}

// ParseDefinition decodes a JSON definition.
func ParseDefinition(b []byte) (*Definition, error) {
	var d Definition
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// Specs returns the node specs the plan was compiled with, sorted by action.
// Persisting them with the definition lets a restarted process rebuild the
// identical plan (same hash) even if the registry has changed since.
func (p *Plan) Specs() []NodeSpec {
	seen := map[string]bool{}
	var out []NodeSpec
	for _, n := range p.Nodes {
		if n.Kind == KStep && !seen[n.Spec.Action] {
			seen[n.Spec.Action] = true
			out = append(out, *n.Spec)
		}
	}
	slices.SortFunc(out, func(a, b NodeSpec) int { return strings.Compare(a.Action, b.Action) })
	return out
}

// Frozen is a plan's persisted form.
type Frozen struct {
	Definition *Definition `json:"definition"`
	Specs      []NodeSpec  `json:"specs"`
}

func (p *Plan) Freeze() Frozen { return Frozen{Definition: p.Def, Specs: p.Specs()} }

// Thaw recompiles a frozen plan with exactly its recorded specs.
func (f Frozen) Thaw() (*Plan, error) {
	reg := NewRegistry()
	for _, s := range f.Specs {
		reg.Register(s)
	}
	return Compile(f.Definition, reg)
}
