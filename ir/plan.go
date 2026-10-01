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
	Root   *Def                 `json:"root"`
}

// Def is one node of the definition tree. Kind is one of
// step | seq | par | cond | map | loop | wait.
type Def struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`

	// step
	Action string            `json:"action,omitempty"`
	Input  map[string]string `json:"input,omitempty"` // name -> ref
	Params json.RawMessage   `json:"params,omitempty"`

	// seq, par
	Nodes []*Def `json:"nodes,omitempty"`

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

	// wait: a duration, or a named signal (optionally with timeout)
	Duration Duration `json:"duration,omitempty"`
	Signal   string   `json:"signal,omitempty"`
	Timeout  Duration `json:"timeout,omitempty"`
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
	KSeq
	KPar
	KCond
	KMap
	KLoop
	KWait
)

var kindNames = []string{"step", "seq", "par", "cond", "map", "loop", "wait"}

func (k Kind) String() string { return kindNames[k] }

type RefKind uint8

const (
	RefInput RefKind = iota // $input
	RefItem                 // $item (innermost map element)
	RefIndex                // $index (innermost map index)
	RefNode                 // <node id>
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

	// cond / loop
	Pred *Pred

	// map
	Over    Ref
	MaxConc int32
	// loop
	MaxIter int32

	// wait
	Wait    time.Duration
	Signal  string
	Timeout time.Duration

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

// Plan is an immutable compiled definition shared by all runs.
type Plan struct {
	Name    string
	Hash    string
	Nodes   []Node
	ByID    map[string]int32
	HasReal bool
	Inputs  map[string]FieldType
	Def     *Definition
}

// Compile validates def against the registry and produces a Plan.
func Compile(def *Definition, reg *Registry) (*Plan, error) {
	if def == nil || def.Root == nil {
		return nil, errors.New("ir: empty definition")
	}
	c := &compiler{reg: reg, plan: &Plan{Name: def.Name, ByID: map[string]int32{}, Inputs: def.Inputs, Def: def}}
	if _, err := c.add(def.Root, -1, 0, -1); err != nil {
		return nil, err
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
	reg  *Registry
	plan *Plan
	defs []*Def
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
	case "seq":
		n.Kind = KSeq
	case "par":
		n.Kind = KPar
	case "cond":
		n.Kind = KCond
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
	case KSeq, KPar:
		kids = d.Nodes
	case KCond:
		if d.If == nil || d.Then == nil {
			return -1, fmt.Errorf("ir: cond %q needs if and then", n.ID)
		}
		kids = []*Def{d.Then}
		if d.Else != nil {
			kids = append(kids, d.Else)
		}
	case KMap:
		if d.Body == nil || d.Over == "" {
			return -1, fmt.Errorf("ir: map %q needs over and body", n.ID)
		}
		kids = []*Def{d.Body}
		childScope = i
		c.plan.Nodes[i].MaxConc = int32(d.MaxConcurrency)
	case KLoop:
		if d.Body == nil || d.While == nil {
			return -1, fmt.Errorf("ir: loop %q needs body and while", n.ID)
		}
		if d.MaxIter <= 0 {
			return -1, fmt.Errorf("ir: loop %q needs max_iter > 0", n.ID)
		}
		kids = []*Def{d.Body}
		c.plan.Nodes[i].MaxIter = int32(d.MaxIter)
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
	case KCond:
		p, err := c.parsePred(i, d.If)
		if err != nil {
			return fmt.Errorf("ir: cond %q: %w", n.ID, err)
		}
		n.Pred = p
	case KLoop:
		// The while condition is evaluated in the loop's own scope, after the
		// body has run, so it may reference nodes inside the body.
		p, err := c.parsePred(n.Children[0], d.While)
		if err != nil {
			return fmt.Errorf("ir: loop %q: %w", n.ID, err)
		}
		n.Pred = p
	case KMap:
		r, err := c.parseRef(i, d.Over)
		if err != nil {
			return fmt.Errorf("ir: map %q over: %w", n.ID, err)
		}
		n.Over = r
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
	if isAncestor(c.plan, t, from) {
		return Ref{}, fmt.Errorf("node %q references its own ancestor", head)
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
