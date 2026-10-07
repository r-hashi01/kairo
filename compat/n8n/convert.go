// Package n8n runs n8n's engine v2 graphs on kairo (ADR 0042, 0043).
//
// n8n's own converter (V1WorkflowConverter) turns a workflow into an engine
// v2 graph; Convert turns that graph into a kairo plan, keeping engine v2's
// semantics: a node runs once every incoming edge is settled and one of
// them is live; an edge is live when its source succeeded and the source's
// output slot is not null; a node with several inputs runs once, with null
// for a dead input; a SplitInBatches node (type "batch") repeats its body
// over parts of its items and gathers what the body sends back.
package n8n

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"kairo/ir"
)

// ConverterVersion is part of the plan names: plans of an older converter
// are kept apart.
const ConverterVersion = 1

// ActionNode runs one n8n v1 node on a worker. Its input is the node's
// input slots by index ("in0", "in1", ...), its params the graph node; its
// output is its output slots (ports, ADR 0043).
const ActionNode = "n8n.node"

// TriggerInput is the run input holding the trigger's output slots.
const TriggerInput = "trigger"

// Spec registers the n8n actions with a registry.
func Spec(r *ir.Registry) {
	r.Register(ir.NodeSpec{Action: ActionNode, Effect: ir.EffectReal, Ports: true})
}

// Graph is an engine v2 WorkflowGraph.
type Graph struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

type Node struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Type   string          `json:"type"` // trigger | v1-node | batch | wait | subworkflow
	Config json.RawMessage `json:"config,omitempty"`
}

type Edge struct {
	From        string `json:"from"`
	To          string `json:"to"`
	OutputIndex int    `json:"outputIndex"`
	InputIndex  int    `json:"inputIndex"`
	IsBackEdge  bool   `json:"isBackEdge,omitempty"`
}

// ErrUnsupported: a graph engine v2 rejects too (it answers 501).
var ErrUnsupported = errors.New("n8n: unsupported graph")

// Suffixes of the nodes a batch node becomes (ADR 0043).
const (
	sliceSuffix = "__slice"
	nextSuffix  = "__next"
	keepSuffix  = "__keep"
	finSuffix   = "__fin"
)

type converter struct {
	g      *Graph
	byID   map[string]*Node
	in     map[string][]Edge // forward edges into a node
	out    map[string][]Edge // forward edges out of a node
	loopOf map[string]string // body node -> its batch node
	back   map[string]Edge   // batch node -> its back edge
}

// Convert turns an engine v2 graph into a kairo definition named name.
func Convert(name string, g *Graph) (*ir.Definition, error) {
	c := &converter{g: g, byID: map[string]*Node{}, in: map[string][]Edge{}, out: map[string][]Edge{},
		loopOf: map[string]string{}, back: map[string]Edge{}}
	triggers := 0
	for i := range g.Nodes {
		n := &g.Nodes[i]
		if n.ID == "" || c.byID[n.ID] != nil {
			return nil, fmt.Errorf("%w: node id %q is empty or not unique", ErrUnsupported, n.ID)
		}
		c.byID[n.ID] = n
		switch n.Type {
		case "trigger":
			triggers++
		case "v1-node", "batch":
		default:
			return nil, fmt.Errorf("%w: node %q of type %q", ErrUnsupported, n.Name, n.Type)
		}
	}
	if triggers != 1 {
		return nil, fmt.Errorf("%w: %d triggers (one is needed)", ErrUnsupported, triggers)
	}
	for _, e := range g.Edges {
		if c.byID[e.From] == nil || c.byID[e.To] == nil {
			return nil, fmt.Errorf("%w: edge %s -> %s: unknown node", ErrUnsupported, e.From, e.To)
		}
		if e.OutputIndex < 0 || e.OutputIndex > ir.MaxPort || e.InputIndex < 0 || e.InputIndex > ir.MaxPort {
			return nil, fmt.Errorf("%w: edge %s -> %s: slot out of range", ErrUnsupported, e.From, e.To)
		}
		if e.IsBackEdge {
			if c.byID[e.To].Type != "batch" || e.InputIndex != 0 {
				return nil, fmt.Errorf("%w: a back edge must enter a batch node's input 0", ErrUnsupported)
			}
			if _, dup := c.back[e.To]; dup {
				return nil, fmt.Errorf("%w: batch node %q has two back edges", ErrUnsupported, c.byID[e.To].Name)
			}
			c.back[e.To] = e
			continue
		}
		for _, o := range c.in[e.To] {
			if o.InputIndex == e.InputIndex {
				return nil, fmt.Errorf("%w: two edges into input %d of %q", ErrUnsupported, e.InputIndex, c.byID[e.To].Name)
			}
		}
		c.in[e.To] = append(c.in[e.To], e)
		c.out[e.From] = append(c.out[e.From], e)
	}
	for id := range c.back {
		if err := c.findBody(id); err != nil {
			return nil, err
		}
	}
	for i := range g.Nodes {
		if n := &g.Nodes[i]; n.Type == "batch" {
			if _, ok := c.back[n.ID]; !ok {
				return nil, fmt.Errorf("%w: batch node %q has no back edge", ErrUnsupported, n.Name)
			}
		}
	}
	root, err := c.graph("", "g")
	if err != nil {
		return nil, err
	}
	return &ir.Definition{Name: name, Root: root}, nil
}

// findBody marks the body of batch node b: the nodes reached from its loop
// slot (1) up to its back edge. engine v2 refuses nested loops, a loop
// entered twice, and a body that leaves the loop but by the done slot.
func (c *converter) findBody(b string) error {
	back := c.back[b]
	body := map[string]bool{}
	var walk func(id string)
	walk = func(id string) {
		if id == b || body[id] {
			return
		}
		body[id] = true
		for _, e := range c.out[id] {
			walk(e.To)
		}
	}
	for _, e := range c.out[b] {
		switch e.OutputIndex {
		case 1:
			walk(e.To)
		case 0:
		default:
			return fmt.Errorf("%w: batch node %q has only slots 0 (done) and 1 (loop)", ErrUnsupported, c.byID[b].Name)
		}
	}
	if !body[back.From] {
		return fmt.Errorf("%w: the back edge of %q does not come from its body", ErrUnsupported, c.byID[b].Name)
	}
	for id := range body {
		if c.byID[id].Type == "batch" || c.loopOf[id] != "" {
			return fmt.Errorf("%w: nested loops", ErrUnsupported)
		}
		for _, e := range c.in[id] {
			if !body[e.From] && e.From != b {
				return fmt.Errorf("%w: %q enters the body of %q from outside", ErrUnsupported, c.byID[e.From].Name, c.byID[b].Name)
			}
		}
		for _, e := range c.out[id] {
			if !body[e.To] {
				return fmt.Errorf("%w: %q leaves the body of %q", ErrUnsupported, c.byID[id].Name, c.byID[b].Name)
			}
		}
		c.loopOf[id] = b
	}
	entries := 0
	for _, e := range c.in[b] {
		if e.InputIndex == 0 {
			entries++
		}
	}
	if entries != 1 {
		return fmt.Errorf("%w: batch node %q is entered %d times", ErrUnsupported, c.byID[b].Name, entries)
	}
	return nil
}

// graph builds the graph of the nodes of loop (""; the root) as a kairo
// graph with id.
func (c *converter) graph(loop, id string) (*ir.Def, error) {
	g := &ir.Def{Kind: "graph", ID: id}
	for i := range c.g.Nodes {
		n := &c.g.Nodes[i]
		if c.loopOf[n.ID] != loop {
			continue
		}
		switch n.Type {
		case "trigger":
			// Its output slots are the run's trigger input; it runs
			// nothing. Slot k reads $input.trigger.k.
			in := map[string]string{}
			for _, e := range c.out[n.ID] {
				k := strconv.Itoa(e.OutputIndex)
				in[k] = "$input." + TriggerInput + "." + k
			}
			g.Nodes = append(g.Nodes, &ir.Def{Kind: "step", ID: n.ID, Action: ir.ActionPass, Input: in, Ports: true})
		case "v1-node":
			params, err := json.Marshal(n)
			if err != nil {
				return nil, err
			}
			// engine v2 does not retry a step, and a step whose outcome is
			// unknown (its worker went away) fails rather than waiting for
			// an operator (ADR 0035).
			g.Nodes = append(g.Nodes, &ir.Def{Kind: "step", ID: n.ID, Action: ActionNode, Input: c.inputs(n.ID), Params: params,
				Retry: &ir.RetryDef{MaxAttempts: 1}, OnUnknown: "fail"})
		case "batch":
			l, err := c.loop(n)
			if err != nil {
				return nil, err
			}
			g.Nodes = append(g.Nodes, l)
		}
	}
	for _, e := range c.g.Edges {
		if e.IsBackEdge || c.loopOf[e.From] != loop || c.loopOf[e.To] != loop {
			continue
		}
		g.Edges = append(g.Edges, ir.EdgeDef{From: e.From, To: e.To, Port: port(e.OutputIndex)})
	}
	return g, nil
}

// inputs is node id's input slots by index: in<k> reads the slot of the
// edge into input k (null when the edge is dead). An edge from a batch
// node's loop slot reads the part the loop's slice took.
func (c *converter) inputs(id string) map[string]string {
	in := map[string]string{}
	for _, e := range c.in[id] {
		from := e.From
		if c.byID[from].Type == "batch" && e.OutputIndex == 1 {
			in["in"+strconv.Itoa(e.InputIndex)] = from + sliceSuffix + ".1"
			continue
		}
		in["in"+strconv.Itoa(e.InputIndex)] = from + "." + strconv.Itoa(e.OutputIndex)
	}
	return in
}

// loop builds batch node b as a kairo loop (ADR 0043): kairo.slice takes
// the next part of the items left; its loop slot runs the body, whose back
// edge adds what it sends back; when nothing is left, the gathered items
// leave by the loop's done port ("0"). A body that sends nothing back ends
// the loop with its done port dead.
func (c *converter) loop(b *Node) (*ir.Def, error) {
	var cfg struct {
		BatchSize int `json:"batchSize"`
	}
	if len(b.Config) > 0 {
		if err := json.Unmarshal(b.Config, &cfg); err != nil {
			return nil, fmt.Errorf("n8n: batch node %q: %w", b.Name, err)
		}
	}
	if cfg.BatchSize < 1 {
		return nil, fmt.Errorf("%w: batch node %q needs a batchSize of at least 1", ErrUnsupported, b.Name)
	}
	var entry Edge
	for _, e := range c.in[b.ID] {
		entry = e
	}
	back := c.back[b.ID]
	body, err := c.graph(b.ID, b.ID+"__body")
	if err != nil {
		return nil, err
	}
	lv := func(name string) string { return b.ID + "." + name }
	assign := func(items ...string) json.RawMessage {
		return json.RawMessage(`{"items":[` + joinComma(items) + `]}`)
	}
	slice := &ir.Def{Kind: "step", ID: b.ID + sliceSuffix, Action: ir.ActionSlice,
		Input:  map[string]string{"items": lv("rest"), "done": lv("arrivals")},
		Params: json.RawMessage(`{"size":` + strconv.Itoa(cfg.BatchSize) + `}`)}
	next := &ir.Def{Kind: "step", ID: b.ID + nextSuffix, Action: ir.ActionAssign,
		Input: map[string]string{"rest": slice.ID + ".2"},
		Params: assign(`{"var":"`+lv("rest")+`","op":"over-write","input":"rest"}`,
			`{"var":"`+lv("go")+`","op":"over-write","value":0}`)}
	keep := &ir.Def{Kind: "step", ID: b.ID + keepSuffix, Action: ir.ActionAssign,
		Input: map[string]string{"out": back.From + "." + strconv.Itoa(back.OutputIndex)},
		Params: assign(`{"var":"`+lv("arrivals")+`","op":"extend","input":"out"}`,
			`{"var":"`+lv("go")+`","op":"over-write","value":1}`)}
	fin := &ir.Def{Kind: "step", ID: b.ID + finSuffix, Action: ir.ActionAssign,
		Input: map[string]string{"done": slice.ID + ".0"},
		Params: assign(`{"var":"`+lv("0")+`","op":"over-write","input":"done"}`,
			`{"var":"`+lv("go")+`","op":"over-write","value":0}`)}
	body.Nodes = append([]*ir.Def{slice, next, fin}, append(body.Nodes, keep)...)
	edges := []ir.EdgeDef{
		{From: slice.ID, To: next.ID, Port: port(1)},
		{From: slice.ID, To: fin.ID, Port: port(0)},
		{From: back.From, To: keep.ID, Port: port(back.OutputIndex)},
	}
	// The body starts once the slice took a part: edges from the loop slot
	// leave from next.
	for _, e := range c.out[b.ID] {
		if e.OutputIndex == 1 {
			edges = append(edges, ir.EdgeDef{From: next.ID, To: e.To})
		}
	}
	body.Edges = append(edges, body.Edges...)
	return &ir.Def{Kind: "loop", ID: b.ID, MaxIter: maxRounds, Ports: true, LoopOutput: "vars",
		Vars: map[string]ir.VarDef{
			"rest":     {Type: "array[any]", Ref: entry.From + "." + strconv.Itoa(entry.OutputIndex)},
			"arrivals": {Type: "array[any]", Value: json.RawMessage(`[]`)},
			"go":       {Type: "integer", Value: json.RawMessage(`1`)},
			"0":        {Type: "array[any]", Value: json.RawMessage(`null`)},
		},
		Break:      &ir.SwitchCase{Logic: "and", Conds: []ir.Condition{{Var: "go", Op: "=", Value: json.RawMessage(`"0"`)}}},
		BreakInput: map[string]string{"go": lv("go")},
		Body:       body}, nil
}

// maxRounds bounds a batch loop's rounds (engine v2 has no bound; one
// round per item at batchSize 1 must fit).
const maxRounds = 1_000_000

func port(i int) *int { return &i }

func joinComma(s []string) string {
	b := []byte{}
	for i, x := range s {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, x...)
	}
	return string(b)
}
