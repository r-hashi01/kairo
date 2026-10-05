package ir

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"
)

// Graphs (ADR 0029). Every control-flow construct compiles to a graph:
// seq is a chain, par has no edges, cond has a test member whose "true"
// and "false" edges lead to its branches. A graph written by hand
// (Sugar == SugarGraph) is checked here: endpoints, handles, entries, no
// cycles, and (in graphOrder) that references only point at nodes that
// are settled before the referring node can run.

// edges compiles the edges and entries of graph node i.
func (c *compiler) edges(i int32, d *Def) error {
	n := &c.plan.Nodes[i]
	k := len(n.Children)
	switch n.Sugar {
	case SugarSeq:
		for m := 0; m+1 < k; m++ {
			n.Edges = append(n.Edges, Edge{From: int32(m), To: int32(m + 1), Handle: HandleSource})
		}
		if k > 0 {
			n.Entry = []int32{0}
		}
	case SugarPar:
		for m := 0; m < k; m++ {
			n.Entry = append(n.Entry, int32(m))
		}
	case SugarGraph:
		if err := c.userEdges(i, d); err != nil {
			return fmt.Errorf("ir: graph %q: %w", n.ID, err)
		}
	}
	return c.finish(i)
}

func (c *compiler) userEdges(i int32, d *Def) error {
	n := &c.plan.Nodes[i]
	member := map[string]int32{}
	for m, ch := range n.Children {
		member[c.plan.Nodes[ch].ID] = int32(m)
	}
	seen := map[Edge]bool{}
	hasIn := make([]bool, len(n.Children))
	for _, ed := range d.Edges {
		from, ok1 := member[ed.From]
		to, ok2 := member[ed.To]
		if !ok1 || !ok2 {
			return fmt.Errorf("edge %s -> %s: both ends must be nodes of this graph", ed.From, ed.To)
		}
		h := ed.Handle
		if h == "" {
			h = HandleSource
		}
		if hs := c.handles(n.Children[from]); !slices.Contains(hs, h) {
			return fmt.Errorf("edge %s -> %s: %s has no handle %q (handles: %v)", ed.From, ed.To, ed.From, h, hs)
		}
		e := Edge{From: from, To: to, Handle: h}
		if seen[e] {
			return fmt.Errorf("duplicate edge %s -[%s]-> %s", ed.From, h, ed.To)
		}
		seen[e] = true
		n.Edges = append(n.Edges, e)
		hasIn[to] = true
	}
	if len(d.Entry) > 0 {
		for _, id := range d.Entry {
			m, ok := member[id]
			if !ok {
				return fmt.Errorf("entry %q is not a node of this graph", id)
			}
			if hasIn[m] {
				return fmt.Errorf("entry %q has incoming edges", id)
			}
			if slices.Contains(n.Entry, m) {
				return fmt.Errorf("duplicate entry %q", id)
			}
			n.Entry = append(n.Entry, m)
		}
	} else {
		for m := range n.Children {
			if !hasIn[m] {
				n.Entry = append(n.Entry, int32(m))
			}
		}
	}
	for m, ch := range n.Children {
		if !hasIn[m] && !slices.Contains(n.Entry, int32(m)) {
			return fmt.Errorf("node %q is not reachable from an entry", c.plan.Nodes[ch].ID)
		}
	}
	if len(n.Children) > 0 && len(n.Entry) == 0 {
		return errors.New("no entry")
	}
	// A fail-branch must lead somewhere.
	for m, ch := range n.Children {
		if cn := &c.plan.Nodes[ch]; cn.Kind == KStep && cn.OnError == OnErrorBranch &&
			!slices.ContainsFunc(n.Edges, func(e Edge) bool { return e.From == int32(m) && e.Handle == HandleFailBranch }) {
			return fmt.Errorf("node %q has on_error fail-branch but no fail-branch edge", cn.ID)
		}
	}
	return nil
}

// handles returns the handles node x can take.
func (c *compiler) handles(x int32) []string {
	n := &c.plan.Nodes[x]
	var hs []string
	switch {
	case n.Kind == KTest:
		return []string{"true", "false"}
	case n.Kind == KStep && len(n.Handles) > 0:
		hs = slices.Clone(n.Handles)
	default:
		hs = []string{HandleSource}
	}
	if n.Kind == KStep && n.OnError == OnErrorBranch {
		hs = append(hs, HandleFailBranch)
	}
	return hs
}

// finish builds the adjacency lists and the start order of graph i, and
// rejects cycles.
func (c *compiler) finish(i int32) error {
	n := &c.plan.Nodes[i]
	k := len(n.Children)
	n.In, n.Out = make([][]int32, k), make([][]int32, k)
	indeg := make([]int, k)
	for e, ed := range n.Edges {
		n.Out[ed.From] = append(n.Out[ed.From], int32(e))
		n.In[ed.To] = append(n.In[ed.To], int32(e))
		indeg[ed.To]++
	}
	// Kahn's algorithm, smallest member first: deterministic, and the
	// member order for chains and parallel groups.
	var ready []int32
	for m := 0; m < k; m++ {
		if indeg[m] == 0 {
			ready = append(ready, int32(m))
		}
	}
	n.Topo = n.Topo[:0]
	for len(ready) > 0 {
		slices.Sort(ready)
		m := ready[0]
		ready = ready[1:]
		n.Topo = append(n.Topo, m)
		for _, e := range n.Out[m] {
			to := n.Edges[e].To
			if indeg[to]--; indeg[to] == 0 {
				ready = append(ready, to)
			}
		}
	}
	if len(n.Topo) != k {
		return fmt.Errorf("ir: graph %q has a cycle (repeat with a loop instead)", n.ID)
	}
	if n.Sugar == SugarGraph {
		words := (k + 63) / 64
		n.reach = make([][]uint64, k)
		for m := range n.reach {
			n.reach[m] = make([]uint64, words)
		}
		for _, m := range n.Topo {
			for _, e := range n.Out[m] {
				to := n.Edges[e].To
				for w := range n.reach[to] {
					n.reach[to][w] |= n.reach[m][w]
				}
				n.reach[to][m/64] |= 1 << (m % 64)
			}
		}
	}
	return nil
}

// addTests gives every cond its test member: a KTest node appended after
// all other nodes, with a "true" edge to then and a "false" edge to else.
func (c *compiler) addTests() error {
	for _, g := range c.conds {
		t := int32(len(c.plan.Nodes))
		gn := c.plan.Nodes[g]
		member := int32(len(gn.Children))
		c.plan.Nodes = append(c.plan.Nodes, Node{
			Kind: KTest, ID: "_test" + strconv.Itoa(int(g)), Parent: g, Idx: member, MapScope: gn.MapScope,
		})
		c.defs = append(c.defs, nil)
		n := &c.plan.Nodes[g]
		n.Children = append(n.Children, t)
		n.Edges = append(n.Edges, Edge{From: member, To: 0, Handle: "true"})
		if member > 1 {
			n.Edges = append(n.Edges, Edge{From: member, To: 1, Handle: "false"})
		}
		n.Entry = []int32{member}
		if err := c.finish(g); err != nil {
			return err
		}
	}
	return nil
}

// graphOrder rejects a reference from node from to node t across members
// of a hand-written graph unless t's member reaches from's member: only
// then is t settled (run or skipped) before from can start.
func (c *compiler) graphOrder(from, t int32) error {
	// The innermost graph enclosing both, and the members they lie in.
	fm := map[int32]int32{} // ancestor -> child on the path to from
	for x := from; c.plan.Nodes[x].Parent >= 0; x = c.plan.Nodes[x].Parent {
		fm[c.plan.Nodes[x].Parent] = x
	}
	for x := t; c.plan.Nodes[x].Parent >= 0; x = c.plan.Nodes[x].Parent {
		g := c.plan.Nodes[x].Parent
		fx, ok := fm[g]
		if !ok {
			continue
		}
		gn := &c.plan.Nodes[g]
		if gn.Kind != KGraph || gn.Sugar != SugarGraph || fx == x {
			return nil
		}
		f, tt := c.plan.Nodes[fx].Idx, c.plan.Nodes[x].Idx
		if gn.reach[f][tt/64]&(1<<(tt%64)) == 0 {
			return fmt.Errorf("node %q may not have run when %q starts: it must be upstream in graph %q",
				c.plan.Nodes[t].ID, c.plan.Nodes[from].ID, gn.ID)
		}
		return nil
	}
	return nil
}

// errorHandling resolves step i's retries and error strategy (ADR 0030).
func (c *compiler) errorHandling(i int32, d *Def) error {
	n := &c.plan.Nodes[i]
	n.MaxAttempts = int32(n.Spec.MaxAttempts)
	if r := d.Retry; r != nil {
		if r.MaxAttempts < 1 {
			return fmt.Errorf("ir: step %q: retry.max_attempts must be at least 1", n.ID)
		}
		n.MaxAttempts = int32(r.MaxAttempts)
		n.RetryInterval = time.Duration(r.Interval)
	}
	switch d.OnUnknown {
	case "", "review":
	case "fail":
		n.UnknownFails = true
	default:
		return fmt.Errorf("ir: step %q: on_unknown must be review or fail", n.ID)
	}
	if e := d.OnError; e != nil {
		switch e.Strategy {
		case "fail-branch":
			n.OnError = OnErrorBranch
		case "default-value":
			if len(n.Handles) > 0 {
				// Its default output chooses no handle (ADR 0030).
				return fmt.Errorf("ir: step %q: a branching step cannot use on_error default-value", n.ID)
			}
			n.OnError = OnErrorDefault
			v := bytes.TrimSpace(e.Value)
			if len(v) == 0 {
				v = []byte("{}")
			}
			if v[0] != '{' || !json.Valid(v) {
				return fmt.Errorf("ir: step %q: on_error.value must be an object", n.ID)
			}
			n.ErrValue = v
		case "", "fail":
		default:
			return fmt.Errorf("ir: step %q: unknown on_error strategy %q", n.ID, e.Strategy)
		}
	}
	return nil
}
