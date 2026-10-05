package core

import (
	"bytes"
	"encoding/json"
	"slices"
	"strconv"

	"kairo/ir"
)

// Graph execution (ADR 0029). A graph activation keeps one state per
// member (unrun, running, done, skipped) and per edge (unknown, taken,
// skipped). A member is ready once none of its incoming edges is unknown:
// it runs if one of them was taken, otherwise it is skipped and its own
// edges are skipped in turn. Members are considered in the plan's
// topological order, so a single pass settles everything that can be
// settled, and simultaneously ready members start in a fixed order.

// startGraph begins graph activation id.
func (m *machine) startGraph(id uint32, a *Act, n *ir.Node) {
	a.G = &Graph{Members: make([]uint8, len(n.Children)), Edges: make([]uint8, len(n.Edges))}
	if a.Parent == 0 && m.entry != "" {
		// The run chose one entry of its root graph; the others' paths
		// are skipped. Only hand-written graphs have entries to choose.
		found := false
		if n.Sugar != ir.SugarGraph {
			m.fail(StatusFailed, "unknown entry "+strconv.Quote(m.entry))
			return
		}
		for _, mi := range n.Entry {
			if m.p.Nodes[n.Children[mi]].ID == m.entry {
				found = true
				continue
			}
			m.skipMember(id, n, a, mi)
		}
		if !found {
			m.fail(StatusFailed, "unknown entry "+strconv.Quote(m.entry))
			return
		}
	}
	m.settle(id)
}

// settle starts or skips every member that became ready, and completes
// the graph when nothing is left to run.
func (m *machine) settle(id uint32) {
	a := m.s.Acts[id]
	if a == nil {
		return
	}
	n := &m.p.Nodes[a.Node]
	g := a.G
	for _, mi := range n.Topo {
		if g.Members[mi] != mUnrun {
			continue
		}
		taken, open := false, false
		for _, e := range n.In[mi] {
			switch g.Edges[e] {
			case eUnknown:
				open = true
			case eTaken:
				taken = true
			}
		}
		if open {
			continue
		}
		if len(n.In[mi]) > 0 && !taken {
			m.skipMember(id, n, a, mi)
			continue
		}
		child := n.Children[mi]
		if cn := &m.p.Nodes[child]; cn.Kind == ir.KTest {
			g.Members[mi] = mDone
			h := "false"
			if m.evalPred(cn.Pred, a.Scope) {
				h = "true"
			}
			m.takeEdges(n, g, mi, h)
			continue
		}
		g.Members[mi] = mRunning
		a.Pending++
		m.start(child, id, a.Scope, mi)
		if m.s.Acts[id] == nil {
			return // the member completed the graph, or failed the run
		}
	}
	if a.Pending == 0 {
		m.complete(id, a, m.graphOutput(n, a))
	}
}

// memberDone records that member c (an activation that just finished with
// out) is done and moves on.
func (m *machine) memberDone(id uint32, a *Act, c *Act, out json.RawMessage) {
	n := &m.p.Nodes[a.Node]
	child := &m.p.Nodes[c.Node]
	mi := c.Idx
	a.G.Members[mi] = mDone
	a.Pending--
	h := ir.HandleSource
	// Only hand-written graphs branch: in seq, par and cond every edge is
	// "source" and every member runs, as before they became graphs.
	if c.Flags&fFailBranch != 0 {
		h = ir.HandleFailBranch
	} else if n.Sugar == ir.SugarGraph && child.Kind == ir.KStep && child.Spec.Branch != "" {
		var v string
		if json.Unmarshal(extract(out, []string{child.Spec.Branch}), &v) != nil ||
			!slices.Contains(child.Handles, v) {
			m.failAt(id, "step "+child.ID+": branch field "+strconv.Quote(child.Spec.Branch)+" is not one of its handles")
			return
		}
		h = v
	}
	m.takeEdges(n, a.G, mi, h)
	m.settle(id)
}

// takeEdges takes member mi's edges with handle h and skips the others.
func (m *machine) takeEdges(n *ir.Node, g *Graph, mi int32, h string) {
	for _, e := range n.Out[mi] {
		if n.Edges[e].Handle == h {
			g.Edges[e] = eTaken
		} else {
			g.Edges[e] = eSkipped
		}
	}
}

// skipMember marks member mi skipped. In a hand-written graph its values
// (and those of the nodes inside it) are cleared, so a reference to a node
// that did not run reads null even in a later loop iteration. Conds keep
// the earlier behaviour of leaving them.
func (m *machine) skipMember(id uint32, n *ir.Node, a *Act, mi int32) {
	a.G.Members[mi] = mSkipped
	if m.tracing {
		// The member has no activation: its step id is the graph's path.
		tmp := Act{Node: n.Children[mi], Parent: id, Idx: mi}
		m.trace(Trace{Kind: TrNodeSkip, Node: tmp.Node, StepID: m.stepID(0, &tmp)})
	}
	for _, e := range n.Out[mi] {
		a.G.Edges[e] = eSkipped
	}
	if n.Sugar != ir.SugarGraph {
		return
	}
	if sc := m.s.Scopes[a.Scope]; sc != nil && len(sc.Vals) > 0 {
		child := n.Children[mi]
		for x := child; x < m.p.Nodes[child].End; x++ {
			delete(sc.Vals, x)
		}
	}
}

// graphOutput is the value of a finished graph.
func (m *machine) graphOutput(n *ir.Node, a *Act) json.RawMessage {
	sc := m.s.Scopes[a.Scope]
	val := func(node int32) json.RawMessage {
		if sc != nil {
			if v := sc.Vals[node]; len(v) > 0 {
				return v
			}
		}
		return null
	}
	switch n.Sugar {
	case ir.SugarSeq:
		if len(n.Children) == 0 {
			return null
		}
		return val(n.Children[len(n.Children)-1])
	case ir.SugarPar:
		return m.memberObject(n, func(int32) bool { return true }, val)
	case ir.SugarCond:
		for mi, c := range n.Children {
			if m.p.Nodes[c].Kind != ir.KTest && a.G.Members[mi] == mDone {
				return val(c)
			}
		}
		return null
	}
	if len(n.Output) > 0 {
		return m.buildObject(n.Output, a.Scope)
	}
	return m.memberObject(n, func(mi int32) bool { return len(n.Out[mi]) == 0 && a.G.Members[mi] == mDone }, val)
}

// memberObject is {id: value} of the members keep selects, in member order.
func (m *machine) memberObject(n *ir.Node, keep func(int32) bool, val func(int32) json.RawMessage) json.RawMessage {
	b := []byte{'{'}
	for mi, c := range n.Children {
		if !keep(int32(mi)) {
			continue
		}
		if len(b) > 1 {
			b = append(b, ',')
		}
		b = strconv.AppendQuote(b, m.p.Nodes[c].ID)
		b = append(b, ':')
		b = append(b, val(c)...)
	}
	return append(b, '}')
}

// migrateGraph rebuilds the graph state of an activation decoded from a
// version 1 snapshot, when seq, par and cond kept their progress in Pos,
// Results and their running child. It runs before a member's activation
// is removed, so a cond still finds its running branch.
func (m *machine) migrateGraph(id uint32, a *Act, n *ir.Node) {
	if a.G != nil {
		return
	}
	g := &Graph{Members: make([]uint8, len(n.Children)), Edges: make([]uint8, len(n.Edges))}
	a.G = g
	switch n.Sugar {
	case ir.SugarSeq:
		for mi := range g.Members {
			switch {
			case int32(mi) < a.Pos:
				g.Members[mi] = mDone
			case int32(mi) == a.Pos:
				g.Members[mi] = mRunning
			}
		}
		for e, ed := range n.Edges {
			if ed.From < a.Pos {
				g.Edges[e] = eTaken
			}
		}
		a.Pos = 0
	case ir.SugarPar:
		for mi := range g.Members {
			if mi < len(a.Results) && a.Results[mi] != nil {
				g.Members[mi] = mDone
			} else {
				g.Members[mi] = mRunning
			}
		}
		a.Results = nil
	case ir.SugarCond:
		running := int32(-1)
		for _, cid := range sortedActs(m.s) {
			if c := m.s.Acts[cid]; c.Parent == id {
				running = c.Idx
			}
		}
		for mi, c := range n.Children {
			switch {
			case m.p.Nodes[c].Kind == ir.KTest:
				g.Members[mi] = mDone
			case int32(mi) == running:
				g.Members[mi] = mRunning
			default:
				g.Members[mi] = mSkipped
			}
		}
		for e, ed := range n.Edges {
			if ed.To == running {
				g.Edges[e] = eTaken
			} else {
				g.Edges[e] = eSkipped
			}
		}
	}
	// Only par counted its running children.
	a.Pending = int32(bytes.Count(g.Members, []byte{mRunning}))
}
