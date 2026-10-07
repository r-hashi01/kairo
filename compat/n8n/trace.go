package n8n

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"kairo/core"
	"kairo/ir"
)

// stepID names the row of node's iteration of an execution: a UUID derived
// from them, so that the same step is the same row however often its
// traces are read (after a restart, the engine delivers what was not
// acknowledged again).
func stepID(execID, node string, iteration int) string {
	h := sha1.Sum([]byte(execID + "\x00" + node + "\x00" + strconv.Itoa(iteration)))
	h[6] = h[6]&0x0f | 0x50 // version 5
	h[8] = h[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

// iterationOf reads the loop round from a kairo step id ("body[2]": 2).
func iterationOf(step string) int {
	i := strings.LastIndexByte(step, '[')
	if i < 0 || !strings.HasSuffix(step, "]") {
		return 0
	}
	n, err := strconv.Atoi(step[i+1 : len(step)-1])
	if err != nil {
		return 0
	}
	return n
}

// nodeOf says which n8n node a plan node stands for: an n8n v1 node is
// itself; a batch node's kairo.slice is the batch node (its rounds are the
// batch node's steps). Other plan nodes (the trigger, the loop's
// variables, the graphs) have no steps of their own.
func nodeOf(n *ir.Node) (string, bool) {
	switch {
	case n.Kind == ir.KStep && n.Spec.Action == ActionNode:
		return n.ID, true
	case n.Kind == ir.KStep && n.Spec.Action == ir.ActionSlice:
		return strings.TrimSuffix(n.ID, sliceSuffix), true
	}
	return "", false
}

// loopOf is the innermost loop holding plan node n, or -1.
func loopOf(p *ir.Plan, n int32) int32 {
	for x := p.Nodes[n].Parent; x >= 0; x = p.Nodes[x].Parent {
		if p.Nodes[x].Kind == ir.KLoop {
			return x
		}
	}
	return -1
}

func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// translate records one trace of execution ri and returns its lifecycle
// events and, at the end, its "ended" response.
func (d *Daemon) translate(ctx context.Context, tx *Tx, ri *runInfo, tr *core.Trace) ([]LifecycleEvent, map[string]any, error) {
	x := ri.exec
	at := time.UnixMilli(tr.At).UTC()
	switch tr.Kind {
	case core.TrRunStart:
		if err := d.Views.Running(ctx, tx, x.ID, at); err != nil {
			return nil, nil, err
		}
		return []LifecycleEvent{{Type: "execution:started", ExecutionID: x.ID, WorkflowID: x.WorkflowID, At: iso(at),
			Mode: x.Mode, HostMode: x.HostMode}}, nil, nil
	case core.TrRunEnd:
		status, typ := "completed", "execution:completed"
		if tr.Status != "completed" {
			status, typ = "failed", "execution:failed"
		}
		if err := d.Views.Finish(ctx, tx, x.ID, status, at); err != nil {
			return nil, nil, err
		}
		end := map[string]any{"type": "ended", "executionId": x.ID, "workflowId": x.WorkflowID, "status": status}
		if ri.last != nil {
			last := map[string]any{"nodeId": ri.last.NodeID, "nodeName": ri.lastName, "status": ri.last.Status,
				"outputs": ri.last.Outputs}
			if len(ri.last.Error) > 0 && string(ri.last.Error) != "null" {
				last["error"] = ri.last.Error
			}
			end["lastStep"] = last
		}
		d.mu.Lock()
		delete(d.runs, x.ID)
		d.mu.Unlock()
		return []LifecycleEvent{{Type: typ, ExecutionID: x.ID, WorkflowID: x.WorkflowID, At: iso(at)}}, end, nil
	case core.TrNodeStart, core.TrNodeEnd, core.TrNodeSkip, core.TrNodeWait:
	default:
		return nil, nil, nil
	}
	if tr.Node < 0 || int(tr.Node) >= len(ri.plan.Nodes) {
		return nil, nil, nil
	}
	pn := &ri.plan.Nodes[tr.Node]
	node, ok := nodeOf(pn)
	if !ok {
		return nil, nil, nil
	}
	it := iterationOf(tr.StepID)
	if tr.Kind == core.TrNodeSkip && pn.Spec.Action != ir.ActionSlice {
		// engine v2 creates no body steps in a loop's terminal round, not
		// even skipped ones.
		if l := loopOf(ri.plan, tr.Node); l >= 0 {
			if r, ok := ri.ended[l]; ok && r == it {
				return nil, nil, nil
			}
		}
	}
	s := &Step{ID: stepID(x.ID, node, it), NodeID: node, Iteration: it, UpdatedAt: at}
	wasWaiting := ri.waiting[s.ID]
	it2 := it
	ev := LifecycleEvent{ExecutionID: x.ID, StepID: s.ID, NodeID: node, NodeName: ri.names[node], Iteration: &it2, At: iso(at)}
	switch tr.Kind {
	case core.TrNodeStart:
		s.Status, ev.Type = "running", "step:started"
	case core.TrNodeSkip:
		s.Status = "skipped"
	case core.TrNodeWait:
		// As engine v2: the step waits, with no lifecycle event (ADR 0045).
		s.Status = "waiting"
	case core.TrNodeEnd:
		if tr.Status == "succeeded" {
			s.Status, ev.Type = "completed", "step:completed"
			s.Outputs = tr.Output
			if pn.Spec.Action == ir.ActionSlice {
				// The batch node's slots: [done, loop] (the rest is the
				// loop's own).
				var ports []json.RawMessage
				if json.Unmarshal(tr.Output, &ports) == nil && len(ports) >= 2 {
					s.Outputs, _ = json.Marshal(ports[:2])
					if string(ports[1]) == "null" {
						if ri.ended == nil {
							ri.ended = map[int32]int{}
						}
						ri.ended[loopOf(ri.plan, tr.Node)] = it
					}
				}
			}
			ev.Outputs = s.Outputs
		} else {
			s.Status, ev.Type = "failed", "step:failed"
			name := tr.ErrType
			if name == "" {
				name = "Error"
			}
			s.Error, _ = json.Marshal(map[string]string{"name": name, "message": tr.Err})
		}
	}
	if err := d.Views.PutStep(ctx, tx, x.ID, s); err != nil {
		return nil, nil, err
	}
	if s.Status == "waiting" {
		if ri.waiting == nil {
			ri.waiting = map[string]bool{}
		}
		ri.waiting[s.ID] = true
	} else if s.Status != "running" {
		delete(ri.waiting, s.ID)
		ri.last, ri.lastName = s, ri.names[node]
	}
	if s.Status == "waiting" || len(ri.waiting) > 0 || tr.Kind == core.TrNodeEnd && wasWaiting {
		if err := d.Views.Live(ctx, tx, x.ID, at); err != nil {
			return nil, nil, err
		}
	}
	if ev.Type == "" {
		return nil, nil, nil
	}
	return []LifecycleEvent{ev}, nil, nil
}
