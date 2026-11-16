package kairo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// ReplayOutcome is how evaluation of a workflow from saved records ended.
type ReplayOutcome string

const (
	// ReplayCompleted means the workflow function returned normally.
	ReplayCompleted ReplayOutcome = "completed"
	// ReplayFailed means the workflow function returned an error or panicked.
	ReplayFailed ReplayOutcome = "failed"
	// ReplayBoundary means the function reached a call that could not be
	// reproduced from a saved terminal record.
	ReplayBoundary ReplayOutcome = "boundary"
)

// ReplayCall describes one call reached while replaying saved records.
type ReplayCall struct {
	RunID  string          `json:"run_id"`
	Kind   string          `json:"kind"` // "call", "wait", or "workflow"
	Name   string          `json:"name"`
	Status string          `json:"status"`
	Input  json.RawMessage `json:"input,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// ReplayResult is a read-only diagnostic evaluation. Run is the durable root
// and remains authoritative; Output and Err are only the local evaluation.
type ReplayResult struct {
	Run      RunInfo
	Calls    []ReplayCall
	Outcome  ReplayOutcome
	Output   json.RawMessage
	Err      error
	Boundary *ReplayCall
}

// ReplayBoundaryError reports the call where saved records stopped being
// sufficient to continue the diagnostic evaluation.
type ReplayBoundaryError struct {
	Call ReplayCall
}

func (e *ReplayBoundaryError) Error() string {
	return fmt.Sprintf("record-only replay stopped at %s (%s: %s)", e.Call.RunID, e.Call.Kind, e.Call.Status)
}

type replayDrive struct {
	k    *Kairo
	root string

	mu       sync.Mutex
	calls    []ReplayCall
	boundary *ReplayCall
	fatal    error
}

// Replay evaluates an existing embedded code workflow from saved terminal
// call records. It does not acquire a drive, create or resume runs, execute
// actions or child workflows, or advance waits.
func (k *Kairo) Replay(ctx context.Context, id string) (ReplayResult, error) {
	if k.remote != nil {
		return ReplayResult{}, ErrNeedsEmbedded
	}
	ri, err := k.runs.getInput(ctx, id)
	if err != nil {
		return ReplayResult{}, err
	}
	if ri.Plan != planWorkflow {
		return ReplayResult{}, fmt.Errorf("kairo: run %s is not a code workflow", id)
	}
	name, version, in, ok := workflowOf(ri.Input)
	if !ok {
		return ReplayResult{}, fmt.Errorf("kairo: workflow %s has no saved workflow input", id)
	}
	fn := k.fnFor(name, version)
	if fn == nil {
		return ReplayResult{}, notHere(name, version)
	}

	drive := &replayDrive{k: k, root: id}
	c := &Context{Context: ctx, k: k, id: id, replay: drive, seen: map[string]int{}, cancelled: func() bool { return false }}
	value, localErr := callWorkflow(fn, c, in)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ReplayResult{}, ctxErr
	}

	drive.mu.Lock()
	calls := append([]ReplayCall(nil), drive.calls...)
	boundary := cloneReplayCall(drive.boundary)
	fatal := drive.fatal
	drive.mu.Unlock()
	if fatal != nil {
		return ReplayResult{}, fatal
	}
	result := ReplayResult{Run: ri, Calls: calls}
	if boundary != nil {
		result.Outcome = ReplayBoundary
		result.Boundary = boundary
		result.Err = &ReplayBoundaryError{Call: *boundary}
		return result, nil
	}
	if localErr != nil {
		result.Outcome = ReplayFailed
		result.Err = localErr
		return result, nil
	}
	out, err := json.Marshal(value)
	if err != nil {
		result.Outcome = ReplayFailed
		result.Err = err
		return result, nil
	}
	result.Outcome = ReplayCompleted
	result.Output = out
	return result, nil
}

func cloneReplayCall(call *ReplayCall) *ReplayCall {
	if call == nil {
		return nil
	}
	copy := *call
	return &copy
}

func replayCallKind(plan string) (kind, name string) {
	if action, ok := strings.CutPrefix(plan, planCall); ok {
		return "call", action
	}
	if wait, ok := strings.CutPrefix(plan, planWait); ok {
		name, _, _ = strings.Cut(wait, "@")
		return "wait", name
	}
	return "call", plan
}

func (d *replayDrive) call(ctx context.Context, id, kind, name string, in any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	boundary := cloneReplayCall(d.boundary)
	d.mu.Unlock()
	if boundary != nil {
		return nil, &ReplayBoundaryError{Call: *boundary}
	}
	input, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	ri, err := d.k.runs.get(ctx, id)
	if err != nil {
		if errors.Is(err, ErrUnknownRun) {
			return nil, d.stop(ReplayCall{RunID: id, Kind: kind, Name: name, Status: "missing", Input: input})
		}
		d.setFatal(err)
		return nil, err
	}
	call := ReplayCall{RunID: id, Kind: kind, Name: name, Status: ri.Status, Input: input, Output: ri.Output, Error: ri.Error}
	if ri.Parent != d.root {
		call.Status = "missing"
		call.Output = nil
		call.Error = ""
		return nil, d.stop(call)
	}
	if ri.trimmed {
		call.Status = "trimmed"
		call.Output = nil
		return nil, d.stop(call)
	}
	if !ri.Finished() {
		return nil, d.stop(call)
	}
	d.append(call)
	if kind == "workflow" {
		return done(id, ri)
	}
	if ri.Status != "completed" {
		return nil, &CallError{RunID: id, Action: calledAction(ri.Plan), Status: ri.Status, Message: ri.Error}
	}
	return ri.Output, nil
}

func (d *replayDrive) append(call ReplayCall) {
	d.mu.Lock()
	d.calls = append(d.calls, call)
	d.mu.Unlock()
}

func (d *replayDrive) stop(call ReplayCall) error {
	d.mu.Lock()
	d.calls = append(d.calls, call)
	if d.boundary == nil {
		copy := call
		d.boundary = &copy
	}
	d.mu.Unlock()
	return &ReplayBoundaryError{Call: call}
}

func (d *replayDrive) setFatal(err error) {
	d.mu.Lock()
	if d.fatal == nil {
		d.fatal = err
	}
	d.mu.Unlock()
}
