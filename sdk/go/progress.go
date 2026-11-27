package kairo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// A run's progress (ADR 0088): its tree, and each run's recorded events,
// in public types, as the TypeScript SDK gives it (sdk/ts/src/progress.ts).
// Only what was recorded, as recorded: nothing inferred (an attempt's start
// is not recorded unless it was a real step's intent), no clocks adjusted.
// Reading changes nothing.

// ErrProgressUnsupported means the store cannot read runs' events (it is
// not a TreeStore), so Progress cannot give a run's progress.
var ErrProgressUnsupported = errors.New("kairo: this store cannot read runs' events")

// RunEventKind is what a recorded event says happened.
type RunEventKind string

const (
	// EventStarted: the run started.
	EventStarted RunEventKind = "started"
	// EventDispatched: a real step's attempt passed its limits and went to
	// run (its intent).
	EventDispatched RunEventKind = "dispatched"
	// EventSucceeded and EventFailed: an attempt's outcome.
	EventSucceeded RunEventKind = "succeeded"
	EventFailed    RunEventKind = "failed"
	// EventWaiting: an attempt reported it waits until a deadline.
	EventWaiting RunEventKind = "waiting"
	// EventTimer: a timer fired (a retry's backoff, a wait's timeout, a
	// sleep).
	EventTimer RunEventKind = "timer"
	// EventSignal: a signal was received.
	EventSignal RunEventKind = "signal"
	// EventRecovered: a step whose process stopped before it was
	// dispatched was given out again.
	EventRecovered RunEventKind = "recovered"
	// EventResolved: a step stopped for review was settled.
	EventResolved RunEventKind = "resolved"
	// EventCancelled: the run was cancelled.
	EventCancelled RunEventKind = "cancelled"
	// EventOther: an event of a kind this SDK does not name.
	EventOther RunEventKind = "other"
)

// RunEvent is a recorded event of a run (ADR 0088). Fields that do not
// apply to its kind are zero.
type RunEvent struct {
	Kind RunEventKind `json:"kind"`
	// At is when it was applied, by the clock of the process that applied
	// it.
	At time.Time `json:"at"`
	// Attempt is the attempt (dispatched, succeeded, failed, waiting).
	Attempt int32 `json:"attempt,omitempty"`
	// Error is a failure's message (failed, resolved as failed, cancelled).
	Error string `json:"error,omitempty"`
	// ErrorType is a failure's type as recorded; "process_lost": the
	// process running the attempt stopped.
	ErrorType string `json:"error_type,omitempty"`
	// Retryable: the failure may be tried again.
	Retryable bool `json:"retryable,omitempty"`
	// Unknown: the attempt's outcome is not known: it may have taken
	// effect.
	Unknown bool `json:"unknown,omitempty"`
	// RetryAfter is how long the retry was asked to wait.
	RetryAfter time.Duration `json:"retry_after,omitempty"`
	// Signal is a signal's name (signal).
	Signal string `json:"signal,omitempty"`
	// Deadline is until when an attempt waits (waiting).
	Deadline time.Time `json:"deadline,omitzero"`
}

// ProgressRun is a run of the tree, as Children gives it, with its events
// (ADR 0088).
type ProgressRun struct {
	RunInfo
	// Kind is what the run is in a code workflow's tree: "workflow",
	// "call", "wait", or "run" (any other plan).
	Kind string `json:"kind"`
	// Name is the workflow's, action's or wait's name; a plain run's plan.
	Name   string     `json:"name"`
	Events []RunEvent `json:"events"`
}

// RunProgress is a run's tree and its recorded events (Kairo.Progress, ADR
// 0088).
type RunProgress struct {
	Root string `json:"root"`
	// Runs: the root first, then the runs under it in creation order.
	Runs []ProgressRun `json:"runs"`
}

// Progress returns workflow id's tree (it and every run under it: its
// calls, waits and child workflows), each run with its recorded events
// (ADR 0088): the attempts, their outcomes, steps whose process stopped.
// Only what was recorded, read in one transaction of the store; it changes
// nothing (no drive, lease, timer or write). The embedded runtime only
// (ErrNeedsEmbedded), on a store that is a TreeStore
// (ErrProgressUnsupported). A run that is not there, or was removed with
// its finished tree, is ErrUnknownRun.
func (k *Kairo) Progress(ctx context.Context, id string) (RunProgress, error) {
	if k.remote != nil {
		return RunProgress{}, ErrNeedsEmbedded
	}
	if k.rt.trees == nil {
		return RunProgress{}, ErrProgressUnsupported
	}
	tree, err := k.rt.trees.Tree(ctx, id)
	if err != nil {
		return RunProgress{}, err
	}
	if len(tree) == 0 {
		return RunProgress{}, fmt.Errorf("%w: %s", ErrUnknownRun, id)
	}
	out := RunProgress{Root: id, Runs: make([]ProgressRun, len(tree))}
	for i := range tree {
		ri := info(&tree[i].Run)
		kind, name := progressKind(ri)
		events := make([]RunEvent, len(tree[i].Events))
		for j, body := range tree[i].Events {
			if events[j], err = runEvent(body); err != nil {
				return RunProgress{}, fmt.Errorf("kairo: run %s, event %d: %w", ri.RunID, j, err)
			}
		}
		out.Runs[i] = ProgressRun{RunInfo: ri, Kind: kind, Name: name, Events: events}
	}
	return out, nil
}

// recordedEvent: the fields of a recorded event (the core's encoding) that
// RunEvent says. The act, vars and meta stay out.
type recordedEvent struct {
	Kind       string `json:"kind"`
	At         int64  `json:"at"`
	Attempt    int32  `json:"attempt"`
	Name       string `json:"name"`
	Err        string `json:"error"`
	ErrType    string `json:"error_type"`
	Retryable  bool   `json:"retryable"`
	Unknown    bool   `json:"unknown"`
	RetryAfter int64  `json:"retry_after_ms"`
	Deadline   int64  `json:"deadline"`
}

var runEventKinds = map[string]RunEventKind{
	"start":     EventStarted,
	"intent":    EventDispatched,
	"step_ok":   EventSucceeded,
	"step_err":  EventFailed,
	"step_wait": EventWaiting,
	"timer":     EventTimer,
	"signal":    EventSignal,
	"recover":   EventRecovered,
	"resolve":   EventResolved,
	"cancel":    EventCancelled,
}

// runEvent is the public form of a recorded event.
func runEvent(body json.RawMessage) (RunEvent, error) {
	var e recordedEvent
	if err := json.Unmarshal(body, &e); err != nil {
		return RunEvent{}, err
	}
	kind, ok := runEventKinds[e.Kind]
	if !ok {
		kind = EventOther
	}
	out := RunEvent{Kind: kind, At: time.UnixMilli(e.At), Error: e.Err, ErrorType: e.ErrType, Retryable: e.Retryable, Unknown: e.Unknown,
		RetryAfter: time.Duration(e.RetryAfter) * time.Millisecond}
	switch kind {
	case EventDispatched, EventSucceeded, EventFailed, EventWaiting:
		out.Attempt = e.Attempt
	}
	if kind == EventSignal {
		out.Signal = e.Name
	}
	if kind == EventWaiting && e.Deadline != 0 {
		out.Deadline = time.UnixMilli(e.Deadline)
	}
	return out, nil
}

// progressKind is what run r is in a code workflow's tree, and its name.
func progressKind(r RunInfo) (kind, name string) {
	if r.Plan == planWorkflow {
		return "workflow", r.Workflow
	}
	if n, ok := strings.CutPrefix(r.Plan, planCall); ok {
		return "call", n
	}
	if n, ok := strings.CutPrefix(r.Plan, planWait); ok {
		// A wait with a timeout: kairo.wait/<signal>@<ms>.
		if i := strings.LastIndex(n, "@"); i >= 0 {
			if ms, err := strconv.ParseInt(n[i+1:], 10, 64); err == nil && ms > 0 {
				n = n[:i]
			}
		}
		return "wait", n
	}
	return "run", r.Plan
}
