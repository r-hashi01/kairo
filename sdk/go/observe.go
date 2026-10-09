package kairo

import (
	"strings"
	"time"
)

// Observation is something that happened in this process's runtime, given
// to Options.Observe as it happens: to count, time and trace runs (metrics,
// OpenTelemetry) without kairo depending on either.
type Observation struct {
	Kind string // ObsRunStarted, ObsRunSettled, ObsStepStarted, ObsStepFinished
	At   time.Time

	RunID    string
	Parent   string // the run that made it (a workflow, of its calls)
	Plan     string // kairo.workflow, kairo.call/<action>, kairo.wait/<signal>
	Workflow string // a workflow run's workflow
	Action   string // a call's or a step's action

	// ObsRunSettled: the run's status (completed, failed, cancelled, or
	// blocked: stopped for review) and its error.
	// ObsStepFinished: how the attempt ended (StepOK, StepRetryable,
	// StepFailed, StepUnknown, StepWaiting, StepPending), its error, and
	// how long the handler ran here.
	Status   string
	Error    string
	StepID   string
	Attempt  int32
	Duration time.Duration
}

// Kinds of Observation.
const (
	ObsRunStarted   = "run.started"
	ObsRunSettled   = "run.settled"
	ObsStepStarted  = "step.started"
	ObsStepFinished = "step.finished"
)

// How a step's attempt ended (Observation.Status of ObsStepFinished).
const (
	StepOK        = "ok"
	StepRetryable = "retryable" // failed, may be tried again
	StepFailed    = "failed"    // failed for good
	StepUnknown   = "unknown"   // its outcome is unknown (invariant 5)
	StepWaiting   = "waiting"   // waits in kairo (a sleep)
	StepPending   = "pending"   // runs elsewhere; its outcome comes later (ADR 0052)
)

// observe gives o to the observer, if there is one. An observer that
// panics is logged, not let stop the runtime.
func (r *runtime) observe(o Observation) {
	if r.observer == nil {
		return
	}
	if o.Action == "" {
		o.Action, _ = strings.CutPrefix(o.Plan, planCall)
		if o.Action == o.Plan {
			o.Action = ""
		}
	}
	if o.At.IsZero() {
		o.At = time.UnixMilli(r.now())
	}
	defer func() {
		if p := recover(); p != nil {
			r.logger.Error("kairo: the observer panicked", "kind", o.Kind, "panic", p)
		}
	}()
	r.observer(o)
}

// stepStatus is how a step's attempt ended.
func stepStatus(res result) string {
	switch {
	case res.Pending != nil:
		return StepPending
	case res.Unknown:
		return StepUnknown
	case res.Err != "" && res.Retryable:
		return StepRetryable
	case res.Err != "":
		return StepFailed
	case res.Wait != nil:
		return StepWaiting
	}
	return StepOK
}
