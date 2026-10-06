// Package task defines what executors receive and what they report back.
// It is shared by the engine, the scheduler, the worker protocol and the
// built-in executors.
package task

import (
	"encoding/json"

	"kairo/ir"
)

// Task is one command handed to an executor.
type Task struct {
	RunID       string          `json:"run_id"`
	StepID      string          `json:"step_id"`
	Act         uint32          `json:"act"`
	Attempt     int32           `json:"attempt"`
	IdemKey     string          `json:"idempotency_key"`
	Action      string          `json:"action"`
	Destination string          `json:"destination"`
	Tenant      string          `json:"tenant"`
	Effect      ir.Effect       `json:"effect"`
	Input       json.RawMessage `json:"input"`
	Params      json.RawMessage `json:"params,omitempty"`
	// EstTokens is the token estimate used for TPM accounting until the
	// executor reports actual usage.
	EstTokens int `json:"est_tokens,omitempty"`

	// Seq is assigned by the dispatcher for tracking outstanding tasks.
	Seq uint64 `json:"seq"`
	// Depth is the nesting depth of the run (ADR 0030). A worker that
	// starts a workflow from this task submits it with Depth+1.
	Depth int `json:"depth,omitempty"`
}

// Key identifies a task attempt.
func (t *Task) Key() Key { return Key{RunID: t.RunID, Act: t.Act, Attempt: t.Attempt} }

type Key struct {
	RunID   string
	Act     uint32
	Attempt int32
}

// Result is what an executor reports.
type Result struct {
	RunID   string          `json:"run_id"`
	Act     uint32          `json:"act"`
	Attempt int32           `json:"attempt"`
	Output  json.RawMessage `json:"output,omitempty"`
	Err     string          `json:"error,omitempty"`
	// Retryable: a definite failure that may be retried (e.g. HTTP 429/5xx
	// where the request is known not to have taken effect).
	Retryable bool `json:"retryable,omitempty"`
	// Unknown: the outcome is not known (timeout, broken connection). Never
	// treated as success.
	Unknown bool `json:"unknown,omitempty"`
	// Tokens actually consumed, for TPM accounting.
	Tokens int `json:"tokens,omitempty"`
	// ErrType classifies a failure; it becomes error_type of a step that
	// fails into its on_error strategy (ADR 0030).
	ErrType string `json:"error_type,omitempty"`
	// Meta is passed through to the step's trace (ADR 0034): e.g. Dify's
	// process_data and execution metadata (token usage).
	Meta json.RawMessage `json:"meta,omitempty"`
	// RateLimited: the destination refused the task for its limits (an
	// HTTP 429, a provider's rate limit error). It lowers the destination's
	// concurrency (ADR 0039); it does not change the failure itself.
	RateLimited bool `json:"rate_limited,omitempty"`
}
