// Package jev runs typed decisions with TypeSafe Jev (ADR 0055) as kairo
// actions: a yes/no probability, a choice among named options, or a score.
// The answers are typed, so a plan branches on them (Outputs, Branch)
// without parsing text.
//
// It is a module of its own: kairo itself uses the standard library only
// (ADR 0018). It serves its actions to kairod as a worker, to a Go program
// that embeds the engine as an executor, and to the SDKs' embedded runtime
// as actions over HTTP(S) (cmd/kairo-jev, ADR 0052).
//
// Actions, and their outputs:
//
//	jev.noul               {"yes": <probability>, "confidence": <number>}
//	jev.choice, jev.choice.<name>
//	                       {"choice": <option>, "probabilities": {...}, "confidence": <number>}
//	jev.score              {"score": <number>, "confidence": <number>}
//
// A choice among given options is declared per decision, under a name of
// its own (jev.choice.route), so its spec can make "choice" an enum of the
// options and branch on it.
//
// The question comes from the step's params (kairod's plans) and its input
// (the SDKs' ctx.call), the input first:
//
//	{"state": <what is judged>, "instructions": <the question>, "criteria": <options>}
//
// criteria: for a choice, the options: ["a", "b"], [{"name": "a", "desc": "..."}]
// or {"a": "...", "b": "..."}; for a score, what the API takes.
package jev

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	jev "github.com/mattn/go-jev"

	"kairo/task"
)

// Destination is the rate-limit key of the actions (ADR 0039).
const Destination = "typesafe/jev"

// Executor answers jev.* steps with a client of the Jev API.
type Executor struct {
	Client *jev.Client
}

// New returns an executor answering with c.
func New(c *jev.Client) *Executor { return &Executor{Client: c} }

type question struct {
	State        any             `json:"state"`
	Instructions any             `json:"instructions"`
	Criteria     json.RawMessage `json:"criteria"`
}

// Kind is the kind of question of an action: "noul", "choice" or "score"
// (jev.choice.route is a choice); "" if the action is not jev's.
func Kind(action string) string {
	rest, ok := strings.CutPrefix(action, "jev.")
	if !ok {
		return ""
	}
	kind, _, _ := strings.Cut(rest, ".")
	switch kind {
	case "noul", "choice", "score":
		return kind
	}
	return ""
}

// Execute answers one step.
func (x *Executor) Execute(ctx context.Context, t *task.Task, _ func([]byte)) task.Result {
	fail := func(format string, a ...any) task.Result {
		return task.Result{Err: fmt.Sprintf(format, a...), ErrType: "invalid"}
	}
	kind := Kind(t.Action)
	if kind == "" {
		return fail("%s: not a jev action (jev.noul, jev.choice[.name], jev.score)", t.Action)
	}
	var q question
	// The params first, the input over them.
	for _, src := range []json.RawMessage{t.Params, t.Input} {
		if len(src) == 0 || string(src) == "null" {
			continue
		}
		var part question
		if err := json.Unmarshal(src, &part); err != nil {
			return fail("%s: the question: %v", t.Action, err)
		}
		if part.State != nil {
			q.State = part.State
		}
		if part.Instructions != nil {
			q.Instructions = part.Instructions
		}
		if len(part.Criteria) > 0 && string(part.Criteria) != "null" {
			q.Criteria = part.Criteria
		}
	}
	if q.Instructions == nil {
		return fail("%s: no instructions", t.Action)
	}
	ask := jev.Question{Type: kind, Instructions: q.Instructions}
	var names []string
	switch {
	case kind == "choice":
		opts, err := options(q.Criteria)
		if err != nil {
			return fail("%s: %v", t.Action, err)
		}
		ask.Criteria = opts
		for _, o := range opts {
			names = append(names, o.Name)
		}
	case len(q.Criteria) > 0:
		ask.Criteria = q.Criteria
	}
	a, err := x.Client.Ask(ctx, q.State, ask)
	if err != nil {
		return failure(err)
	}
	var out any
	switch kind {
	case "noul":
		out = map[string]any{"yes": a.Noul, "confidence": a.Confidence}
	case "score":
		out = map[string]any{"score": a.Score, "confidence": a.Confidence}
	case "choice":
		// The answer must be one of the options: the plan's enum and its
		// branches are made of them.
		known := false
		for _, n := range names {
			known = known || n == a.Choice
		}
		if !known {
			return task.Result{Err: fmt.Sprintf("%s: the answer %q is not one of %v", t.Action, a.Choice, names), ErrType: "invalid_answer"}
		}
		probs := a.Probabilities
		if probs == nil {
			probs = map[string]float64{}
		}
		out = map[string]any{"choice": a.Choice, "probabilities": probs, "confidence": a.Confidence}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return fail("%s: %v", t.Action, err)
	}
	return task.Result{Output: b}
}

// failure classifies an error of the Jev API.
func failure(err error) task.Result {
	var api *jev.APIError
	switch {
	case errors.As(err, &api) && (api.Status == 429 || api.Status == 529):
		// go-jev retried already: the limits hold. It did not take effect.
		return task.Result{Err: err.Error(), Retryable: true, RateLimited: true, ErrType: "rate_limited"}
	case errors.As(err, &api) && api.Status >= 400 && api.Status < 500:
		return task.Result{Err: err.Error(), ErrType: fmt.Sprintf("http_%d", api.Status)}
	case errors.As(err, &api):
		return task.Result{Err: err.Error(), Unknown: true, ErrType: fmt.Sprintf("http_%d", api.Status)}
	default:
		// Not connected, timed out, an answer not understood: unknown. A
		// decision acts on nothing outside, so it may be asked again.
		return task.Result{Err: err.Error(), Unknown: true, ErrType: "jev"}
	}
}

// options reads a choice's criteria: a list of names, a list of
// {"name", "desc"}, or an object of name to description (its order kept).
func options(raw json.RawMessage) (jev.Options, error) {
	if len(raw) == 0 {
		return nil, errors.New("a choice needs its options (criteria)")
	}
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err == nil {
		var opts jev.Options
		for _, item := range list {
			var name string
			if json.Unmarshal(item, &name) == nil {
				opts = append(opts, jev.Option{Name: name})
				continue
			}
			var o struct {
				Name string `json:"name"`
				Desc any    `json:"desc"`
			}
			if err := json.Unmarshal(item, &o); err != nil || o.Name == "" {
				return nil, fmt.Errorf("an option: %s", item)
			}
			opts = append(opts, jev.Option{Name: o.Name, Desc: o.Desc})
		}
		if len(opts) == 0 {
			return nil, errors.New("a choice needs its options (criteria)")
		}
		return opts, nil
	}
	// An object: read in order (a Go map would lose it).
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("criteria: a list of options or an object, not %s", raw)
	}
	var opts jev.Options
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		var desc any
		if err := dec.Decode(&desc); err != nil {
			return nil, err
		}
		opts = append(opts, jev.Option{Name: tok.(string), Desc: desc})
	}
	if len(opts) == 0 {
		return nil, errors.New("a choice needs its options (criteria)")
	}
	return opts, nil
}
