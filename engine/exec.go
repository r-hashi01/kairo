package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strconv"
	"strings"

	"kairo/blob"
	"kairo/core"
	"kairo/ir"
	"kairo/sched"
	"kairo/task"
)

// Executor runs tasks in-process. It is called from a bounded pool of
// goroutines (per registration, not per run), never from a shard loop, so
// it may block on I/O. CPU-heavy work belongs in a separate worker process
// connected through the worker protocol.
//
// ctx is cancelled when the step is abandoned (timeout, run cancelled,
// sibling branch failed) or the engine stops (ADR 0026). Whatever the
// executor returns after that is ignored by the run.
type Executor interface {
	Execute(ctx context.Context, t *task.Task, emit func(chunk []byte)) task.Result
}

type ExecutorFunc func(ctx context.Context, t *task.Task, emit func([]byte)) task.Result

func (f ExecutorFunc) Execute(ctx context.Context, t *task.Task, emit func([]byte)) task.Result {
	return f(ctx, t, emit)
}

// RegisterExecutor serves actions with ex. Each task runs on a goroutine
// of its own while it is outstanding; how many at once is bounded by
// concurrency for this registration, and by the engine-wide budget of the
// resource the actions use (ADR 0039). concurrency <= 0: the budget alone.
func (e *Engine) RegisterExecutor(actions []string, concurrency int, ex Executor) {
	slots := e.execBudget(resourceOf(e.cfg.Registry, actions))
	if concurrency <= 0 || concurrency > cap(slots) {
		concurrency = cap(slots)
	}
	p := e.disp.NewPoller(actions, concurrency)
	e.execMu.Lock()
	e.pollers = append(e.pollers, p)
	e.execMu.Unlock()
	go func() {
		for {
			select {
			case <-e.stopped:
				return
			case dl := <-p.C:
				select {
				case slots <- struct{}{}:
				case <-e.stopped:
					return
				}
				go func() {
					defer func() { <-slots }()
					e.runTask(ex, dl)
					e.disp.Poll(p, 1)
				}()
			}
		}
	}()
}

// execBudget is the engine-wide semaphore of in-process tasks of resource.
func (e *Engine) execBudget(resource string) chan struct{} {
	e.execMu.Lock()
	defer e.execMu.Unlock()
	if e.budgets == nil {
		e.budgets = map[string]chan struct{}{}
	}
	b := e.budgets[resource]
	if b == nil {
		b = make(chan struct{}, budget(resource))
		e.budgets[resource] = b
	}
	return b
}

func (e *Engine) runTask(ex Executor, dl sched.Delivery) {
	t := dl.Task
	if dl.Ctx.Err() != nil {
		// Aborted before it started: its result would be ignored anyway.
		e.Complete(t, task.Result{Err: "aborted", Retryable: true})
		return
	}
	ctx := dl.Ctx
	in, err := e.ResolveInput(t.Input)
	if err != nil {
		e.Complete(t, task.Result{Err: "resolving input: " + err.Error(), Retryable: true})
		return
	}
	tt := *t
	tt.Input = in
	emit := func(chunk []byte) { e.publishChunk(t.RunID, t.StepID, chunk) }
	res := ex.Execute(ctx, &tt, emit)
	e.Complete(t, res)
}

// Complete reports a task's result. It is called on the executor's
// goroutine: moving a large output into the blob store happens here, off
// the shard loop.
func (e *Engine) Complete(t *task.Task, res task.Result) {
	e.disp.Finish(t, sched.Outcome{Tokens: res.Tokens, RateLimited: res.RateLimited, TimedOut: timedOut(res)})
	ev := core.Event{Act: t.Act, Attempt: t.Attempt, Meta: res.Meta}
	if len(ev.Meta) > e.cfg.BlobThreshold {
		// Like a large output: the log keeps only a reference (ADR 0034).
		if key, err := blob.PutRunContent(e.blobs, t.RunID, ev.Meta); err == nil {
			ev.Meta, _ = json.Marshal(map[string]any{"$blob": key, "size": len(res.Meta)})
		} else {
			log.Printf("kairo: run %s: storing step metadata: %v (dropped)", t.RunID, err)
			ev.Meta = nil
		}
	}
	if res.Err != "" || res.Unknown {
		ev.Kind = core.EvStepErr
		ev.Err = res.Err
		if ev.Err == "" {
			ev.Err = "outcome unknown"
		}
		ev.Retryable, ev.Unknown, ev.ErrType = res.Retryable, res.Unknown, res.ErrType
	} else {
		ev.Kind = core.EvStepOK
		ev.Data = res.Output
		if res.Wait != nil {
			// Wait until the deadline, then end with this output (ADR 0045).
			ev.Kind, ev.Deadline, ev.Data = core.EvStepWait, res.Wait.Until, res.Wait.Output
		}
		if len(ev.Data) > e.cfg.BlobThreshold {
			env, err := e.externalize(t, ev.Data)
			if err != nil {
				ev = core.Event{Kind: core.EvStepErr, Act: t.Act, Attempt: t.Attempt, Err: "storing output: " + err.Error(), Retryable: true}
			} else {
				ev.Data = env
			}
		}
	}
	e.shardFor(t.RunID).inbox.Push(msg{kind: mEvent, runID: t.RunID, ev: ev})
}

// deleteBlobs drops a finished run's blobs (ADR 0024). Stores that cannot
// delete groups keep them.
func (e *Engine) deleteBlobs(runID string) {
	g, ok := e.blobs.(blob.Grouper)
	if !ok {
		return
	}
	if err := g.DeleteGroup(blob.RunGroup(runID)); err != nil && !errors.Is(err, blob.ErrNoGroups) {
		log.Printf("kairo: deleting blobs of run %s: %v", runID, err)
	}
}

// externalize stores out as a blob and returns the envelope that replaces
// it in the state: the reference plus the node's declared typed fields.
func (e *Engine) externalize(t *task.Task, out json.RawMessage) (json.RawMessage, error) {
	key, err := blob.PutRunContent(e.blobs, t.RunID, out)
	if err != nil {
		return nil, err
	}
	env := struct {
		Blob   string                     `json:"$blob"`
		Size   int                        `json:"size"`
		Fields map[string]json.RawMessage `json:"fields,omitempty"`
	}{Blob: key, Size: len(out)}
	spec := e.cfg.Registry.Lookup(t.Action)
	if t := bytes.TrimSpace(out); len(t) > 0 && t[0] == '[' {
		// A list short enough to be ports (ADR 0043): its length and its
		// dead ports are kept, so that the edges are decided without the
		// blob; a live port reads as a reference into it.
		var ports []json.RawMessage
		if json.Unmarshal(t, &ports) == nil && len(ports) <= ir.MaxPort+1 {
			env.Fields = map[string]json.RawMessage{"$ports": json.RawMessage(strconv.Itoa(len(ports)))}
			for i, p := range ports {
				if bytes.Equal(bytes.TrimSpace(p), []byte("null")) {
					env.Fields[strconv.Itoa(i)] = p
				}
			}
		}
	}
	if len(spec.Outputs) > 0 {
		var obj map[string]json.RawMessage
		if json.Unmarshal(out, &obj) == nil {
			for f, ft := range spec.Outputs {
				// Lists are kept whatever their size: the plan maps over them.
				if v, ok := obj[f]; ok && (len(v) <= 1024 || ft.Type == ir.FieldList) {
					if env.Fields == nil {
						env.Fields = map[string]json.RawMessage{}
					}
					env.Fields[f] = v
				}
			}
		}
	}
	return json.Marshal(env)
}

var blobMarker = []byte(`"$blob"`)

// ResolveInput replaces blob references in a task input by their content.
func (e *Engine) ResolveInput(in json.RawMessage) (json.RawMessage, error) {
	if !bytes.Contains(in, blobMarker) {
		return in, nil
	}
	var v any
	if err := json.Unmarshal(in, &v); err != nil {
		return nil, err
	}
	v, err := e.resolveValue(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (e *Engine) resolveValue(v any) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		if ref, ok := x["$blob"].(string); ok {
			data, err := e.blobs.Get(ref)
			if err != nil {
				return nil, err
			}
			var val any
			if err := json.Unmarshal(data, &val); err != nil {
				return nil, err
			}
			if path, _ := x["$path"].(string); path != "" {
				for _, seg := range strings.Split(path, ".") {
					switch c := val.(type) {
					case map[string]any:
						val = c[seg]
					case []any:
						// A port of a list of ports (ADR 0043).
						i, err := strconv.Atoi(seg)
						if err != nil || i < 0 || i >= len(c) {
							val = nil
						} else {
							val = c[i]
						}
					default:
						val = nil
					}
					if val == nil {
						break
					}
				}
			}
			return val, nil
		}
		for k, vv := range x {
			r, err := e.resolveValue(vv)
			if err != nil {
				return nil, err
			}
			x[k] = r
		}
	case []any:
		for i, vv := range x {
			r, err := e.resolveValue(vv)
			if err != nil {
				return nil, err
			}
			x[i] = r
		}
	}
	return v, nil
}

// timedOut reports a task that did not answer in time (ADR 0039: it lowers
// its destination's concurrency, as a refusal does).
func timedOut(res task.Result) bool {
	if res.Err == "" {
		return false
	}
	t := strings.ToLower(res.ErrType + " " + res.Err)
	return strings.Contains(t, "timeout") || strings.Contains(t, "timed out") || strings.Contains(t, "deadline exceeded")
}
