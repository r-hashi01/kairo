package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"kairo/blob"
	"kairo/core"
	"kairo/ir"
	"kairo/live"
	"kairo/task"
)

// Executor runs tasks in-process. It is called from a bounded pool of
// goroutines (per registration, not per run), never from a shard loop, so
// it may block on I/O. CPU-heavy work belongs in a separate worker process
// connected through the worker protocol.
type Executor interface {
	Execute(ctx context.Context, t *task.Task, emit func(chunk []byte)) task.Result
}

type ExecutorFunc func(ctx context.Context, t *task.Task, emit func([]byte)) task.Result

func (f ExecutorFunc) Execute(ctx context.Context, t *task.Task, emit func([]byte)) task.Result {
	return f(ctx, t, emit)
}

// RegisterExecutor serves actions with ex using concurrency goroutines that
// pull tasks from the dispatcher.
func (e *Engine) RegisterExecutor(actions []string, concurrency int, ex Executor) {
	if concurrency <= 0 {
		concurrency = 1
	}
	p := e.disp.NewPoller(actions, concurrency)
	e.execMu.Lock()
	e.pollers = append(e.pollers, p)
	e.execMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-e.stopped; cancel() }()
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				select {
				case <-e.stopped:
					return
				case t := <-p.C:
					e.runTask(ctx, ex, t)
					e.disp.Poll(p, 1)
				}
			}
		}()
	}
}

func (e *Engine) runTask(ctx context.Context, ex Executor, t *task.Task) {
	in, err := e.ResolveInput(t.Input)
	if err != nil {
		e.Complete(t, task.Result{Err: "resolving input: " + err.Error(), Retryable: true})
		return
	}
	tt := *t
	tt.Input = in
	emit := func(chunk []byte) {
		e.live.Publish(live.Chunk{RunID: t.RunID, StepID: t.StepID, Data: chunk})
	}
	res := ex.Execute(ctx, &tt, emit)
	e.Complete(t, res)
}

// Complete reports a task's result. It is called on the executor's
// goroutine: moving a large output into the blob store happens here, off
// the shard loop.
func (e *Engine) Complete(t *task.Task, res task.Result) {
	e.disp.Done(t, res.Tokens)
	ev := core.Event{Act: t.Act, Attempt: t.Attempt}
	if res.Err != "" || res.Unknown {
		ev.Kind = core.EvStepErr
		ev.Err = res.Err
		if ev.Err == "" {
			ev.Err = "outcome unknown"
		}
		ev.Retryable, ev.Unknown = res.Retryable, res.Unknown
	} else {
		ev.Kind = core.EvStepOK
		ev.Data = res.Output
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

// externalize stores out as a blob and returns the envelope that replaces
// it in the state: the reference plus the node's declared typed fields.
func (e *Engine) externalize(t *task.Task, out json.RawMessage) (json.RawMessage, error) {
	key, err := blob.PutContent(e.blobs, out)
	if err != nil {
		return nil, err
	}
	env := struct {
		Blob   string                     `json:"$blob"`
		Size   int                        `json:"size"`
		Fields map[string]json.RawMessage `json:"fields,omitempty"`
	}{Blob: key, Size: len(out)}
	spec := e.cfg.Registry.Lookup(t.Action)
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
					m, ok := val.(map[string]any)
					if !ok {
						val = nil
						break
					}
					val = m[seg]
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
