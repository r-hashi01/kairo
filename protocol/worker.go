package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"sync"

	"kairo/task"
)

// Handler executes one task in a worker process.
type Handler func(ctx context.Context, t *task.Task, emit func(chunk []byte)) task.Result

// Worker is the Go client of the protocol (the reference for SDKs in other
// languages).
type Worker struct {
	Name        string
	Actions     []string
	Concurrency int
	Handler     Handler
	Token       string // the server's Token, if it has one (ADR 0037)
}

// Run connects to the runtime and serves tasks until ctx is cancelled or
// the connection breaks.
func (wk *Worker) Run(ctx context.Context, network, addr string) error {
	var d net.Dialer
	c, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return err
	}
	defer c.Close()
	conc := max(wk.Concurrency, 1)
	r := bufio.NewReaderSize(c, 64<<10)
	w := bufio.NewWriterSize(c, 64<<10)
	var wmu sync.Mutex
	send := func(t MsgType, v any) error {
		wmu.Lock()
		defer wmu.Unlock()
		if err := WriteFrame(w, t, v); err != nil {
			return err
		}
		return w.Flush()
	}
	if err := send(MsgHello, Hello{Worker: wk.Name, Actions: wk.Actions, Credit: conc, Token: wk.Token}); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-ctx.Done(); c.Close() }()

	type job struct {
		t   *task.Task
		ctx context.Context
	}
	// Cancel functions of received tasks, by seq (ADR 0026).
	var cmu sync.Mutex
	cancels := map[uint64]context.CancelFunc{}
	tasks := make(chan job, conc)
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range tasks {
				seq := j.t.Seq
				emit := func(b []byte) { send(MsgChunk, Chunk{Seq: seq, Data: b}) }
				res := wk.Handler(j.ctx, j.t, emit)
				cmu.Lock()
				if cancel := cancels[seq]; cancel != nil {
					cancel()
					delete(cancels, seq)
				}
				cmu.Unlock()
				send(MsgResult, Result{Seq: seq, Output: res.Output, Err: res.Err, Retryable: res.Retryable, Unknown: res.Unknown, Tokens: res.Tokens, ErrType: res.ErrType, Meta: res.Meta, RateLimited: res.RateLimited})
			}
		}()
	}
	var rerr error
	for {
		typ, body, err := ReadFrame(r)
		if err != nil {
			rerr = err
			break
		}
		switch typ {
		case MsgTask:
			var t task.Task
			if err := json.Unmarshal(body, &t); err != nil {
				continue
			}
			tctx, cancel := context.WithCancel(ctx)
			cmu.Lock()
			cancels[t.Seq] = cancel
			cmu.Unlock()
			tasks <- job{t: &t, ctx: tctx}
		case MsgCancel:
			var cn Cancel
			if json.Unmarshal(body, &cn) != nil {
				continue
			}
			cmu.Lock()
			if cancel := cancels[cn.Seq]; cancel != nil {
				cancel()
			}
			cmu.Unlock()
		}
	}
	close(tasks)
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return rerr
}
