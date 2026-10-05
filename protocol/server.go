package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"kairo/engine"
	"kairo/sched"
	"kairo/task"
)

// Server accepts worker connections for an engine.
type Server struct {
	E *engine.Engine
}

func (s *Server) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go s.handle(c)
	}
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReaderSize(c, 64<<10)
	w := bufio.NewWriterSize(c, 64<<10)
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	typ, body, err := ReadFrame(r)
	if err != nil || typ != MsgHello {
		return
	}
	var h Hello
	if json.Unmarshal(body, &h) != nil || len(h.Actions) == 0 {
		return
	}
	c.SetReadDeadline(time.Time{})
	if h.Credit <= 0 {
		h.Credit = 1
	}
	d := s.E.Dispatcher()
	p := d.NewPoller(h.Actions, h.Credit)

	type sent struct {
		t    *task.Task
		stop func() bool // unregisters the Cancel sender
	}
	var mu sync.Mutex
	outstanding := map[uint64]sent{}
	// take removes a task from outstanding; it no longer gets a Cancel.
	take := func(seq uint64) *task.Task {
		mu.Lock()
		o, ok := outstanding[seq]
		delete(outstanding, seq)
		mu.Unlock()
		if !ok {
			return nil
		}
		o.stop()
		return o.t
	}
	var wmu sync.Mutex // the writer goroutine and Cancel senders share w
	done := make(chan struct{})
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		for {
			var dl sched.Delivery
			select {
			case dl = <-p.C:
			case <-done:
				return
			}
			t := dl.Task
			if dl.Ctx.Err() != nil {
				// Aborted before it was sent: its result would be ignored.
				s.E.Complete(t, task.Result{Err: "aborted", Retryable: true})
				d.Poll(p, 1)
				continue
			}
			in, err := s.E.ResolveInput(t.Input)
			if err != nil {
				s.E.Complete(t, task.Result{Err: "resolving input: " + err.Error(), Retryable: true})
				d.Poll(p, 1)
				continue
			}
			seq := t.Seq
			tt := *t
			tt.Input = in
			// Registered before the frame is written (a large frame reaches
			// the worker before WriteFrame returns, and so may its Result),
			// under wmu, which the Cancel sender also needs: a Cancel never
			// overtakes its task.
			wmu.Lock()
			mu.Lock()
			outstanding[seq] = sent{t: t, stop: context.AfterFunc(dl.Ctx, func() {
				wmu.Lock()
				defer wmu.Unlock()
				if WriteFrame(w, MsgCancel, Cancel{Seq: seq}) != nil || w.Flush() != nil {
					c.Close()
				}
			})}
			mu.Unlock()
			err = WriteFrame(w, MsgTask, &tt)
			if err == nil && len(p.C) == 0 {
				err = w.Flush()
			}
			wmu.Unlock()
			if err != nil {
				c.Close()
				return
			}
		}
	}()

	for {
		typ, body, err := ReadFrame(r)
		if err != nil {
			break
		}
		switch typ {
		case MsgResult:
			var res Result
			if json.Unmarshal(body, &res) != nil {
				continue
			}
			t := take(res.Seq)
			if t == nil {
				continue
			}
			s.E.Complete(t, task.Result{Output: res.Output, Err: res.Err, Retryable: res.Retryable, Unknown: res.Unknown, Tokens: res.Tokens, ErrType: res.ErrType, Meta: res.Meta})
			d.Poll(p, 1)
		case MsgCredit:
			var cr Credit
			if json.Unmarshal(body, &cr) == nil && cr.N > 0 {
				d.Poll(p, cr.N)
			}
		case MsgChunk:
			var ch Chunk
			if json.Unmarshal(body, &ch) != nil {
				continue
			}
			mu.Lock()
			o, ok := outstanding[ch.Seq]
			mu.Unlock()
			if t := o.t; ok {
				s.E.PublishChunk(t.RunID, t.StepID, ch.Data)
			}
		}
	}
	close(done)
	c.Close() // unblocks a writer stuck on a dead connection
	<-writerDone
	d.Unpoll(p) // requeues tasks that were never sent
	mu.Lock()
	lost := outstanding
	outstanding = nil
	mu.Unlock()
	if len(lost) > 0 {
		log.Printf("kairo: worker %q disconnected with %d tasks outstanding", h.Worker, len(lost))
	}
	for _, o := range lost {
		o.stop()
		// The task may or may not have run.
		s.E.Complete(o.t, task.Result{Err: "worker disconnected", Unknown: true})
	}
}
