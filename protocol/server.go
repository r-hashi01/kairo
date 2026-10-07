package protocol

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"kairo/engine"
	"kairo/sched"
	"kairo/task"
)

// Server accepts worker connections for an engine.
type Server struct {
	E *engine.Engine
	// Token, if set, must be in every worker's Hello (ADR 0037).
	Token string

	once sync.Once
	// asking counts the connections that asked for RunEnd: with none, a
	// run's end costs nothing here.
	asking atomic.Int64
	rmu    sync.Mutex
	// By run: the connections that asked for RunEnd and were sent a task
	// of it (ADR 0044).
	runs map[string]map[*endConn]struct{}
}

// endConn is a connection that asked for RunEnd: its writer sends the
// ends queued here.
type endConn struct {
	runs map[string]struct{} // under Server.rmu
	mu   sync.Mutex
	ends []string
	wake chan struct{}
}

// sending records that c is about to be sent a task of run. It is false
// if the run has already finished: the task is not sent (it was abandoned,
// and its RunEnd might have gone by already). The check is under rmu, so a
// run that finishes after it finds c in the table.
func (s *Server) sending(c *endConn, run string) bool {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if _, ok := c.runs[run]; ok {
		return true
	}
	if s.E.Finished(run) {
		return false
	}
	if s.runs == nil {
		s.runs = map[string]map[*endConn]struct{}{}
	}
	cs := s.runs[run]
	if cs == nil {
		cs = map[*endConn]struct{}{}
		s.runs[run] = cs
	}
	cs[c] = struct{}{}
	c.runs[run] = struct{}{}
	return true
}

// runEnded queues a RunEnd on the connections that were sent a task of
// run. It is called from the shard loop: it only wakes their writers.
func (s *Server) runEnded(run string) {
	if s.asking.Load() == 0 {
		return
	}
	s.rmu.Lock()
	cs := s.runs[run]
	delete(s.runs, run)
	for c := range cs {
		delete(c.runs, run)
	}
	s.rmu.Unlock()
	for c := range cs {
		c.mu.Lock()
		c.ends = append(c.ends, run)
		c.mu.Unlock()
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
}

// forget removes a closed connection.
func (s *Server) forget(c *endConn) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	for run := range c.runs {
		if cs := s.runs[run]; cs != nil {
			delete(cs, c)
			if len(cs) == 0 {
				delete(s.runs, run)
			}
		}
	}
	c.runs = nil
}

func (s *Server) Serve(l net.Listener) error {
	s.once.Do(func() { s.E.OnRunEnd(s.runEnded) })
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
	if s.Token != "" && subtle.ConstantTimeCompare([]byte(h.Token), []byte(s.Token)) != 1 {
		log.Printf("kairo: worker %q rejected: bad token", h.Worker)
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
	var ec *endConn
	var wake chan struct{} // nil (never ready) without RunEnd
	if h.RunEnd {
		ec = &endConn{runs: map[string]struct{}{}, wake: make(chan struct{}, 1)}
		wake = ec.wake
		s.asking.Add(1)
		defer func() {
			s.forget(ec)
			s.asking.Add(-1)
		}()
	}

	go func() {
		defer close(writerDone)
		for {
			var dl sched.Delivery
			select {
			case dl = <-p.C:
			case <-wake:
				ec.mu.Lock()
				ends := ec.ends
				ec.ends = nil
				ec.mu.Unlock()
				wmu.Lock()
				var err error
				for _, run := range ends {
					if err = WriteFrame(w, MsgRunEnd, RunEnd{RunID: run}); err != nil {
						break
					}
				}
				if err == nil {
					err = w.Flush()
				}
				wmu.Unlock()
				if err != nil {
					c.Close()
					return
				}
				continue
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
			if ec != nil && !s.sending(ec, t.RunID) {
				// Its run has finished: the task was abandoned.
				s.E.Complete(t, task.Result{Err: "aborted", Retryable: true})
				d.Poll(p, 1)
				continue
			}
			// (Recorded before the task is written: its run's end comes
			// after it, from this goroutine.)
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
			s.E.Complete(t, task.Result{Output: res.Output, Err: res.Err, Retryable: res.Retryable, Unknown: res.Unknown, Tokens: res.Tokens, ErrType: res.ErrType, Meta: res.Meta, RateLimited: res.RateLimited, Wait: res.Wait})
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
