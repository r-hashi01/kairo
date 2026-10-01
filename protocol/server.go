package protocol

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"kairo/engine"
	"kairo/live"
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

	var mu sync.Mutex
	outstanding := map[uint64]*task.Task{}
	done := make(chan struct{})
	writerDone := make(chan struct{})

	go func() {
		defer close(writerDone)
		for {
			var t *task.Task
			select {
			case t = <-p.C:
			case <-done:
				return
			}
			mu.Lock()
			outstanding[t.Seq] = t
			mu.Unlock()
			in, err := s.E.ResolveInput(t.Input)
			if err != nil {
				mu.Lock()
				delete(outstanding, t.Seq)
				mu.Unlock()
				s.E.Complete(t, task.Result{Err: "resolving input: " + err.Error(), Retryable: true})
				d.Poll(p, 1)
				continue
			}
			tt := *t
			tt.Input = in
			if err := WriteFrame(w, MsgTask, &tt); err != nil {
				c.Close()
				return
			}
			if len(p.C) == 0 {
				if err := w.Flush(); err != nil {
					c.Close()
					return
				}
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
			mu.Lock()
			t := outstanding[res.Seq]
			delete(outstanding, res.Seq)
			mu.Unlock()
			if t == nil {
				continue
			}
			s.E.Complete(t, task.Result{Output: res.Output, Err: res.Err, Retryable: res.Retryable, Unknown: res.Unknown, Tokens: res.Tokens})
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
			t := outstanding[ch.Seq]
			mu.Unlock()
			if t != nil {
				s.E.Live().Publish(live.Chunk{RunID: t.RunID, StepID: t.StepID, Data: ch.Data})
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
	for _, t := range lost {
		// The task may or may not have run.
		s.E.Complete(t, task.Result{Err: "worker disconnected", Unknown: true})
	}
}
