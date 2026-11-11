package protocol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/r-hashi01/kairo/task"
)

// Handler executes one task in a worker process.
type Handler func(ctx context.Context, t *task.Task, emit func(chunk []byte)) task.Result

// ErrDrainUnsupported means this worker connection did not negotiate the
// graceful-drain capability with kairod (ADR 0075).
var ErrDrainUnsupported = errors.New("protocol: kairod does not support graceful worker drain")

const capabilityNegotiationTimeout = time.Second

// Worker is the Go client of the protocol (the reference for SDKs in other
// languages).
type Worker struct {
	Name    string
	Actions []string
	// CallbackActions are URL-backed actions that can use daemon-owned
	// callbacks when the server selects the capability (ADR 0070).
	CallbackActions []string
	Concurrency     int
	Handler         Handler
	Token           string // the server's Token, if it has one (ADR 0037)

	mu      sync.Mutex
	session *workerSession
}

type workerSession struct {
	sendMu sync.Mutex
	w      *bufio.Writer

	mu          sync.Mutex
	draining    bool
	negotiated  bool
	drainCap    bool
	drainAck    bool
	inflight    int
	pendingAcks int
	changed     chan struct{}
	done        chan struct{}
	err         error
	doneOnce    sync.Once
}

func newWorkerSession(w *bufio.Writer) *workerSession {
	return &workerSession{w: w, changed: make(chan struct{}), done: make(chan struct{})}
}

func (s *workerSession) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *workerSession) finish(err error) {
	s.doneOnce.Do(func() {
		s.mu.Lock()
		s.err = err
		s.notifyLocked()
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *workerSession) send(t MsgType, v any) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if err := WriteFrame(s.w, t, v); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *workerSession) sendResult(v Result) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	needsAck := s.draining
	if needsAck {
		s.pendingAcks++
	}
	s.mu.Unlock()
	err := WriteFrame(s.w, MsgResult, v)
	if err == nil {
		err = s.w.Flush()
	}
	s.mu.Lock()
	s.inflight--
	if err != nil && needsAck {
		s.pendingAcks--
	}
	s.notifyLocked()
	s.mu.Unlock()
	return err
}

func (s *workerSession) waitNegotiated(ctx context.Context) error {
	timer := time.NewTimer(capabilityNegotiationTimeout)
	defer timer.Stop()
	for {
		s.mu.Lock()
		if s.negotiated {
			capable := s.drainCap
			s.mu.Unlock()
			if !capable {
				return ErrDrainUnsupported
			}
			return nil
		}
		changed, err := s.changed, s.err
		s.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ErrDrainUnsupported
		case <-s.done:
			return ErrDrainUnsupported
		case <-changed:
		}
	}
}

func (s *workerSession) beginDrain(ctx context.Context) error {
	if err := s.waitNegotiated(ctx); err != nil {
		return err
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return nil
	}
	s.draining = true
	s.notifyLocked()
	s.mu.Unlock()
	if err := WriteFrame(s.w, MsgCredit, Credit{N: -1}); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *workerSession) waitDrained(ctx context.Context) error {
	for {
		s.mu.Lock()
		if s.drainAck && s.inflight == 0 && s.pendingAcks == 0 {
			s.mu.Unlock()
			return nil
		}
		changed, err := s.changed, s.err
		s.mu.Unlock()
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			s.mu.Lock()
			err := s.err
			s.mu.Unlock()
			if err == nil {
				err = errors.New("protocol: worker connection closed while draining")
			}
			return err
		case <-changed:
		}
	}
}

// Drain stops this connection from receiving new tasks and waits until all
// tasks already sent to it have returned results accepted by the runtime.
// The connection remains open for Close to stop it (ADR 0073). It returns
// ErrDrainUnsupported when this connection did not negotiate that capability
// with kairod (ADR 0075).
func (wk *Worker) Drain(ctx context.Context) error {
	wk.mu.Lock()
	s := wk.session
	wk.mu.Unlock()
	if s == nil {
		return errors.New("protocol: worker is not connected")
	}
	if err := s.beginDrain(ctx); err != nil {
		return fmt.Errorf("protocol: starting worker drain: %w", err)
	}
	return s.waitDrained(ctx)
}

// Inflight is the number of tasks received whose Result has not been sent.
func (wk *Worker) Inflight() int {
	wk.mu.Lock()
	s := wk.session
	wk.mu.Unlock()
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight
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
	s := newWorkerSession(w)
	wk.mu.Lock()
	if wk.session != nil {
		wk.mu.Unlock()
		return errors.New("protocol: worker is already connected")
	}
	wk.session = s
	wk.mu.Unlock()
	defer func() {
		wk.mu.Lock()
		if wk.session == s {
			wk.session = nil
		}
		wk.mu.Unlock()
	}()
	capabilities := []string{CapabilityGracefulDrain}
	if len(wk.CallbackActions) > 0 {
		capabilities = append(capabilities, CapabilityAsyncActionCallback)
	}
	if err := s.send(MsgHello, Hello{Worker: wk.Name, Actions: wk.Actions, Credit: conc, Token: wk.Token,
		Capabilities: capabilities, CallbackActions: wk.CallbackActions}); err != nil {
		s.finish(err)
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { <-ctx.Done(); c.Close() }()

	type job struct {
		t   *task.Task
		ctx context.Context
	}
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
				emit := func(b []byte) { _ = s.send(MsgChunk, Chunk{Seq: seq, Data: b}) }
				res := wk.Handler(j.ctx, j.t, emit)
				cmu.Lock()
				if cancel := cancels[seq]; cancel != nil {
					cancel()
					delete(cancels, seq)
				}
				cmu.Unlock()
				if err := s.sendResult(Result{Seq: seq, Output: res.Output, Err: res.Err, Retryable: res.Retryable, Unknown: res.Unknown, Tokens: res.Tokens, ErrType: res.ErrType, Meta: res.Meta, RateLimited: res.RateLimited, Wait: res.Wait, Pending: res.Pending}); err != nil {
					s.finish(err)
				}
			}
		}()
	}
	var rerr error
	for {
		typ, body, err := ReadFrame(r)
		if err != nil {
			rerr = err
			s.finish(err)
			break
		}
		if typ != MsgHello {
			s.mu.Lock()
			if !s.negotiated {
				s.negotiated = true
				s.notifyLocked()
			}
			s.mu.Unlock()
		}
		switch typ {
		case MsgHello:
			var welcome Welcome
			if json.Unmarshal(body, &welcome) != nil {
				continue
			}
			s.mu.Lock()
			s.negotiated = true
			s.drainCap = hasCapability(welcome.Capabilities, CapabilityGracefulDrain)
			s.notifyLocked()
			s.mu.Unlock()
		case MsgTask:
			var t task.Task
			if err := json.Unmarshal(body, &t); err != nil {
				continue
			}
			tctx, cancel := context.WithCancel(ctx)
			cmu.Lock()
			cancels[t.Seq] = cancel
			cmu.Unlock()
			s.mu.Lock()
			s.inflight++
			s.notifyLocked()
			s.mu.Unlock()
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
		case MsgCredit:
			var cr Credit
			if json.Unmarshal(body, &cr) != nil {
				continue
			}
			s.mu.Lock()
			if s.draining && cr.N == 0 {
				s.drainAck = true
			} else if s.draining && cr.N > 0 {
				s.pendingAcks = max(s.pendingAcks-cr.N, 0)
			}
			s.notifyLocked()
			s.mu.Unlock()
		}
	}
	close(tasks)
	wg.Wait()
	s.finish(rerr)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return rerr
}
