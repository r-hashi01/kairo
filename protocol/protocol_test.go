package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/task"
)

func writeRawFrame(w *bufio.Writer, typ MsgType, body []byte) error {
	var header [5]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(body)+1))
	header[4] = byte(typ)
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	return w.Flush()
}

func waitProtocol(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not met")
		}
		time.Sleep(time.Millisecond)
	}
}

type stalledListener struct{ release chan struct{} }

func (l *stalledListener) Accept() (net.Conn, error) {
	<-l.release
	return nil, net.ErrClosed
}
func (*stalledListener) Close() error   { return nil }
func (*stalledListener) Addr() net.Addr { return stalledAddr{} }

type stalledAddr struct{}

func (stalledAddr) Network() string { return "stalled" }
func (stalledAddr) String() string  { return "stalled" }

func setup(t *testing.T) (*engine.Engine, string) {
	return setupServer(t, nil)
}

func TestWriteFrameRejectsOversizedBody(t *testing.T) {
	w := bufio.NewWriter(io.Discard)
	err := WriteFrame(w, MsgResult, map[string]string{"output": strings.Repeat("x", maxFrame)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("WriteFrame error = %v, want ErrFrameTooLarge", err)
	}
}

func TestServerClosesSessionOnMalformedKnownMessage(t *testing.T) {
	_, sock := setup(t)
	for _, typ := range []MsgType{MsgResult, MsgCredit, MsgChunk} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			c, err := net.Dial("unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			r, w := bufio.NewReader(c), bufio.NewWriter(c)
			if err := WriteFrame(w, MsgHello, Hello{Worker: "malformed", Actions: []string{"code.run"}, Credit: 1}); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			body := []byte("{")
			if typ == MsgCredit {
				body = []byte("{}")
			}
			if err := writeRawFrame(w, typ, body); err != nil {
				t.Fatal(err)
			}
			if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := ReadFrame(r); !errors.Is(err, io.EOF) {
				t.Fatalf("read after malformed %v = %v, want EOF", typ, err)
			}
		})
	}
}

func TestMalformedResultMakesOutstandingRealTaskUnknown(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"send","action":"send"}}`)
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	if err := WriteFrame(w, MsgHello, Hello{Worker: "malformed", Actions: []string{"send"}, Credit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if typ, _, err := ReadFrame(r); err != nil || typ != MsgTask {
		t.Fatalf("task frame = %v, %v", typ, err)
	}
	if err := writeRawFrame(w, MsgResult, []byte("{")); err != nil {
		t.Fatal(err)
	}
	waitProtocol(t, func() bool {
		ri, err := e.Get(context.Background(), id)
		return err == nil && ri.Status == "blocked" && len(ri.Reviews) == 1
	})
}

func TestWorkerClosesSessionOnMalformedKnownMessage(t *testing.T) {
	for _, typ := range []MsgType{MsgHello, MsgTask, MsgCancel, MsgCredit} {
		t.Run(fmt.Sprint(typ), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			serverErr := make(chan error, 1)
			go func() {
				c, err := listener.Accept()
				if err != nil {
					serverErr <- err
					return
				}
				defer c.Close()
				r, w := bufio.NewReader(c), bufio.NewWriter(c)
				if got, _, err := ReadFrame(r); err != nil || got != MsgHello {
					serverErr <- fmt.Errorf("worker hello = %v, %v", got, err)
					return
				}
				body := []byte("null")
				if typ == MsgCredit {
					body = []byte("{}")
				}
				serverErr <- writeRawFrame(w, typ, body)
			}()
			wk := &Worker{Name: "malformed", Actions: []string{"x"}, Concurrency: 1,
				Handler: func(context.Context, *task.Task, func([]byte)) task.Result { return task.Result{} }}
			err = wk.Run(context.Background(), "tcp", listener.Addr().String())
			if err == nil || !strings.Contains(err.Error(), "malformed") {
				t.Fatalf("Worker.Run error = %v, want malformed message error", err)
			}
			if err := <-serverErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkerResultEncodingFailureFailsDrainAndRealTask(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"send","action":"send"}}`)
	started := make(chan struct{})
	release := make(chan struct{})
	wk := &Worker{Name: "bad-result", Actions: []string{"send"}, Concurrency: 1,
		Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
			close(started)
			<-release
			return task.Result{Output: json.RawMessage(`{`)}
		}}
	runErr := make(chan error, 1)
	go func() { runErr <- wk.Run(context.Background(), "unix", sock) }()
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not start")
	}
	drainErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		drainErr <- wk.Drain(ctx)
	}()
	waitProtocol(t, func() bool {
		wk.mu.Lock()
		s := wk.session
		wk.mu.Unlock()
		if s == nil {
			return false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.drainAck
	})
	close(release)
	select {
	case err := <-drainErr:
		if err == nil {
			t.Fatal("Drain succeeded after the result failed to encode")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not settle after the result failed to encode")
	}
	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("Worker.Run succeeded after the result failed to encode")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Worker.Run did not stop after the result failed to encode")
	}
	if got := wk.Inflight(); got != 0 {
		t.Fatalf("inflight = %d, want 0", got)
	}
	waitProtocol(t, func() bool {
		ri, err := e.Get(context.Background(), id)
		return err == nil && ri.Status == "blocked" && len(ri.Reviews) == 1
	})
}

func setupServer(t *testing.T, configure func(*Server)) (*engine.Engine, string) {
	t.Helper()
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "code.run", Effect: ir.EffectUnprotected})
	reg.Register(ir.NodeSpec{Action: "send", Effect: ir.EffectReal})
	e, err := engine.New(engine.Config{Shards: 2, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	dir, _ := os.MkdirTemp("", "kairo")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "w.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	s := &Server{E: e}
	if configure != nil {
		configure(s)
	}
	go s.Serve(l)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Errorf("server shutdown: %v", err)
		}
		e.Close()
	})
	return e, sock
}

func TestServerShutdownClosesSessionsAndReconcilesOutstandingTasks(t *testing.T) {
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "send", Effect: ir.EffectReal})
	e, err := engine.New(engine.Config{Shards: 1, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	s := &Server{E: e}
	listeners := make([]net.Listener, 2)
	served := make(chan error, len(listeners))
	for i := range listeners {
		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func(l net.Listener) { served <- s.Serve(l) }(listeners[i])
	}
	waitProtocol(t, func() bool {
		s.lifeMu.Lock()
		defer s.lifeMu.Unlock()
		return len(s.listeners) == len(listeners)
	})

	c, err := net.Dial("tcp", listeners[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	if err := WriteFrame(w, MsgHello, Hello{Worker: "shutdown", Actions: []string{"send"}, Credit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"send","action":"send"}}`)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if typ, _, err := ReadFrame(r); err != nil || typ != MsgTask {
		t.Fatalf("task frame = %v, %v", typ, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var shutdowns sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < cap(errs); i++ {
		shutdowns.Add(1)
		go func() {
			defer shutdowns.Done()
			errs <- s.Shutdown(ctx)
		}()
	}
	shutdowns.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Shutdown = %v", err)
		}
	}
	for range listeners {
		if err := <-served; err != nil {
			t.Fatalf("Serve = %v", err)
		}
	}
	_ = c.Close()
	waitProtocol(t, func() bool {
		ri, err := e.Get(context.Background(), id)
		return err == nil && ri.Status == "blocked" && len(ri.Reviews) == 1 && e.Dispatcher().Tracked() == 0
	})
}

func TestServerShutdownHonorsContextAndCanBeWaitedAgain(t *testing.T) {
	e, err := engine.New(engine.Config{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	s := &Server{E: e}
	l := &stalledListener{release: make(chan struct{})}
	served := make(chan error, 1)
	go func() { served <- s.Serve(l) }()
	waitProtocol(t, func() bool {
		s.lifeMu.Lock()
		defer s.lifeMu.Unlock()
		return len(s.listeners) == 1
	})

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Shutdown(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v, want context.Canceled", err)
	}
	close(l.release)
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown = %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve = %v", err)
	}
}

func TestAsyncCallbackCapabilityCompletesPendingTask(t *testing.T) {
	e, sock := setupServer(t, func(s *Server) {
		s.Callback = func(*task.Task) *task.Callback {
			return &task.Callback{URL: "https://callbacks.example/v1/action-results/token", Until: time.Now().Add(time.Minute).UnixMilli()}
		}
	})
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"call","action":"send"}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan *task.Task, 1)
	wk := &Worker{Name: "async", Actions: []string{"send"}, CallbackActions: []string{"send"}, Concurrency: 1,
		Handler: func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
			copy := *tk
			seen <- &copy
			return task.Result{Pending: true}
		}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	var tk *task.Task
	select {
	case tk = <-seen:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not receive the task")
	}
	if tk.Callback == nil || !strings.Contains(tk.Callback.URL, "/v1/action-results/") {
		t.Fatalf("callback = %+v", tk.Callback)
	}
	// The SDK process can disappear before its pending result reaches the
	// daemon. The authenticated callback still settles the same real attempt,
	// whether the shard sees it before or after the disconnect outcome.
	cancel()
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	wctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	ri, err := e.Wait(wctx, id)
	if err != nil || string(ri.Output) != `{"ok":true}` {
		t.Fatalf("wait = %+v, %v", ri, err)
	}
	// A callback retry after an ambiguous response is accepted and does not
	// alter the already-settled run.
	if err := e.Callback(context.Background(), tk, task.Result{Output: json.RawMessage(`{"ok":false}`)}); err != nil {
		t.Fatal(err)
	}
}

func plan(t *testing.T, e *engine.Engine, js string) {
	d, _ := ir.ParseDefinition([]byte(js))
	if _, err := e.RegisterPlan(d); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteWorker(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"map","id":"m","over":"$input.xs","body":
	  {"kind":"step","id":"sq","action":"code.run","input":{"x":"$item"}}}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := &Worker{Name: "w1", Actions: []string{"code.run"}, Concurrency: 4, Handler: func(_ context.Context, tk *task.Task, emit func([]byte)) task.Result {
		var in struct{ X int }
		json.Unmarshal(tk.Input, &in)
		emit([]byte("working"))
		out, _ := json.Marshal(in.X * in.X)
		return task.Result{Output: out}
	}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t", Input: json.RawMessage(`{"xs":[1,2,3,4,5,6,7,8,9,10]}`)})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wc := context.WithTimeout(context.Background(), 5*time.Second)
	defer wc()
	ri, err := e.Wait(wctx, id)
	if err != nil || string(ri.Output) != "[1,4,9,16,25,36,49,64,81,100]" {
		t.Fatalf("%v %+v", err, ri)
	}
}

func TestServerRejectsHugeInitialCredit(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"code.run"}}`)
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	if err := WriteFrame(w, MsgHello, Hello{Worker: "huge", Actions: []string{"code.run"}, Credit: MaxCredit + 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadFrame(r); !errors.Is(err, io.EOF) {
		t.Fatalf("huge-credit connection read = %v, want EOF", err)
	}
	c.Close()

	// Rejecting one session leaves the listener and dispatcher usable.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := &Worker{Name: "normal", Actions: []string{"code.run"}, Concurrency: 1,
		Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
			return task.Result{Output: json.RawMessage(`{"ok":true}`)}
		}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	wctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if ri, err := e.Wait(wctx, id); err != nil || ri.Status != "completed" {
		t.Fatalf("normal worker after rejection = %+v, %v", ri, err)
	}
}

func TestExcessCreditClosesBlockedWriterWithoutBlockingDispatcher(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"code.run"}}`)
	serverConn, workerConn := net.Pipe()
	handled := make(chan struct{})
	go func() {
		(&Server{E: e}).handle(serverConn)
		close(handled)
	}()
	t.Cleanup(func() { workerConn.Close() })
	w := bufio.NewWriter(workerConn)
	if err := WriteFrame(w, MsgHello, Hello{Worker: "stuck", Actions: []string{"code.run"}, Credit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}

	// Reading one byte proves the server writer entered Flush. net.Pipe has
	// no socket buffer, so it stays blocked while the rest is unread.
	if err := workerConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	if _, err := io.ReadFull(workerConn, one[:]); err != nil {
		t.Fatalf("server did not start writing the task: %v", err)
	}
	if err := WriteFrame(w, MsgCredit, Credit{N: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("invalid credit did not close the blocked session")
	}

	// The unprotected task is retried by a healthy worker. This also proves
	// the rejected session did not wedge the dispatcher goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := &Worker{Name: "healthy", Actions: []string{"code.run"}, Concurrency: 1,
		Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
			return task.Result{Output: json.RawMessage(`{"ok":true}`)}
		}}
	go wk.Run(ctx, "unix", sock)
	wctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if ri, err := e.Wait(wctx, id); err != nil || ri.Status != "completed" {
		t.Fatalf("retry after rejected session = %+v, %v", ri, err)
	}
}

func TestWorkerDrainStopsDeliveryAndWaitsForAcceptedResult(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"a","action":"code.run"},{"kind":"step","id":"b","action":"code.run"}]}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Int32
	wk := &Worker{Name: "draining", Actions: []string{"code.run"}, Concurrency: 1, Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
		first.Add(1)
		close(started)
		<-release
		return task.Result{Output: json.RawMessage(`1`)}
	}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first task did not start")
	}
	drained := make(chan error, 1)
	go func() {
		dctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		drained <- wk.Drain(dctx)
	}()
	select {
	case err := <-drained:
		t.Fatalf("drain returned before the result: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	var second atomic.Int32
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	other := &Worker{Name: "other", Actions: []string{"code.run"}, Concurrency: 1, Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
		second.Add(1)
		return task.Result{Output: json.RawMessage(`2`)}
	}}
	go other.Run(ctx2, "unix", sock)
	close(release)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	wctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if _, err := e.Wait(wctx, id); err != nil {
		t.Fatal(err)
	}
	if first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("tasks after drain: draining worker=%d other=%d", first.Load(), second.Load())
	}
}

func TestWorkerDrainNeedsNegotiatedCapability(t *testing.T) {
	var wire bytes.Buffer
	s := newWorkerSession(bufio.NewWriter(&wire))
	s.negotiated = true
	wk := &Worker{session: s}
	err := wk.Drain(context.Background())
	if !errors.Is(err, ErrDrainUnsupported) {
		t.Fatalf("Drain error = %v, want ErrDrainUnsupported", err)
	}
	if err := s.w.Flush(); err != nil || wire.Len() != 0 {
		t.Fatalf("unsupported Drain wrote %d bytes: %v", wire.Len(), err)
	}
}

func TestWorkerDrainWaitsForCapabilityNegotiation(t *testing.T) {
	var wire bytes.Buffer
	s := newWorkerSession(bufio.NewWriter(&wire))
	go func() {
		time.Sleep(time.Millisecond)
		s.mu.Lock()
		s.negotiated = true
		s.drainCap = true
		s.notifyLocked()
		s.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.beginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	typ, _, err := ReadFrame(bufio.NewReader(&wire))
	if err != nil || typ != MsgCredit {
		t.Fatalf("drain frame = %v, %v, want Credit", typ, err)
	}
}

func TestServerNegotiatesDrainBeforeWork(t *testing.T) {
	_, sock := setup(t)
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	if err := WriteFrame(w, MsgHello, Hello{Worker: "new", Actions: []string{"code.run"}, Credit: 1,
		Capabilities: []string{CapabilityGracefulDrain, "unknown-v1"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	typ, body, err := ReadFrame(r)
	if err != nil || typ != MsgHello {
		t.Fatalf("first frame = %v, %v, want Hello", typ, err)
	}
	var welcome Welcome
	if err := json.Unmarshal(body, &welcome); err != nil {
		t.Fatal(err)
	}
	if len(welcome.Capabilities) != 1 || welcome.Capabilities[0] != CapabilityGracefulDrain {
		t.Fatalf("selected capabilities = %v", welcome.Capabilities)
	}
}

func TestServerAcceptsLegacyDrainWithoutCapabilityHello(t *testing.T) {
	_, sock := setup(t)
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	if err := WriteFrame(w, MsgHello, Hello{Worker: "legacy-drain", Actions: []string{"code.run"}, Credit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(w, MsgCredit, Credit{N: -1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	typ, body, err := ReadFrame(r)
	if err != nil || typ != MsgCredit {
		t.Fatalf("drain ack = %v, %v, want Credit", typ, err)
	}
	var credit Credit
	if err := json.Unmarshal(body, &credit); err != nil || credit.N != 0 {
		t.Fatalf("drain ack = %+v, %v", credit, err)
	}
}

// A worker dying mid-task: the unprotected task is retried elsewhere; the
// real one has an unknown outcome and stops for review.
func TestWorkerDisconnect(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"par","nodes":[
	  {"kind":"step","id":"calc","action":"code.run"},{"kind":"step","id":"send","action":"send"}]}}`)
	ctx1, kill := context.WithCancel(context.Background())
	var got atomic.Int32
	crashed := make(chan struct{})
	t.Cleanup(func() { close(crashed) })
	// The handler never returns: the process "crashes" with both tasks in hand.
	hang := &Worker{Name: "dying", Actions: []string{"code.run", "send"}, Concurrency: 2, Handler: func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		got.Add(1)
		<-crashed
		return task.Result{Err: "crashed", Unknown: true}
	}}
	go hang.Run(ctx1, "unix", sock)
	id, _ := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	for got.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	kill()
	ctx2, stop := context.WithCancel(context.Background())
	defer stop()
	good := &Worker{Name: "good", Actions: []string{"code.run"}, Concurrency: 1, Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
		return task.Result{Output: json.RawMessage(`42`)}
	}}
	go good.Run(ctx2, "unix", sock)
	deadline := time.Now().Add(5 * time.Second)
	for {
		ri, _ := e.Get(context.Background(), id)
		if ri.Status == "blocked" && len(ri.Reviews) == 1 && strings.HasPrefix(ri.Reviews[0].StepID, "send") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not block for review: %+v", ri)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// submit is Submit for tests that only need the run id.
func submit(e *engine.Engine, req engine.SubmitRequest) (string, error) {
	r, err := e.Submit(context.Background(), req)
	return r.RunID, err
}

// Cancelling a run reaches a task running in a remote worker (ADR 0026).
func TestWorkerReceivesCancel(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"code.run"}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	stopped := make(chan struct{})
	wk := &Worker{Name: "w1", Actions: []string{"code.run"}, Handler: func(ctx context.Context, tk *task.Task, _ func([]byte)) task.Result {
		close(started)
		<-ctx.Done()
		close(stopped)
		return task.Result{Err: "cancelled"}
	}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	e.Cancel(id, "user")
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker's task was not cancelled")
	}
	waitTracked(t, e)
}

// A worker that predates Cancel skips the frame; its late result is still
// accepted and releases the task.
func TestOldWorkerIgnoresCancel(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"code.run"}}`)
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	WriteFrame(w, MsgHello, Hello{Worker: "old", Actions: []string{"code.run"}, Credit: 1})
	w.Flush()
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	typ, body, err := ReadFrame(r)
	if err != nil || typ != MsgTask {
		t.Fatalf("%v %v", typ, err)
	}
	var tk task.Task
	json.Unmarshal(body, &tk)
	e.Cancel(id, "user")
	typ, body, err = ReadFrame(r)
	if err != nil || typ != MsgCancel {
		t.Fatalf("got %v %v, want a Cancel frame", typ, err)
	}
	var cn Cancel
	if json.Unmarshal(body, &cn); cn.Seq != tk.Seq {
		t.Fatalf("cancel for seq %d, task has %d", cn.Seq, tk.Seq)
	}
	if e.Dispatcher().Tracked() != 1 {
		t.Fatal("the task was released before its result")
	}
	WriteFrame(w, MsgResult, Result{Seq: tk.Seq, Output: json.RawMessage(`{}`)})
	w.Flush()
	waitTracked(t, e)
	wctx, wc := context.WithTimeout(context.Background(), 5*time.Second)
	defer wc()
	if ri, err := e.Wait(wctx, id); err != nil || ri.Status != "cancelled" {
		t.Fatalf("%v %+v", err, ri)
	}
}

func waitTracked(t *testing.T, e *engine.Engine) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for e.Dispatcher().Tracked() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("dispatcher still tracks %d tasks", e.Dispatcher().Tracked())
		}
		time.Sleep(time.Millisecond)
	}
}

// A server with a token disconnects workers without it, and serves the
// ones that have it (ADR 0037).
func TestWorkerToken(t *testing.T) {
	e, _ := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"code.run"}}`)
	sock := filepath.Join(t.TempDir(), "t.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := &Server{E: e, Token: "secret"}
	go srv.Serve(l)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("server shutdown: %v", err)
		}
	}()

	for _, tok := range []string{"", "wrong"} {
		c, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		r, w := bufio.NewReader(c), bufio.NewWriter(c)
		WriteFrame(w, MsgHello, Hello{Worker: "x", Actions: []string{"code.run"}, Credit: 1, Token: tok})
		w.Flush()
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, err := ReadFrame(r); !errors.Is(err, io.EOF) {
			t.Fatalf("token %q: got %v, want the connection closed", tok, err)
		}
		c.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wk := &Worker{Name: "ok", Actions: []string{"code.run"}, Token: "secret",
		Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
			return task.Result{Output: json.RawMessage(`{"ok":true}`)}
		}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wc := context.WithTimeout(context.Background(), 5*time.Second)
	defer wc()
	if ri, err := e.Wait(wctx, id); err != nil || ri.Status != "completed" {
		t.Fatalf("%v %+v", err, ri)
	}
}

// A worker that asked for RunEnd gets one per run it was sent a task of,
// after those tasks; one that did not ask gets none; a closed connection
// is forgotten (ADR 0044).
func TestRunEnd(t *testing.T) {
	reg := ir.NewRegistry()
	reg.Register(ir.NodeSpec{Action: "code.run", Effect: ir.EffectUnprotected})
	reg.Register(ir.NodeSpec{Action: "other", Effect: ir.EffectUnprotected})
	reg.Register(ir.NodeSpec{Action: "solo", Effect: ir.EffectUnprotected})
	e, err := engine.New(engine.Config{Shards: 2, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	srv := &Server{E: e}
	go srv.Serve(l)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("server shutdown: %v", err)
		}
		e.Close()
	})
	plan(t, e, `{"name":"p","root":{"kind":"seq","nodes":[
	  {"kind":"step","id":"a","action":"code.run"},
	  {"kind":"step","id":"b","action":"code.run"},
	  {"kind":"step","id":"c","action":"other"}]}}`)

	type wconn struct {
		c net.Conn
		r *bufio.Reader
		w *bufio.Writer
	}
	dial := func(actions []string, runEnd bool) *wconn {
		c, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		x := &wconn{c, bufio.NewReader(c), bufio.NewWriter(c)}
		WriteFrame(x.w, MsgHello, Hello{Worker: "w", Actions: actions, Credit: 4, RunEnd: runEnd})
		x.w.Flush()
		return x
	}
	asks := dial([]string{"code.run"}, true)
	old := dial([]string{"other"}, false)
	// Serves one task: answers it and returns its run.
	serve := func(x *wconn) string {
		t.Helper()
		x.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		typ, body, err := ReadFrame(x.r)
		if err != nil || typ != MsgTask {
			t.Fatalf("want a task, got %v %v", typ, err)
		}
		var tk task.Task
		json.Unmarshal(body, &tk)
		WriteFrame(x.w, MsgResult, Result{Seq: tk.Seq, Output: json.RawMessage(`{}`)})
		x.w.Flush()
		return tk.RunID
	}
	var ids []string
	for round := 0; round < 2; round++ {
		id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		if serve(asks) != id || serve(asks) != id || serve(old) != id {
			t.Fatal("tasks of another run")
		}
		wctx, wc := context.WithTimeout(context.Background(), 5*time.Second)
		ri, err := e.Wait(wctx, id)
		wc()
		if err != nil || ri.Status != "completed" {
			t.Fatalf("%v %+v", err, ri)
		}
		asks.c.SetReadDeadline(time.Now().Add(5 * time.Second))
		typ, body, err := ReadFrame(asks.r)
		var end RunEnd
		json.Unmarshal(body, &end)
		if err != nil || typ != MsgRunEnd || end.RunID != id {
			t.Fatalf("want RunEnd for %s, got %v %s %v", id, typ, body, err)
		}
	}
	// Nothing more: one RunEnd per run, none for the worker that did not
	// ask.
	for _, x := range []*wconn{asks, old} {
		x.c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		if typ, body, err := ReadFrame(x.r); err == nil {
			t.Fatalf("unexpected frame %v %s", typ, body)
		}
	}
	// A task of a run that has finished is not sent (it was abandoned; its
	// RunEnd may have gone by).
	ec := &endConn{runs: map[string]struct{}{}, wake: make(chan struct{}, 1)}
	if srv.sending(ec, ids[0]) || len(ec.runs) != 0 {
		t.Fatal("a finished run's task would be sent")
	}
	// A run ends while the connection that took its task is gone: the
	// connection is forgotten.
	plan(t, e, `{"name":"q","root":{"kind":"step","id":"s","action":"solo"}}`)
	gone := dial([]string{"solo"}, true)
	id, _ := submit(e, engine.SubmitRequest{Plan: "q", Tenant: "t"})
	if serve(gone) != id {
		t.Fatal("task of another run")
	}
	gone.c.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		srv.rmu.Lock()
		n := len(srv.runs)
		srv.rmu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d runs still tracked after the connections closed", n)
		}
		time.Sleep(time.Millisecond)
	}
}

// A worker's result can be a wait: the step ends at the deadline with the
// output the worker gave (ADR 0045).
func TestWorkerResultWaits(t *testing.T) {
	e, sock := setup(t)
	plan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"code.run"}}`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	until := time.Now().Add(200 * time.Millisecond)
	wk := &Worker{Name: "w", Actions: []string{"code.run"}, Concurrency: 1, Handler: func(context.Context, *task.Task, func([]byte)) task.Result {
		return task.Result{Wait: &task.Wait{Until: until.UnixMilli(), Output: json.RawMessage(`{"later":true}`)}}
	}}
	go wk.Run(ctx, "unix", sock)
	id, err := submit(e, engine.SubmitRequest{Plan: "p", Tenant: "t"})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wc := context.WithTimeout(context.Background(), 5*time.Second)
	defer wc()
	ri, err := e.Wait(wctx, id)
	if err != nil || ri.Status != "completed" || string(ri.Output) != `{"later":true}` {
		t.Fatalf("%v %+v", err, ri)
	}
	if time.Now().Before(until.Truncate(time.Millisecond)) {
		t.Fatal("ended before the deadline")
	}
}
