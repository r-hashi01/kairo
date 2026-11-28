package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/wal"
)

// --- ADR 0034: a trace with a log position is published once it is durable ---

// gatedSink holds the first Append after arm until release says how it
// ends; after a failure every later Append fails too (like wal.FileSink).
type gatedSink struct {
	*wal.MemSink
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan error
	broken  error
}

func newGatedSink() *gatedSink {
	return &gatedSink{MemSink: &wal.MemSink{}, entered: make(chan struct{}), release: make(chan error, 1)}
}

func (g *gatedSink) arm() {
	g.mu.Lock()
	g.armed = true
	g.mu.Unlock()
}

func (g *gatedSink) Append(b []byte) error {
	g.mu.Lock()
	if g.broken != nil {
		err := g.broken
		g.mu.Unlock()
		return err
	}
	armed := g.armed
	g.armed = false
	g.mu.Unlock()
	if armed {
		close(g.entered)
		if err := <-g.release; err != nil {
			g.mu.Lock()
			g.broken = err
			g.mu.Unlock()
			return err
		}
	}
	return g.MemSink.Append(b)
}

// feedDurableSetup runs a one-step run of the file tier whose step waits
// for the test. It returns once the subscriber has the step's start, with
// the step still running.
func feedDurableSetup(t *testing.T) (e *Engine, sink *gatedSink, f *Feed, id string, started Cursor, finish chan struct{}) {
	t.Helper()
	sink = newGatedSink()
	e = newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, Feeds: []string{"dify"},
		Sinks: func(tier Tier, _ int) (wal.Sink, error) {
			switch tier {
			case TierMemory:
				return &wal.MemSink{}, nil
			case TierFile:
				return sink, nil
			}
			return nil, nil
		}})
	mustPlan(t, e, `{"name":"one","root":{"kind":"step","id":"a","action":"llm"}}`)
	finish = make(chan struct{})
	e.RegisterExecutor([]string{"llm"}, 1, ExecutorFunc(func(ctx context.Context, _ *task.Task, _ func([]byte)) task.Result {
		select {
		case <-finish:
		case <-ctx.Done(): // the engine is closing
		}
		return task.Result{Output: json.RawMessage(`"a"`)}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	// Whatever the outcome, let a held write go before the engine closes
	// (Close waits for the committer).
	t.Cleanup(func() {
		select {
		case sink.release <- nil:
		default:
		}
		e.Close()
	})
	f, err := e.Subscribe("dify")
	if err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	id, err = submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &ft})
	if err != nil {
		t.Fatal(err)
	}
	got := readUntil(t, f, func(en FeedEntry) bool { return en.Trace != nil && en.Trace.Kind == core.TrNodeStart })
	if s := kinds(got); s != "run+ +a" {
		t.Fatalf("feed: %s", s)
	}
	return e, sink, f, id, got[len(got)-1].Cursor, finish
}

// finishUnconfirmed lets the step end while the log write of its
// completion is held by the sink, and returns once the shard has applied
// the completion and the feed has taken in what the shard handed it.
func finishUnconfirmed(t *testing.T, e *Engine, sink *gatedSink, id string, finish chan struct{}) {
	t.Helper()
	sink.arm()
	close(finish)
	<-sink.entered
	waitFor(t, func() bool { ri, _ := e.Get(context.Background(), id); return ri.Status == "completed" })
	barrier := make(chan struct{})
	e.feed.q.Push(feedMsg{barrier: barrier})
	<-barrier
}

// pending returns what Next has for f right now, without waiting.
func pending(t *testing.T, f *Feed) []FeedEntry {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	es, err := f.Next(ctx, 0)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	return es
}

// A completion whose log write failed never reaches the subscriber, and
// a cursor past what the log made durable cannot be acknowledged: the
// subscriber cannot record a completion the log does not have.
func TestFeedWithholdsTracesTheLogFailedToWrite(t *testing.T) {
	e, sink, f, id, started, finish := feedDurableSetup(t)
	finishUnconfirmed(t, e, sink, id, finish)
	sink.release <- errors.New("disk on fire")
	waitFor(t, func() bool { return e.Stats().FailedLogs == 1 })
	barrier := make(chan struct{})
	e.feed.q.Push(feedMsg{barrier: barrier})
	<-barrier
	if es := pending(t, f); len(es) > 0 {
		t.Fatalf("published traces the log failed to write: %s", kinds(es))
	}
	past := Cursor{Shard: started.Shard, Tier: started.Tier, LSN: started.LSN + 1}
	if err := f.Ack(past); !errors.Is(err, ErrNotDurable) {
		t.Fatalf("acknowledged a cursor past the durable log: %v", err)
	}
	if err := f.Ack(started); err != nil {
		t.Fatalf("acknowledging a durable cursor: %v", err)
	}
}

// Normally the completion is published once its log write is durable.
func TestFeedPublishesTracesOnceDurable(t *testing.T) {
	e, sink, f, id, started, finish := feedDurableSetup(t)
	finishUnconfirmed(t, e, sink, id, finish)
	if es := pending(t, f); len(es) > 0 {
		t.Fatalf("published before the log was durable: %s", kinds(es))
	}
	past := Cursor{Shard: started.Shard, Tier: started.Tier, LSN: started.LSN + 1}
	if err := f.Ack(past); !errors.Is(err, ErrNotDurable) {
		t.Fatalf("acknowledged a cursor past the durable log: %v", err)
	}
	sink.release <- nil
	got := readUntil(t, f, isRunEnd)
	if s := kinds(got); s != "-a run-" {
		t.Fatalf("after the write: %s", s)
	}
	if err := f.Ack(got[len(got)-1].Cursor); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return e.Stats().FeedBacklog == 0 })
}
