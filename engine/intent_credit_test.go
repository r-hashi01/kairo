package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/wal"
)

// breakableSink appends until it is broken, then fails every append: a
// log that went bad for good.
type breakableSink struct {
	*wal.MemSink
	broken *atomic.Bool
}

func (s breakableSink) Append(b []byte) error {
	if s.broken.Load() {
		return errors.New("disk on fire")
	}
	return s.MemSink.Append(b)
}

// A real step admitted for a worker and held for its intent (ADR 0085)
// gives the worker's credit back when its shard's log fails before the
// intent is durable: a worker shared with other shards keeps serving them.
// The step itself is not handed over (its intent is not durable).
func TestFailedLogReleasesCreditOfHeldIntent(t *testing.T) {
	var broken atomic.Bool
	e := newEngine(t, Config{Shards: 2, Sinks: func(tier Tier, shard int) (wal.Sink, error) {
		if tier != TierMemory {
			return nil, nil
		}
		if shard == 0 {
			return breakableSink{&wal.MemSink{}, &broken}, nil
		}
		return &wal.MemSink{}, nil
	}})
	defer e.Close()
	mustPlan(t, e, `{"name":"one","root":{"kind":"step","id":"send","action":"send"}}`)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	idOn := func(shard int) string {
		for i := 0; ; i++ {
			id := "run-" + strconv.Itoa(i)
			if e.shardFor(id) == e.shards[shard] {
				return id
			}
		}
	}
	bad, good := idOn(0), idOn(1)
	mt := TierMemory
	// Its start is durable; its step waits in the dispatcher for a worker.
	if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &mt, RunID: bad}); err != nil {
		t.Fatal(err)
	}
	broken.Store(true)
	var mu sync.Mutex
	sent := map[string]int{}
	// One worker for both shards: it admits bad's step, whose intent then
	// cannot be made durable.
	e.RegisterExecutor([]string{"send"}, 1, ExecutorFunc(func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		mu.Lock()
		sent[tk.RunID]++
		mu.Unlock()
		return task.Result{Output: json.RawMessage(`"sent"`)}
	}))
	waitFor(t, func() bool { return e.Stats().FailedLogs == 1 })
	if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &mt, RunID: good}); err != nil {
		t.Fatal(err)
	}
	if ri := wait(t, e, good); ri.Status != "completed" {
		t.Fatalf("%+v", ri)
	}
	mu.Lock()
	defer mu.Unlock()
	if sent[bad] != 0 || sent[good] != 1 {
		t.Fatalf("sent %v: the failed shard's step must not run, the other once", sent)
	}
}
