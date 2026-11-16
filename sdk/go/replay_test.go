package kairo

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
)

func TestRecordOnlyReplayUsesSavedCallsAndStopsAtBoundary(t *testing.T) {
	ctx := context.Background()
	k, err := Open(ctx, Options{Store: NewMemStore()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	var actions atomic.Int32
	Action(k, "double", Unprotected, func(_ *TaskContext, n int) (int, error) {
		actions.Add(1)
		return n * 2, nil
	})
	Workflow(k, "calculate", func(ctx *Context, n int) (int, error) {
		value, err := Call[int](ctx, "double", n)
		if err != nil {
			return 0, err
		}
		return value + 1, nil
	})
	if err := k.Start(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := Run[int](ctx, k, "calculate", 2, WithID("replay-records-1"))
	if err != nil || got != 5 {
		t.Fatalf("Run = %d, %v", got, err)
	}
	before, err := k.Get(ctx, "replay-records-1")
	if err != nil {
		t.Fatal(err)
	}

	replay, err := k.Replay(ctx, "replay-records-1")
	if err != nil {
		t.Fatal(err)
	}
	var output int
	if err := json.Unmarshal(replay.Output, &output); err != nil {
		t.Fatal(err)
	}
	if replay.Outcome != ReplayCompleted || output != 5 || replay.Err != nil {
		t.Fatalf("Replay = outcome %q, output %s, err %v", replay.Outcome, replay.Output, replay.Err)
	}
	if len(replay.Calls) != 1 || replay.Calls[0].Kind != "call" || replay.Calls[0].Name != "double" || replay.Calls[0].Status != "completed" {
		t.Fatalf("calls = %#v", replay.Calls)
	}
	if actions.Load() != 1 {
		t.Fatalf("action ran during replay: %d runs", actions.Load())
	}

	// Change the function registered for the same saved version. The broad
	// error handling must not turn an unrecorded call into a completed replay.
	Workflow(k, "calculate", func(ctx *Context, n int) (int, error) {
		_, _ = Call[int](ctx, "double", n)
		_, _ = Call[int](ctx, "new-call", n)
		return 99, nil
	}, Draining())
	replay, err = k.Replay(ctx, "replay-records-1")
	if err != nil {
		t.Fatal(err)
	}
	if replay.Outcome != ReplayBoundary || replay.Boundary == nil || replay.Boundary.Name != "new-call" || replay.Boundary.Status != "missing" || replay.Err == nil {
		t.Fatalf("boundary replay = %#v", replay)
	}
	if len(replay.Calls) != 2 {
		t.Fatalf("boundary calls = %#v", replay.Calls)
	}
	if actions.Load() != 1 {
		t.Fatalf("action ran during boundary replay: %d runs", actions.Load())
	}
	after, err := k.Get(ctx, "replay-records-1")
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != before.Status || !after.Updated.Equal(before.Updated) || string(after.Output) != string(before.Output) {
		t.Fatalf("replay changed root: before %#v, after %#v", before, after)
	}
}
