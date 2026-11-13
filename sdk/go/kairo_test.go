package kairo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// Call ids are the TypeScript SDK's: sdk/testdata/callids.jsonl (shared
// with the Python SDK's tests) was made by its
// canonical and callId (sdk/ts/src/workflow.ts), on inputs at the edges
// (key order with astral characters, <>&, U+2028, -0, 1e21, nesting).
func TestCallIDsMatchTypeScript(t *testing.T) {
	f, err := os.Open("../testdata/callids.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var c struct {
			Input     any    `json:"input"`
			Canonical string `json:"canonical"`
			ID        string `json:"id"`
		}
		if err := json.Unmarshal(sc.Bytes(), &c); err != nil {
			t.Fatal(err)
		}
		canon, err := canonical(c.Input)
		if err != nil {
			t.Fatal(err)
		}
		if canon != c.Canonical {
			t.Errorf("canonical(%v) = %s, TypeScript: %s", c.Input, canon, c.Canonical)
		}
		if id := callID("wf-1", "kairo.call/llm", canon, 2); id != c.ID {
			t.Errorf("callID(%v) = %s, TypeScript: %s", c.Input, id, c.ID)
		}
		n++
	}
	if n < 19 {
		t.Fatalf("%d cases", n)
	}
}

func TestKeepFinishedSetting(t *testing.T) {
	for _, c := range []struct {
		opt  time.Duration
		env  string
		want time.Duration
	}{{0, "", 24 * time.Hour}, {0, "7d", 7 * 24 * time.Hour}, {0, "30m", 30 * time.Minute}, {0, "forever", Forever}, {time.Second, "7d", time.Second}} {
		if got, err := keepFinished(c.opt, c.env); err != nil || got != c.want {
			t.Errorf("%v %q: %v %v", c.opt, c.env, got, err)
		}
	}
	if _, err := keepFinished(0, "a week"); err == nil {
		t.Error("a bad value was taken")
	}
}

func TestSharedLimitsRequireCapableEmbeddedStore(t *testing.T) {
	k, err := Open(context.Background(), Options{Store: NewMemStore()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	Action(k, "shared", Unprotected, func(*TaskContext, any) (any, error) { return nil, nil }, Rate(60), SharedLimits())
	if err := k.Start(context.Background()); err == nil {
		t.Fatal("SharedLimits with MemStore was accepted")
	}
}

func TestRuntimeDestinationsShareActualWeightAndRetryAfter(t *testing.T) {
	var mu sync.Mutex
	var resolved []string
	k, err := Open(context.Background(), Options{Store: NewMemStore(), DestinationLimits: func(destination string) (DestinationLimits, error) {
		mu.Lock()
		resolved = append(resolved, destination)
		mu.Unlock()
		return DestinationLimits{WeightPerMin: 60_000}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	runs := 0
	var firstFinished, secondStarted time.Time
	Action(k, "metered", Unprotected, func(ctx *TaskContext, value string) (int, error) {
		runs++
		switch value {
		case "first":
			ctx.SetWeight(10_100)
			firstFinished = time.Now()
		case "second":
			secondStarted = time.Now()
		}
		return runs, nil
	})
	retryAttempts := 0
	var retryBegan time.Time
	Action(k, "retry-after", Unprotected, func(*TaskContext, any) (time.Duration, error) {
		retryAttempts++
		if retryAttempts == 1 {
			retryBegan = time.Now()
			return 0, RetryAfter(errors.New("busy"), 80*time.Millisecond)
		}
		return time.Since(retryBegan), nil
	}, MaxAttempts(2), Backoff(time.Millisecond))
	type output struct {
		A, B       int
		RetryDelay time.Duration
	}
	Workflow(k, "w", func(ctx *Context, _ any) (output, error) {
		a, err := Call[int](ctx, "metered", "same", CallDestination("provider-a"))
		if err != nil {
			return output{}, err
		}
		b, err := Call[int](ctx, "metered", "same", CallDestination("provider-b"))
		if err != nil {
			return output{}, err
		}
		if _, err := Call[int](ctx, "metered", "first", CallDestination("provider-a")); err != nil {
			return output{}, err
		}
		if _, err := Call[int](ctx, "metered", "second", CallDestination("provider-a")); err != nil {
			return output{}, err
		}
		delay, err := Call[time.Duration](ctx, "retry-after", nil, CallDestination("provider-a"))
		return output{A: a, B: b, RetryDelay: delay}, err
	})
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := Run[output](context.Background(), k, "w", nil, WithID("runtime-destinations-1"))
	if err != nil {
		t.Fatal(err)
	}
	if out.A == out.B {
		t.Fatal("different destinations reused one call identity")
	}
	mu.Lock()
	seenA, seenB := false, false
	for _, destination := range resolved {
		seenA = seenA || destination == "provider-a"
		seenB = seenB || destination == "provider-b"
	}
	mu.Unlock()
	if !seenA || !seenB {
		t.Fatalf("resolved destinations = %v", resolved)
	}
	if waited := secondStarted.Sub(firstFinished); waited < 75*time.Millisecond {
		t.Fatalf("weight debt waited %v", waited)
	}
	if out.RetryDelay < 60*time.Millisecond {
		t.Fatalf("retry-after waited %v", out.RetryDelay)
	}
}

// cancelBeforeDone serializes a cancellation immediately before the workflow
// driver sends done. It makes the ADR 0061 race deterministic.
type cancelBeforeDone struct {
	runs
	once sync.Once
	err  error
}

func (r *cancelBeforeDone) signal(ctx context.Context, runID, name string, payload any) error {
	if name == "done" {
		r.once.Do(func() {
			r.err = r.runs.cancel(ctx, runID)
			if r.err == nil {
				_, r.err = r.runs.wait(ctx, runID)
			}
		})
		if r.err != nil {
			return r.err
		}
	}
	return r.runs.signal(ctx, runID, name, payload)
}

func TestCancellationRecordedBeforeDoneWins(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(*Context, any) (string, error)
	}{
		{name: "success", fn: func(*Context, any) (string, error) { return "local value", nil }},
		{name: "failure", fn: func(*Context, any) (string, error) { return "", errors.New("local failure") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k, err := Open(context.Background(), Options{Store: NewMemStore()})
			if err != nil {
				t.Fatal(err)
			}
			defer k.Close()
			Workflow(k, "race", tc.fn)
			if err := k.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			k.runs = &cancelBeforeDone{runs: k.runs}
			id := "cancel-before-done-" + tc.name
			if _, err := Run[string](context.Background(), k, "race", nil, WithID(id)); !errors.Is(err, ErrCancelled) {
				t.Fatalf("live result: %v", err)
			}
			if _, err := Run[string](context.Background(), k, "race", nil, WithID(id)); !errors.Is(err, ErrCancelled) {
				t.Fatalf("recorded result: %v", err)
			}
			if err := k.Cancel(context.Background(), id); err != nil {
				t.Fatalf("repeated cancellation: %v", err)
			}
		})
	}
}

func TestCancelAfterCompletionReportsSettled(t *testing.T) {
	k, err := Open(context.Background(), Options{Store: NewMemStore()})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	Workflow(k, "quick", func(*Context, any) (string, error) { return "done", nil })
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := Run[string](context.Background(), k, "quick", nil, WithID("already-complete")); err != nil || got != "done" {
		t.Fatalf("run: %q %v", got, err)
	}
	if err := k.Cancel(context.Background(), "already-complete"); !errors.Is(err, ErrRunSettled) {
		t.Fatalf("cancel: %v", err)
	}
}
