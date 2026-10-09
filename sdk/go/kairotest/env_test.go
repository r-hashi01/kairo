package kairotest_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
	"github.com/r-hashi01/kairo/sdk/go/kairotest"
)

type Order struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// A workflow that sleeps a day and waits an hour for an approval, tested
// without waiting: the clock is the test's.
func TestEnv(t *testing.T) {
	env := kairotest.New(t)
	charged := 0
	kairo.Action(env.K, "charge", kairo.Real, func(_ *kairo.TaskContext, o Order) (string, error) {
		charged++
		return "charged " + o.ID, nil
	})
	kairo.Workflow(env.K, "refund", func(ctx *kairo.Context, o Order) (string, error) {
		if err := ctx.Sleep(24 * time.Hour); err != nil {
			return "", err
		}
		by, err := kairo.WaitFor[string](ctx, "approve", kairo.WaitTimeout(time.Hour))
		if errors.Is(err, kairo.ErrTimedOut) {
			return "not approved", nil
		}
		if err != nil {
			return "", err
		}
		r, err := kairo.Call[string](ctx, "charge", o)
		return r + " by " + by, err
	})
	env.Start()
	ctx := context.Background()
	began := time.Now()

	if _, err := kairo.Run[string](ctx, env.K, "refund", Order{ID: "o-1", Amount: 5}, kairo.WithID("r-1")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("%v", err)
	}
	env.Advance(23 * time.Hour)
	if _, err := kairo.Await[string](ctx, env.K, "r-1"); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("an hour early: %v", err)
	}
	env.Advance(time.Hour)
	env.Signal("r-1", "approve", "alice")
	out, err := kairo.Await[string](ctx, env.K, "r-1")
	if err != nil || out != "charged o-1 by alice" || charged != 1 {
		t.Fatalf("%q %v, charged %d", out, err, charged)
	}
	calls := env.Calls("r-1")
	var got []string
	for _, c := range calls {
		got = append(got, c.Kind+" "+c.Name+" "+c.Status)
	}
	want := []string{"call kairo.sleep completed", "wait approve completed", "call charge completed"}
	if !equalJSON(got, want) {
		t.Fatalf("calls: %v", got)
	}
	var in Order
	if json.Unmarshal(calls[2].Input, &in) != nil || in.ID != "o-1" || string(calls[1].Output) != `"alice"` {
		t.Fatalf("charge's input %s, the approval %s", calls[2].Input, calls[1].Output)
	}

	// No approval within the hour.
	if _, err := kairo.Run[string](ctx, env.K, "refund", Order{ID: "o-2"}, kairo.WithID("r-2")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("%v", err)
	}
	env.Advance(25 * time.Hour)
	if out, err := kairo.Await[string](ctx, env.K, "r-2"); err != nil || out != "not approved" || charged != 1 {
		t.Fatalf("%q %v", out, err)
	}
	if !env.Now().Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).Add(25 * time.Hour)) {
		t.Fatalf("the clock: %v", env.Now())
	}
	if d := time.Since(began); d > 10*time.Second {
		t.Logf("took %v (two days and an hour of the workflow's time)", d)
	}
}

func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
