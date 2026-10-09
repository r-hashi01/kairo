package kairo_test

import (
	"context"
	"fmt"
	"log"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

type Req struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
}

// The README's quick start, in memory: a workflow that classifies a
// refund, waits for an approval when it is risky, and refunds once.
func Example() {
	ctx := context.Background()
	k, err := kairo.Open(ctx, kairo.Options{Store: kairo.NewMemStore()})
	if err != nil {
		log.Fatal(err)
	}
	defer k.Close()

	kairo.Action(k, "classify", kairo.Unprotected, func(_ *kairo.TaskContext, r Req) (bool, error) {
		return r.Amount > 100, nil
	})
	kairo.Action(k, "refund-payment", kairo.Real, func(_ *kairo.TaskContext, r Req) (string, error) {
		return "refunded " + r.Order, nil
	})
	kairo.Workflow(k, "refund", func(ctx *kairo.Context, r Req) (string, error) {
		risky, err := kairo.Call[bool](ctx, "classify", r)
		if err != nil {
			return "", err
		}
		if risky {
			if _, err := kairo.WaitFor[string](ctx, "approve"); err != nil {
				return "", err
			}
		}
		return kairo.Call[string](ctx, "refund-payment", r)
	})
	if err := k.Start(ctx); err != nil {
		log.Fatal(err)
	}
	out, err := kairo.Run[string](ctx, k, "refund", Req{Order: "o-42", Amount: 50}, kairo.WithID("refund-o-42"))
	fmt.Println(out, err)

	// A risky one waits for its approval, sent ahead here.
	if _, err := k.Submit(ctx, "refund", Req{Order: "o-43", Amount: 500}, kairo.WithID("refund-o-43")); err != nil {
		log.Fatal(err)
	}
	if err := k.Signal(ctx, "refund-o-43", "approve", "alice"); err != nil {
		log.Fatal(err)
	}
	out, err = kairo.Await[string](ctx, k, "refund-o-43")
	fmt.Println(out, err)
	// Output:
	// refunded o-42 <nil>
	// refunded o-43 <nil>
}
