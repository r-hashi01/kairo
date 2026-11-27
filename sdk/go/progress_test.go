package kairo_test

import (
	"context"
	"errors"
	"testing"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// A store that cannot read runs' events does not give progress, and no
// other read stands in for it (ADR 0088).
func TestProgressNeedsATreeStore(t *testing.T) {
	ctx := context.Background()
	store := struct{ kairo.Store }{kairo.NewMemStore()} // Tree not promoted
	k, err := kairo.Open(ctx, kairo.Options{Store: store})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Workflow(k, "quick", func(*kairo.Context, any) (string, error) { return "done", nil })
	if err := k.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := kairo.Run[string](ctx, k, "quick", nil, kairo.WithID("quick-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Progress(ctx, "quick-1"); !errors.Is(err, kairo.ErrProgressUnsupported) {
		t.Fatalf("%v", err)
	}
}
