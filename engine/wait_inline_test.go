package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/blob"
)

// gatedBlobs holds reads of one run's blobs until it is opened: the time
// between a run's end and its output's inlining, made as long as the test
// needs.
type gatedBlobs struct {
	blob.Store
	group string
	open  chan struct{}
}

func (g gatedBlobs) Get(key string) ([]byte, error) {
	if strings.HasPrefix(key, g.group) {
		<-g.open
	}
	return g.Store.Get(key)
}

// A run whose output refers to blobs is inlined before its blobs are
// deleted. Wait, called while that is under way, returns the inlined
// output once it is there, never the reference to blobs about to go
// (issue #93). Get may still see the reference meanwhile: the blobs are
// not gone yet.
func TestWaitDoesNotReturnOutputBeingInlined(t *testing.T) {
	blobs := gatedBlobs{Store: blob.NewMem(), group: blob.RunGroup("inl") + "/", open: make(chan struct{})}
	e := newEngine(t, Config{Shards: 1, BlobThreshold: 64, Blobs: blobs})
	defer e.Close()
	mustPlan(t, e, oneStep)
	e.RegisterExecutor([]string{"llm"}, 1, keptBigExec())
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	mt := TierMemory
	if _, err := submit(e, SubmitRequest{Plan: "one", Tenant: "t", Tier: &mt, RunID: "inl"}); err != nil {
		t.Fatal(err)
	}
	// The run has ended and its output, a reference, waits to be inlined.
	waitFor(t, func() bool {
		ri, err := e.Get(context.Background(), "inl")
		return err == nil && ri.Status == "completed" && strings.Contains(string(ri.Output), `"$blob"`)
	})
	got := make(chan RunInfo, 1)
	go func() {
		ri, err := e.Wait(context.Background(), "inl")
		if err != nil {
			t.Error(err)
		}
		got <- ri
	}()
	close(blobs.open)
	ri := <-got
	if strings.Contains(string(ri.Output), `"$blob"`) || !strings.Contains(string(ri.Output), "xxxx") {
		t.Fatalf("Wait returned %s", ri.Output)
	}
}
