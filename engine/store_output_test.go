package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/task"
)

// failingPuts fails every write of a run's content: a large output cannot
// be moved out of the log.
type failingPuts struct{ blob.Store }

func (failingPuts) Put(key string, _ []byte) error {
	if strings.HasPrefix(key, "run/") {
		return errors.New("blob store unavailable")
	}
	return nil
}

// An executor's success whose output cannot be stored is not a failure of
// the action: a real step it ran is not run again. Its outcome is unknown
// to the run, so it stops for review (or fails, with on_unknown: fail,
// ADR 0035). Steps that may run again (not real, or real and declared
// idempotent) are retried as before. Protected steps never reach an
// executor: they are evaluated inside the core.
func TestOutputStoreFailureDoesNotRerunRealStep(t *testing.T) {
	for _, tc := range []struct {
		name      string
		effect    ir.Effect
		idem      bool
		onUnknown string
		calls     int32
		status    string
	}{
		{name: "real", effect: ir.EffectReal, calls: 1, status: "blocked"},
		{name: "real-on-unknown-fail", effect: ir.EffectReal, onUnknown: `,"on_unknown":"fail"`, calls: 1, status: "failed"},
		{name: "real-idempotent", effect: ir.EffectReal, idem: true, calls: 3, status: "failed"},
		{name: "unprotected", effect: ir.EffectUnprotected, calls: 3, status: "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := ir.NewRegistry()
			reg.Register(ir.NodeSpec{Action: "act", Effect: tc.effect, IdempotentRetry: tc.idem})
			e := newEngine(t, Config{Shards: 1, BlobThreshold: 64, Blobs: failingPuts{blob.NewMem()}, Registry: reg})
			defer e.Close()
			mustPlan(t, e, `{"name":"p","root":{"kind":"step","id":"s","action":"act",
			  "retry":{"max_attempts":3,"interval":"1ms"}`+tc.onUnknown+`}}`)
			var calls atomic.Int32
			e.RegisterExecutor([]string{"act"}, 1, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
				calls.Add(1)
				out, _ := json.Marshal(map[string]string{"text": strings.Repeat("x", 300)})
				return task.Result{Output: out}
			}))
			if err := e.Start(); err != nil {
				t.Fatal(err)
			}
			mt := TierMemory
			id, err := submit(e, SubmitRequest{Plan: "p", Tenant: "t", Tier: &mt})
			if err != nil {
				t.Fatal(err)
			}
			var ri RunInfo
			waitFor(t, func() bool {
				ri, _ = e.Get(context.Background(), id)
				return ri.Status == "blocked" || ri.Status == "failed" || ri.Status == "completed"
			})
			if ri.Status != tc.status || calls.Load() != tc.calls {
				t.Fatalf("status %s, executor called %d times; want %s, %d (%s)", ri.Status, calls.Load(), tc.status, tc.calls, ri.Error)
			}
			if tc.status == "blocked" && len(ri.Reviews) != 1 {
				t.Fatalf("reviews %+v", ri.Reviews)
			}
		})
	}
}
