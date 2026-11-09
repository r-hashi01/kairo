package engine

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkflowDriveClaimsAreFencedAndExpire(t *testing.T) {
	var add atomic.Int64
	now := time.Now().Truncate(time.Millisecond)
	e := newEngine(t, Config{Shards: 2, Now: func() time.Time { return now.Add(time.Duration(add.Load())) }})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", RunID: "wf", Input: json.RawMessage(`{"workflow":"orders","version":"1"}`), Drive: true}); err != nil {
		t.Fatal(err)
	}
	rows, err := e.DriveCandidates(context.Background())
	if err != nil || len(rows) != 1 || !rows[0].Ready {
		t.Fatalf("candidates: %+v %v", rows, err)
	}
	c1, ok, err := e.ClaimWorkflow(context.Background(), "wf", "one", time.Minute)
	if err != nil || !ok || c1.Token == "" || c1.Run.RunID != "wf" || string(c1.Run.Input) == "" {
		t.Fatalf("first claim: %+v %v %v", c1, ok, err)
	}
	if _, ok, err := e.ClaimWorkflow(context.Background(), "wf", "two", time.Minute); err != nil || ok {
		t.Fatalf("second claim: %v %v", ok, err)
	}
	if _, ok, err := e.RenewWorkflow(context.Background(), "wf", "one", "wrong", time.Minute); err != nil || ok {
		t.Fatalf("wrong renewal: %v %v", ok, err)
	}
	if _, ok, err := e.RenewWorkflow(context.Background(), "wf", "one", c1.Token, 2*time.Minute); err != nil || !ok {
		t.Fatalf("renewal: %v %v", ok, err)
	}
	if ok, err := e.ReleaseWorkflow(context.Background(), "wf", "one", c1.Token, false); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	rows, _ = e.DriveCandidates(context.Background())
	if len(rows) != 1 || rows[0].Ready {
		t.Fatalf("released without new work: %+v", rows)
	}

	e.ReadyWorkflow("wf")
	c2, ok, err := e.ClaimWorkflow(context.Background(), "wf", "two", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim after ready: %+v %v %v", c2, ok, err)
	}
	add.Store(int64(2 * time.Minute))
	c3, ok, err := e.ClaimWorkflow(context.Background(), "wf", "three", time.Minute)
	if err != nil || !ok || c3.Token == c2.Token {
		t.Fatalf("claim after expiry: %+v %v %v", c3, ok, err)
	}
}

func TestChildCompletionReadiesClaimedParent(t *testing.T) {
	e := newEngine(t, Config{Shards: 2})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", RunID: "parent", Drive: true}); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := e.ClaimWorkflow(context.Background(), "parent", "driver", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim: %+v %v %v", claim, ok, err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", RunID: "child", Parent: "parent", Drive: true}); err != nil {
		t.Fatal(err)
	}
	if err := e.FinishWorkflow(context.Background(), "child", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	wait(t, e, "child")
	if ok, err := e.ReleaseWorkflow(context.Background(), "parent", "driver", claim.Token, false); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	waitFor(t, func() bool {
		rows, _ := e.DriveCandidates(context.Background())
		return len(rows) == 1 && rows[0].Run.RunID == "parent" && rows[0].Ready
	})
}

func TestWaitQuiescentReportsTimerWithoutPolling(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, EvictAfter: time.Millisecond})
	defer e.Close()
	mustPlan(t, e, `{"name":"long-wait","root":{"kind":"wait","id":"w","signal":"go","timeout":"1h"}}`)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	if _, err := submit(e, SubmitRequest{Plan: "long-wait", RunID: "waiting", Tier: &ft}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		ri, err := e.Get(context.Background(), "waiting")
		return err == nil && ri.Evicted
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ri, err := e.WaitQuiescent(ctx, "waiting")
	if err != nil || ri.RunID != "waiting" || ri.NextAt <= time.Now().UnixMilli() {
		t.Fatalf("quiescent: %+v %v", ri, err)
	}
}
