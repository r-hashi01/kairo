package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/task"
)

func TestListCodeWorkflows(t *testing.T) {
	at := time.UnixMilli(1_800_000_000_000)
	e := newEngine(t, Config{Shards: 4, DataDir: t.TempDir(), NoSync: true, Now: func() time.Time { return at }})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	start := func(id, workflow string, meta string, parent string) {
		req := SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &ft, RunID: id, Parent: parent, Workflow: workflow}
		if meta != "" {
			req.Meta = json.RawMessage(meta)
		}
		if _, err := submit(e, req); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	start("b", "orders", `{"user":"b"}`, "")
	start("a", "orders", `{"user":"a"}`, "")
	start("c", "other", "", "")
	start("child", "orders", `{"user":"child"}`, "a")
	if _, err := submit(e, SubmitRequest{Plan: "child-wait", Tenant: "t", Tier: &ft, RunID: "graph-root"}); err != nil {
		t.Fatal(err)
	}
	if err := e.FinishWorkflow(context.Background(), "a", json.RawMessage(`{"ok":true}`)); !errors.Is(err, ErrChildrenUnsettled) {
		t.Fatalf("finish with child: %v", err)
	}
	if err := e.CancelTree(context.Background(), "child", "done"); err != nil {
		t.Fatal(err)
	}
	wait(t, e, "child")
	if err := e.FinishWorkflow(context.Background(), "a", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	wait(t, e, "a")
	if err := e.CancelTree(context.Background(), "c", "stop"); err != nil {
		t.Fatal(err)
	}
	wait(t, e, "c")

	rows, err := e.List(context.Background(), ListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if got := listIDs(rows); got != "a,b,c" {
		t.Fatalf("all: %s (%+v)", got, rows)
	}
	if rows[0].CreatedAt != at.UnixMilli() || rows[0].UpdatedAt != at.UnixMilli() || string(rows[0].Meta) != `{"user":"a"}` {
		t.Fatalf("metadata: %+v", rows[0])
	}
	rows, err = e.List(context.Background(), ListFilter{Workflow: "orders"})
	if err != nil || listIDs(rows) != "a,b" {
		t.Fatalf("workflow: %s %v", listIDs(rows), err)
	}
	rows, err = e.List(context.Background(), ListFilter{Status: "completed"})
	if err != nil || listIDs(rows) != "a" {
		t.Fatalf("status: %s %v", listIDs(rows), err)
	}
	rows, err = e.List(context.Background(), ListFilter{Since: at, Until: at.Add(time.Millisecond), After: "a", Limit: 1})
	if err != nil || listIDs(rows) != "b" {
		t.Fatalf("page: %s %v", listIDs(rows), err)
	}
	if rows, err = e.List(context.Background(), ListFilter{After: "missing"}); err != nil || len(rows) != 0 {
		t.Fatalf("missing cursor: %+v %v", rows, err)
	}
}

func TestListSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	at := time.UnixMilli(1_800_000_100_000)
	startEngine := func() *Engine {
		e := newEngine(t, Config{Shards: 2, DataDir: dir, NoSync: true, Now: func() time.Time { return at }})
		registerParentPlans(t, e)
		if err := e.Start(); err != nil {
			t.Fatal(err)
		}
		return e
	}
	ft := TierFile
	e1 := startEngine()
	for _, id := range []string{"active", "finished"} {
		if _, err := submit(e1, SubmitRequest{Plan: "kairo.workflow", Tenant: "t", Tier: &ft, RunID: id, Workflow: "orders", Meta: json.RawMessage(`{"kept":true}`), KeepOutput: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e1.FinishWorkflow(context.Background(), "finished", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	wait(t, e1, "finished")
	waitFor(t, func() bool {
		ri, err := e1.Get(context.Background(), "active")
		return err == nil && ri.Evicted
	})
	e1.Close()

	e2 := startEngine()
	defer e2.Close()
	rows, err := e2.List(context.Background(), ListFilter{Workflow: "orders"})
	if err != nil || listIDs(rows) != "active,finished" {
		t.Fatalf("after restart: %s %+v %v", listIDs(rows), rows, err)
	}
	for _, row := range rows {
		if row.CreatedAt != at.UnixMilli() || row.UpdatedAt != at.UnixMilli() || string(row.Meta) != `{"kept":true}` {
			t.Fatalf("after restart metadata: %+v", row)
		}
	}
}

func TestListIncludesEvictedWorkflow(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, EvictAfter: time.Millisecond})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tier: &ft, RunID: "evicted", Workflow: "orders"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		ri, err := e.Get(context.Background(), "evicted")
		return err == nil && ri.Evicted
	})
	rows, err := e.List(context.Background(), ListFilter{Workflow: "orders"})
	if err != nil || len(rows) != 1 || rows[0].RunID != "evicted" || !rows[0].Evicted || rows[0].CreatedAt == 0 {
		t.Fatalf("evicted: %+v %v", rows, err)
	}
}

func TestListIncludesFailedWorkflow(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true})
	defer e.Close()
	mustPlan(t, e, `{"name":"kairo.workflow","root":{"kind":"step","id":"fail","action":"llm"}}`)
	e.RegisterExecutor([]string{"llm"}, 1, ExecutorFunc(func(context.Context, *task.Task, func([]byte)) task.Result {
		return task.Result{Err: "failed deliberately"}
	}))
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tier: &ft, RunID: "failed", Workflow: "orders"}); err != nil {
		t.Fatal(err)
	}
	if ri := wait(t, e, "failed"); ri.Status != "failed" {
		t.Fatalf("outcome: %+v", ri)
	}
	rows, err := e.List(context.Background(), ListFilter{Status: "failed"})
	if err != nil || listIDs(rows) != "failed" {
		t.Fatalf("failed: %+v %v", rows, err)
	}
}

func TestListDropsExpiredFinishedWorkflow(t *testing.T) {
	var now atomic.Int64
	now.Store(1_800_000_200_000)
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true, IdempotencyTTL: time.Second,
		Now: func() time.Time { return time.UnixMilli(now.Load()) }})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	ft := TierFile
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tier: &ft, RunID: "expired", Workflow: "orders"}); err != nil {
		t.Fatal(err)
	}
	if err := e.FinishWorkflow(context.Background(), "expired", json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	wait(t, e, "expired")
	now.Add(1001)
	rows, err := e.List(context.Background(), ListFilter{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("expired: %+v %v", rows, err)
	}
	if _, err := submit(e, SubmitRequest{Plan: "kairo.workflow", Tier: &ft, RunID: "newer", Workflow: "orders"}); err != nil {
		t.Fatal(err)
	}
	rows, err = e.List(context.Background(), ListFilter{After: "expired"})
	if err != nil || len(rows) != 0 {
		t.Fatalf("expired cursor: %+v %v", rows, err)
	}
}

func TestWorkflowMetadataValidation(t *testing.T) {
	e := newEngine(t, Config{Shards: 1, DataDir: t.TempDir(), NoSync: true})
	defer e.Close()
	registerParentPlans(t, e)
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	for _, req := range []SubmitRequest{
		{Plan: "child-wait", RunID: "wrong-plan", Workflow: "orders"},
		{Plan: "kairo.workflow", RunID: "meta-without-workflow", Meta: json.RawMessage(`{}`)},
		{Plan: "kairo.workflow", RunID: "meta-array", Workflow: "orders", Meta: json.RawMessage(`[]`)},
	} {
		if _, err := e.Submit(context.Background(), req); err == nil {
			t.Fatalf("accepted %+v", req)
		}
	}
}

func listIDs(rows []RunInfo) string {
	var out string
	for i, row := range rows {
		if i > 0 {
			out += ","
		}
		out += row.RunID
	}
	return out
}
