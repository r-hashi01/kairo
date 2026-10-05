package sched

import (
	"testing"
	"time"

	"kairo/task"
)

func TestAdmissionFairnessAndLimits(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxActive: 2, MaxQueued: 5})
	var started []string
	start := func(name string) func() { return func() { started = append(started, name) } }
	a.Admit("A", start("A1"))
	a.Admit("A", start("A2"))
	// Full: A queues 3, B queues 2, then the queue is full.
	for _, n := range []string{"A3", "A4", "A5"} {
		if tk, err := a.Admit("A", start(n)); tk == nil || err != nil {
			t.Fatal(tk, err)
		}
	}
	a.Admit("B", start("B1"))
	a.Admit("B", start("B2"))
	if _, err := a.Admit("C", start("C1")); err != ErrOverloaded {
		t.Fatalf("expected overload, got %v", err)
	}
	for range 5 {
		a.Release("A")
	}
	want := "A1 A2 A3 B1 A4 B2 A5"
	if got := join(started); got != want {
		t.Fatalf("start order %q, want %q (round-robin between tenants)", got, want)
	}
}

func join(s []string) string {
	out := ""
	for i, x := range s {
		if i > 0 {
			out += " "
		}
		out += x
	}
	return out
}

func TestDispatcherRoundRobinBetweenTenants(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	for i := 0; i < 6; i++ {
		d.Submit(&task.Task{RunID: "a", Act: uint32(i), Tenant: "A", Action: "x", Destination: "x"})
	}
	for i := 0; i < 2; i++ {
		d.Submit(&task.Task{RunID: "b", Act: uint32(i), Tenant: "B", Action: "x", Destination: "x"})
	}
	time.Sleep(10 * time.Millisecond)
	p := d.NewPoller([]string{"x"}, 8)
	var order []string
	for i := 0; i < 8; i++ {
		tk := <-p.C
		order = append(order, tk.Task.Tenant)
	}
	// B must not wait behind all of A's tasks.
	if got := join(order); got != "A B A B A A A A" {
		t.Fatalf("order %s", got)
	}
}

func TestDispatcherRateLimit(t *testing.T) {
	d := NewDispatcher(func(string) DestLimits { return DestLimits{RPM: 600, BurstSeconds: 0.5} }) // 10/s, burst 5
	defer d.Close()
	p := d.NewPoller([]string{"llm"}, 100)
	start := time.Now()
	for i := 0; i < 10; i++ {
		d.Submit(&task.Task{RunID: "r", Act: uint32(i), Tenant: "t", Action: "llm", Destination: "llm"})
	}
	for i := 0; i < 10; i++ {
		<-p.C
	}
	// 5 immediately (burst), 5 more at 10/s => ~0.5s.
	if el := time.Since(start); el < 400*time.Millisecond || el > 900*time.Millisecond {
		t.Fatalf("10 requests at 10/s with burst 5 took %v", el)
	}
}

func TestDispatcherTenantConcurrency(t *testing.T) {
	d := NewDispatcher(func(string) DestLimits { return DestLimits{TenantConcurrency: 2} })
	defer d.Close()
	p := d.NewPoller([]string{"x"}, 10)
	var tasks []*task.Task
	for i := 0; i < 5; i++ {
		tk := &task.Task{RunID: "a", Act: uint32(i), Tenant: "A", Action: "x", Destination: "x"}
		tasks = append(tasks, tk)
		d.Submit(tk)
	}
	got := []*task.Task{(<-p.C).Task, (<-p.C).Task}
	select {
	case <-p.C:
		t.Fatal("tenant exceeded its concurrency cap")
	case <-time.After(30 * time.Millisecond):
	}
	d.Done(got[0], 0)
	select {
	case <-p.C:
	case <-time.After(time.Second):
		t.Fatal("slot not reused after Done")
	}
}

func TestDispatcherUnpollRequeues(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	p1 := d.NewPoller([]string{"x"}, 4)
	for i := 0; i < 3; i++ {
		d.Submit(&task.Task{RunID: "r", Act: uint32(i), Tenant: "t", Action: "x", Destination: "x"})
	}
	time.Sleep(10 * time.Millisecond)
	d.Unpoll(p1) // three tasks were delivered to p1.C but never taken
	p2 := d.NewPoller([]string{"x"}, 4)
	for i := 0; i < 3; i++ {
		select {
		case <-p2.C:
		case <-time.After(time.Second):
			t.Fatal("tasks of a departed worker were not requeued")
		}
	}
}

func TestAdmissionCancel(t *testing.T) {
	a := NewAdmission(AdmissionConfig{MaxActive: 1})
	var started []string
	run := func(n string) func() { return func() { started = append(started, n) } }
	a.Admit("A", run("a1"))
	tb, _ := a.Admit("B", run("b1"))
	tc, _ := a.Admit("C", run("c1"))
	if !tb.Cancel() {
		t.Fatal("queued run could not be cancelled")
	}
	if tb.Cancel() {
		t.Fatal("cancelled twice")
	}
	a.Release("A")
	if join(started) != "a1 c1" {
		t.Fatalf("started %q", join(started))
	}
	if tc.Cancel() {
		t.Fatal("a started run was cancelled")
	}
	if active, queued := a.Stats(); active != 1 || queued != 0 {
		t.Fatalf("active %d queued %d", active, queued)
	}
}

func TestAbortCancelsDeliveredTask(t *testing.T) {
	d := NewDispatcher(func(string) DestLimits { return DestLimits{TenantConcurrency: 1} })
	defer d.Close()
	p := d.NewPoller([]string{"x"}, 2)
	a := &task.Task{RunID: "r", Act: 1, Tenant: "t", Action: "x", Destination: "x"}
	b := &task.Task{RunID: "r", Act: 2, Tenant: "t", Action: "x", Destination: "x"}
	d.Submit(a)
	d.Submit(b)
	dl := <-p.C
	d.Abort(a.Key())
	select {
	case <-dl.Ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("aborting a delivered task did not cancel its context")
	}
	// The slot stays taken until the worker reports Done (ADR 0026).
	select {
	case <-p.C:
		t.Fatal("concurrency slot released before Done")
	case <-time.After(30 * time.Millisecond):
	}
	d.Done(a, 0)
	select {
	case dl := <-p.C:
		if dl.Ctx.Err() != nil {
			t.Fatal("the next task arrived cancelled")
		}
		d.Done(dl.Task, 0)
	case <-time.After(time.Second):
		t.Fatal("slot not reused after Done")
	}
	waitTracked(t, d, 0)
}

func TestAbortLeavesNothingBehind(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	// Aborted while queued (no worker yet), after delivery, after Done and
	// while delivered but not taken (then Unpoll): no key is remembered.
	for i := 0; i < 1000; i++ {
		d.Submit(&task.Task{RunID: "q", Act: uint32(i), Tenant: "t", Action: "x", Destination: "x"})
		d.Abort(task.Key{RunID: "q", Act: uint32(i)})
	}
	p := d.NewPoller([]string{"x"}, 1)
	for i := 0; i < 1000; i++ {
		tk := &task.Task{RunID: "r", Act: uint32(i), Tenant: "t", Action: "x", Destination: "x"}
		d.Submit(tk)
		dl := <-p.C
		if i%2 == 0 {
			d.Abort(tk.Key())
			<-dl.Ctx.Done()
			d.Done(tk, 0)
		} else {
			d.Done(tk, 0)
			d.Abort(tk.Key())
		}
		d.Poll(p, 1)
	}
	d.Unpoll(p)
	p2 := d.NewPoller([]string{"y"}, 4)
	for i := 0; i < 4; i++ {
		d.Submit(&task.Task{RunID: "u", Act: uint32(i), Tenant: "t", Action: "y", Destination: "y"})
	}
	waitFor(t, func() bool { return len(p2.C) == 4 })
	for i := 0; i < 2; i++ {
		d.Abort(task.Key{RunID: "u", Act: uint32(i)})
	}
	d.Unpoll(p2) // two requeued, two dropped as aborted
	waitTracked(t, d, 2)
	if q := d.Queued(); q != 2 {
		t.Fatalf("queued %d, want 2", q)
	}
	p3 := d.NewPoller([]string{"y"}, 4)
	for i := 0; i < 2; i++ {
		dl := <-p3.C
		if dl.Task.RunID != "u" || dl.Task.Act < 2 {
			t.Fatalf("an aborted task was requeued: %+v", dl.Task)
		}
		d.Done(dl.Task, 0)
	}
	waitTracked(t, d, 0)
}

func waitTracked(t *testing.T, d *Dispatcher, n int) {
	t.Helper()
	waitFor(t, func() bool { return d.Tracked() == n })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}
