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
		if q, err := a.Admit("A", start(n)); !q || err != nil {
			t.Fatal(q, err)
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
		order = append(order, tk.Tenant)
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
	got := []*task.Task{<-p.C, <-p.C}
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
