package sched

import (
	"testing"
	"time"

	"github.com/r-hashi01/kairo/task"
)

// recv returns which of ps got the next task, and the task.
func recv(t *testing.T, ps ...*Poller) (int, *task.Task) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		for i, p := range ps {
			select {
			case dl := <-p.C:
				return i, dl.Task
			default:
			}
		}
		select {
		case <-timeout:
			t.Fatal("no task delivered")
		case <-time.After(time.Millisecond):
		}
	}
}

// The tasks of a run go to the affine poller that took its first task while
// it has credit; with none left, to another, which keeps the run from then
// on; a poller that is gone keeps no run (ADR 0046).
func TestAffinePollersKeepARunTogether(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	p1 := d.NewAffinePoller([]string{"x"}, 3)
	p2 := d.NewAffinePoller([]string{"x"}, 3)
	act := uint32(0)
	submit := func(run string) {
		act++
		d.Submit(&task.Task{RunID: run, Act: act, Tenant: "t", Action: "x", Destination: "x"})
	}
	// Runs a and b start on different pollers (rotation), then each
	// keeps its poller.
	submit("a")
	ia, _ := recv(t, p1, p2)
	submit("b")
	ib, _ := recv(t, p1, p2)
	if ia == ib {
		t.Fatal("two runs started on the same poller")
	}
	for range 2 {
		submit("a")
		if i, _ := recv(t, p1, p2); i != ia {
			t.Fatal("a task of run a went to another poller")
		}
	}
	// a's poller has no credit left: a's next task goes to the other one,
	// which keeps run a from then on.
	ps := []*Poller{p1, p2}
	submit("a")
	if i, _ := recv(t, p1, p2); i != ib {
		t.Fatal("with no credit, run a's task did not go to the other poller")
	}
	d.Poll(ps[ia], 3)
	submit("a")
	if i, _ := recv(t, p1, p2); i != ib {
		t.Fatal("run a did not stay on its new poller")
	}
	// The run's poller goes away: its runs go to the one left.
	d.Unpoll(ps[ib])
	submit("a")
	if i, _ := recv(t, p1, p2); i != ia {
		t.Fatal("run a's task did not go to the poller left")
	}
	// The table is emptied by the runs' ends and by the poller going away.
	d.RunEnded("a")
	d.RunEnded("b")
	affineRuns := func() int {
		n := 0
		d.affineRuns.Range(func(any, any) bool { n++; return true })
		return n
	}
	deadline := time.Now().Add(5 * time.Second)
	for affineRuns() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d runs still kept after their ends", affineRuns())
		}
		time.Sleep(time.Millisecond)
	}
	submit("c")
	recv(t, p1, p2)
	if affineRuns() != 1 {
		t.Fatalf("%d runs kept, want 1", affineRuns())
	}
	d.Unpoll(ps[ia])
	for affineRuns() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("a run kept after its poller went away")
		}
		time.Sleep(time.Millisecond)
	}
}

// Pollers that keep nothing per run still take turns.
func TestPlainPollersRotate(t *testing.T) {
	d := NewDispatcher(nil)
	defer d.Close()
	p1 := d.NewPoller([]string{"x"}, 4)
	p2 := d.NewPoller([]string{"x"}, 4)
	got := map[int]int{}
	for i := range 4 {
		d.Submit(&task.Task{RunID: "a", Act: uint32(i + 1), Tenant: "t", Action: "x", Destination: "x"})
		w, _ := recv(t, p1, p2)
		got[w]++
	}
	if got[0] != 2 || got[1] != 2 {
		t.Fatalf("tasks of one run on plain pollers: %v, want 2 each", got)
	}
}
