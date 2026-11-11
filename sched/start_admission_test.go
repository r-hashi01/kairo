package sched

import (
	"testing"
	"time"
)

func TestStartAdmissionWindowFairnessAndBound(t *testing.T) {
	now := time.Unix(0, 0)
	a := NewStartAdmission(0, func() time.Time { return now })
	key := StartKey{Shard: 2, Tier: 2}
	var started []string
	start := func(name string) func(*StartTicket) { return func(*StartTicket) { started = append(started, name) } }
	t1, err := a.Admit(key, "A", start("A1"))
	if err != nil {
		t.Fatal(err)
	}
	var tickets []*StartTicket
	for _, x := range []struct{ tenant, name string }{{"A", "A2"}, {"A", "A3"}, {"B", "B1"}, {"B", "B2"}} {
		tk, err := a.Admit(key, x.tenant, start(x.name))
		if err != nil {
			t.Fatal(err)
		}
		tickets = append(tickets, tk)
	}
	if _, err := a.Admit(key, "C", start("C1")); err != ErrOverloaded {
		t.Fatalf("queue beyond four windows: %v", err)
	}
	if got := len(started); got != 1 {
		t.Fatalf("started %d with window 1", got)
	}
	now = now.Add(time.Millisecond)
	a.ConfirmRound(key, 1, []*StartTicket{t1}, now)
	if got := join(started); got != "A1 A2 B1" {
		t.Fatalf("start order %q, want tenant round robin", got)
	}
	st := a.Stats()[0]
	if st.Window != 2 || st.Inflight != 2 || st.Queued != 2 || st.Rejected != 1 {
		t.Fatalf("stats after first round: %+v", st)
	}
	// The two starts released by the same storage round adjust only once.
	now = now.Add(time.Millisecond)
	a.ConfirmRound(key, 2, []*StartTicket{tickets[0], tickets[2]}, now)
	if got := a.Stats()[0].Window; got != 3 {
		t.Fatalf("window = %d, want 3", got)
	}
}

func TestStartAdmissionCancellationAndDecrease(t *testing.T) {
	now := time.Unix(0, 0)
	a := NewStartAdmission(0, func() time.Time { return now })
	key := StartKey{Shard: 0, Tier: 2}
	t1, _ := a.Admit(key, "A", func(*StartTicket) {})
	queued, _ := a.Admit(key, "B", func(*StartTicket) { t.Fatal("cancelled start ran") })
	if !queued.Cancel() || queued.Cancel() {
		t.Fatal("queued cancellation was not one-shot")
	}
	a.FailRound(key, 1)
	if st := a.Stats()[0]; st.Window != 1 || st.Inflight != 1 || st.Queued != 0 {
		t.Fatalf("after failure: %+v", st)
	}
	a.Drop(t1)
	if st := a.Stats()[0]; st.Inflight != 0 {
		t.Fatalf("after drop: %+v", st)
	}
}

func TestStartAdmissionDecreaseNeverLeavesInflightAboveWindow(t *testing.T) {
	now := time.Unix(0, 0)
	a := NewStartAdmission(0, func() time.Time { return now })
	key := StartKey{Shard: 0, Tier: 2}
	a.lane(key).window = 4
	var tickets []*StartTicket
	for range 4 {
		ticket, err := a.Admit(key, "A", func(*StartTicket) {})
		if err != nil {
			t.Fatal(err)
		}
		tickets = append(tickets, ticket)
	}
	a.FailRound(key, 1)
	if st := a.Stats()[0]; st.Window != 4 || st.Inflight != 4 {
		t.Fatalf("failed round advertised less than existing inflight: %+v", st)
	}

	// A slow successful round can release the existing starts and halve.
	l := a.lane(key)
	l.srtt, l.baseline = 3*time.Millisecond, time.Millisecond
	now = now.Add(4 * time.Millisecond)
	a.ConfirmRound(key, 2, tickets, now)
	if st := a.Stats()[0]; st.Window != 2 || st.Inflight != 0 {
		t.Fatalf("slow successful round did not halve: %+v", st)
	}
}

func TestStartAdmissionEmitsBoundedMeasurements(t *testing.T) {
	now := time.Unix(0, 0)
	a := NewStartAdmission(1, func() time.Time { return now })
	var got []StartObservation
	a.SetObserver(func(o StartObservation) { got = append(got, o) })
	key := StartKey{Shard: 3, Tier: 2}
	first, err := a.Admit(key, "one", func(*StartTicket) {})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := a.Admit(key, "two", func(*StartTicket) {})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Millisecond)
	a.ObserveInbox(first, now)
	now = now.Add(3 * time.Millisecond)
	a.ConfirmRound(key, 1, []*StartTicket{first}, now)
	if queued.e.state != 1 {
		t.Fatal("queued start was not admitted")
	}
	want := map[StartObservationKind]bool{StartGauge: false, StartQueueWait: false, StartInboxWait: false, StartWALWait: false}
	for _, o := range got {
		if _, ok := want[o.Kind]; ok {
			want[o.Kind] = true
		}
		if o.Key != key {
			t.Fatalf("observation key = %+v", o.Key)
		}
	}
	for kind, seen := range want {
		if !seen {
			t.Fatalf("observation kind %d missing: %#v", kind, got)
		}
	}
}
