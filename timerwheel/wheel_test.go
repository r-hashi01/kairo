package timerwheel

import (
	"math/rand"
	"testing"
)

func TestWheelFiresExactlyOnTime(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	start := int64(1_700_000_000_000)
	w := New[int](start)
	expiry := map[int]int64{}
	cancelled := map[int]bool{}
	handles := map[int]Handle{}
	for i := 0; i < 20000; i++ {
		var d int64
		switch i % 4 {
		case 0:
			d = rng.Int63n(100)
		case 1:
			d = rng.Int63n(10_000)
		case 2:
			d = rng.Int63n(5_000_000)
		default:
			d = rng.Int63n(3 * 24 * 3600 * 1000)
		}
		expiry[i] = start + d
		handles[i] = w.Schedule(start+d, i)
	}
	for i := 0; i < 20000; i += 7 {
		if !w.Cancel(handles[i]) {
			t.Fatalf("cancel %d failed", i)
		}
		cancelled[i] = true
	}
	now := start
	fired := map[int]bool{}
	var out []int
	for w.Len() > 0 {
		next, ok := w.NextDeadline()
		if !ok {
			t.Fatal("wheel non-empty but no deadline")
		}
		if next < now {
			t.Fatalf("deadline %d in the past (now %d)", next, now)
		}
		// Sometimes jump past the deadline, sometimes land exactly on it.
		if rng.Intn(3) == 0 {
			now = next + rng.Int63n(50)
		} else {
			now = next
		}
		out = w.Advance(now, out[:0])
		for _, v := range out {
			if cancelled[v] {
				t.Fatalf("cancelled timer %d fired", v)
			}
			if fired[v] {
				t.Fatalf("timer %d fired twice", v)
			}
			if expiry[v] > now {
				t.Fatalf("timer %d fired early: expiry %d now %d", v, expiry[v], now)
			}
			fired[v] = true
		}
	}
	for i := range expiry {
		if !cancelled[i] && !fired[i] {
			t.Fatalf("timer %d never fired", i)
		}
	}
	if w.Cancel(handles[1]) {
		t.Fatal("cancel of fired timer succeeded")
	}
}

func TestWheelNoDeadlineWhenEmpty(t *testing.T) {
	w := New[int](0)
	if _, ok := w.NextDeadline(); ok {
		t.Fatal("empty wheel reported a deadline")
	}
	h := w.Schedule(10, 1)
	w.Cancel(h)
	if _, ok := w.NextDeadline(); ok {
		t.Fatal("wheel reported a deadline after cancelling its only timer")
	}
}

func TestWheelLateAdvanceFiresEverything(t *testing.T) {
	w := New[int](0)
	for i := 0; i < 1000; i++ {
		w.Schedule(int64(i*37), i)
	}
	out := w.Advance(1_000_000, nil)
	if len(out) != 1000 || w.Len() != 0 {
		t.Fatalf("fired %d, remaining %d", len(out), w.Len())
	}
}

func BenchmarkScheduleCancel(b *testing.B) {
	w := New[int](0)
	for i := 0; i < b.N; i++ {
		h := w.Schedule(int64(i%100000)+1, i)
		w.Cancel(h)
	}
}
