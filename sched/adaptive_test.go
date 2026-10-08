package sched

import (
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/task"
)

// A destination that refuses tasks beyond capacity: the dispatcher does
// not limit it until the first refusal, then keeps what it hands out near
// the capacity (ADR 0039).
func TestConcurrencyFollowsTheDestination(t *testing.T) {
	const capacity, workers, total = 6, 64, 1500
	d := NewDispatcher(nil)
	defer d.Close()
	p := d.NewPoller([]string{"llm"}, workers)
	var outstanding, peak, refused atomic.Int64
	var wg sync.WaitGroup
	results := make(chan int64, total)
	for range workers {
		go func() {
			for dl := range p.C {
				n := outstanding.Add(1)
				limited := n > capacity
				if limited {
					refused.Add(1)
				}
				time.Sleep(time.Millisecond)
				outstanding.Add(-1)
				results <- n
				d.Finish(dl.Task, Outcome{RateLimited: limited})
				d.Poll(p, 1)
				wg.Done()
			}
		}()
	}
	wg.Add(total)
	for i := range total {
		d.Submit(&task.Task{RunID: "r", StepID: "s" + strconv.Itoa(i), Act: uint32(i + 1), Action: "llm", Destination: "llm", Tenant: "t"})
	}
	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(30 * time.Second):
		t.Fatalf("stuck: queued %d tracked %d", d.Queued(), d.Tracked())
	}
	close(results)
	// The last third: settled.
	var late []int64
	i := 0
	for n := range results {
		if i >= 2*total/3 {
			late = append(late, n)
		}
		if n > peak.Load() {
			peak.Store(n)
		}
		i++
	}
	over := 0
	for _, n := range late {
		if n > capacity {
			over++
		}
	}
	if refused.Load() == 0 {
		t.Fatal("the destination never refused: the test did not exercise the limit")
	}
	if frac := float64(over) / float64(len(late)); frac > 0.2 {
		t.Fatalf("%.0f%% of the settled tasks were over capacity (peak %d, refused %d)", 100*frac, peak.Load(), refused.Load())
	}
}

// Without refusals, a destination is not limited.
func TestConcurrencyUnlimitedWithoutRefusals(t *testing.T) {
	const workers = 40
	d := NewDispatcher(nil)
	defer d.Close()
	p := d.NewPoller([]string{"x"}, workers)
	var outstanding, peak atomic.Int64
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			dl := <-p.C
			n := outstanding.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			wg.Done()
			<-release
			d.Finish(dl.Task, Outcome{})
		}()
	}
	for i := range workers {
		d.Submit(&task.Task{RunID: "r", StepID: "s" + strconv.Itoa(i), Act: uint32(i + 1), Action: "x", Destination: "x", Tenant: "t"})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d of %d tasks started together", peak.Load(), workers)
	}
	close(release)
}

// observe, step by step on a given clock.
func TestObserveDecreasesOncePerRoundTripAndRecovers(t *testing.T) {
	t0 := time.Unix(0, 0)
	ds := &dest{limit: math.MaxInt32, inflight: 15}
	// Refusals before any success: the first halves what was outstanding,
	// the burst right after it (one signal) does not halve again.
	ds.observe(t0, 100*time.Millisecond, true)
	if ds.limit != 8 {
		t.Fatalf("limit %v after the first refusal of 16 outstanding, want 8", ds.limit)
	}
	ds.observe(t0.Add(10*time.Millisecond), 100*time.Millisecond, true)
	if ds.limit != 8 {
		t.Fatalf("limit %v after a refusal in the same round trip, want 8", ds.limit)
	}
	ds.observe(t0.Add(200*time.Millisecond), 100*time.Millisecond, true)
	if ds.limit != 4 {
		t.Fatalf("limit %v after a refusal a round trip later, want 4", ds.limit)
	}
	// Successes with latencies that vary by 10x: it grows again.
	now := t0.Add(time.Second)
	for i := 0; i < 2000; i++ {
		rtt := 200 * time.Millisecond
		if i%2 == 0 {
			rtt = 2 * time.Second
		}
		now = now.Add(rtt)
		ds.observe(now, rtt, false)
	}
	if ds.limit < 20 {
		t.Fatalf("limit %v after 2000 successes, want it grown back", ds.limit)
	}
}
