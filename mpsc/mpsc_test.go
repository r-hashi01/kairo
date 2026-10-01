package mpsc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// No lost wake-ups: with many producers and one consumer that waits on
// Ready, every pushed item is eventually drained.
func TestNoLostWakeups(t *testing.T) {
	q := New[int]()
	const producers, per = 8, 20000
	var got atomic.Int64
	done := make(chan struct{})
	go func() {
		var buf []int
		for got.Load() < producers*per {
			<-q.Ready()
			buf = q.Drain(buf)
			got.Add(int64(len(buf)))
		}
		close(done)
	}()
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				q.Push(i)
				if i%1000 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}()
	}
	wg.Wait()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer stuck: drained %d of %d, queue holds %d", got.Load(), producers*per, q.Len())
	}
}
