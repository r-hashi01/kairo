package sqltest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"kairo/engine"
	"kairo/store/sqlstore"
	"kairo/wal"
)

// Measure logs ack latency and engine throughput on db (ADR 0020 asks for
// numbers, not pass/fail: network databases depend on their setup). Runs
// only with KAIRO_COMPARE=1.
func Measure(t *testing.T, db *sql.DB, d sqlstore.Dialect) {
	if os.Getenv("KAIRO_COMPARE") == "" {
		t.Skip("set KAIRO_COMPARE=1")
	}
	o := sqlstore.Options{Namespace: namespace(t.Name() + time.Now().String())}

	// Ack latency through a group-committing Committer.
	sink, err := sqlstore.OpenSink(db, d, "measure", o)
	if err != nil {
		t.Fatal(err)
	}
	const n = 4000
	var mu sync.Mutex
	type pend struct {
		lsn uint64
		at  time.Time
	}
	var queue []pend
	var lats []time.Duration
	done := make(chan struct{})
	c := wal.NewCommitter(sink, func(lsn uint64, err error) {
		if err != nil {
			t.Error(err)
		}
		now := time.Now()
		mu.Lock()
		i := 0
		for ; i < len(queue) && queue[i].lsn <= lsn; i++ {
			lats = append(lats, now.Sub(queue[i].at))
		}
		queue = queue[i:]
		if len(lats) == n {
			close(done)
		}
		mu.Unlock()
	})
	payload := make([]byte, 200)
	for i := 1; i <= n; i++ {
		mu.Lock()
		queue = append(queue, pend{uint64(i), time.Now()})
		c.Submit(wal.Frame(c.Buffer(), payload), uint64(i))
		mu.Unlock()
		if i%50 == 0 {
			time.Sleep(200 * time.Microsecond)
		}
	}
	<-done
	c.Close()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	// Five-node runs at the file tier, 256 in flight, 4 shards.
	e := Engine(t, db, d, o, engine.Config{Shards: 4})
	ft := engine.TierFile
	const runs = 2000
	sem := make(chan struct{}, 256)
	var wg sync.WaitGroup
	c0, t0 := cpu(), time.Now()
	for i := 0; i < runs; i++ {
		sem <- struct{}{}
		id, err := e.Submit(engine.SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: fmt.Sprint(i % 8), Tier: &ft})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Wait(context.Background(), id)
			<-sem
		}()
	}
	wg.Wait()
	el, used := time.Since(t0), cpu()-c0
	e.Close()
	t.Logf("MEASURE %-8s ack p50=%v p99=%v  runs/s=%.0f  cpu/run=%.0fµs",
		d.Name, lats[n/2].Round(time.Microsecond), lats[n*99/100].Round(time.Microsecond),
		runs/el.Seconds(), float64(used.Microseconds())/runs)
}

func cpu() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
