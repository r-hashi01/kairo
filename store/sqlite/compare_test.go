package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"kairo/engine"
	"kairo/wal"
)

// TestCompareBackends measures the file and SQLite backends side by side
// for ADR 0018. It is slow and machine dependent, so it runs only with
// KAIRO_COMPARE=1:
//
//	KAIRO_COMPARE=1 go test -run TestCompareBackends -v
func TestCompareBackends(t *testing.T) {
	if os.Getenv("KAIRO_COMPARE") == "" {
		t.Skip("set KAIRO_COMPARE=1")
	}
	type result struct {
		name                  string
		p50, p99              time.Duration
		runsPerSec, cpuPerRun float64
		bytes                 int64
	}
	var res []result
	for _, backend := range []string{"file", "sqlite"} {
		r := result{name: backend}
		r.p50, r.p99 = ackLatency(t, backend)
		r.runsPerSec, r.cpuPerRun, r.bytes = throughput(t, backend)
		res = append(res, r)
	}
	t.Logf("%-7s %10s %10s %12s %12s %12s", "backend", "ack p50", "ack p99", "runs/s", "cpu µs/run", "log bytes")
	for _, r := range res {
		t.Logf("%-7s %10v %10v %12.0f %12.1f %12d", r.name, r.p50.Round(time.Microsecond), r.p99.Round(time.Microsecond), r.runsPerSec, r.cpuPerRun, r.bytes)
	}
	f, s := res[0], res[1]
	t.Logf("sqlite/file: ack p99 x%.2f (criterion <= 1.5), throughput x%.2f (criterion >= 0.7)",
		float64(s.p99)/float64(f.p99), s.runsPerSec/f.runsPerSec)
}

func openSink(t *testing.T, backend, dir string) wal.Sink {
	switch backend {
	case "file":
		s, err := wal.OpenFile(dir, "lat", false)
		if err != nil {
			t.Fatal(err)
		}
		return s
	default:
		s, err := OpenSink(filepath.Join(dir, "lat.db"))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}

// ackLatency: 4000 records submitted in bursts through a group-committing
// Committer with real syncs; latency from submit to durable ack.
func ackLatency(t *testing.T, backend string) (p50, p99 time.Duration) {
	sink := openSink(t, backend, t.TempDir())
	defer sink.Close()
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
	return lats[n/2], lats[n*99/100]
}

func cpu() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// throughput: five-node runs at the file tier with real syncs, 256 in
// flight, compaction on; then the size of the log left on disk.
func throughput(t *testing.T, backend string) (runsPerSec, cpuPerRun float64, logBytes int64) {
	dir := t.TempDir()
	var x *env
	if backend == "sqlite" {
		x = start(t, dir, engine.Config{Shards: 4})
	} else {
		x = startWith(t, engine.Config{Shards: 4, DataDir: dir})
	}
	ft := engine.TierFile
	const runs = 4000
	sem := make(chan struct{}, 256)
	var wg sync.WaitGroup
	c0, t0 := cpu(), time.Now()
	for i := 0; i < runs; i++ {
		sem <- struct{}{}
		id, err := x.e.Submit(engine.SubmitRequest{Plan: "five", Input: json.RawMessage(`{"q":"x"}`), Tenant: fmt.Sprint(i % 8), Tier: &ft})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			x.e.Wait(context.Background(), id)
			<-sem
		}()
	}
	wg.Wait()
	el, used := time.Since(t0), cpu()-c0
	x.close()
	filepath.Walk(filepath.Join(dir, "wal"), func(_ string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			logBytes += fi.Size()
		}
		return nil
	})
	return runs / el.Seconds(), float64(used.Microseconds()) / runs, logBytes
}
