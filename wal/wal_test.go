package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestFrameScanTornTail(t *testing.T) {
	var b []byte
	for i := 0; i < 10; i++ {
		b = Frame(b, []byte(fmt.Sprintf("record-%d", i)))
	}
	full := len(b)
	b = Frame(b, []byte("torn"))
	b = b[:len(b)-2]
	var got []string
	n, err := Scan(b, func(r []byte) error { got = append(got, string(r)); return nil })
	if err != errTorn || n != full || len(got) != 10 {
		t.Fatalf("n=%d full=%d err=%v got=%d", n, full, err, len(got))
	}
	b[5] ^= 0xff // corrupt the first record
	got = nil
	Scan(b, func(r []byte) error { got = append(got, string(r)); return nil })
	if len(got) != 0 {
		t.Fatal("corrupt record accepted")
	}
}

// Invariant: writes are append-only. Reopening starts a new segment; bytes
// already written (including a torn tail) are never modified.
func TestFileSinkAppendOnly(t *testing.T) {
	dir := t.TempDir()
	s1, err := OpenFile(dir, "shard-000", false)
	if err != nil {
		t.Fatal(err)
	}
	s1.Append(Frame(nil, []byte("a")))
	s1.Append(Frame(nil, []byte("b")))
	s1.f.Write([]byte{0x05, 0x01}) // torn tail from a "crash"
	s1.Close()
	seg1, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	before, _ := os.ReadFile(seg1[0])

	s2, err := OpenFile(dir, "shard-000", false)
	if err != nil {
		t.Fatal(err)
	}
	s2.Append(Frame(nil, []byte("c")))
	var got []string
	s2.ReadAll(func(r []byte) error { got = append(got, string(r)); return nil })
	s2.Close()
	after, _ := os.ReadFile(seg1[0])
	if !bytes.Equal(before, after) {
		t.Fatal("existing segment was modified")
	}
	if fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("read %v", got)
	}
}

// Group commit: while one write is in progress, later submissions are
// merged into the next write, and acks report monotonically increasing LSNs.
func TestCommitterGroupsWrites(t *testing.T) {
	var writes int
	var mu sync.Mutex
	sink := &MemSink{Delay: func() {
		mu.Lock()
		writes++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}}
	acks := make(chan uint64, 1000)
	c := NewCommitter(sink, func(lsn uint64, err error) { acks <- lsn })
	for i := 1; i <= 200; i++ {
		c.Submit(Frame(c.Buffer(), []byte{byte(i)}), uint64(i))
		if i%20 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	var last uint64
	for last < 200 {
		l := <-acks
		if l <= last {
			t.Fatalf("ack %d after %d", l, last)
		}
		last = l
	}
	c.Close()
	if writes >= 200 {
		t.Fatalf("no grouping: %d writes for 200 submissions", writes)
	}
	n := 0
	sink.ReadAll(func([]byte) error { n++; return nil })
	if n != 200 {
		t.Fatalf("%d records", n)
	}
	t.Logf("200 submissions -> %d writes", writes)
}

// Durable-ack latency with real fsync, under concurrent submitters.
func TestFileCommitLatency(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	s, err := OpenFile(t.TempDir(), "lat", false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	type pend struct {
		lsn uint64
		at  time.Time
	}
	var mu sync.Mutex
	var queue []pend
	var lats []time.Duration
	done := make(chan struct{})
	const n = 2000
	c := NewCommitter(s, func(lsn uint64, err error) {
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
	payload := bytes.Repeat([]byte("x"), 200)
	for i := 1; i <= n; i++ {
		mu.Lock()
		queue = append(queue, pend{uint64(i), time.Now()})
		c.Submit(Frame(c.Buffer(), payload), uint64(i))
		mu.Unlock()
		if i%50 == 0 {
			time.Sleep(200 * time.Microsecond)
		}
	}
	<-done
	c.Close()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	t.Logf("fsync group commit ack latency: p50=%v p99=%v max=%v", lats[n/2], lats[n*99/100], lats[n-1])
}
