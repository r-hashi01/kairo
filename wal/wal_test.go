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
	s2.ReadAll(func(_ uint64, r []byte) error { got = append(got, string(r)); return nil })
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
	sink.ReadAll(func(uint64, []byte) error { n++; return nil })
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

func readLSNs(t *testing.T, s Sink) (lsns []uint64, recs []string) {
	t.Helper()
	if err := s.ReadAll(func(l uint64, r []byte) error {
		lsns = append(lsns, l)
		recs = append(recs, string(r))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return
}

// ADR 0016: LSN-named segments, rotation by size, LSN continuity across
// reopen and v0 segments, and Retire deleting whole segments only.
func TestFileSinkSegmentsAndRetire(t *testing.T) {
	dir := t.TempDir()
	// A v0 segment (sequence-named) with three records.
	var legacy []byte
	for _, r := range []string{"v0a", "v0b", "v0c"} {
		legacy = Frame(legacy, []byte(r))
	}
	os.WriteFile(filepath.Join(dir, "shard-000.00000001.wal"), legacy, 0o644)

	s, err := OpenFile(dir, "shard-000", true)
	if err != nil {
		t.Fatal(err)
	}
	s.SegmentSize = 64 // rotate after roughly every 4 records
	for i := 0; i < 20; i++ {
		if err := s.Append(Frame(nil, []byte(fmt.Sprintf("r%02d", i)))); err != nil {
			t.Fatal(err)
		}
	}
	lsns, recs := readLSNs(t, s)
	if len(lsns) != 23 || lsns[0] != 1 || recs[3] != "r00" || lsns[22] != 23 {
		t.Fatalf("lsns %v recs %v", lsns, recs)
	}
	for i := 1; i < len(lsns); i++ {
		if lsns[i] != lsns[i-1]+1 {
			t.Fatalf("LSN gap at %d: %v", i, lsns)
		}
	}
	if s.Segments() < 4 {
		t.Fatalf("no rotation: %d segments", s.Segments())
	}
	s.Close()

	// Reopen: numbering continues after the last record.
	s, err = OpenFile(dir, "shard-000", true)
	if err != nil {
		t.Fatal(err)
	}
	s.Append(Frame(nil, []byte("after")))
	lsns, recs = readLSNs(t, s)
	if lsns[len(lsns)-1] != 24 || recs[len(recs)-1] != "after" {
		t.Fatalf("after reopen: last lsn %d %q", lsns[len(lsns)-1], recs[len(recs)-1])
	}

	// Retire below 10: whole segments whose records are all < 10 go, the
	// segment holding LSN 10 stays; contents of the rest are untouched.
	segsBefore := s.Segments()
	paths, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	contents := map[string]string{}
	for _, p := range paths {
		b, _ := os.ReadFile(p)
		contents[p] = string(b)
	}
	if err := s.Retire(10); err != nil {
		t.Fatal(err)
	}
	lsns, _ = readLSNs(t, s)
	if lsns[0] > 10 || lsns[0] == 1 || s.Segments() >= segsBefore {
		t.Fatalf("retire(10): first lsn %d, segments %d -> %d", lsns[0], segsBefore, s.Segments())
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err == nil && string(b) != contents[p] {
			t.Fatalf("retire modified %s", p)
		}
	}
	// Retiring everything keeps the current segment.
	s.Retire(1 << 62)
	if s.Segments() != 1 {
		t.Fatalf("current segment must survive, have %d", s.Segments())
	}
	s.Close()
	s, err = OpenFile(dir, "shard-000", true)
	if err != nil {
		t.Fatal(err)
	}
	s.Append(Frame(nil, []byte("x")))
	lsns, _ = readLSNs(t, s)
	if lsns[len(lsns)-1] != 25 {
		t.Fatalf("numbering after full retire: %v", lsns)
	}
	s.Close()
}

func TestMemSinkRetire(t *testing.T) {
	m := &MemSink{}
	for i := 0; i < 10; i++ {
		m.Append(Frame(nil, []byte{byte(i)}))
	}
	m.Retire(6)
	lsns, _ := readLSNs(t, m)
	if len(lsns) != 5 || lsns[0] != 6 {
		t.Fatalf("%v", lsns)
	}
}
