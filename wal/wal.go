// Package wal is the durability log: per-shard, append-only, group-committed.
//
// The runtime asks exactly one thing of the storage underneath: append these
// bytes and acknowledge once they are safe outside the current failure
// domain. That is the Sink interface. Everything else (framing, LSNs, group
// commit) lives here, independent of where the bytes end up.
//
// LSNs number the records of one log from 1. A sink knows the LSN of every
// record it holds (segments carry their first LSN, ADR 0016), so records
// that are no longer needed can be retired a whole segment at a time
// without renumbering the rest.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log"
	"sync"

	"kairo/mpsc"
)

// Sink is the pluggable destination of a shard's log.
type Sink interface {
	// Append durably appends batch (one or more framed records). It
	// returns once the bytes are acknowledged from outside the failure
	// domain (fsync for a local file, quorum ack for a replicated log).
	Append(batch []byte) error
	// ReadAll calls fn with every complete record still held, in order,
	// with its LSN. A torn tail (partial final write) is ignored.
	ReadAll(fn func(lsn uint64, rec []byte) error) error
	// Next is the LSN the next appended record will get. It survives
	// retirement: a log whose records were all retired keeps counting.
	Next() uint64
	Close() error
}

// Retirer is implemented by sinks that can drop records no longer needed.
// Retire may discard records with LSN < lsn, and only whole segments: the
// content of a segment is never rewritten or truncated.
type Retirer interface {
	Retire(lsn uint64) error
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Frame appends one framed record: uvarint length, crc32c, payload.
func Frame(b, payload []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(payload)))
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(payload, castagnoli))
	return append(b, payload...)
}

var errTorn = errors.New("wal: torn record")

// Scan calls fn for each complete record in data and returns the number of
// bytes consumed. It stops at the first incomplete or corrupt record.
func Scan(data []byte, fn func(rec []byte) error) (int, error) {
	off := 0
	for off < len(data) {
		n, k := binary.Uvarint(data[off:])
		if k <= 0 || uint64(len(data)-off-k) < 4+n {
			return off, errTorn
		}
		sum := binary.LittleEndian.Uint32(data[off+k:])
		rec := data[off+k+4 : off+k+4+int(n)]
		if crc32.Checksum(rec, castagnoli) != sum {
			return off, errTorn
		}
		if err := fn(rec); err != nil {
			return off, err
		}
		off += k + 4 + int(n)
	}
	return off, nil
}

// count returns the number of complete, valid records in data (checks
// CRCs; for reading).
func count(data []byte) uint64 {
	var n uint64
	Scan(data, func([]byte) error { n++; return nil })
	return n
}

// countFrames counts the records of a batch built with Frame, reading only
// the length prefixes (for the append path).
func countFrames(data []byte) uint64 {
	var n uint64
	for off := 0; off < len(data); n++ {
		l, k := binary.Uvarint(data[off:])
		if k <= 0 {
			break
		}
		off += k + 4 + int(l)
	}
	return n
}

// MemSink keeps the log in memory. It survives the loss of a run but not of
// the process; it backs the "memory" tier and tests.
type MemSink struct {
	mu     sync.Mutex
	chunks []memChunk
	next   uint64 // LSN of the next record
	// Delay, if set, is called before each Append returns (tests use it to
	// hold acknowledgements back).
	Delay func()
}

type memChunk struct {
	first, n uint64
	data     []byte
}

func (m *MemSink) Append(batch []byte) error {
	if m.Delay != nil {
		m.Delay()
	}
	n := countFrames(batch)
	m.mu.Lock()
	if m.next == 0 {
		m.next = 1
	}
	m.chunks = append(m.chunks, memChunk{first: m.next, n: n, data: append([]byte(nil), batch...)})
	m.next += n
	m.mu.Unlock()
	return nil
}

func (m *MemSink) ReadAll(fn func(uint64, []byte) error) error {
	m.mu.Lock()
	chunks := append([]memChunk(nil), m.chunks...)
	m.mu.Unlock()
	for _, c := range chunks {
		lsn := c.first
		if _, err := Scan(c.data, func(rec []byte) error {
			err := fn(lsn, rec)
			lsn++
			return err
		}); err != nil && err != errTorn {
			return err
		}
	}
	return nil
}

// Retire drops whole appended batches whose records are all below lsn.
func (m *MemSink) Retire(lsn uint64) error {
	m.mu.Lock()
	i := 0
	for i < len(m.chunks) && m.chunks[i].first+m.chunks[i].n <= lsn {
		i++
	}
	m.chunks = append([]memChunk(nil), m.chunks[i:]...)
	m.mu.Unlock()
	return nil
}

func (m *MemSink) Next() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return max(m.next, 1)
}

func (m *MemSink) Close() error { return nil }

// Bytes returns a copy of everything still held.
func (m *MemSink) Bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b []byte
	for _, c := range m.chunks {
		b = append(b, c.data...)
	}
	return b
}

// Committer performs group commit for one shard. The shard hands over a
// buffer of framed records together with the LSN of the last record in it;
// the committer's goroutine writes every buffer that queued up while the
// previous write was in progress with a single Append, then reports the
// highest durable LSN. The shard never waits for it.
type Committer struct {
	sink  Sink
	q     *mpsc.Queue[batch]
	ack   func(lsn uint64, err error)
	done  chan struct{}
	pool  sync.Pool
	close chan struct{}
}

type batch struct {
	data   []byte
	lsn    uint64
	retire uint64 // if non-zero: a retire request, not data
}

// NewCommitter starts a committer. ack is called from the committer's
// goroutine after each durable write.
func NewCommitter(sink Sink, ack func(lsn uint64, err error)) *Committer {
	c := &Committer{sink: sink, q: mpsc.New[batch](), ack: ack, done: make(chan struct{}), close: make(chan struct{})}
	go c.loop()
	return c
}

// Buffer returns an empty buffer to accumulate records in.
func (c *Committer) Buffer() []byte {
	if b, ok := c.pool.Get().(*[]byte); ok {
		return (*b)[:0]
	}
	return make([]byte, 0, 64<<10)
}

// Submit queues data (records up to lsn) for commit. Ownership of data
// passes to the committer.
func (c *Committer) Submit(data []byte, lsn uint64) {
	c.q.Push(batch{data: data, lsn: lsn})
}

// Retire asks the sink, if it is a Retirer, to drop records below lsn. It
// runs on the committer's goroutine after the writes queued before it.
func (c *Committer) Retire(lsn uint64) {
	c.q.Push(batch{retire: lsn})
}

func (c *Committer) Close() {
	close(c.close)
	<-c.done
}

func (c *Committer) loop() {
	defer close(c.done)
	var bufs []batch
	var joined []byte
	for {
		stopping := false
		select {
		case <-c.q.Ready():
		case <-c.close:
			stopping = true
		}
		bufs = c.q.Drain(bufs)
		if len(bufs) > 0 {
			c.process(bufs, &joined)
		}
		if stopping {
			return
		}
	}
}

func (c *Committer) process(bufs []batch, joined *[]byte) {
	var data []batch
	var retire uint64
	for _, b := range bufs {
		if b.retire != 0 {
			retire = max(retire, b.retire)
			continue
		}
		data = append(data, b)
	}
	if len(data) > 0 {
		c.write(data, joined)
	}
	if r, ok := c.sink.(Retirer); ok && retire != 0 {
		if err := r.Retire(retire); err != nil {
			log.Printf("kairo: wal retire below %d: %v", retire, err)
		}
	}
}

func (c *Committer) write(bufs []batch, joined *[]byte) {
	var data []byte
	if len(bufs) == 1 {
		data = bufs[0].data
	} else {
		*joined = (*joined)[:0]
		for _, b := range bufs {
			*joined = append(*joined, b.data...)
		}
		data = *joined
	}
	err := c.sink.Append(data)
	lsn := bufs[len(bufs)-1].lsn
	for _, b := range bufs {
		d := b.data
		if cap(d) <= 1<<20 {
			c.pool.Put(&d)
		}
	}
	c.ack(lsn, err)
}
