// Package wal is the durability log: per-shard, append-only, group-committed.
//
// The runtime asks exactly one thing of the storage underneath: append these
// bytes and acknowledge once they are safe outside the current failure
// domain. That is the Sink interface. Everything else (framing, LSNs, group
// commit) lives here, independent of where the bytes end up.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"

	"kairo/mpsc"
)

// Sink is the pluggable destination of a shard's log.
type Sink interface {
	// Append durably appends batch. It returns once the bytes are
	// acknowledged from outside the failure domain (fsync for a local file,
	// quorum ack for a replicated log).
	Append(batch []byte) error
	// ReadAll calls fn with every complete record previously appended, in
	// order. A torn tail (partial final write) is silently ignored.
	ReadAll(fn func(rec []byte) error) error
	Close() error
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

// MemSink keeps the log in memory. It survives the loss of a run but not of
// the process; it backs the "shared memory" tier in tests and in embedded
// use where the host process is the failure domain.
type MemSink struct {
	mu   sync.Mutex
	data []byte
	// Delay, if set, is called before each Append returns (tests use it to
	// hold acknowledgements back).
	Delay func()
}

func (m *MemSink) Append(batch []byte) error {
	if m.Delay != nil {
		m.Delay()
	}
	m.mu.Lock()
	m.data = append(m.data, batch...)
	m.mu.Unlock()
	return nil
}

func (m *MemSink) ReadAll(fn func([]byte) error) error {
	m.mu.Lock()
	data := append([]byte(nil), m.data...)
	m.mu.Unlock()
	_, err := Scan(data, fn)
	if err == errTorn {
		err = nil
	}
	return err
}

func (m *MemSink) Close() error { return nil }

// Bytes returns a copy of everything appended so far.
func (m *MemSink) Bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.data...)
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
	data []byte
	lsn  uint64
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

func (c *Committer) Close() {
	close(c.close)
	<-c.done
}

func (c *Committer) loop() {
	defer close(c.done)
	var bufs []batch
	var joined []byte
	for {
		select {
		case <-c.q.Ready():
		case <-c.close:
			bufs = c.q.Drain(bufs)
			if len(bufs) > 0 {
				c.write(bufs, &joined)
			}
			return
		}
		bufs = c.q.Drain(bufs)
		if len(bufs) == 0 {
			continue
		}
		c.write(bufs, &joined)
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
