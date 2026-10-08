package engine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/r-hashi01/kairo/wal"
)

// Finished-run markers (ADR 0027). When a run of the file tier or above
// finishes, the shard appends a small marker (run id, finish time, status)
// to its done log and keeps it in an in-memory index for IdempotencyTTL.
// Submit, Get and Wait consult the index, so a RunID that finished before
// a restart is still recognized. The run's snapshot and blobs are deleted,
// and its records released for retirement, only once the marker is
// durable.

// markerVersion 2 adds the kept flag (ADR 0050); version 1 markers are
// read as not kept.
const markerVersion = 2

type marker struct {
	runID  string
	at     int64 // unix ms
	status string
	// kept: the run's output is in the blob store under keptKey(runID,
	// gen), written before this marker (ADR 0050).
	kept bool
	gen  uint64
}

// keptKey is where a finished run's kept output is (ADR 0050). Not under
// the run's blob group, which is deleted when it finishes (ADR 0024). gen
// is the log position of the event that finished that run: each run with
// the same id has its own key, so deleting an earlier run's output can
// never delete a later one's, whatever order the I/O workers take.
func keptKey(runID string, gen uint64) string { return fmt.Sprintf("kept/%s/%d", runID, gen) }

var errBadMarker = errors.New("engine: malformed done-log record")

func encodeMarker(b []byte, m marker) []byte {
	b = append(b, markerVersion)
	b = binary.AppendVarint(b, m.at)
	b = binary.AppendUvarint(b, uint64(len(m.runID)))
	b = append(b, m.runID...)
	b = binary.AppendUvarint(b, uint64(len(m.status)))
	b = append(b, m.status...)
	if !m.kept {
		return append(b, 0)
	}
	b = append(b, 1)
	return binary.AppendUvarint(b, m.gen)
}

func decodeMarker(rec []byte) (marker, error) {
	var m marker
	if len(rec) == 0 || (rec[0] != 1 && rec[0] != markerVersion) {
		return m, errBadMarker
	}
	version := rec[0]
	rec = rec[1:]
	at, k := binary.Varint(rec)
	if k <= 0 {
		return m, errBadMarker
	}
	m.at, rec = at, rec[k:]
	str := func() (string, bool) {
		n, k := binary.Uvarint(rec)
		if k <= 0 || uint64(len(rec)-k) < n {
			return "", false
		}
		s := string(rec[k : k+int(n)])
		rec = rec[k+int(n):]
		return s, true
	}
	var ok1, ok2 bool
	m.runID, ok1 = str()
	m.status, ok2 = str()
	if version >= 2 && ok1 && ok2 {
		if len(rec) == 0 || rec[0] > 1 {
			return m, errBadMarker
		}
		m.kept, rec = rec[0] == 1, rec[1:]
		if m.kept {
			gen, k := binary.Uvarint(rec)
			if k <= 0 {
				return m, errBadMarker
			}
			m.gen, rec = gen, rec[k:]
		}
	}
	if !ok1 || !ok2 || len(rec) != 0 {
		return m, errBadMarker
	}
	return m, nil
}

type doneEntry struct {
	at     int64
	lsn    uint64
	status string
	kept   bool   // the output is kept (ADR 0050)
	gen    uint64 // its keptKey generation
}

// doneIndex is written by the shard loop and read by Submit, Get and Wait.
type doneIndex struct {
	mu sync.RWMutex
	m  map[string]doneEntry
}

// get returns the marker of id if it finished at or after cut.
func (x *doneIndex) get(id string, cut int64) (doneEntry, bool) {
	x.mu.RLock()
	en, ok := x.m[id]
	x.mu.RUnlock()
	return en, ok && en.at >= cut
}

type doneRef struct {
	id  string
	at  int64
	lsn uint64
}

type doneWait struct {
	lsn   uint64
	after func()
}

// doneLog is a shard's log of markers. Only the shard loop touches it
// (recovery runs before the loop starts).
type doneLog struct {
	sink      wal.Sink
	committer *wal.Committer
	buf       []byte
	lsn       uint64 // last assigned
	durable   uint64
	failed    error
	waiting   []doneWait // finish jobs waiting for their marker, in LSN order
	order     []doneRef  // markers in the index, oldest first, from head
	head      int
	expired   int // markers dropped since the last Retire
	warned    bool
	idx       doneIndex
	// expiredKept: kept outputs recovery found no longer needed (expired,
	// or replaced by a later marker), deleted once the whole log is read
	// (ADR 0050).
	expiredKept []keptRef
	// deleting: done-log LSNs of markers whose kept output is being (or
	// failed to be) deleted. The log is not retired past them, so a
	// restart finds those markers and deletes again: an output is never
	// left behind with nothing that points at it (ADR 0050).
	deleting map[uint64]struct{}
}

// keptRef names a kept output, and the done-log LSN of its marker.
type keptRef struct {
	id       string
	gen, lsn uint64
}

// markerRetireEvery: the done log is retired once this many markers expired, so
// a database is not asked to delete on every finish.
var markerRetireEvery = 1024

// expireChunk bounds how many markers are dropped per hold of the index
// lock.
const expireChunk = 4096

func newDoneLog(s *shard, sink wal.Sink) *doneLog {
	d := &doneLog{sink: sink, idx: doneIndex{m: map[string]doneEntry{}}, deleting: map[uint64]struct{}{}}
	d.committer = wal.NewCommitter(sink, func(lsn uint64, err error) {
		s.inbox.Push(msg{kind: mDoneAck, lsn: lsn, err: err})
	})
	d.buf = d.committer.Buffer()
	return d
}

func (s *shard) markerCut() int64 { return s.now - s.e.cfg.IdempotencyTTL.Milliseconds() }

// marker returns the unexpired marker of id, if any. Safe from any
// goroutine.
func (e *Engine) marker(id string) (doneEntry, bool) {
	s := e.shardFor(id)
	if s.doneLog == nil {
		return doneEntry{}, false
	}
	return s.doneLog.idx.get(id, e.cfg.Now().UnixMilli()-e.cfg.IdempotencyTTL.Milliseconds())
}

// addMarker logs a marker for a finished run; after runs once it is
// durable. kept: its output was durably put under keptKey(id, gen) first.
func (s *shard) addMarker(id, status string, kept bool, gen uint64, after func()) {
	d := s.doneLog
	d.buf = wal.Frame(d.buf, encodeMarker(nil, marker{runID: id, at: s.now, status: status, kept: kept, gen: gen}))
	d.lsn++
	d.idx.mu.Lock()
	d.idx.m[id] = doneEntry{at: s.now, lsn: d.lsn, status: status, kept: kept, gen: gen}
	d.idx.mu.Unlock()
	d.order = append(d.order, doneRef{id: id, at: s.now, lsn: d.lsn})
	if after != nil {
		d.waiting = append(d.waiting, doneWait{lsn: d.lsn, after: after})
	}
	s.expireMarkers()
}

// forgetMarker drops id from the index: a new run with this id started,
// so Get and Wait must not report the old one.
func (s *shard) forgetMarker(id string) {
	d := s.doneLog
	if d == nil {
		return
	}
	d.idx.mu.Lock()
	en, ok := d.idx.m[id]
	delete(d.idx.m, id)
	d.idx.mu.Unlock()
	if ok && en.kept {
		// The earlier run's output, under its own generation: the new
		// run's will have another key.
		s.deleteKept([]keptRef{{id: id, gen: en.gen, lsn: en.lsn}})
	}
}

// deleteKept deletes kept outputs off the loop. Their markers are kept in
// the log until each is gone (mKeptDeleted), so a crash or a failure is
// retried at the next start (ADR 0050).
func (s *shard) deleteKept(refs []keptRef) {
	d := s.doneLog
	for _, r := range refs {
		d.deleting[r.lsn] = struct{}{}
	}
	s.e.doIO(func() {
		for _, r := range refs {
			if s.e.deleteKept(r.id, r.gen) == nil {
				s.inbox.Push(msg{kind: mKeptDeleted, lsn: r.lsn})
			}
		}
	})
}

func (s *shard) doneAck(lsn uint64, err error) {
	d := s.doneLog
	if err != nil {
		// Keep waiting: the runs keep their snapshots and records, so
		// nothing is forgotten, only not cleaned up.
		if d.failed == nil {
			s.failedLogs.Add(1)
		}
		d.failed = err
		log.Printf("kairo: shard %d: done log write failed: %v", s.id, err)
		return
	}
	d.durable = max(d.durable, lsn)
	i := 0
	for ; i < len(d.waiting) && d.waiting[i].lsn <= d.durable; i++ {
		d.waiting[i].after()
	}
	if i > 0 {
		n := copy(d.waiting, d.waiting[i:])
		clear(d.waiting[n:])
		d.waiting = d.waiting[:n]
	}
}

// expireMarkers drops markers older than IdempotencyTTL, and the oldest
// ones beyond the per-shard cap, from the index; the done log is retired
// behind them.
func (s *shard) expireMarkers() {
	d := s.doneLog
	cut := s.markerCut()
	limit := s.e.doneMax
	warn := false
	var kept []keptRef // outputs to delete with their markers (ADR 0050)
	for more := true; more; {
		// In chunks, so readers are not held off for a whole backlog (the
		// first finish after a long idle period may expire millions).
		d.idx.mu.Lock()
		more = false
		for n := 0; d.head < len(d.order); n++ {
			if n == expireChunk {
				more = true
				break
			}
			ref := d.order[d.head]
			over := len(d.order)-d.head > limit
			if ref.at >= cut && !over {
				break
			}
			if over && ref.at >= cut && !d.warned {
				d.warned, warn = true, true
			}
			if en, ok := d.idx.m[ref.id]; ok && en.lsn == ref.lsn {
				delete(d.idx.m, ref.id)
				if en.kept {
					kept = append(kept, keptRef{id: ref.id, gen: en.gen, lsn: ref.lsn})
				}
			}
			d.order[d.head] = doneRef{}
			d.head++
			d.expired++
		}
		d.idx.mu.Unlock()
	}
	if warn {
		log.Printf("kairo: shard %d: more than %d finished-run markers; dropping the oldest before IdempotencyTTL (raise IdempotencyMax)", s.id, limit)
	}
	if len(kept) > 0 {
		s.deleteKept(kept)
	}
	if d.head > 1024 && d.head*2 > len(d.order) {
		n := copy(d.order, d.order[d.head:])
		clear(d.order[n:])
		d.order, d.head = d.order[:n], 0
	}
	if d.expired >= markerRetireEvery && d.failed == nil {
		d.expired = 0
		d.committer.Retire(s.markerBound())
	}
}

// markerBound is the first done-log LSN still needed: the oldest marker
// in the index, or of a kept output still being deleted.
func (s *shard) markerBound() uint64 {
	d := s.doneLog
	b := d.lsn + 1
	if d.head < len(d.order) {
		b = d.order[d.head].lsn
	}
	for lsn := range d.deleting {
		b = min(b, lsn)
	}
	return b
}

// recoverMarkers loads the unexpired markers of the done log.
func (s *shard) recoverMarkers() error {
	d := s.doneLog
	cut := s.markerCut()
	err := d.sink.ReadAll(func(lsn uint64, rec []byte) error {
		m, err := decodeMarker(rec)
		if err != nil {
			return err
		}
		d.expired++
		if m.at < cut {
			if m.kept {
				// Expired while the engine was down, or its delete did
				// not happen: delete its output now (ADR 0050).
				d.expiredKept = append(d.expiredKept, keptRef{id: m.runID, gen: m.gen, lsn: lsn})
			}
			return nil
		}
		if old, ok := d.idx.m[m.runID]; ok && old.kept {
			// A later run with this id: the earlier output is not needed.
			d.expiredKept = append(d.expiredKept, keptRef{id: m.runID, gen: old.gen, lsn: old.lsn})
		}
		d.idx.m[m.runID] = doneEntry{at: m.at, lsn: lsn, status: m.status, kept: m.kept, gen: m.gen}
		d.order = append(d.order, doneRef{id: m.runID, at: m.at, lsn: lsn})
		return nil
	})
	if err != nil {
		return err
	}
	next := d.sink.Next()
	d.lsn, d.durable = next-1, next-1
	d.expired -= len(d.order)
	// Before the loop starts: deleted here, in order. One that fails keeps
	// its marker in the log for the next start.
	for _, r := range d.expiredKept {
		if s.e.deleteKept(r.id, r.gen) != nil {
			d.deleting[r.lsn] = struct{}{}
		}
	}
	d.expiredKept = nil
	return nil
}

// writeMarkers durably logs markers for runs recovery found finished but
// unmarked (a crash right after they finished). It runs before the loop
// starts, writing to the sink directly.
func (s *shard) writeMarkers(ms []marker) error {
	d := s.doneLog
	var b []byte
	for _, m := range ms {
		b = wal.Frame(b, encodeMarker(nil, m))
	}
	if err := d.sink.Append(b); err != nil {
		return err
	}
	for _, m := range ms {
		d.lsn++
		if old, ok := d.idx.m[m.runID]; ok && old.kept && old.gen != m.gen && s.e.deleteKept(m.runID, old.gen) != nil {
			// An earlier run's output, replaced (before the loop starts).
			d.deleting[old.lsn] = struct{}{}
		}
		d.idx.m[m.runID] = doneEntry{at: m.at, lsn: d.lsn, status: m.status, kept: m.kept, gen: m.gen}
		d.order = append(d.order, doneRef{id: m.runID, at: m.at, lsn: d.lsn})
	}
	d.durable = d.lsn
	return nil
}
