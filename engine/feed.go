package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"sync"

	"kairo/core"
	"kairo/live"
	"kairo/mpsc"
)

// The execution event feed (ADR 0034). Shards produce traces as they apply
// events and hand them over through a queue; the feed keeps them until
// every subscription has acknowledged them. While a trace of the file tier
// or above is unacknowledged, no run state past it is snapshotted and no
// log record from it on is retired, so a restart can always produce the
// traces after the acknowledged position again by replaying the log.
//
// Traces of the none and memory tiers and live chunks carry no log
// position: they reach the subscribers that are connected, and are not
// produced again.

// Cursor is a position in the feed: the shard, the tier of its log, the
// LSN of the event that produced the entry and the entry's number among
// that event's traces. LSN 0: not durable, never produced again.
type Cursor struct {
	Shard int    `json:"shard"`
	Tier  Tier   `json:"tier"`
	LSN   uint64 `json:"lsn"`
	Seq   uint32 `json:"seq"`
}

func (c Cursor) after(d Cursor) bool {
	return c.LSN > d.LSN || c.LSN == d.LSN && c.Seq > d.Seq
}

// FeedEntry is a trace of a run, or a live chunk of a step's output
// (Trace nil). A step's chunks come before its TrNodeEnd.
type FeedEntry struct {
	Cursor Cursor
	RunID  string
	Trace  *core.Trace
	StepID string // chunk
	Chunk  []byte
}

var (
	// ErrLagged: the subscription fell more than Config.FeedLimit entries
	// behind and was cut; Subscribe again to start over from the present.
	ErrLagged = errors.New("engine: feed subscription lagged behind and was cut")
	// ErrUnknownFeed: the name is not in Config.Feeds.
	ErrUnknownFeed = errors.New("engine: unknown feed subscription")
	// ErrFeedClosed: the subscription was replaced by a newer Subscribe,
	// or the engine stopped.
	ErrFeedClosed = errors.New("engine: feed closed")
)

type feedSub struct {
	name   string
	acks   map[[2]int]Cursor // (shard, tier) -> acknowledged up to
	read   []int             // per shard: absolute index of the next entry to deliver
	since  []int             // per shard: entries without a position before this were before the connection
	next   int               // shard Next starts with, for fairness
	lagged bool
	cur    *Feed
	ackMu  sync.Mutex // orders Acks and their persistence
}

type feedBuf struct {
	base    int // absolute index of entries[0]
	entries []FeedEntry
}

type feedMsg struct {
	shard   int
	entries []FeedEntry
}

type feedHub struct {
	e       *Engine
	limit   int
	q       *mpsc.Queue[feedMsg]
	mu      sync.Mutex
	subs    map[string]*feedSub
	bufs    []feedBuf
	changed chan struct{} // closed when entries arrive or subscriptions change
	bounds  [][tierCount]uint64
	hold    int  // Config.FeedHold, resolved
	held    bool // backpressure wanted (ADR 0039)
	applyMu sync.Mutex
	applied bool // backpressure applied to the admission
	durable int  // buffered entries with a log position (they need acknowledgements)
	stop    chan struct{}
	done    chan struct{}
}

func newFeedHub(e *Engine) *feedHub {
	h := &feedHub{e: e, limit: e.cfg.FeedLimit, q: mpsc.New[feedMsg](), subs: map[string]*feedSub{},
		bufs: make([]feedBuf, e.cfg.Shards), changed: make(chan struct{}), bounds: make([][tierCount]uint64, e.cfg.Shards),
		stop: make(chan struct{}), done: make(chan struct{})}
	h.hold = e.cfg.FeedHold
	if h.hold == 0 {
		h.hold = h.limit / 2
	}
	for _, name := range e.cfg.Feeds {
		h.subs[name] = &feedSub{name: name, acks: map[[2]int]Cursor{}, read: make([]int, e.cfg.Shards), since: make([]int, e.cfg.Shards)}
	}
	for i := range h.bounds {
		for t := range h.bounds[i] {
			h.bounds[i][t] = math.MaxUint64
		}
	}
	return h
}

func feedKey(name string) string { return "feed/" + name }

// load reads the persisted acknowledgements. A subscription seen for the
// first time has none: it starts at the present (setStart after recovery).
func (h *feedHub) load() error {
	for _, s := range h.subs {
		data, err := h.e.snaps.Get(feedKey(s.name))
		if err != nil {
			continue // not acknowledged yet (blob.ErrNotFound) or unreadable
		}
		var cs []Cursor
		if err := json.Unmarshal(data, &cs); err != nil {
			return fmt.Errorf("feed %s: %w", s.name, err)
		}
		for _, c := range cs {
			s.acks[[2]int{c.Shard, int(c.Tier)}] = c
		}
	}
	h.computeBounds(false)
	return nil
}

// regenerateFrom is the first LSN of shard's tier t whose traces some
// subscription has not acknowledged: recovery produces the traces of
// events from it on again (and Next skips what is acknowledged).
func (h *feedHub) regenerateFrom(shard int, t Tier) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	from := uint64(math.MaxUint64)
	for _, s := range h.subs {
		if a, ok := s.acks[[2]int{shard, int(t)}]; ok {
			if a.Seq == math.MaxUint32 {
				from = min(from, a.LSN+1)
			} else {
				from = min(from, a.LSN)
			}
		}
	}
	return from
}

// setStart makes subscriptions without acknowledgements start at the
// present: everything in the logs now counts as acknowledged.
func (h *feedHub) setStart(shard int, t Tier, lsn uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.subs {
		k := [2]int{shard, int(t)}
		if _, ok := s.acks[k]; !ok {
			s.acks[k] = Cursor{Shard: shard, Tier: t, LSN: lsn, Seq: math.MaxUint32}
		}
	}
}

func (h *feedHub) run() {
	defer close(h.done)
	var buf []feedMsg
	for {
		select {
		case <-h.q.Ready():
		case <-h.stop:
			return
		}
		buf = h.q.Drain(buf)
		h.mu.Lock()
		for _, m := range buf {
			b := &h.bufs[m.shard]
			b.entries = append(b.entries, m.entries...)
			for _, en := range m.entries {
				if en.Cursor.LSN > 0 {
					h.durable++
				}
			}
		}
		clear(buf)
		h.checkLag()
		h.notify()
		h.mu.Unlock()
		h.reconcile()
	}
}

// pressure updates whether the subscribers fall behind (ADR 0039): more
// than Config.FeedHold entries unacknowledged (entries without a log
// position, live chunks, need none and do not count) hold back new runs
// until at most half of it are. Runs in progress go on: what lets their
// entries be acknowledged is that they proceed. The database behind the
// subscribers is then what bounds the throughput. Called with h.mu held;
// reconcile applies it.
func (h *feedHub) pressure() {
	if h.hold < 0 {
		return
	}
	switch {
	case !h.held && h.durable > h.hold:
		h.held = true
	case h.held && h.durable <= h.hold/2:
		h.held = false
	}
}

// reconcile brings the admission in line with the latest pressure. It is
// called, without h.mu, after anything that adds or trims entries; the
// last call applies the latest state whatever the order of the callers.
func (h *feedHub) reconcile() {
	h.applyMu.Lock()
	defer h.applyMu.Unlock()
	h.mu.Lock()
	h.pressure()
	want := h.held
	h.mu.Unlock()
	if want == h.applied {
		return
	}
	h.applied = want
	if want {
		log.Printf("kairo: feed subscribers are behind: holding back new runs")
	} else {
		log.Printf("kairo: feed subscribers caught up: admitting runs")
	}
	h.e.adm.Hold(want)
}

func (h *feedHub) notify() {
	close(h.changed)
	h.changed = make(chan struct{})
}

// checkLag cuts subscriptions holding more than limit entries.
func (h *feedHub) checkLag() {
	total := 0
	for i := range h.bufs {
		total += len(h.bufs[i].entries)
	}
	if total <= h.limit {
		return // nobody can hold more than there is
	}
	for _, s := range h.subs {
		if s.lagged {
			continue
		}
		if h.unacked(s) > h.limit {
			s.lagged = true
			log.Printf("kairo: feed %s: more than %d unacknowledged entries; cut (Subscribe again)", s.name, h.limit)
			h.computeBounds(true)
		}
	}
}

// unacked counts the entries s still holds.
func (h *feedHub) unacked(s *feedSub) int {
	n := 0
	for i := range h.bufs {
		b := &h.bufs[i]
		for k, en := range b.entries {
			if !h.acked(s, en, b.base+k) {
				n++
			}
		}
	}
	return n
}

func (h *feedHub) acked(s *feedSub, en FeedEntry, abs int) bool {
	if en.Cursor.LSN == 0 {
		// Delivered is enough; a subscriber not connected never gets it.
		return s.cur == nil || abs < s.read[en.Cursor.Shard] || abs < s.since[en.Cursor.Shard]
	}
	a, ok := s.acks[[2]int{en.Cursor.Shard, int(en.Cursor.Tier)}]
	return ok && !en.Cursor.after(a)
}

// computeBounds recomputes, per shard and tier, the LSN up to which state
// may be snapshotted and records retired, trims what every subscription
// has acknowledged, and (push) tells the shards.
func (h *feedHub) computeBounds(push bool) {
	for i := range h.bounds {
		for t := TierFile; t < tierCount; t++ {
			b := uint64(math.MaxUint64)
			for _, s := range h.subs {
				if s.lagged {
					continue
				}
				if a, ok := s.acks[[2]int{i, int(t)}]; ok {
					if a.Seq == math.MaxUint32 {
						b = min(b, a.LSN) // the whole event is acknowledged
					} else {
						b = min(b, max(a.LSN, 1)-1)
					}
				}
			}
			if b != h.bounds[i][t] {
				h.bounds[i][t] = b
				if push {
					h.e.shards[i].inbox.Push(msg{kind: mFeedBound, tier: t, lsn: b})
				}
			}
		}
		// Trim the prefix every live subscription is done with.
		buf := &h.bufs[i]
		n := 0
		for n < len(buf.entries) {
			all := true
			for _, s := range h.subs {
				if !s.lagged && !h.acked(s, buf.entries[n], buf.base+n) {
					all = false
					break
				}
			}
			if !all {
				break
			}
			n++
		}
		if n > 0 {
			for _, en := range buf.entries[:n] {
				if en.Cursor.LSN > 0 {
					h.durable--
				}
			}
			clear(buf.entries[:n])
			buf.entries = buf.entries[n:]
			buf.base += n
			// Give the array back once most of it is behind us.
			if cap(buf.entries) > 1024 && len(buf.entries) < cap(buf.entries)/4 {
				buf.entries = slices.Clone(buf.entries)
			}
		}
	}
}

// Feed is a subscription's consumer handle.
type Feed struct {
	h   *feedHub
	sub *feedSub
}

// Subscribe connects to the subscription name (one of Config.Feeds). It
// delivers what the subscription has not acknowledged and then new
// entries; entries without a log position are only delivered while
// connected. A previous Feed of the same name is closed.
func (e *Engine) Subscribe(name string) (*Feed, error) {
	h := e.feed
	if h == nil {
		return nil, ErrUnknownFeed
	}
	h.mu.Lock()
	s := h.subs[name]
	if s == nil {
		h.mu.Unlock()
		return nil, ErrUnknownFeed
	}
	lagged := s.lagged
	h.mu.Unlock()
	if lagged {
		// Start over from the present, as each shard sees it: what was cut
		// is lost to it. The shards also stop snapshotting past it.
		starts := make([][tierCount]uint64, len(e.shards))
		for i, sh := range e.shards {
			reply := make(chan [tierCount]uint64, 1)
			sh.inbox.Push(msg{kind: mFeedStart, feedReply: reply})
			select {
			case starts[i] = <-reply:
			case <-e.stopped:
				return nil, ErrFeedClosed
			}
		}
		s.ackMu.Lock()
		cs := make([]Cursor, 0, len(starts)*2)
		for i := range starts {
			for t := TierFile; t < tierCount; t++ {
				cs = append(cs, Cursor{Shard: i, Tier: t, LSN: starts[i][t], Seq: math.MaxUint32})
			}
		}
		err := h.persist(s.name, cs)
		if err == nil {
			h.mu.Lock()
			s.lagged = false
			for _, c := range cs {
				s.acks[[2]int{c.Shard, int(c.Tier)}] = c
			}
			h.mu.Unlock()
		}
		s.ackMu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	h.mu.Lock()
	for i := range h.bufs {
		s.read[i] = h.bufs[i].base // redeliver what is not acknowledged
		s.since[i] = h.bufs[i].base + len(h.bufs[i].entries)
	}
	f := &Feed{h: h, sub: s}
	s.cur = f
	h.computeBounds(true)
	h.notify()
	h.mu.Unlock()
	h.reconcile()
	return f, nil
}

// Next blocks until entries are available and returns them,
// in order per shard (at most limit; 0 means 1024).
func (f *Feed) Next(ctx context.Context, limit int) ([]FeedEntry, error) {
	h := f.h
	if limit <= 0 {
		limit = 1024
	}
	for {
		h.mu.Lock()
		if f.sub.cur != f {
			h.mu.Unlock()
			return nil, ErrFeedClosed
		}
		if f.sub.lagged {
			h.mu.Unlock()
			return nil, ErrLagged
		}
		var out []FeedEntry
		n := len(h.bufs)
		for k := 0; k < n; k++ {
			i := (f.sub.next + k) % n
			b := &h.bufs[i]
			// Entries trimmed meanwhile were acknowledged.
			f.sub.read[i] = max(f.sub.read[i], b.base)
			for f.sub.read[i] < b.base+len(b.entries) && len(out) < limit {
				abs := f.sub.read[i]
				en := b.entries[abs-b.base]
				f.sub.read[i]++
				if en.Cursor.LSN > 0 && h.acked(f.sub, en, abs) {
					continue // produced again after a restart, already acknowledged
				}
				if en.Cursor.LSN == 0 && abs < f.sub.since[i] {
					continue // from before this connection
				}
				out = append(out, en)
			}
		}
		f.sub.next = (f.sub.next + 1) % n
		ch := h.changed
		if len(out) > 0 {
			h.computeBounds(true) // delivered live entries can go
		}
		h.mu.Unlock()
		if len(out) > 0 {
			h.reconcile()
			return out, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-h.stop:
			return nil, ErrFeedClosed
		}
	}
}

// Ack acknowledges every entry of c's shard and tier up to c. It is
// persisted before it lets the engine snapshot or retire past it.
func (f *Feed) Ack(c Cursor) error {
	h := f.h
	if c.LSN == 0 {
		return nil // entries without a position need no acknowledgement
	}
	if c.Shard < 0 || c.Shard >= len(h.bufs) || c.Tier < TierFile || c.Tier >= tierCount {
		return fmt.Errorf("engine: cursor %+v is not of this engine", c)
	}
	// Persisted first, applied after: the engine never snapshots or
	// retires past an acknowledgement that a crash could lose.
	f.sub.ackMu.Lock()
	defer f.sub.ackMu.Unlock()
	h.mu.Lock()
	k := [2]int{c.Shard, int(c.Tier)}
	if a, ok := f.sub.acks[k]; ok && !c.after(a) {
		h.mu.Unlock()
		return nil
	}
	// The traces of one event arrive together: if c is the last one of its
	// event, the event is acknowledged as a whole.
	last := true
	for _, en := range h.bufs[c.Shard].entries {
		if en.Cursor.Tier == c.Tier && en.Cursor.LSN == c.LSN && en.Cursor.Seq > c.Seq {
			last = false
			break
		}
	}
	if last {
		c.Seq = math.MaxUint32
	}
	cs := make([]Cursor, 0, len(f.sub.acks)+1)
	for kk, a := range f.sub.acks {
		if kk != k {
			cs = append(cs, a)
		}
	}
	cs = append(cs, c)
	h.mu.Unlock()
	if err := h.persist(f.sub.name, cs); err != nil {
		return err
	}
	h.mu.Lock()
	f.sub.acks[k] = c
	h.computeBounds(true)
	h.mu.Unlock()
	h.reconcile()
	return nil
}

func (h *feedHub) persist(name string, cs []Cursor) error {
	slices.SortFunc(cs, func(a, b Cursor) int {
		if a.Shard != b.Shard {
			return a.Shard - b.Shard
		}
		return int(a.Tier) - int(b.Tier)
	})
	data, _ := json.Marshal(cs)
	return h.e.snaps.Put(feedKey(name), data)
}

// Unsubscribe removes subscription name until the next start: it stops
// holding the engine back and its acknowledgements are deleted, so if it
// is still in Config.Feeds at the next start, it starts from the present.
func (e *Engine) Unsubscribe(name string) error {
	h := e.feed
	if h == nil {
		return ErrUnknownFeed
	}
	h.mu.Lock()
	s := h.subs[name]
	if s == nil {
		h.mu.Unlock()
		return ErrUnknownFeed
	}
	delete(h.subs, name)
	if s.cur != nil {
		s.cur = nil
	}
	h.computeBounds(true)
	h.notify()
	h.mu.Unlock()
	h.reconcile()
	return e.snaps.Delete(feedKey(name))
}

// Close disconnects; unacknowledged entries stay for the next Subscribe.
func (f *Feed) Close() {
	f.h.mu.Lock()
	if f.sub.cur == f {
		f.sub.cur = nil
	}
	f.h.notify()
	f.h.mu.Unlock()
}

// backlog is the largest number of unacknowledged entries of a live
// subscription.
func (h *feedHub) backlog() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := 0
	for _, s := range h.subs {
		if s.lagged {
			continue
		}
		m = max(m, h.unacked(s))
	}
	return m
}

// publishChunk sends a step's live output to watchers and subscriptions.
func (e *Engine) publishChunk(runID, stepID string, data []byte) {
	e.live.Publish(live.Chunk{RunID: runID, StepID: stepID, Data: data})
	if e.feed != nil {
		e.feed.q.Push(feedMsg{shard: e.shardFor(runID).id, entries: []FeedEntry{{
			Cursor: Cursor{Shard: e.shardFor(runID).id}, RunID: runID, StepID: stepID, Chunk: data}}})
	}
}

// PublishChunk is for executors outside the engine (the worker protocol):
// a step's live output, to watchers and feed subscriptions.
func (e *Engine) PublishChunk(runID, stepID string, data []byte) { e.publishChunk(runID, stepID, data) }

// --- shard side (runs on the shard loop) -----------------------------------

// feedTraces hands the traces of an event of r logged at lsn to the feed.
func (s *shard) feedTraces(r *run, lsn uint64, traces []core.Trace) {
	if len(traces) == 0 {
		return
	}
	c := Cursor{Shard: s.id, Tier: r.tier}
	if r.tier >= TierFile {
		c.LSN = lsn
	}
	out := make([]FeedEntry, len(traces))
	for i := range traces {
		c.Seq = uint32(i)
		t := traces[i]
		out[i] = FeedEntry{Cursor: c, RunID: r.id, Trace: &t}
	}
	s.e.feed.q.Push(feedMsg{shard: s.id, entries: out})
}

// feedOK reports whether r's state may be snapshotted (or its records
// released): the feed has its traces acknowledged.
func (s *shard) feedOK(r *run) bool {
	if s.e.feed == nil || r.tier < TierFile {
		return true
	}
	// Events that produced no traces lose nothing when snapshotted over.
	b := s.feedBound[r.tier]
	return b == math.MaxUint64 || r.feedLSN <= b
}

// waitFeed runs f once the feed's acknowledgements cover r.
func (s *shard) waitFeed(r *run, f func()) {
	if r.afterFeed == nil {
		s.feedWait = append(s.feedWait, r)
	}
	r.afterFeed = f
}

func (s *shard) feedAdvanced() {
	wait := s.feedWait
	s.feedWait = nil
	for _, r := range wait {
		f := r.afterFeed
		if f == nil {
			continue
		}
		if !s.feedOK(r) {
			s.feedWait = append(s.feedWait, r)
			continue
		}
		r.afterFeed = nil
		f()
	}
}
