package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
)

// The event partitions (ADR 0038). A partition is a shard of the engine:
// a run's events are all in its shard, in order, and the engine's feed
// acknowledges per shard. Each partition is held by at most one worker
// connection at a time; the daemon hands partitions out, takes them back
// from workers that hold more than their share when others need some, and
// gives the partitions of a closed connection to others.
//
// A worker acknowledges entries one by one, once what they mean is in
// Dify's database. The daemon acknowledges the engine's feed up to the
// first entry not yet acknowledged, so after a restart of either side the
// engine delivers again what a worker had not acknowledged, and a partition
// taken over by another worker starts with those entries. Entries without
// a log position (live chunks) are delivered once, to the holder at the
// time, and need no acknowledgement.

type fentry struct {
	ev    Event
	cur   engine.Cursor
	acked bool
	sent  bool // a chunk sent to its holder: never sent again
}

type partition struct {
	id      int
	entries []*fentry // not yet acknowledged, in order (acknowledged ones are dropped from the front)
	base    int       // absolute index of entries[0]
	holder  *conn
	gen     uint64 // assignment generation; acknowledgements of older ones are ignored
	plans   map[string]*ir.Plan
	index   map[string]*fentry // entries to acknowledge, by id
	chunks  int                // chunk entries in entries
}

type conn struct {
	id      uint64
	name    string
	want    int
	parts   map[int]int // partition -> absolute index of the next entry to send
	notices []Event     // assignments and revocations to send
	changed chan struct{}
	closed  bool
}

type feed struct {
	d       *Daemon
	mu      sync.Mutex
	parts   []*partition
	conns   map[uint64]*conn
	nextID  uint64
	gen     uint64
	acks    chan struct{} // wakes the acknowledging loop
	pending map[[2]int]engine.Cursor
}

func newFeed(d *Daemon, n int) *feed {
	f := &feed{d: d, conns: map[uint64]*conn{}, acks: make(chan struct{}, 1), pending: map[[2]int]engine.Cursor{}}
	for i := 0; i < n; i++ {
		f.parts = append(f.parts, &partition{id: i, plans: map[string]*ir.Plan{}, index: map[string]*fentry{}})
	}
	return f
}

// entryID names an entry by its feed position: stable across restarts.
func entryID(c engine.Cursor) string {
	return fmt.Sprintf("%d.%d.%d.%d", c.Shard, c.Tier, c.LSN, c.Seq)
}

func parseEntryID(s string) (engine.Cursor, bool) {
	p := strings.Split(s, ".")
	if len(p) != 4 {
		return engine.Cursor{}, false
	}
	var n [4]uint64
	for i := range p {
		v, err := strconv.ParseUint(p[i], 10, 64)
		if err != nil {
			return engine.Cursor{}, false
		}
		n[i] = v
	}
	return engine.Cursor{Shard: int(n[0]), Tier: engine.Tier(n[1]), LSN: n[2], Seq: uint32(n[3])}, true
}

func (f *feed) notify(c *conn) {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

// add appends a feed entry to its partition.
func (f *feed) add(ctx context.Context, en engine.FeedEntry) {
	if en.Cursor.Shard < 0 || en.Cursor.Shard >= len(f.parts) {
		return
	}
	f.mu.Lock()
	p := f.parts[en.Cursor.Shard]
	plan, known := p.plans[en.RunID]
	f.mu.Unlock()
	if !known {
		// The run's plan, to name its nodes in Dify's records (ADR 0041:
		// the records' work): asked of the engine once per run, and
		// remembered even when the run is gone (nil).
		if ri, err := f.d.E.Get(ctx, en.RunID); err == nil {
			f.d.mu.Lock()
			plan = f.d.plans[ri.Plan]
			f.d.mu.Unlock()
		}
	}
	ev := f.d.event(plan, en)
	ev.Run = en.RunID
	ev.Partition = en.Cursor.Shard
	e := &fentry{ev: ev, cur: en.Cursor, acked: en.Cursor.LSN == 0}
	if en.Cursor.LSN > 0 {
		e.ev.ID = entryID(en.Cursor)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	p.plans[en.RunID] = plan
	if en.Cursor.LSN == 0 {
		// A live chunk goes to the holder of the time, once; with no
		// holder it is gone (ADR 0038).
		if p.holder == nil {
			return
		}
		p.chunks++
	} else {
		p.index[e.ev.ID] = e
	}
	p.entries = append(p.entries, e)
	if p.holder != nil {
		f.notify(p.holder)
	}
	f.trim(p)
}

// trim drops acknowledged entries from the front of p, records how far
// the engine's feed can be acknowledged and wakes the acknowledging loop.
// Called with f.mu held.
func (f *feed) trim(p *partition) {
	n := 0
	for n < len(p.entries) && p.entries[n].acked {
		e := p.entries[n]
		if e.cur.LSN > 0 {
			k := [2]int{e.cur.Shard, int(e.cur.Tier)}
			f.pending[k] = e.cur
		}
		if e.ev.Kind == "run_end" {
			delete(p.plans, e.ev.Run)
		}
		if e.cur.LSN == 0 {
			p.chunks--
		} else {
			delete(p.index, e.ev.ID)
		}
		n++
	}
	if n == 0 {
		return
	}
	p.entries = slices.Delete(p.entries, 0, n)
	p.base += n
	if len(p.entries) == 0 {
		f.balance() // it may move now
	}
	select {
	case f.acks <- struct{}{}:
	default:
	}
}

// ackLoop acknowledges the engine's feed up to what workers acknowledged.
func (f *feed) ackLoop(ctx context.Context, fd *engine.Feed) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-f.acks:
		}
		f.mu.Lock()
		cs := make([]engine.Cursor, 0, len(f.pending))
		for _, c := range f.pending {
			cs = append(cs, c)
		}
		clear(f.pending)
		f.mu.Unlock()
		for _, c := range cs {
			if err := fd.Ack(c); err != nil {
				return err
			}
		}
	}
}

// ack marks the entries ids of partition generation gen acknowledged.
func (f *feed) ack(c *conn, gen uint64, ids []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	touched := map[*partition]bool{}
	for _, id := range ids {
		cur, ok := parseEntryID(id)
		if !ok || cur.Shard < 0 || cur.Shard >= len(f.parts) {
			continue
		}
		p := f.parts[cur.Shard]
		if p.holder != c || gen != p.gen {
			continue // no longer this worker's (or an older assignment): the new holder does it again
		}
		if e := p.index[id]; e != nil {
			e.acked = true
			touched[p] = true
		}
	}
	for p := range touched {
		f.trim(p)
	}
}

// balance hands out partitions without a holder and moves partitions from
// connections over their share to connections under it. Called with f.mu
// held.
func (f *feed) balance() {
	var conns []*conn
	for _, c := range f.conns {
		if !c.closed {
			conns = append(conns, c)
		}
	}
	if len(conns) == 0 {
		return
	}
	slices.SortFunc(conns, func(a, b *conn) int { return int(a.id) - int(b.id) })
	share := (len(f.parts) + len(conns) - 1) / len(conns)
	room := func(c *conn) int { return min(c.want, share) - len(c.parts) }
	// Take back what is over the share, if someone has room.
	needs := 0
	for _, c := range conns {
		needs += max(room(c), 0)
	}
	free := 0
	for _, p := range f.parts {
		if p.holder == nil {
			free++
		}
	}
	// Only idle partitions move: one with entries not acknowledged has runs
	// whose downstream its holder is still producing; it moves once they
	// are acknowledged (trim balances again). A run is never processed by
	// two holders at once.
	for _, c := range conns {
		for needs > free && len(c.parts) > share {
			victim := -1
			for pid := range c.parts {
				if len(f.parts[pid].entries) == 0 && (victim < 0 || pid > victim) {
					victim = pid
				}
			}
			if victim < 0 {
				break
			}
			f.revoke(f.parts[victim])
			free++
		}
	}
	for _, p := range f.parts {
		if p.holder != nil {
			continue
		}
		var best *conn
		for _, c := range conns {
			if room(c) > 0 && (best == nil || len(c.parts) < len(best.parts)) {
				best = c
			}
		}
		if best == nil {
			return
		}
		f.assign(p, best)
	}
}

func (f *feed) assign(p *partition, c *conn) {
	f.gen++
	p.gen = f.gen
	p.holder = c
	c.parts[p.id] = p.base // from the first entry not acknowledged
	c.notices = append(c.notices, Event{Kind: "assigned", Partition: p.id, Gen: p.gen})
	f.notify(c)
}

func (f *feed) revoke(p *partition) {
	c := p.holder
	if c == nil {
		return
	}
	delete(c.parts, p.id)
	c.notices = append(c.notices, Event{Kind: "revoked", Partition: p.id, Gen: p.gen})
	f.notify(c)
	p.holder = nil
}

func (f *feed) open(name string, want int) *conn {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	c := &conn{id: f.nextID, name: name, want: max(want, 1), parts: map[int]int{}, changed: make(chan struct{}, 1)}
	f.conns[c.id] = c
	f.balance()
	return c
}

func (f *feed) close(c *conn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c.closed = true
	for pid := range c.parts {
		f.parts[pid].holder = nil
	}
	c.parts = map[int]int{}
	delete(f.conns, c.id)
	f.balance()
}

// next returns what to send to c: notices first, then entries of its
// partitions.
func (f *feed) next(c *conn, limit int) []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := c.notices
	c.notices = nil
	pids := make([]int, 0, len(c.parts))
	for pid := range c.parts {
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	for _, pid := range pids {
		p := f.parts[pid]
		i := max(c.parts[pid], p.base)
		for ; i < p.base+len(p.entries) && len(out) < limit; i++ {
			e := p.entries[i-p.base]
			if e.cur.LSN == 0 {
				if e.sent {
					continue // sent to an earlier holder
				}
				e.sent = true
			}
			ev := e.ev
			ev.Gen = p.gen
			out = append(out, ev)
		}
		c.parts[pid] = i
		f.compact(p, c)
	}
	return out
}

// compact drops the chunks c was sent from p once they pile up behind an
// entry not yet acknowledged (a long run holds the front). Called with
// f.mu held.
func (f *feed) compact(p *partition, c *conn) {
	if p.chunks < 1024 {
		return
	}
	sentTo := c.parts[p.id]
	kept := p.entries[:0]
	next := p.base
	for i, e := range p.entries {
		abs := p.base + i
		if e.cur.LSN == 0 && e.sent && abs < sentTo {
			p.chunks--
			continue
		}
		if abs < sentTo {
			next++
		}
		kept = append(kept, e)
	}
	clear(p.entries[len(kept):])
	p.entries = kept
	c.parts[p.id] = next
}

// GET /v1/feed?want=N&worker=<name>: the events of the partitions this
// connection holds, as JSON lines, until the client goes away. Notices
// ("assigned", "revoked") tell which partitions it holds; on "assigned"
// the partition's entries not yet acknowledged follow from the first.
// Empty lines are keep-alives.
func (d *Daemon) getFeed(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		fail(w, 500, errors.New("streaming unsupported"))
		return
	}
	want, _ := strconv.Atoi(r.URL.Query().Get("want"))
	if want <= 0 {
		want = len(d.feed.parts)
	}
	c := d.feed.open(r.URL.Query().Get("worker"), want)
	defer d.feed.close(c)
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Kairo-Conn", strconv.FormatUint(c.id, 10))
	w.WriteHeader(200)
	enc := json.NewEncoder(w)
	enc.Encode(Event{Kind: "hello", Conn: c.id, Partitions: len(d.feed.parts)})
	fl.Flush()
	keepAlive := time.NewTimer(15 * time.Second)
	defer keepAlive.Stop()
	for {
		batch := d.feed.next(c, 512)
		for _, ev := range batch {
			if enc.Encode(ev) != nil {
				return
			}
		}
		if len(batch) > 0 {
			fl.Flush()
			continue
		}
		select {
		case <-c.changed:
		case <-keepAlive.C:
			if _, err := w.Write([]byte("\n")); err != nil {
				return
			}
			fl.Flush()
			keepAlive.Reset(15 * time.Second)
		case <-r.Context().Done():
			return
		}
	}
}

// POST /v1/feed/ack {"conn": <id>, "acks": [{"gen": g, "ids": [...]}]}:
// the entries are in Dify's database.
func (d *Daemon) postAck(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Conn uint64 `json:"conn"`
		Acks []struct {
			Gen uint64   `json:"gen"`
			IDs []string `json:"ids"`
		} `json:"acks"`
	}
	if err := readJSON(r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	d.feed.mu.Lock()
	c := d.feed.conns[req.Conn]
	d.feed.mu.Unlock()
	if c == nil {
		fail(w, 409, errors.New("connection closed: its partitions went to others"))
		return
	}
	for _, a := range req.Acks {
		d.feed.ack(c, a.Gen, a.IDs)
	}
	reply(w, 200, map[string]bool{"ok": true})
}

// kinds names traces in the event stream.
var kinds = map[core.TraceKind]string{
	core.TrRunStart: "run_start", core.TrNodeStart: "node_start", core.TrNodeEnd: "node_end",
	core.TrNodeSkip: "node_skip", core.TrNodeRetry: "node_retry", core.TrNodeReview: "node_review",
	core.TrRoundStart: "round_start", core.TrRoundEnd: "round_end", core.TrVarUpdate: "var_update",
	core.TrRunEnd: "run_end",
}

// PartitionState describes a partition with entries not yet acknowledged:
// its holder, how many entries it keeps and the first of them.
type PartitionState struct {
	ID      int    `json:"id"`
	Holder  uint64 `json:"holder"`
	Gen     uint64 `json:"gen"`
	Entries int    `json:"entries"`
	Head    string `json:"head"` // kind, run and id of the first entry
}

// FeedState lists the partitions with entries not yet acknowledged, for
// looking into a stuck feed.
func (d *Daemon) FeedState() []PartitionState {
	f := d.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []PartitionState
	for _, p := range f.parts {
		if len(p.entries) == 0 {
			continue
		}
		ps := PartitionState{ID: p.id, Gen: p.gen, Entries: len(p.entries)}
		if p.holder != nil {
			ps.Holder = p.holder.id
		}
		h := p.entries[0]
		ps.Head = fmt.Sprintf("%s %s %s acked=%v", h.ev.Kind, h.ev.Run, h.ev.ID, h.acked)
		out = append(out, ps)
	}
	return out
}
