package sched

import (
	"sort"
	"sync"
	"time"
)

// StartKey identifies one independent durable-start path: a shard and a
// durability tier (ADR 0074).
type StartKey struct {
	Shard int
	Tier  int
}

// StartStat is the live state of one durable-start admission window.
type StartStat struct {
	Shard       int     `json:"shard"`
	Tier        int     `json:"tier"`
	Window      int     `json:"window"`
	Inflight    int     `json:"inflight"`
	Queued      int     `json:"queued"`
	Rejected    uint64  `json:"rejected,omitempty"`
	QueueWaitMS float64 `json:"queue_wait_ms,omitempty"`
	InboxWaitMS float64 `json:"inbox_wait_ms,omitempty"`
	WALWaitMS   float64 `json:"wal_wait_ms,omitempty"`
}

// StartAdmission admits durable starts according to the storage round trips
// observed for each shard and tier. Its queues are round-robin by tenant.
// It performs no I/O and calls start only without its lock held (ADR 0074).
type StartAdmission struct {
	mu    sync.Mutex
	now   func() time.Time
	max   int
	lanes map[StartKey]*startLane
}

type startLane struct {
	window, inflight int
	queues           map[string][]*startEntry
	ring             []string
	rr, queued       int
	rejected         uint64
	srtt, baseline   time.Duration
	queueWait        time.Duration
	inboxWait        time.Duration
	walWait          time.Duration
	lastRound        uint64
}

type startEntry struct {
	start      func(*StartTicket)
	tenant     string
	queuedAt   time.Time
	admittedAt time.Time
	inboxAt    time.Time
	state      uint8 // 0 queued, 1 admitted, 2 done/cancelled
	ticket     *StartTicket
}

// StartTicket is one start's place in durable-start admission.
type StartTicket struct {
	a   *StartAdmission
	key StartKey
	e   *startEntry
}

// NewStartAdmission makes adaptive durable-start admission. max is an
// optional safety ceiling per shard and tier; zero leaves the window adaptive.
func NewStartAdmission(max int, now func() time.Time) *StartAdmission {
	if now == nil {
		now = time.Now
	}
	return &StartAdmission{now: now, max: max, lanes: map[StartKey]*startLane{}}
}

func (a *StartAdmission) lane(key StartKey) *startLane {
	l := a.lanes[key]
	if l == nil {
		l = &startLane{window: 1, queues: map[string][]*startEntry{}}
		a.lanes[key] = l
	}
	return l
}

// Admit starts now if the lane's window has room, queues fairly otherwise,
// or returns ErrOverloaded when four windows are already waiting.
func (a *StartAdmission) Admit(key StartKey, tenant string, start func(*StartTicket)) (*StartTicket, error) {
	now := a.now()
	e := &startEntry{start: start, tenant: tenant, queuedAt: now}
	ticket := &StartTicket{a: a, key: key, e: e}
	e.ticket = ticket
	a.mu.Lock()
	l := a.lane(key)
	if l.queued == 0 && l.inflight < l.window {
		l.inflight++
		e.state, e.admittedAt = 1, now
		a.mu.Unlock()
		start(ticket)
		return ticket, nil
	}
	if l.queued >= 4*l.window {
		l.rejected++
		a.mu.Unlock()
		return nil, ErrOverloaded
	}
	if len(l.queues[tenant]) == 0 {
		l.ring = append(l.ring, tenant)
	}
	l.queues[tenant] = append(l.queues[tenant], e)
	l.queued++
	a.mu.Unlock()
	return ticket, nil
}

// Cancel withdraws a start that has not passed admission yet.
func (t *StartTicket) Cancel() bool {
	a := t.a
	a.mu.Lock()
	defer a.mu.Unlock()
	if t.e.state != 0 {
		return false
	}
	l := a.lane(t.key)
	a.removeQueued(l, t.e)
	t.e.state = 2
	return true
}

// Drop releases an admitted start without using it as a storage observation
// (for example, a duplicate RunID that found the original run).
func (a *StartAdmission) Drop(t *StartTicket) {
	if t == nil {
		return
	}
	a.mu.Lock()
	l := a.lane(t.key)
	if t.e.state == 0 {
		a.removeQueued(l, t.e)
		t.e.state = 2
		a.mu.Unlock()
		return
	}
	if t.e.state != 1 {
		a.mu.Unlock()
		return
	}
	t.e.state = 2
	l.inflight--
	starts := a.startQueued(l)
	a.mu.Unlock()
	for _, start := range starts {
		start()
	}
}

// ObserveInbox records how long an admitted start waited for its shard.
func (a *StartAdmission) ObserveInbox(t *StartTicket, now time.Time) {
	if t == nil {
		return
	}
	a.mu.Lock()
	if t.e.state == 1 {
		l := a.lane(t.key)
		l.inboxWait = moving(l.inboxWait, nonnegative(now.Sub(t.e.admittedAt)), 8)
		t.e.inboxAt = now
	}
	a.mu.Unlock()
}

// ConfirmRound releases starts made durable by one group-commit round and
// adapts the lane once for that round.
func (a *StartAdmission) ConfirmRound(key StartKey, round uint64, tickets []*StartTicket, now time.Time) {
	if len(tickets) == 0 {
		return
	}
	a.mu.Lock()
	l := a.lane(key)
	saturated := l.inflight >= l.window || l.queued > 0
	var rtt time.Duration
	var walWait time.Duration
	for _, t := range tickets {
		if t == nil || t.key != key || t.e.state != 1 {
			continue
		}
		rtt = max(rtt, nonnegative(now.Sub(t.e.admittedAt)))
		from := t.e.inboxAt
		if from.IsZero() {
			from = t.e.admittedAt
		}
		walWait = max(walWait, nonnegative(now.Sub(from)))
		t.e.state = 2
		l.inflight--
	}
	if round != l.lastRound {
		l.lastRound = round
		l.walWait = moving(l.walWait, walWait, 8)
		a.observe(l, rtt, false, saturated)
	}
	starts := a.startQueued(l)
	a.mu.Unlock()
	for _, start := range starts {
		start()
	}
}

// FailRound lowers the lane after a failed durability round. Its admitted
// starts remain held because their durability is unknown.
func (a *StartAdmission) FailRound(key StartKey, round uint64) {
	a.mu.Lock()
	l := a.lane(key)
	if round != l.lastRound {
		l.lastRound = round
		a.observe(l, 0, true, true)
	}
	a.mu.Unlock()
}

func (a *StartAdmission) observe(l *startLane, rtt time.Duration, failed, saturated bool) {
	if rtt > 0 {
		if l.srtt == 0 {
			l.srtt, l.baseline = rtt, rtt
		} else {
			l.srtt = moving(l.srtt, rtt, 8)
			l.baseline = moving(l.baseline, rtt, 64)
		}
	}
	bad := failed || (l.baseline > 0 && l.srtt > 2*l.baseline)
	if bad {
		// Existing starts cannot be withdrawn after admission. Keep the
		// advertised window at least that large; no new start enters until
		// acknowledgements bring inflight below the reduced window.
		l.window = max(l.inflight, max(1, (l.window+1)/2))
		return
	}
	if saturated && (a.max == 0 || l.window < a.max) {
		l.window++
	}
}

func (a *StartAdmission) startQueued(l *startLane) []func() {
	var starts []func()
	for l.inflight < l.window && len(l.ring) > 0 {
		idx := l.rr % len(l.ring)
		tenant := l.ring[idx]
		q := l.queues[tenant]
		e := q[0]
		now := a.now()
		e.state, e.admittedAt = 1, now
		l.queueWait = moving(l.queueWait, nonnegative(now.Sub(e.queuedAt)), 8)
		l.queued--
		l.inflight++
		starts = append(starts, func() { e.start(e.ticket) })
		if len(q) == 1 {
			delete(l.queues, tenant)
			l.ring = append(l.ring[:idx], l.ring[idx+1:]...)
			if len(l.ring) == 0 {
				l.rr = 0
			} else {
				l.rr = idx % len(l.ring)
			}
		} else {
			l.queues[tenant] = q[1:]
			l.rr = (idx + 1) % len(l.ring)
		}
	}
	return starts
}

func (a *StartAdmission) removeQueued(l *startLane, e *startEntry) {
	q := l.queues[e.tenant]
	for i, x := range q {
		if x == e {
			q = append(q[:i], q[i+1:]...)
			l.queued--
			break
		}
	}
	if len(q) != 0 {
		l.queues[e.tenant] = q
		return
	}
	delete(l.queues, e.tenant)
	for i, tenant := range l.ring {
		if tenant == e.tenant {
			l.ring = append(l.ring[:i], l.ring[i+1:]...)
			if len(l.ring) == 0 {
				l.rr = 0
			} else {
				l.rr %= len(l.ring)
			}
			return
		}
	}
}

// Stats returns lanes ordered by shard and tier.
func (a *StartAdmission) Stats() []StartStat {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]StartStat, 0, len(a.lanes))
	for key, l := range a.lanes {
		out = append(out, StartStat{Shard: key.Shard, Tier: key.Tier, Window: l.window, Inflight: l.inflight, Queued: l.queued,
			Rejected: l.rejected, QueueWaitMS: ms(l.queueWait), InboxWaitMS: ms(l.inboxWait), WALWaitMS: ms(l.walWait)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Shard != out[j].Shard {
			return out[i].Shard < out[j].Shard
		}
		return out[i].Tier < out[j].Tier
	})
	return out
}

func moving(old, sample time.Duration, weight int64) time.Duration {
	if old == 0 {
		return sample
	}
	return time.Duration((int64(old)*(weight-1) + int64(sample)) / weight)
}

func nonnegative(d time.Duration) time.Duration { return max(0, d) }
func ms(d time.Duration) float64                { return float64(d) / float64(time.Millisecond) }
