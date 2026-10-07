package sched

import (
	"context"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kairo/mpsc"
	"kairo/task"
)

// DestLimits are the quotas of one destination (e.g. "openai/gpt-4o").
type DestLimits struct {
	RPM int // requests per minute, 0 = unlimited
	TPM int // tokens per minute, 0 = unlimited
	// BurstSeconds sizes the buckets: how many seconds of quota may be used
	// at once. Default 10.
	BurstSeconds float64
	// TenantConcurrency caps outstanding tasks per tenant at this
	// destination (0 = unlimited).
	TenantConcurrency int
	// Concurrency caps outstanding tasks at this destination (0 =
	// unlimited). Below it, the dispatcher finds how many the destination
	// takes from its answers (ADR 0039).
	Concurrency int
}

// Outcome is how a task went, for the destination's concurrency (ADR
// 0039). RateLimited: the destination refused it for its limits (an HTTP
// 429, a provider's rate limit error); TimedOut: it did not answer in
// time. Both lower the destination's concurrency.
type Outcome struct {
	Tokens      int
	RateLimited bool
	TimedOut    bool
}

// The adaptive concurrency of a destination (ADR 0039): unlimited (up to a
// declared Concurrency) until the destination first refuses a task for its
// limits; then half of what was outstanding, halved again on a refusal (at
// most once per round trip), and grown by one per round trip while the
// recent latency stays within latencyTolerance of the long-run one.
const (
	latencyTolerance = 2.0
	srttWeight       = 0.125
	baseWeight       = 1.0 / 64
	statsEvery       = 100 * time.Millisecond
)

// Dispatcher holds ready tasks and hands them to pulling workers, visiting
// tenants round-robin and respecting each destination's RPM/TPM buckets.
// It is a single goroutine that owns all of its state; producers and
// workers talk to it through a non-blocking queue.
type Dispatcher struct {
	q     *mpsc.Queue[dmsg]
	now   func() time.Time
	dests map[string]*dest
	// pollers waiting for work, by action
	pollers map[string][]*Poller
	// destinations that have queued tasks, by action
	byAction map[string][]*dest
	limits   func(dest string) DestLimits
	timer    *time.Timer
	timerAt  time.Time
	seq      uint64
	// pending: queued tasks. aborted marks the pending ones to drop when
	// they reach the head of their queue. delivered: tasks handed to a
	// poller and not yet Done, with the cancel of their context (ADR 0026).
	pending   map[task.Key]struct{}
	aborted   map[task.Key]struct{}
	delivered map[task.Key]handed
	stop      chan struct{}
	done      chan struct{}

	// affine: by run, the affine poller its tasks go to (ADR 0046).
	// affinePollers counts the affine pollers: with none, RunEnded does
	// nothing.
	affine        map[string]*Poller
	affinePollers atomic.Int64
	// affineRuns mirrors affine's keys for RunEnded, which runs on other
	// goroutines: only a run in the table costs a message when it ends.
	affineRuns sync.Map

	statsMu sync.Mutex
	dstats  []DestStat
	statsAt time.Time
	queued  int
	tracked int // len(pending) + len(aborted) + len(delivered)
}

type dmsgKind uint8

const (
	mSubmit dmsgKind = iota
	mPoll
	mUnpoll
	mDone
	mAbort
	mAbortAll
	mWake
	mRunEnd
)

type dmsg struct {
	kind    dmsgKind
	task    *task.Task
	poller  *Poller
	credit  int
	dest    string
	tenant  string
	est     int
	actual  int
	limited bool
	key     task.Key
	run     string
}

type handed struct {
	task   *task.Task
	ctx    context.Context
	cancel context.CancelFunc
	at     time.Time // when it was delivered
}

// Delivery is a task handed to a worker. Ctx is cancelled when the task is
// aborted (step timeout, run cancelled) or reported Done.
type Delivery struct {
	Task *task.Task
	Ctx  context.Context
}

// Poller is a worker's standing request for tasks. Tasks are delivered on
// C; the worker grants more credit with Dispatcher.Poll.
type Poller struct {
	Actions []string
	C       chan Delivery
	credit  int
	gone    bool
	// affine: a worker that keeps state per run gets a run's tasks while
	// it has credit (ADR 0046). runs: the runs it is the poller of.
	affine bool
	runs   map[string]struct{}
}

type dest struct {
	name     string
	action   string
	lim      DestLimits
	rpm, tpm bucket
	tenants  map[string]*tenantQ
	ring     []string
	rr       int
	queued   int
	listed   bool

	// Adaptive concurrency (ADR 0039). limited: the destination refused a
	// task once; until then limit is the declared cap.
	inflight      int
	limit         float64
	limited       bool
	srtt, baseRTT time.Duration // recent and long-run round trip
	lastDecrease  time.Time
	refused       uint64
}

func (ds *dest) maxLimit() float64 {
	if ds.lim.Concurrency > 0 {
		return float64(ds.lim.Concurrency)
	}
	return math.MaxInt32
}

// observe adjusts the concurrency after a task took rtt.
func (ds *dest) observe(now time.Time, rtt time.Duration, limited bool) {
	if limited {
		ds.refused++
		// At most one decrease per round trip: refusals of tasks sent
		// together are one signal. Before any success, the refused task's
		// own round trip is the measure.
		window := ds.srtt
		if window == 0 {
			window = rtt
		}
		switch {
		case !ds.limited:
			// The first refusal: half of what was outstanding.
			ds.limited = true
			ds.limit = math.Max(1, float64(ds.inflight+1)/2)
			ds.lastDecrease = now
		case now.Sub(ds.lastDecrease) >= window:
			ds.limit = math.Max(1, ds.limit/2)
			ds.lastDecrease = now
		}
		return
	}
	if rtt > 0 {
		if ds.srtt == 0 {
			ds.srtt, ds.baseRTT = rtt, rtt
		} else {
			ds.srtt += time.Duration(srttWeight * float64(rtt-ds.srtt))
			ds.baseRTT += time.Duration(baseWeight * float64(rtt-ds.baseRTT))
		}
	}
	switch {
	case !ds.limited:
		return // not limited: nothing to grow
	case float64(ds.srtt) > latencyTolerance*float64(ds.baseRTT):
		// Recent latency well above the long-run one: the destination slows
		// down under the load; stay.
	default:
		ds.limit += 1 / ds.limit
	}
	ds.limit = math.Min(ds.limit, ds.maxLimit())
}

type tenantQ struct {
	q        []*task.Task
	inflight int
}

type bucket struct {
	perMs  float64
	cap    float64
	tokens float64
	last   time.Time
}

func newBucket(perMinute int, burstSec float64, now time.Time) bucket {
	if perMinute <= 0 {
		return bucket{perMs: -1}
	}
	perMs := float64(perMinute) / 60000
	c := math.Max(1, perMs*burstSec*1000)
	return bucket{perMs: perMs, cap: c, tokens: c, last: now}
}

func (b *bucket) unlimited() bool { return b.perMs < 0 }

func (b *bucket) refill(now time.Time) {
	if b.unlimited() {
		return
	}
	dt := float64(now.Sub(b.last).Microseconds()) / 1000
	b.last = now
	b.tokens = math.Min(b.cap, b.tokens+dt*b.perMs)
}

// wait returns how long until n tokens are available (0 if now).
func (b *bucket) wait(n float64) time.Duration {
	if b.unlimited() {
		return 0
	}
	n = math.Min(n, b.cap)
	if b.tokens >= n {
		return 0
	}
	return time.Duration((n-b.tokens)/b.perMs*1e6) + time.Millisecond
}

func NewDispatcher(limits func(dest string) DestLimits) *Dispatcher {
	return newDispatcher(limits, time.Now)
}

func newDispatcher(limits func(dest string) DestLimits, now func() time.Time) *Dispatcher {
	if limits == nil {
		limits = func(string) DestLimits { return DestLimits{} }
	}
	d := &Dispatcher{
		q:         mpsc.New[dmsg](),
		now:       now,
		dests:     map[string]*dest{},
		pollers:   map[string][]*Poller{},
		byAction:  map[string][]*dest{},
		limits:    limits,
		pending:   map[task.Key]struct{}{},
		aborted:   map[task.Key]struct{}{},
		delivered: map[task.Key]handed{},
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go d.loop()
	return d
}

// Submit queues a task. Never blocks.
func (d *Dispatcher) Submit(t *task.Task) { d.q.Push(dmsg{kind: mSubmit, task: t}) }

// NewPoller registers a worker for actions with an initial credit.
func (d *Dispatcher) NewPoller(actions []string, credit int) *Poller {
	if credit < 1 {
		credit = 1
	}
	p := &Poller{Actions: actions, C: make(chan Delivery, credit)}
	d.q.Push(dmsg{kind: mPoll, poller: p, credit: credit})
	return p
}

// NewAffinePoller is NewPoller for a worker that keeps state per run: the
// tasks of a run go to the poller that took the run's earlier tasks while
// it has credit (ADR 0046).
func (d *Dispatcher) NewAffinePoller(actions []string, credit int) *Poller {
	if credit < 1 {
		credit = 1
	}
	p := &Poller{Actions: actions, C: make(chan Delivery, credit), affine: true, runs: map[string]struct{}{}}
	d.affinePollers.Add(1)
	d.q.Push(dmsg{kind: mPoll, poller: p, credit: credit})
	return p
}

// RunEnded forgets which poller a finished run's tasks went to (ADR 0046).
func (d *Dispatcher) RunEnded(runID string) {
	if d.affinePollers.Load() == 0 {
		return
	}
	if _, ok := d.affineRuns.Load(runID); ok {
		d.q.Push(dmsg{kind: mRunEnd, run: runID})
	}
}

// Poll grants n more credits to p. The worker must have room for them:
// the number of tasks received but not yet completed plus the outstanding
// credit must not exceed cap(p.C).
func (d *Dispatcher) Poll(p *Poller, n int) { d.q.Push(dmsg{kind: mPoll, poller: p, credit: n}) }

// Unpoll removes p. Tasks already delivered to p.C but not taken are
// returned to their queues; the worker must report Done for the ones it
// took, even if it gives up on them.
func (d *Dispatcher) Unpoll(p *Poller) {
	if p.affine {
		d.affinePollers.Add(-1)
	}
	d.q.Push(dmsg{kind: mUnpoll, poller: p})
}

// Done reports that a task finished, with its actual token usage. Its
// concurrency slot is released only now, not when it was aborted, so a
// worker that ignores cancellation still counts against the limits.
func (d *Dispatcher) Done(t *task.Task, tokens int) { d.Finish(t, Outcome{Tokens: tokens}) }

// Finish is Done with how the task went, which adjusts its destination's
// concurrency (ADR 0039).
func (d *Dispatcher) Finish(t *task.Task, o Outcome) {
	d.q.Push(dmsg{kind: mDone, task: t, key: t.Key(), dest: t.Destination, tenant: t.Tenant, est: t.EstTokens,
		actual: o.Tokens, limited: o.RateLimited || o.TimedOut})
}

// Abort drops a queued task, or cancels the context of a delivered one
// (ADR 0026). A task that is neither (already done) is left alone.
func (d *Dispatcher) Abort(k task.Key) { d.q.Push(dmsg{kind: mAbort, key: k}) }

// Tracked is how many task keys the dispatcher remembers (queued, marked
// aborted or delivered). It must return to 0 when all work is done.
func (d *Dispatcher) Tracked() int {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return d.tracked
}

// AbortAll cancels the contexts of all delivered tasks (the engine is
// stopping). Their slots are still released by Done.
func (d *Dispatcher) AbortAll() { d.q.Push(dmsg{kind: mAbortAll}) }

// DestStat is a destination's concurrency (ADR 0039). Limit is 0 while
// the destination has not refused a task (not limited).
type DestStat struct {
	Name     string  `json:"name"`
	Inflight int     `json:"inflight"`
	Limit    float64 `json:"limit,omitempty"`
	Refused  uint64  `json:"refused,omitempty"`
}

// Dests is each destination's concurrency, by name.
func (d *Dispatcher) Dests() []DestStat {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return slices.Clone(d.dstats)
}

// Queued is the number of tasks waiting for quota or workers.
func (d *Dispatcher) Queued() int {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return d.queued
}

func (d *Dispatcher) Close() {
	close(d.stop)
	<-d.done
}

func (d *Dispatcher) loop() {
	defer close(d.done)
	var buf []dmsg
	for {
		select {
		case <-d.q.Ready():
		case <-d.stop:
			// Messages still queued (an AbortAll among them) are not
			// processed: cancel what was handed out.
			for _, h := range d.delivered {
				h.cancel()
			}
			return
		}
		buf = d.q.Drain(buf)
		touched := map[*dest]struct{}{}
		wakeAll := false
		for i := range buf {
			m := &buf[i]
			switch m.kind {
			case mSubmit:
				ds := d.dest(m.task.Destination, m.task.Action)
				d.seq++
				m.task.Seq = d.seq
				tq := ds.tenants[m.task.Tenant]
				if tq == nil {
					tq = &tenantQ{}
					ds.tenants[m.task.Tenant] = tq
				}
				if len(tq.q) == 0 {
					ds.ring = append(ds.ring, m.task.Tenant)
				}
				tq.q = append(tq.q, m.task)
				ds.queued++
				d.pending[m.task.Key()] = struct{}{}
				if !ds.listed {
					ds.listed = true
					d.byAction[ds.action] = append(d.byAction[ds.action], ds)
				}
				touched[ds] = struct{}{}
			case mPoll:
				p := m.poller
				if p.gone {
					continue
				}
				if p.credit == 0 {
					for _, a := range p.Actions {
						d.pollers[a] = append(d.pollers[a], p)
					}
				}
				p.credit += m.credit
				for _, a := range p.Actions {
					for _, ds := range d.byAction[a] {
						touched[ds] = struct{}{}
					}
				}
			case mUnpoll:
				p := m.poller
				p.gone = true
				d.removePoller(p)
				for run := range p.runs {
					delete(d.affine, run)
					d.affineRuns.Delete(run)
				}
				p.runs = nil
				for {
					select {
					case dl := <-p.C:
						if d.requeue(dl.Task) {
							touched[d.dests[dl.Task.Destination]] = struct{}{}
						}
						continue
					default:
					}
					break
				}
			case mDone:
				// The same task: a reused key may belong to a newer one.
				var rtt time.Duration
				h, ok := d.delivered[m.key]
				if ok && h.task == m.task {
					h.cancel() // releases the context
					delete(d.delivered, m.key)
					rtt = d.now().Sub(h.at)
				}
				ds := d.dests[m.dest]
				if ds == nil {
					continue
				}
				// Every delivered task counted once in inflight, and every
				// Done is of a delivered task (even when a newer task reused
				// its key).
				ds.inflight--
				if ok && h.task == m.task {
					ds.observe(d.now(), rtt, m.limited)
				}
				if tq := ds.tenants[m.tenant]; tq != nil {
					tq.inflight--
					if tq.inflight <= 0 && len(tq.q) == 0 {
						delete(ds.tenants, m.tenant)
					}
				}
				if !ds.tpm.unlimited() && m.actual > 0 {
					// Settle the estimate against actual usage.
					ds.tpm.tokens += float64(m.est - m.actual)
				}
				touched[ds] = struct{}{}
			case mAbort:
				if _, ok := d.pending[m.key]; ok {
					d.aborted[m.key] = struct{}{}
				} else if h, ok := d.delivered[m.key]; ok {
					h.cancel()
				}
			case mAbortAll:
				for _, h := range d.delivered {
					h.cancel()
				}
			case mWake:
				wakeAll = true
			case mRunEnd:
				if p := d.affine[m.run]; p != nil {
					delete(d.affine, m.run)
					delete(p.runs, m.run)
				}
				d.affineRuns.Delete(m.run)
			}
		}
		if wakeAll {
			for _, ds := range d.dests {
				touched[ds] = struct{}{}
			}
		}
		var nextWake time.Duration
		for ds := range touched {
			if w := d.match(ds); w > 0 && (nextWake == 0 || w < nextWake) {
				nextWake = w
			}
		}
		if nextWake > 0 {
			d.arm(nextWake)
		}
		d.statsMu.Lock()
		n := 0
		for _, ds := range d.dests {
			n += ds.queued
		}
		// The destinations' concurrency, for Stats: not on every round.
		if now := d.now(); now.Sub(d.statsAt) >= statsEvery {
			d.statsAt = now
			d.dstats = d.dstats[:0]
			for _, ds := range d.dests {
				st := DestStat{Name: ds.name, Inflight: ds.inflight, Refused: ds.refused}
				if ds.limited {
					st.Limit = ds.limit
				}
				d.dstats = append(d.dstats, st)
			}
			slices.SortFunc(d.dstats, func(a, b DestStat) int { return strings.Compare(a.Name, b.Name) })
		}
		d.queued = n
		d.tracked = len(d.pending) + len(d.aborted) + len(d.delivered)
		d.statsMu.Unlock()
	}
}

func (d *Dispatcher) arm(after time.Duration) {
	at := d.now().Add(after)
	if d.timer != nil && !d.timerAt.IsZero() && d.timerAt.Before(at) && d.timerAt.After(d.now()) {
		return
	}
	d.timerAt = at
	if d.timer == nil {
		d.timer = time.AfterFunc(after, func() { d.q.Push(dmsg{kind: mWake}) })
		return
	}
	d.timer.Reset(after)
}

func (d *Dispatcher) dest(name, action string) *dest {
	ds := d.dests[name]
	if ds == nil {
		now := d.now()
		lim := d.limits(name)
		if lim.BurstSeconds <= 0 {
			lim.BurstSeconds = 10
		}
		ds = &dest{
			name: name, action: action, lim: lim,
			rpm:     newBucket(lim.RPM, lim.BurstSeconds, now),
			tpm:     newBucket(lim.TPM, lim.BurstSeconds, now),
			tenants: map[string]*tenantQ{},
		}
		ds.limit = ds.maxLimit()
		d.dests[name] = ds
	}
	return ds
}

// requeue returns a task that was delivered but never taken to the head of
// its queue. A task aborted in the meantime is dropped instead (false).
func (d *Dispatcher) requeue(t *task.Task) bool {
	k := t.Key()
	aborted := false
	ds := d.dest(t.Destination, t.Action)
	ds.inflight-- // it was delivered
	if h, ok := d.delivered[k]; ok && h.task == t {
		aborted = h.ctx.Err() != nil
		h.cancel()
		delete(d.delivered, k)
	}
	tq := ds.tenants[t.Tenant]
	if tq == nil {
		tq = &tenantQ{}
		ds.tenants[t.Tenant] = tq
	}
	tq.inflight--
	if aborted {
		if tq.inflight <= 0 && len(tq.q) == 0 {
			delete(ds.tenants, t.Tenant)
		}
		return false
	}
	d.pending[k] = struct{}{}
	if len(tq.q) == 0 {
		ds.ring = append(ds.ring, t.Tenant)
	}
	tq.q = append([]*task.Task{t}, tq.q...)
	ds.queued++
	if !ds.listed {
		ds.listed = true
		d.byAction[ds.action] = append(d.byAction[ds.action], ds)
	}
	return true
}

func (d *Dispatcher) removePoller(p *Poller) {
	for _, a := range p.Actions {
		ps := d.pollers[a]
		for i, x := range ps {
			if x == p {
				ps = append(ps[:i], ps[i+1:]...)
				break
			}
		}
		if len(ps) == 0 {
			delete(d.pollers, a)
		} else {
			d.pollers[a] = ps
		}
	}
}

// match hands tasks of ds to waiting pollers. It returns how long to wait
// before quota allows more (0 if not rate limited).
func (d *Dispatcher) match(ds *dest) time.Duration {
	now := d.now()
	ds.rpm.refill(now)
	ds.tpm.refill(now)
	for ds.queued > 0 {
		ps := d.pollers[ds.action]
		if len(ps) == 0 {
			return 0
		}
		if float64(ds.inflight) >= math.Floor(ds.limit) {
			return 0 // a Done makes room
		}
		// Pick the next tenant round-robin that is under its concurrency cap.
		var t *task.Task
		var tq *tenantQ
		for i := 0; i < len(ds.ring); i++ {
			idx := (ds.rr + i) % len(ds.ring)
			name := ds.ring[idx]
			q := ds.tenants[name]
			// Drop aborted tasks at the head.
			for len(q.q) > 0 {
				k := q.q[0].Key()
				if _, ab := d.aborted[k]; !ab {
					break
				}
				delete(d.aborted, k)
				delete(d.pending, k)
				q.q = q.q[1:]
				ds.queued--
			}
			if len(q.q) == 0 {
				ds.ring = append(ds.ring[:idx], ds.ring[idx+1:]...)
				i--
				if q.inflight <= 0 {
					delete(ds.tenants, name)
				}
				continue
			}
			if c := ds.lim.TenantConcurrency; c > 0 && q.inflight >= c {
				continue
			}
			t, tq = q.q[0], q
			ds.rr = (idx + 1) % max(1, len(ds.ring))
			break
		}
		if t == nil {
			return 0
		}
		est := float64(max(t.EstTokens, 0))
		w := max(ds.rpm.wait(1), ds.tpm.wait(est))
		if w > 0 {
			return w
		}
		if !ds.rpm.unlimited() {
			ds.rpm.tokens--
		}
		if !ds.tpm.unlimited() {
			ds.tpm.tokens -= math.Min(est, ds.tpm.cap)
		}
		tq.q = tq.q[1:]
		ds.queued--
		tq.inflight++
		if len(tq.q) == 0 {
			for i, name := range ds.ring {
				if ds.tenants[name] == tq {
					ds.ring = append(ds.ring[:i], ds.ring[i+1:]...)
					if len(ds.ring) > 0 {
						ds.rr = i % len(ds.ring)
					} else {
						ds.rr = 0
					}
					break
				}
			}
		}
		k := t.Key()
		delete(d.pending, k)
		ctx, cancel := context.WithCancel(context.Background())
		d.delivered[k] = handed{t, ctx, cancel, now}
		ds.inflight++
		p, rotate := ps[0], true
		if a := d.affine[t.RunID]; a != nil && !a.gone && a.credit > 0 && slices.Contains(a.Actions, ds.action) {
			// The run's poller (ADR 0046): it has credit, so it is waiting.
			p, rotate = a, false
		} else if p.affine {
			if old := d.affine[t.RunID]; old != nil {
				delete(old.runs, t.RunID)
			}
			if d.affine == nil {
				d.affine = map[string]*Poller{}
			}
			d.affine[t.RunID] = p
			d.affineRuns.Store(t.RunID, struct{}{})
			p.runs[t.RunID] = struct{}{}
		}
		p.credit--
		p.C <- Delivery{Task: t, Ctx: ctx} // cannot block: credit <= free capacity of C
		if p.credit == 0 {
			d.removePoller(p)
		} else if rotate {
			// Rotate pollers so work spreads across workers.
			d.pollers[ds.action] = append(ps[1:], p)
		}
	}
	return 0
}
