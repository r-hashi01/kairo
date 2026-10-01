package engine

import (
	"encoding/json"
	"errors"
	"log"
	"sort"
	"sync/atomic"
	"time"

	"kairo/core"
	"kairo/ir"
	"kairo/live"
	"kairo/mpsc"
	"kairo/obs"
	"kairo/task"
	"kairo/timerwheel"
	"kairo/wal"
)

// A shard is one event loop that exclusively owns a subset of runs (chosen
// by hashing the run id). Nothing in the loop blocks on I/O: log writes go
// to the committer, snapshot writes and loads go to the I/O pool, tasks go
// to the dispatcher, and all of them answer by pushing a message back into
// the inbox. There are no per-run goroutines and no locks around run state.
type shard struct {
	e     *Engine
	id    int
	inbox *mpsc.Queue[msg]
	runs  map[string]*run
	wheel *timerwheel.Wheel[timerRef]
	timer *time.Timer
	armed int64 // tick the OS timer is armed for, 0 if none
	logs  [tierCount]*shardLog
	cmds  []core.Command
	fired []timerRef
	now   int64

	inMemory atomic.Int64
	evicted  atomic.Int64
	wakeups  atomic.Uint64 // loop iterations, for the no-polling test
}

// shardLog is the per-tier log of a shard with its output-commit queue:
// commands whose release must wait until the log is durable up to an LSN.
type shardLog struct {
	tier      Tier
	sink      wal.Sink
	committer *wal.Committer
	buf       []byte
	lsn       uint64 // last assigned
	durable   uint64
	held      []held
	failed    error
}

type heldKind uint8

const (
	hDispatch heldKind = iota
	hDone
	hEvict
)

type held struct {
	lsn  uint64
	kind heldKind
	run  *run
	task *task.Task
}

type run struct {
	id      string
	plan    *ir.Plan
	tenant  string
	tier    Tier
	st      *core.State
	snap    []byte // encoded state while evicted (in memory, or pending write)
	onDisk  bool   // evicted and the snapshot is in the snapshot store
	loading bool
	writing bool         // a snapshot write is in progress
	pending []core.Event // events that arrived while loading
	timers  map[uint32]timerwheel.Handle
	lastLSN uint64
	holds   int
	done    bool
	status  core.RunStatus
	reviews []Review    // steps needing review (kept outside the state so an evicted run can report them)
	waits   []core.Wait // steps waiting for a signal, while quiescent
}

type timerRef struct {
	run   *run
	act   uint32
	timer uint32
}

type startReq struct {
	runID  string
	tenant string
	plan   *ir.Plan
	tier   Tier
	input  json.RawMessage
}

type msgKind uint8

const (
	mStart msgKind = iota
	mEvent
	mAck
	mLoaded
	mSnapStored
	mQuery
	mStop
)

type msg struct {
	kind  msgKind
	runID string
	ev    core.Event
	start *startReq
	tier  Tier
	lsn   uint64
	err   error
	data  []byte
	run   *run
	reply chan queryReply
}

type queryReply struct {
	info  RunInfo
	found bool
}

func newShard(e *Engine, id int, sinks [tierCount]wal.Sink) *shard {
	s := &shard{
		e:     e,
		id:    id,
		inbox: mpsc.New[msg](),
		runs:  map[string]*run{},
		wheel: timerwheel.New[timerRef](e.cfg.Now().UnixMilli()),
		timer: time.NewTimer(time.Hour),
	}
	s.timer.Stop()
	for t := TierMemory; t < tierCount; t++ {
		if sinks[t] == nil {
			continue
		}
		t := t
		l := &shardLog{tier: t, sink: sinks[t]}
		l.committer = wal.NewCommitter(sinks[t], func(lsn uint64, err error) {
			s.inbox.Push(msg{kind: mAck, tier: t, lsn: lsn, err: err})
		})
		l.buf = l.committer.Buffer()
		s.logs[t] = l
	}
	return s
}

func (s *shard) close() {
	for _, l := range s.logs {
		if l != nil {
			l.committer.Close()
			l.sink.Close()
		}
	}
}

func (s *shard) loop() {
	defer s.e.wg.Done()
	var batch []msg
	for {
		select {
		case <-s.inbox.Ready():
		case <-s.timer.C:
			s.armed = 0
		}
		s.wakeups.Add(1)
		s.now = s.e.cfg.Now().UnixMilli()
		batch = s.inbox.Drain(batch)
		for i := range batch {
			if batch[i].kind == mStop {
				s.flush()
				return
			}
			s.handle(&batch[i])
		}
		s.fired = s.wheel.Advance(s.now, s.fired[:0])
		for _, f := range s.fired {
			delete(f.run.timers, f.timer)
			s.event(f.run, core.Event{Kind: core.EvTimer, Act: f.act, Timer: f.timer})
		}
		s.flush()
		s.rearm()
	}
}

// flush hands accumulated log records to the committers (group commit: one
// hand-off per loop iteration, however many events were applied).
func (s *shard) flush() {
	for _, l := range s.logs {
		if l != nil && len(l.buf) > 0 {
			l.committer.Submit(l.buf, l.lsn)
			l.buf = l.committer.Buffer()
		}
	}
}

func (s *shard) rearm() {
	next, ok := s.wheel.NextDeadline()
	if !ok {
		if s.armed != 0 {
			s.timer.Stop()
			s.armed = 0
		}
		return
	}
	if s.armed == next {
		return
	}
	d := time.Duration(next-s.now) * time.Millisecond
	if d < 0 {
		d = 0
	}
	s.timer.Reset(d)
	s.armed = next
}

func (s *shard) handle(m *msg) {
	switch m.kind {
	case mStart:
		s.startRun(m.start)
	case mEvent:
		r := s.runs[m.runID]
		if r == nil || r.done {
			return
		}
		s.event(r, m.ev)
	case mAck:
		s.ack(m.tier, m.lsn, m.err)
	case mLoaded:
		s.loaded(m.run, m.data, m.err)
	case mSnapStored:
		r := m.run
		r.writing = false
		if m.err != nil {
			log.Printf("kairo: shard %d: snapshot of %s: %v (kept in memory)", s.id, r.id, m.err)
			return
		}
		if r.st != nil || r.snap == nil || r.done {
			return
		}
		if m.lsn == r.lastLSN {
			r.snap = nil
			r.onDisk = true
			return
		}
		// Evicted again with newer state while the write was in progress.
		s.writeSnapshot(r)
	case mQuery:
		r := s.runs[m.runID]
		if r == nil {
			m.reply <- queryReply{}
			return
		}
		m.reply <- queryReply{info: s.info(r), found: true}
	}
}

func (s *shard) info(r *run) RunInfo {
	ri := RunInfo{RunID: r.id, Plan: r.plan.Name, Tenant: r.tenant, Tier: r.tier, Status: r.status.String(), Evicted: r.st == nil, Reviews: r.reviews, Waits: r.waits}
	if st := r.st; st != nil {
		ri.Output, ri.Error = st.Output, st.Error
	}
	return ri
}

// updateWaits refreshes the list of waiting steps once the run has nothing
// in flight, and tells live subscribers (a UI prompting for an approval or
// the next user message) what it is waiting for.
func (s *shard) updateWaits(r *run) {
	var w []core.Wait
	if r.st.Quiescent() {
		w = core.Waits(r.plan, r.st)
	}
	if len(w) == 0 && len(r.waits) == 0 {
		return
	}
	changed := len(w) != len(r.waits)
	for i := 0; !changed && i < len(w); i++ {
		changed = w[i].Act != r.waits[i].Act
	}
	r.waits = w
	if changed && len(w) > 0 {
		b, _ := json.Marshal(map[string]any{"type": "waiting", "waits": w, "reviews": r.reviews})
		s.e.live.Publish(live.Chunk{RunID: r.id, Control: true, Data: b})
	}
}

func reviews(r *run) []Review {
	var out []Review
	for id, a := range r.st.Acts {
		if a.NeedsReview() {
			out = append(out, Review{Act: id, StepID: core.StepID(r.plan, r.st, id)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Act < out[j].Act })
	return out
}

func (s *shard) startRun(sr *startReq) {
	if _, dup := s.runs[sr.runID]; dup {
		s.e.adm.Release(sr.tenant)
		return
	}
	r := &run{id: sr.runID, plan: sr.plan, tenant: sr.tenant, tier: sr.tier, st: core.NewState(sr.runID), timers: map[uint32]timerwheel.Handle{}}
	s.runs[r.id] = r
	s.inMemory.Add(1)
	if l := s.logFor(r); l != nil {
		l.buf = wal.Frame(l.buf, encodeStart(nil, &startMeta{RunID: r.id, Plan: r.plan.Name, PlanHash: r.plan.Hash, Tenant: r.tenant, Tier: r.tier}))
		l.lsn++
		r.lastLSN = l.lsn
	}
	s.e.obs.Emit(obs.Record{At: s.now, Type: "run.start", RunID: r.id, Tenant: r.tenant, Data: sr.input})
	s.event(r, core.Event{Kind: core.EvStart, Data: sr.input})
}

func (s *shard) logFor(r *run) *shardLog {
	if r.tier == TierNone {
		return nil
	}
	return s.logs[r.tier]
}

// event applies ev to r: the only place where run state changes.
func (s *shard) event(r *run, ev core.Event) {
	if r.done {
		return
	}
	if r.st == nil {
		r.pending = append(r.pending, ev)
		s.load(r)
		return
	}
	ev.At = s.now
	out, err := core.Apply(r.plan, r.st, &ev, s.cmds[:0])
	s.cmds = out[:0]
	if err != nil {
		return // ignored (stale) events are not logged
	}
	s.record(r, &ev)
	if r.status = r.st.Status; r.status == core.StatusBlocked || r.reviews != nil {
		r.reviews = reviews(r)
	}
	s.commands(r, out)
	if !r.done {
		s.updateWaits(r)
	}
	if !r.done {
		s.maybeEvict(r)
	}
}

// record appends ev to r's log; the record is written by the next flush.
func (s *shard) record(r *run, ev *core.Event) {
	l := s.logFor(r)
	if l == nil {
		return
	}
	var tmp [256]byte
	l.buf = wal.Frame(l.buf, encodeEvent(tmp[:0], r.id, ev))
	l.lsn++
	r.lastLSN = l.lsn
}

func (s *shard) commands(r *run, out []core.Command) {
	for i := range out {
		c := &out[i]
		switch c.Kind {
		case core.CmdDispatch:
			s.dispatch(r, c)
		case core.CmdTimer:
			h := s.wheel.Schedule(c.At, timerRef{run: r, act: c.Act, timer: c.Timer})
			r.timers[c.Timer] = h
		case core.CmdCancelTimer:
			if h, ok := r.timers[c.Timer]; ok {
				s.wheel.Cancel(h)
				delete(r.timers, c.Timer)
			}
		case core.CmdAbort:
			s.e.disp.Abort(task.Key{RunID: r.id, Act: c.Act, Attempt: c.Attempt})
		case core.CmdReview:
			s.e.obs.Emit(obs.Record{At: s.now, Type: "step.review", RunID: r.id, Tenant: r.tenant, StepID: c.StepID, Attempt: c.Attempt,
				Err: "outcome unknown: needs review"})
		case core.CmdDone:
			s.done(r)
		}
	}
}

func (s *shard) dispatch(r *run, c *core.Command) {
	n := &r.plan.Nodes[c.Node]
	t := &task.Task{
		RunID: r.id, StepID: c.StepID, Act: c.Act, Attempt: c.Attempt, IdemKey: c.IdemKey,
		Action: n.Spec.Action, Destination: n.Spec.Destination, Tenant: r.tenant,
		Effect: n.Spec.Effect, Input: c.Input, Params: n.Params,
	}
	t.EstTokens = s.e.cfg.EstimateTokens(t)
	s.e.obs.Emit(obs.Record{At: s.now, Type: "step.dispatch", RunID: r.id, Tenant: r.tenant, StepID: c.StepID,
		Action: t.Action, Effect: t.Effect.String(), Attempt: c.Attempt, Data: c.Input})
	l := s.logFor(r)
	if n.Spec.Effect != ir.EffectReal || l == nil {
		// Unprotected: releasing it before the log is durable is safe;
		// at worst it runs again after a crash.
		s.e.disp.Submit(t)
		return
	}
	// Real: record the intent, and release the command only once the log is
	// durable up to the intent, which also covers every transition before it.
	ev := core.Event{Kind: core.EvIntent, At: s.now, Act: c.Act, Attempt: c.Attempt}
	if _, err := core.Apply(r.plan, r.st, &ev, nil); err != nil {
		return
	}
	s.record(r, &ev)
	s.hold(l, r, hDispatch, t)
}

func (s *shard) hold(l *shardLog, r *run, k heldKind, t *task.Task) {
	if l.durable >= r.lastLSN && l.failed == nil {
		s.release(held{lsn: r.lastLSN, kind: k, run: r, task: t})
		return
	}
	r.holds++
	l.held = append(l.held, held{lsn: r.lastLSN, kind: k, run: r, task: t})
}

func (s *shard) ack(t Tier, lsn uint64, err error) {
	l := s.logs[t]
	if err != nil {
		// Durability can no longer be guaranteed for this log: keep holding
		// everything that depends on it.
		l.failed = err
		log.Printf("kairo: shard %d: %s log write failed: %v", s.id, t, err)
		return
	}
	if lsn > l.durable {
		l.durable = lsn
	}
	i := 0
	for ; i < len(l.held); i++ {
		h := l.held[i]
		if h.lsn > l.durable {
			break
		}
		h.run.holds--
		s.release(h)
	}
	if i > 0 {
		n := copy(l.held, l.held[i:])
		clear(l.held[n:])
		l.held = l.held[:n]
	}
}

func (s *shard) release(h held) {
	switch h.kind {
	case hDispatch:
		if !h.run.done {
			s.e.disp.Submit(h.task)
		}
	case hDone:
		s.finish(h.run)
	case hEvict:
		s.maybeEvict(h.run)
	}
}

// done is called when the core reports a terminal status. Completion is
// reported to clients only once it is durable.
func (s *shard) done(r *run) {
	r.done = true
	for id, h := range r.timers {
		s.wheel.Cancel(h)
		delete(r.timers, id)
	}
	if l := s.logFor(r); l != nil {
		s.hold(l, r, hDone, nil)
		return
	}
	s.finish(r)
}

func (s *shard) finish(r *run) {
	ri := s.info(r)
	delete(s.runs, r.id)
	s.inMemory.Add(-1)
	s.e.obs.Emit(obs.Record{At: s.now, Type: "run.done", RunID: r.id, Tenant: r.tenant, Data: r.st.Output, Err: r.st.Error})
	if r.onDisk || r.tier >= TierFile {
		id := r.id
		s.e.doIO(func() { s.e.snaps.Delete("snap/" + id) })
	}
	s.e.adm.Release(r.tenant)
	s.e.finish(ri)
}

// maybeEvict snapshots a run that is only waiting (timers far away,
// signals, operator review) and drops its state from memory.
func (s *shard) maybeEvict(r *run) {
	if r.st == nil || r.done || s.e.cfg.EvictAfter < 0 || !r.st.Quiescent() || r.holds > 0 {
		return
	}
	next := int64(-1)
	for _, a := range r.st.Acts {
		if a.Timer != 0 && (next < 0 || a.TimerAt < next) {
			next = a.TimerAt
		}
	}
	if next >= 0 && next-s.now < s.e.cfg.EvictAfter.Milliseconds() {
		return
	}
	l := s.logFor(r)
	if l != nil && l.durable < r.lastLSN {
		// Only snapshot what is already durable, so a snapshot never
		// refers to log positions that a crash could lose.
		s.hold(l, r, hEvict, nil)
		return
	}
	r.snap = encodeSnapshot(r.lastLSN, r.st)
	r.st = nil
	s.inMemory.Add(-1)
	s.evicted.Add(1)
	if r.tier >= TierFile && !r.writing {
		s.writeSnapshot(r)
	}
}

// writeSnapshot persists r.snap. Writes of one run are serialized so an
// older snapshot can never overwrite a newer one.
func (s *shard) writeSnapshot(r *run) {
	r.writing = true
	data, lsn := r.snap, r.lastLSN
	s.e.doIO(func() {
		err := s.e.snaps.Put("snap/"+r.id, data)
		s.inbox.Push(msg{kind: mSnapStored, run: r, lsn: lsn, err: err})
	})
}

func (s *shard) load(r *run) {
	if r.loading {
		return
	}
	if r.snap != nil {
		s.loaded(r, r.snap, nil)
		return
	}
	r.loading = true
	id := r.id
	s.e.doIO(func() {
		data, err := s.e.snaps.Get("snap/" + id)
		s.inbox.Push(msg{kind: mLoaded, run: r, data: data, err: err})
	})
}

func (s *shard) loaded(r *run, data []byte, err error) {
	r.loading = false
	if err == nil {
		var st *core.State
		_, st, err = decodeSnapshot(data)
		if err == nil {
			r.st = st
		}
	}
	if err != nil {
		log.Printf("kairo: shard %d: loading snapshot of %s: %v", s.id, r.id, err)
		return
	}
	r.snap = nil
	r.onDisk = false
	s.inMemory.Add(1)
	s.evicted.Add(-1)
	pending := r.pending
	r.pending = nil
	for _, ev := range pending {
		if r.st == nil {
			// Evicted again while replaying queued events.
			r.pending = append(r.pending, ev)
			s.load(r)
			continue
		}
		s.event(r, ev)
	}
}

var errPlanMissing = errors.New("plan not registered")
