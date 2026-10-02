package engine

import (
	"bytes"
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
	pins  map[string]*run // finished runs whose snapshot is being deleted
	wheel *timerwheel.Wheel[timerRef]
	timer *time.Timer
	armed int64 // tick the OS timer is armed for, 0 if none
	logs  [tierCount]*shardLog
	cmds  []core.Command
	fired []timerRef
	now   int64

	inMemory   atomic.Int64
	evicted    atomic.Int64
	wakeups    atomic.Uint64 // loop iterations, for the no-polling test
	failedLogs atomic.Int32
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
	retired   uint64 // last Retire boundary handed to the committer
	checkedAt uint64 // external growth (durable - own) at the last compaction check
	own       uint64 // checkpoint records written by compaction itself
}

type heldKind uint8

const (
	hDispatch heldKind = iota
	hDone
	hEvict
	hCheckpoint // a checkpoint record became durable: advance the run's base
	hStarted    // the start is durable: answer Submit
)

type held struct {
	lsn     uint64
	kind    heldKind
	run     *run
	task    *task.Task
	snapLSN uint64 // hCheckpoint
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
	// Log retention (ADR 0016). Before its first durable checkpoint a run
	// needs everything from its start record. After it, only its latest
	// checkpoint record (cpRec, so recovery can find it) and its own
	// records after the snapshot (the first of which is postSnap).
	startLSN  uint64
	cpLSN     uint64 // snapshot LSN of the latest durable checkpoint
	cpRec     uint64 // LSN of that checkpoint record
	postSnap  uint64 // first own record after cpLSN, 0 if none yet
	pendSnap  uint64 // LSN of a snapshot being written, not yet checkpointed
	pendFirst uint64 // first own record after pendSnap
	finishJob func() // finished while a snapshot write was in flight: run after it
	started   bool   // the start is durable (Submit answered)
	startWait []chan startReply
	compactCP bool        // the snapshot in flight was requested by compaction
	starts    []*startReq // runs reusing this id, started once the snapshot is deleted
	holds     int
	done      bool
	status    core.RunStatus
	reviews   []Review    // steps needing review (kept outside the state so an evicted run can report them)
	waits     []core.Wait // steps waiting for a signal, while quiescent
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
	reply  chan startReply // answered once the start is durable
}

type startReply struct {
	existing bool
}

type msgKind uint8

const (
	mStart msgKind = iota
	mEvent
	mAck
	mLoaded
	mSnapStored
	mSnapDeleted
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
		pins:  map[string]*run{},
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
		if r.done {
			// Deleting only now orders the delete after the write.
			if r.finishJob != nil {
				s.e.doIO(r.finishJob)
				r.finishJob = nil
			}
			return
		}
		if m.err != nil {
			log.Printf("kairo: shard %d: snapshot of %s: %v (kept in memory)", s.id, r.id, m.err)
			return
		}
		own := r.compactCP
		r.compactCP = false
		s.checkpointRecord(r, m.lsn, own)
		if r.st != nil || r.snap == nil {
			return
		}
		if m.lsn == r.lastLSN {
			r.snap = nil
			r.onDisk = true
			return
		}
		// Evicted again with newer state while the write was in progress.
		s.writeSnapshot(r)
	case mSnapDeleted:
		r := m.run
		if s.pins[r.id] == r {
			delete(s.pins, r.id)
		}
		// A new run reusing this id waited for the old snapshot to go, so
		// that delete cannot remove the new run's snapshot.
		for _, sr := range r.starts {
			s.startRun(sr)
		}
		r.starts = nil
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
	if r, dup := s.runs[sr.runID]; dup {
		// The id is an idempotency key: answer for the existing run.
		s.e.adm.Release(sr.tenant)
		if r.started {
			sr.reply <- startReply{existing: true}
		} else {
			r.startWait = append(r.startWait, sr.reply)
		}
		return
	}
	if old := s.pins[sr.runID]; old != nil {
		old.starts = append(old.starts, sr)
		return
	}
	r := &run{id: sr.runID, plan: sr.plan, tenant: sr.tenant, tier: sr.tier, st: core.NewState(sr.runID), timers: map[uint32]timerwheel.Handle{}}
	s.runs[r.id] = r
	s.inMemory.Add(1)
	if l := s.logFor(r); l != nil {
		l.buf = wal.Frame(l.buf, encodeStart(nil, s.meta(r)))
		l.lsn++
		r.lastLSN, r.startLSN = l.lsn, l.lsn
	}
	s.e.obs.Emit(obs.Record{At: s.now, Type: "run.start", RunID: r.id, Tenant: r.tenant, Data: sr.input})
	r.startWait = append(r.startWait, sr.reply)
	s.event(r, core.Event{Kind: core.EvStart, Data: sr.input})
	if l := s.logFor(r); l != nil {
		// Durable once the start record and the EvStart event are. Held at
		// lastLSN (at or after both) to keep the held list in LSN order.
		s.hold(l, r, hStarted, nil)
	} else {
		s.release(held{kind: hStarted, run: r})
	}
}

func (s *shard) meta(r *run) *startMeta {
	return &startMeta{RunID: r.id, Plan: r.plan.Name, PlanHash: r.plan.Hash, Tenant: r.tenant, Tier: r.tier}
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
	if r.pendSnap != 0 && r.pendFirst == 0 {
		r.pendFirst = l.lsn
	}
	if r.cpRec != 0 && r.postSnap == 0 {
		r.postSnap = l.lsn
	}
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
	s.holdAt(l, held{lsn: r.lastLSN, kind: k, run: r, task: t})
}

// holdAt keeps h until the log is durable up to h.lsn. h.lsn must not be
// below any LSN already held (held is kept in LSN order).
func (s *shard) holdAt(l *shardLog, h held) {
	if l.durable >= h.lsn && l.failed == nil {
		s.release(h)
		return
	}
	h.run.holds++
	l.held = append(l.held, h)
}

func (s *shard) ack(t Tier, lsn uint64, err error) {
	l := s.logs[t]
	if err != nil {
		// Durability can no longer be guaranteed for this log: keep holding
		// everything that depends on it.
		if l.failed == nil {
			s.failedLogs.Add(1)
		}
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
		if h.run.holds == 0 && h.kind != hEvict {
			// An eviction may have been skipped while this was held.
			s.maybeEvict(h.run)
		}
	}
	if i > 0 {
		n := copy(l.held, l.held[i:])
		clear(l.held[n:])
		l.held = l.held[:n]
	}
	s.maybeCompact(l)
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
	case hStarted:
		r := h.run
		r.started = true
		for i, w := range r.startWait {
			w <- startReply{existing: i > 0} // the first waiter submitted it
		}
		r.startWait = nil
	case hCheckpoint:
		r, snap := h.run, h.snapLSN
		if r.done || snap < r.cpLSN {
			return
		}
		post := snap + 1 // conservative: don't know the run's first record after snap
		switch {
		case snap == r.pendSnap:
			post = r.pendFirst
			r.pendSnap, r.pendFirst = 0, 0
		case snap == r.cpLSN:
			post = r.postSnap // the same snapshot, logged again
		}
		r.cpLSN, r.cpRec, r.postSnap = snap, h.lsn, post
	}
}

// compactStale is how many compaction intervals a run's pin may lag before
// compaction moves it forward.
const compactStale = 4

// pin is the lowest LSN r still needs (ADR 0016).
func (r *run) pin() uint64 {
	if r.cpRec == 0 {
		return r.startLSN
	}
	if r.postSnap != 0 && r.postSnap < r.cpRec {
		return r.postSnap
	}
	return r.cpRec
}

// startSnapshot encodes r's (durable) state for a snapshot write and
// remembers it so the checkpoint can tell which records came after it.
func (s *shard) startSnapshot(r *run) []byte {
	r.pendSnap, r.pendFirst = r.lastLSN, 0
	return encodeSnapshot(r.lastLSN, r.st)
}

// checkpointRecord logs that r's snapshot up to snapLSN is stored. The
// run's retention base moves only once that record is durable, so a crash
// can never lose both the start record and the checkpoint.
//
// own marks records written on compaction's initiative: they do not count
// as log growth, so compaction cannot keep triggering itself on an idle
// engine.
func (s *shard) checkpointRecord(r *run, snapLSN uint64, own bool) {
	l := s.logFor(r)
	if l == nil || r.tier < TierFile {
		return
	}
	l.buf = wal.Frame(l.buf, encodeCheckpoint(nil, s.meta(r), snapLSN))
	l.lsn++
	if own {
		l.own++
	}
	s.holdAt(l, held{lsn: l.lsn, kind: hCheckpoint, run: r, snapLSN: snapLSN})
}

// maybeCompact runs when a log's durable LSN has advanced by CompactEvery
// records since the last check (event driven: no timer). It retires the
// records no live run needs, and checkpoints runs that hold the boundary
// back so the next check can move it.
func (s *shard) maybeCompact(l *shardLog) {
	every := s.e.cfg.CompactEvery
	ext := l.durable - min(l.durable, l.own) // growth from the workload
	if every < 0 || l.failed != nil || ext < l.checkedAt+uint64(every) {
		return
	}
	l.checkedAt = ext
	bound := l.durable + 1
	// Runs whose pin is older than a few check intervals are moved forward.
	// A waiting run is rewritten about once per compactStale intervals of
	// workload growth, never on its own.
	stale := l.durable - min(l.durable, uint64(every)*compactStale)
	for _, r := range s.runs {
		if r.tier != l.tier {
			continue
		}
		p := r.pin()
		bound = min(bound, p)
		if p > stale || r.done || r.writing {
			continue
		}
		switch {
		case r.st != nil:
			s.checkpointLive(r) // snapshot it where it stands
		case r.onDisk && r.cpRec != 0:
			s.checkpointRecord(r, r.cpLSN, true) // evicted: log the checkpoint again, further ahead
		}
	}
	// Finished runs whose snapshot is still being deleted keep their
	// records, so a crash before the delete cannot resurrect them.
	for _, r := range s.pins {
		if r.tier == l.tier {
			bound = min(bound, r.pin())
		}
	}
	if bound > l.retired {
		l.retired = bound
		l.committer.Retire(bound)
	}
}

// checkpointLive snapshots a run that stays in memory (it is not evicted),
// so its old records can be retired. Only durable state is snapshotted.
func (s *shard) checkpointLive(r *run) {
	l := s.logFor(r)
	if r.tier < TierFile || r.st == nil || r.writing || l == nil || l.durable < r.lastLSN {
		return
	}
	r.writing, r.compactCP = true, true
	data, lsn := s.startSnapshot(r), r.lastLSN
	s.e.doIO(func() {
		err := s.e.snaps.Put("snap/"+r.id, data)
		s.inbox.Push(msg{kind: mSnapStored, run: r, lsn: lsn, err: err})
	})
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
	s.e.adm.Release(r.tenant)

	// The run's blobs are deleted now (ADR 0024). If its output refers to
	// blobs, it is inlined first, and waiters are told only after that, so
	// nobody is handed a reference to a deleted blob.
	inline := bytes.Contains(ri.Output, []byte(`"$blob"`))
	if inline {
		s.e.rememberFinished(ri) // Get still resolves it: the blobs are not gone yet
	} else {
		s.e.finish(ri)
	}
	id, tier := r.id, r.tier
	job := func() {
		if inline {
			if out, err := s.e.ResolveInput(ri.Output); err == nil {
				ri.Output = out
			} else {
				log.Printf("kairo: run %s: inlining its output: %v", id, err)
			}
			s.e.finish(ri)
		}
		s.e.deleteBlobs(id)
		if tier >= TierFile {
			s.e.snaps.Delete("snap/" + id)
			s.inbox.Push(msg{kind: mSnapDeleted, run: r})
		}
	}
	if tier >= TierFile {
		// Keep the run's records until its snapshot and blobs are gone, so a
		// crash in between cannot resurrect it (ADR 0016).
		s.pins[r.id] = r
		if r.writing {
			r.finishJob = job // after the in-flight snapshot write
			return
		}
	}
	s.e.doIO(job)
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
	r.snap = s.startSnapshot(r)
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
