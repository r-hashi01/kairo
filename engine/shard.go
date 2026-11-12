package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/live"
	"github.com/r-hashi01/kairo/metrics"
	"github.com/r-hashi01/kairo/mpsc"
	"github.com/r-hashi01/kairo/obs"
	"github.com/r-hashi01/kairo/sched"
	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/timerwheel"
	"github.com/r-hashi01/kairo/wal"
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
	// doneLog holds finished-run markers (ADR 0027); nil if disabled.
	doneLog *doneLog
	cmds    []core.Command
	// Feed (ADR 0034): traces of this loop iteration, the LSN per tier up
	// to which run state may be snapshotted and records retired, and the
	// runs waiting for it to advance.
	traces    []core.Trace
	feedOut   []FeedEntry
	feedBound [tierCount]uint64
	feedWait  []*run
	fired     []timerRef
	now       int64
	// inputWaits: GetInput's replies for evicted runs being loaded for
	// them (made on first use: no cost to runs nobody asks about).
	inputWaits map[*run][]chan queryReply
	// children is owned by the parent shard. The bool says the child is
	// finished; closed rejects new children once terminal settlement starts.
	children map[string]map[string]bool
	closed   map[string]bool
	// drives exists only for code workflows (ADR 0069).
	drives map[string]*driveState
	// quietWaits is allocated only while callers wait for quiescence.
	quietWaits map[string]map[uint64]chan queryReply

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
	hCallback   // a callback decision is durable: answer its HTTP request
)

type held struct {
	lsn     uint64
	kind    heldKind
	run     *run
	task    *task.Task
	snapLSN uint64 // hCheckpoint
	reply   chan error
}

type run struct {
	id       string
	parent   string
	workflow string
	meta     json.RawMessage
	created  int64
	updated  int64
	plan     *ir.Plan
	tenant   string
	tier     Tier
	keep     bool // keep the output after it finishes (ADR 0050)
	st       *core.State
	snap     []byte // encoded state while evicted (in memory, or pending write)
	onDisk   bool   // evicted and the snapshot is in the snapshot store
	loading  bool
	writing  bool           // a snapshot write is in progress
	pending  []pendingEvent // events that arrived while loading
	timers   map[uint32]timerwheel.Handle
	lastLSN  uint64
	// Log retention (ADR 0016). Before its first durable checkpoint a run
	// needs everything from its start record. After it, only its latest
	// checkpoint record (cpRec, so recovery can find it) and its own
	// records after the snapshot (the first of which is postSnap).
	startLSN    uint64
	startTicket *sched.StartTicket
	cpLSN       uint64 // snapshot LSN of the latest durable checkpoint
	cpRec       uint64 // LSN of that checkpoint record
	postSnap    uint64 // first own record after cpLSN, 0 if none yet
	pendSnap    uint64 // LSN of a snapshot being written, not yet checkpointed
	pendFirst   uint64 // first own record after pendSnap
	finishJob   func() // finished while a snapshot write was in flight: run after it
	afterFeed   func() // waits for the feed's acknowledgements (ADR 0034)
	feedLSN     uint64 // LSN of the run's last event that produced traces
	started     bool   // the start is durable (Submit answered)
	startWait   []chan startReply
	compactCP   bool        // the snapshot in flight was requested by compaction
	starts      []*startReq // runs reusing this id, started once the snapshot is deleted
	holds       int
	done        bool
	status      core.RunStatus
	reviews     []Review    // steps needing review (kept outside the state so an evicted run can report them)
	waits       []core.Wait // steps waiting for a signal, while quiescent
	unreached   bool        // recovered child: wait for its parent's replay (ADR 0067)
}

type driveState struct {
	ready uint64
	seen  uint64
	token string
	owner string
	until int64
}

type pendingEvent struct {
	ev      core.Event
	reply   chan error          // non-nil for a callback waiting for durability
	applied chan signalDecision // non-nil for a synchronous signal decision
}

type timerRef struct {
	run   *run
	act   uint32
	timer uint32
}

type startReq struct {
	runID         string
	parent        string
	workflow      string
	meta          json.RawMessage
	tenant        string
	plan          *ir.Plan
	tier          Tier
	input         json.RawMessage
	entry         string
	limits        RunLimits
	depth         int
	vars          json.RawMessage
	keep          bool            // SubmitRequest.KeepOutput (ADR 0050)
	drive         bool            // explicit code-workflow reach (ADR 0069)
	reply         chan startReply // answered once the start is durable
	startTicket   *sched.StartTicket
	initialSignal *SignalRequest
	inboxObserved bool
}

type startReply struct {
	existing bool
	err      error
}

type attachReply struct {
	added bool
	err   error
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
	mDoneAck
	mFeedBound
	mFeedStart
	mKept
	mKeptDeleted
	mStop
	mQueryInput // GetInput (ADR 0060)
	mAttach
	mDetach
	mChildDone
	mQueryChildren
	mGuardedEvent
	mList
	mQueryQuiescent
	mCancelQuiescent
	mDriveReady
	mDriveSnapshot
	mDriveClaim
	mDriveRenew
	mDriveRelease
	mCallback
	mApplySignal
)

type listItem struct {
	info   RunInfo
	runID  string
	marker bool
}

type msg struct {
	kind          msgKind
	runID         string
	ev            core.Event
	start         *startReq
	tier          Tier
	lsn           uint64
	err           error
	data          []byte
	run           *run
	reply         chan queryReply
	child         string
	attachReply   chan attachReply
	childrenReply chan []string
	errReply      chan error
	appliedReply  chan signalDecision
	listFilter    ListFilter
	listReply     chan []listItem
	quietID       uint64
	driveToken    string
	driveOwner    string
	driveUntil    int64
	driveRetry    bool
	driveReply    chan driveReply
	driveList     chan []DriveCandidate
	// mFeedStart
	feedReply chan [tierCount]uint64
	// mKept: the marker to add once the output is kept (ADR 0050)
	after func()
}

type queryReply struct {
	info  RunInfo
	found bool
}

func newShard(e *Engine, id int, sinks [tierCount]wal.Sink, done wal.Sink) *shard {
	s := &shard{
		e:        e,
		id:       id,
		inbox:    mpsc.New[msg](),
		runs:     map[string]*run{},
		pins:     map[string]*run{},
		children: map[string]map[string]bool{},
		closed:   map[string]bool{},
		drives:   map[string]*driveState{},
		wheel:    timerwheel.New[timerRef](e.cfg.Now().UnixMilli()),
		timer:    time.NewTimer(time.Hour),
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
	if done != nil {
		s.doneLog = newDoneLog(s, done)
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
	if d := s.doneLog; d != nil {
		d.committer.Close()
		d.sink.Close()
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
	if d := s.doneLog; d != nil && len(d.buf) > 0 {
		d.committer.Submit(d.buf, d.lsn)
		d.buf = d.committer.Buffer()
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
	case mAttach:
		r := s.runs[m.runID]
		if r == nil || r.done || s.closed[m.runID] || r.plan.Name != "kairo.workflow" || m.child == m.runID {
			m.attachReply <- attachReply{err: ErrParentUnavailable}
			return
		}
		cs := s.children[m.runID]
		if cs == nil {
			cs = map[string]bool{}
			s.children[m.runID] = cs
		}
		_, existed := cs[m.child]
		if !existed {
			cs[m.child] = false
		}
		m.attachReply <- attachReply{added: !existed}
	case mDetach:
		if cs := s.children[m.runID]; cs != nil {
			delete(cs, m.child)
			if len(cs) == 0 {
				delete(s.children, m.runID)
			}
		}
	case mChildDone:
		if cs := s.children[m.runID]; cs != nil {
			if _, ok := cs[m.child]; ok {
				cs[m.child] = true
			}
		}
		s.driveReady(m.runID)
	case mQueryChildren:
		var ids []string
		for id := range s.children[m.runID] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		m.childrenReply <- ids
	case mGuardedEvent:
		r := s.runs[m.runID]
		if r == nil || r.done {
			m.errReply <- ErrUnknownRun
			return
		}
		for _, done := range s.children[m.runID] {
			if !done {
				m.errReply <- ErrChildrenUnsettled
				return
			}
		}
		s.closed[m.runID] = true
		s.event(r, m.ev)
		m.errReply <- nil
	case mList:
		items := make([]listItem, 0)
		for _, r := range s.runs {
			ri := s.info(r)
			if listMatches(ri, m.listFilter) {
				items = append(items, listItem{info: ri})
			}
		}
		if d := s.doneLog; d != nil {
			d.idx.mu.RLock()
			cut := s.markerCut()
			for id, en := range d.idx.m {
				if en.at >= cut && en.parent == "" && en.workflow != "" && listEntryMatches(en, m.listFilter) {
					items = append(items, listItem{runID: id, marker: true})
				}
			}
			d.idx.mu.RUnlock()
		}
		m.listReply <- items
	case mQueryQuiescent:
		r := s.runs[m.runID]
		if r == nil {
			m.reply <- queryReply{}
			return
		}
		if r.st == nil {
			if s.quietWaits == nil {
				s.quietWaits = map[string]map[uint64]chan queryReply{}
			}
			s.addQuietWait(m.runID, m.quietID, m.reply)
			s.load(r)
			return
		}
		if r.st.Quiescent() {
			ri := s.info(r)
			ri.NextAt = nextRunTimer(r.st)
			m.reply <- queryReply{info: ri, found: true}
			return
		}
		s.addQuietWait(m.runID, m.quietID, m.reply)
	case mCancelQuiescent:
		if ws := s.quietWaits[m.runID]; ws != nil {
			delete(ws, m.quietID)
			if len(ws) == 0 {
				delete(s.quietWaits, m.runID)
			}
		}
	case mDriveReady:
		s.driveReady(m.runID)
	case mDriveSnapshot:
		out := make([]DriveCandidate, 0, len(s.drives))
		for id, d := range s.drives {
			r := s.runs[id]
			if r == nil || r.done || r.unreached {
				continue
			}
			if d.token != "" && d.until <= s.now {
				d.token, d.owner, d.until = "", "", 0
				d.ready++
			}
			active := d.token != "" && d.until > s.now
			out = append(out, DriveCandidate{Run: s.info(r), Ready: !active && d.ready > d.seen, Until: d.until})
		}
		m.driveList <- out
	case mDriveClaim:
		r, d := s.runs[m.runID], s.drives[m.runID]
		if d != nil && d.token != "" && d.until <= s.now {
			d.token, d.owner, d.until = "", "", 0
			d.ready++
		}
		if r == nil || r.done || r.unreached || d == nil || d.token != "" || d.ready <= d.seen {
			m.driveReply <- driveReply{}
			return
		}
		d.seen, d.token, d.owner, d.until = d.ready, m.driveToken, m.driveOwner, m.driveUntil
		m.driveReply <- driveReply{ok: true, until: d.until}
	case mDriveRenew:
		d := s.drives[m.runID]
		if d == nil || d.token != m.driveToken || d.owner != m.driveOwner || d.until <= s.now {
			m.driveReply <- driveReply{}
			return
		}
		d.until = m.driveUntil
		m.driveReply <- driveReply{ok: true, until: d.until}
	case mDriveRelease:
		d := s.drives[m.runID]
		if d == nil || d.token != m.driveToken || d.owner != m.driveOwner {
			m.driveReply <- driveReply{}
			return
		}
		d.token, d.owner, d.until = "", "", 0
		if m.driveRetry {
			d.ready++
		}
		ready := d.ready > d.seen
		m.driveReply <- driveReply{ok: true}
		if ready {
			s.e.notifyDrive()
		}
	case mEvent:
		r := s.runs[m.runID]
		if r == nil || r.done {
			return
		}
		s.event(r, m.ev)
	case mCallback:
		r := s.runs[m.runID]
		if r == nil {
			if s.e.Finished(m.runID) {
				m.errReply <- nil
			} else {
				m.errReply <- ErrUnknownRun
			}
			return
		}
		s.eventReply(r, m.ev, m.errReply, nil, true)
	case mApplySignal:
		r := s.runs[m.runID]
		if r == nil {
			m.appliedReply <- signalDecision{}
			m.errReply <- nil
			return
		}
		if r.done {
			match, conflict, early := matchSignalEvent(r.st.Output, m.ev)
			switch {
			case conflict:
				m.appliedReply <- signalDecision{}
				m.errReply <- ErrSignalConflict
			case match:
				m.appliedReply <- signalDecision{applied: true, duplicate: true, early: early}
				s.ackEvent(r, m.errReply)
			default:
				m.appliedReply <- signalDecision{}
				m.errReply <- nil
			}
			return
		}
		s.eventReply(r, m.ev, m.errReply, m.appliedReply, true)
	case mAck:
		s.ack(m.tier, m.lsn, m.err)
	case mDoneAck:
		s.doneAck(m.lsn, m.err)
	case mKept:
		// The output is durably kept (or could not be: then it is not, and
		// the run reads as trimmed once forgotten): now the marker.
		s.addMarker(m.run, string(m.data), m.err == nil, m.lsn, m.after)
	case mKeptDeleted:
		delete(s.doneLog.deleting, m.lsn)
	case mFeedBound:
		s.feedBound[m.tier] = m.lsn
		s.feedAdvanced()
	case mFeedStart:
		// A subscription starts over from the present: nothing past here
		// may be snapshotted until it acknowledges (ADR 0034).
		var at [tierCount]uint64
		for t := TierFile; t < tierCount; t++ {
			if l := s.logs[t]; l != nil {
				at[t] = l.lsn
				s.feedBound[t] = min(s.feedBound[t], l.lsn)
			}
		}
		m.feedReply <- at
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
	case mQueryInput:
		r := s.runs[m.runID]
		if r == nil {
			m.reply <- queryReply{}
			return
		}
		if r.st != nil {
			ri := s.info(r)
			ri.Input = r.st.Input
			m.reply <- queryReply{info: ri, found: true}
			return
		}
		// Evicted: answered once it is loaded.
		if s.inputWaits == nil {
			s.inputWaits = map[*run][]chan queryReply{}
		}
		s.inputWaits[r] = append(s.inputWaits[r], m.reply)
		s.load(r)
	}
}

// answerInputs answers GetInput's replies waiting for r to be loaded (with
// no input if it could not be).
func (s *shard) answerInputs(r *run) {
	replies := s.inputWaits[r]
	if len(replies) == 0 {
		return
	}
	delete(s.inputWaits, r)
	ri := s.info(r)
	if r.st != nil {
		ri.Input = r.st.Input
	}
	for _, reply := range replies {
		reply <- queryReply{info: ri, found: true}
	}
}

func (s *shard) info(r *run) RunInfo {
	ri := RunInfo{RunID: r.id, Parent: r.parent, Workflow: r.workflow, Meta: r.meta, Plan: r.plan.Name, Tenant: r.tenant, Tier: r.tier,
		Status: r.status.String(), Evicted: r.st == nil, Reviews: r.reviews, Waits: r.waits}
	ri.CreatedAt, ri.UpdatedAt = r.created, r.updated
	if st := r.st; st != nil {
		ri.Output, ri.Error, ri.Exceptions = st.Output, st.Error, int(st.Exceptions)
		ri.Vars = core.RunVars(st)
	}
	return ri
}

func nextRunTimer(st *core.State) int64 {
	next := int64(0)
	if st.DeadlineTimer != 0 {
		next = st.Deadline
	}
	for _, a := range st.Acts {
		if a.Timer != 0 && (next == 0 || a.TimerAt < next) {
			next = a.TimerAt
		}
	}
	return next
}

func (s *shard) addQuietWait(runID string, id uint64, reply chan queryReply) {
	if s.quietWaits == nil {
		s.quietWaits = map[string]map[uint64]chan queryReply{}
	}
	ws := s.quietWaits[runID]
	if ws == nil {
		ws = map[uint64]chan queryReply{}
		s.quietWaits[runID] = ws
	}
	ws[id] = reply
}

func (s *shard) answerQuiet(r *run) {
	ws := s.quietWaits[r.id]
	if len(ws) == 0 || r.st == nil || !r.st.Quiescent() {
		return
	}
	delete(s.quietWaits, r.id)
	ri := s.info(r)
	ri.NextAt = nextRunTimer(r.st)
	for _, reply := range ws {
		reply <- queryReply{info: ri, found: true}
	}
}

func (s *shard) driveReady(runID string) {
	d := s.drives[runID]
	if d == nil {
		return
	}
	d.ready++
	s.e.notifyDrive()
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
	if !sr.inboxObserved {
		sr.inboxObserved = true
		s.e.starts.ObserveInbox(sr.startTicket, s.e.cfg.Now())
	}
	if r, dup := s.runs[sr.runID]; dup {
		s.e.starts.Drop(sr.startTicket)
		if sr.parent != "" && sr.parent != r.parent {
			s.e.adm.Release(sr.tenant)
			s.e.detach(sr.parent, sr.runID)
			sr.reply <- startReply{err: ErrParentUnavailable}
			return
		}
		// The id is an idempotency key: answer for the existing run.
		s.e.adm.Release(sr.tenant)
		if r.unreached {
			r.unreached = false
			s.event(r, core.Event{Kind: core.EvRecover})
		}
		if sr.drive {
			s.driveReady(r.id)
		}
		if r.started {
			sr.reply <- startReply{existing: true}
		} else {
			r.startWait = append(r.startWait, sr.reply)
		}
		return
	}
	if d := s.doneLog; d != nil {
		if _, ok := d.idx.get(sr.runID, s.markerCut()); ok {
			s.e.starts.Drop(sr.startTicket)
			// Finished within IdempotencyTTL (ADR 0027).
			s.e.adm.Release(sr.tenant)
			sr.reply <- startReply{existing: true}
			return
		}
	}
	if old := s.pins[sr.runID]; old != nil {
		old.starts = append(old.starts, sr)
		return
	}
	r := &run{id: sr.runID, parent: sr.parent, workflow: sr.workflow, meta: sr.meta, plan: sr.plan, tenant: sr.tenant, tier: sr.tier, keep: sr.keep, startTicket: sr.startTicket,
		st: core.NewState(sr.runID), timers: map[uint32]timerwheel.Handle{}}
	s.forgetMarker(r.id) // an expired marker of an earlier run with this id
	s.forgetTree(r.id)
	s.runs[r.id] = r
	if r.plan.Name == "kairo.workflow" {
		s.drives[r.id] = &driveState{ready: 1}
		s.e.notifyDrive()
	}
	s.inMemory.Add(1)
	ev := core.Event{Kind: core.EvStart, At: s.now, Data: sr.input, Name: sr.entry,
		MaxSteps: int32(sr.limits.MaxSteps), Depth: int32(sr.depth), Vars: sr.vars}
	if d := sr.limits.MaxDuration; d > 0 {
		ev.Deadline = s.now + d.Milliseconds()
	}
	var initial core.Event
	if sr.initialSignal != nil {
		initial = core.Event{Kind: core.EvSignal, At: s.now, Name: sr.initialSignal.Name, Data: sr.initialSignal.Payload,
			Meta: encodeSignalMeta(sr.initialSignal, true)}
	}
	if l := s.logFor(r); l != nil {
		rec := encodeStart(nil, s.meta(r))
		if sr.initialSignal != nil {
			rec = encodeInitialStart(nil, s.meta(r), &ev, &initial)
		}
		l.buf = wal.Frame(l.buf, rec)
		l.lsn++
		r.lastLSN, r.startLSN = l.lsn, l.lsn
	}
	s.e.obs.Emit(obs.Record{At: s.now, Type: "run.start", RunID: r.id, Tenant: r.tenant, Data: sr.input})
	r.startWait = append(r.startWait, sr.reply)
	if sr.initialSignal != nil {
		s.eventReply(r, ev, nil, nil, false)
		s.eventReply(r, initial, nil, nil, false)
	} else {
		s.event(r, ev)
	}
	if l := s.logFor(r); l != nil {
		// Durable once the start record and the EvStart event are. Held at
		// lastLSN (at or after both) to keep the held list in LSN order.
		s.hold(l, r, hStarted, nil)
	} else {
		s.release(held{kind: hStarted, run: r})
	}
}

func (s *shard) meta(r *run) *startMeta {
	return &startMeta{RunID: r.id, Parent: r.parent, Plan: r.plan.Name, PlanHash: r.plan.Hash, Tenant: r.tenant, Workflow: r.workflow,
		Meta: r.meta, Created: r.created, Updated: r.updated, Tier: r.tier, Keep: r.keep}
}

func (s *shard) logFor(r *run) *shardLog {
	if r.tier == TierNone {
		return nil
	}
	return s.logs[r.tier]
}

// event applies ev to r: the only place where run state changes.
func (s *shard) event(r *run, ev core.Event) {
	s.eventReply(r, ev, nil, nil, true)
}

func (s *shard) eventReply(r *run, ev core.Event, reply chan error, applied chan signalDecision, record bool) {
	if r.done {
		if applied != nil {
			applied <- signalDecision{}
		}
		s.ackEvent(r, reply)
		return
	}
	if r.st == nil {
		r.pending = append(r.pending, pendingEvent{ev: ev, reply: reply, applied: applied})
		s.load(r)
		return
	}
	ev.At = s.now
	wasBlocked := r.status == core.StatusBlocked
	var out []core.Command
	var err error
	if s.e.feed != nil {
		out, s.traces, err = core.ApplyTraced(r.plan, r.st, &ev, s.cmds[:0], s.traces[:0])
	} else {
		out, err = core.Apply(r.plan, r.st, &ev, s.cmds[:0])
	}
	s.cmds = out[:0]
	if err != nil {
		if applied != nil {
			applied <- signalDecision{}
		}
		s.ackEvent(r, reply)
		return // ignored (stale) events are not logged
	}
	if applied != nil {
		applied <- signalDecision{applied: true}
	}
	if ev.Kind == core.EvStart && r.created == 0 {
		r.created = ev.At
	}
	r.updated = ev.At
	if record {
		s.record(r, &ev)
	}
	s.ackEvent(r, reply)
	if s.e.feed != nil && len(s.traces) > 0 {
		// Before the commands: a task's chunks must not overtake the trace
		// of its start.
		s.feedTraces(r, r.lastLSN, s.traces)
		if r.tier >= TierFile {
			r.feedLSN = r.lastLSN
		}
	}
	if r.status = r.st.Status; r.status == core.StatusBlocked || r.reviews != nil {
		r.reviews = reviews(r)
	}
	if !wasBlocked && r.status == core.StatusBlocked {
		s.e.measure(metrics.Measurement{Name: metrics.RunBlocked, Kind: metrics.Counter, Unit: "{event}", Value: 1,
			Labels: metrics.Labels{Reason: "real_outcome_unknown"}})
	}
	s.commands(r, out)
	if !r.done {
		s.updateWaits(r)
		if s.quietWaits != nil {
			s.answerQuiet(r)
		}
	}
	if !r.done {
		s.maybeEvict(r)
	}
}

func (s *shard) ackEvent(r *run, reply chan error) {
	if reply == nil {
		return
	}
	if l := s.logFor(r); l != nil {
		s.holdAt(l, held{lsn: r.lastLSN, kind: hCallback, run: r, reply: reply})
		return
	}
	reply <- nil
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
		Action: n.Spec.Action, Destination: c.Destination, Tenant: r.tenant,
		Effect: n.Spec.Effect, Input: c.Input, Params: n.Params, Deadline: c.At,
	}
	t.EstTokens = s.e.cfg.EstimateTokens(t)
	t.EstWeight = t.EstTokens
	t.Depth = int(r.st.Depth)
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
		s.e.starts.FailRound(sched.StartKey{Shard: s.id, Tier: int(t)}, lsn)
		log.Printf("kairo: shard %d: %s log write failed: %v", s.id, t, err)
		return
	}
	if lsn > l.durable {
		l.durable = lsn
	}
	i := 0
	var starts []*sched.StartTicket
	for ; i < len(l.held); i++ {
		h := l.held[i]
		if h.lsn > l.durable {
			break
		}
		h.run.holds--
		if h.kind == hStarted && h.run.startTicket != nil {
			starts = append(starts, h.run.startTicket)
			h.run.startTicket = nil
		}
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
	s.e.starts.ConfirmRound(sched.StartKey{Shard: s.id, Tier: int(t)}, lsn, starts, s.e.cfg.Now())
	s.maybeCompact(l)
}

func (s *shard) release(h held) {
	switch h.kind {
	case hDispatch:
		// Not if the step was aborted while its intent was becoming durable
		// (a failed map element, a timeout, ADR 0032).
		if r := h.run; !r.done && r.st != nil && core.Dispatched(r.st, h.task.Act, h.task.Attempt) {
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
	case hCallback:
		h.reply <- nil
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
	if b := s.feedBound[l.tier]; s.e.feed != nil && b != math.MaxUint64 {
		bound = min(bound, b+1) // keep what the feed may need again
	}
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
	if r.tier < TierFile || r.st == nil || r.writing || l == nil || l.durable < r.lastLSN || !s.feedOK(r) {
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
	ri.FinishedAt = time.UnixMilli(r.updated)
	s.e.childDone(r.parent, r.id)
	if r.tier < TierFile || s.doneLog == nil {
		s.forgetTree(r.id)
	}
	delete(s.runs, r.id)
	if _, ok := s.drives[r.id]; ok {
		delete(s.drives, r.id)
		s.e.notifyDrive()
	}
	if s.quietWaits != nil {
		if ws := s.quietWaits[r.id]; len(ws) > 0 {
			delete(s.quietWaits, r.id)
			for _, reply := range ws {
				reply <- queryReply{info: ri, found: true}
			}
		}
	}
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
	if tier < TierFile {
		s.e.doIO(job)
		return
	}
	// Keep the run's records until its snapshot and blobs are gone, so a
	// crash in between cannot resurrect it (ADR 0016).
	s.pins[r.id] = r
	var start func()
	start = func() {
		if !s.feedOK(r) {
			// The records must stay until the feed has its traces (ADR 0034).
			s.waitFeed(r, start)
			return
		}
		if r.writing {
			r.finishJob = job // after the in-flight snapshot write
			return
		}
		s.e.doIO(job)
	}
	if s.doneLog == nil {
		start()
		return
	}
	// Clean up only once the marker is durable: until then the records
	// are what tells a restart that the run finished (ADR 0027).
	if !r.keep {
		s.addMarker(r, ri.Status, false, 0, start)
		return
	}
	// Its output first, then the marker that says it is there: a marker
	// never points at an output that a crash lost (ADR 0050). A crash in
	// between leaves the run's records: recovery finishes it again.
	// Its generation: the log position of the event that finished it,
	// which recovery finds again if it has to keep the output itself.
	out, status, gen := ri.Output, ri.Status, r.lastLSN
	s.e.doIO(func() {
		err := s.e.keepOutput(id, gen, out)
		if err != nil {
			log.Printf("kairo: run %s: keeping its output: %v", id, err)
		}
		s.inbox.Push(msg{kind: mKept, runID: id, run: r, data: []byte(status), lsn: gen, err: err, after: start})
	})
}

func (s *shard) forgetTree(id string) {
	delete(s.children, id)
	delete(s.closed, id)
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
	if !s.feedOK(r) {
		s.waitFeed(r, func() { s.maybeEvict(r) })
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
		s.answerInputs(r)
		if ws := s.quietWaits[r.id]; len(ws) > 0 {
			delete(s.quietWaits, r.id)
			for _, reply := range ws {
				reply <- queryReply{info: s.info(r), found: true}
			}
		}
		for _, p := range r.pending {
			if p.applied != nil {
				p.applied <- signalDecision{}
			}
			if p.reply != nil {
				p.reply <- err
			}
		}
		r.pending = nil
		return
	}
	defer s.answerInputs(r)
	r.snap = nil
	r.onDisk = false
	s.inMemory.Add(1)
	s.evicted.Add(-1)
	pending := r.pending
	r.pending = nil
	for _, p := range pending {
		if r.st == nil {
			// Evicted again while replaying queued events.
			r.pending = append(r.pending, p)
			s.load(r)
			continue
		}
		s.eventReply(r, p.ev, p.reply, p.applied, true)
	}
	if s.quietWaits != nil {
		s.answerQuiet(r)
	}
}

var errPlanMissing = errors.New("plan not registered")
