// Package engine wires the pieces into a runtime: shards own runs and apply
// events to the core, the dispatcher allocates quota and feeds pulling
// workers, the durability log, observability stream and live stream are
// three independent write paths.
package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/live"
	"github.com/r-hashi01/kairo/mpsc"
	"github.com/r-hashi01/kairo/obs"
	"github.com/r-hashi01/kairo/sched"
	"github.com/r-hashi01/kairo/seal"
	"github.com/r-hashi01/kairo/task"
	"github.com/r-hashi01/kairo/wal"
)

// Tier is the durability tier of a run, chosen at admission.
type Tier uint8

const (
	TierNone       Tier = iota // nothing is logged; a crash loses the run
	TierMemory                 // log kept in (shared) memory
	TierFile                   // log fsynced to local files
	TierReplicated             // log acknowledged by replicas (bring your own Sink)
	tierCount
)

var tierNames = []string{"none", "memory", "file", "replicated"}

func (t Tier) String() string { return tierNames[t] }

func (t Tier) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

func (t *Tier) UnmarshalText(b []byte) error {
	for i, n := range tierNames {
		if n == string(b) {
			*t = Tier(i)
			return nil
		}
	}
	return fmt.Errorf("unknown tier %q", b)
}

type Config struct {
	// Shards is the number of shards (one event loop each). Default:
	// GOMAXPROCS.
	Shards   int
	Registry *ir.Registry

	// DataDir enables the file tier (wal/, snapshots/, blobs/ below it).
	DataDir string
	// NoSync skips fsync for the file tier (for tmpfs, or tests).
	NoSync bool
	// Sinks overrides the log sink per tier and shard. Returning nil, nil
	// means the tier is unavailable for that shard.
	Sinks func(t Tier, shard int) (wal.Sink, error)
	// Blobs holds large payloads; Snapshots holds evicted run states.
	// Defaults: directories under DataDir, or in-memory stores.
	Blobs     blob.Store
	Snapshots blob.Store

	// DefaultTier applies when a submission does not ask for one.
	DefaultTier Tier
	// RealMinTier is the minimum tier for plans containing real effects.
	// Default: TierFile if available, else TierMemory.
	RealMinTier Tier

	// BlobThreshold: outputs larger than this many bytes are stored as
	// blobs; only the reference and typed fields stay in the state.
	// Default 16 KiB.
	BlobThreshold int

	// CompactEvery: every time a log's durable LSN advances by this many
	// records, records no live run needs are retired (ADR 0016) and runs
	// holding them back are checkpointed. Default 65536; negative disables.
	CompactEvery int
	// SegmentSize: file-tier log segment size (default 64 MiB).
	SegmentSize int64

	// RecentRuns is how many finished runs Get, Wait and Submit's
	// idempotency remember with their output, in memory (default 100,000).
	RecentRuns int

	// IdempotencyTTL is how long a finished run of the file tier or above
	// is remembered across restarts (ADR 0027): a RunID submitted again
	// within it starts nothing. Default 24h; negative disables the markers.
	IdempotencyTTL time.Duration
	// IdempotencyMax caps the markers kept in memory (default 10,000,000);
	// beyond it the oldest are forgotten early, with a warning.
	IdempotencyMax int
	// DoneLogs overrides the per-shard log of finished-run markers.
	// Default: files under DataDir/wal, or memory (with a warning) if
	// there is no DataDir.
	DoneLogs func(shard int) (wal.Sink, error)

	// EvictAfter: a run that has nothing in flight and will not be woken
	// for at least this long is snapshotted and dropped from memory.
	// Default 2s; negative disables eviction.
	EvictAfter time.Duration

	// RunLimits applies to runs that do not set their own (ADR 0030).
	RunLimits RunLimits
	// Feeds names the durable subscriptions of the execution event feed
	// (ADR 0034). While a subscription has not acknowledged an entry, run
	// state is not snapshotted past it and the log is kept from it on.
	Feeds []string
	// FeedLimit cuts a subscription holding more unacknowledged entries
	// than this (default 1,000,000).
	FeedLimit int
	// FeedHold holds back new runs (they wait for admission; runs in
	// progress go on) while more entries than this are unacknowledged,
	// until at most half of it are (ADR 0039): the subscribers' storage
	// bounds the throughput. 0: FeedLimit/2; negative: never.
	FeedHold int

	// MaxDepth rejects runs nested deeper than this (a workflow called as a
	// tool from a workflow ...); 0 means no limit.
	MaxDepth int

	Admission sched.AdmissionConfig
	Limits    func(dest string) sched.DestLimits
	// EstimateTokens estimates a task's token use for TPM accounting.
	EstimateTokens func(t *task.Task) int

	Observe obs.Sink

	// Keys, if set, encrypts and authenticates everything stored: every
	// log, snapshots and blobs (ADR 0021). Data written with keys can only
	// be read with them; tampering makes Start fail.
	Keys seal.Keys

	// Now is the clock (tests may override).
	Now func() time.Time
}

// SubmitRequest starts a run.
type SubmitRequest struct {
	Plan   string          `json:"plan"`
	Input  json.RawMessage `json:"input"`
	Tenant string          `json:"tenant"`
	Tier   *Tier           `json:"tier,omitempty"`
	RunID  string          `json:"run_id,omitempty"`
	// Entry starts the run at one entry node of its root graph (e.g. the
	// trigger that fired); the other entries' paths are skipped. Empty
	// starts at all of them (ADR 0029).
	Entry string `json:"entry,omitempty"`
	// Limits overrides Config.RunLimits; Depth is the nesting depth of the
	// run (0 for a top-level run; a worker starting a workflow from a task
	// passes the task's Depth+1).
	Limits *RunLimits `json:"limits,omitempty"`
	Depth  int        `json:"depth,omitempty"`
	// Vars gives initial values to the plan's run variables (an object;
	// e.g. a Dify conversation's variables, ADR 0033).
	Vars json.RawMessage `json:"vars,omitempty"`
	// KeepOutput keeps the run's output after it finishes, for
	// IdempotencyTTL, across restarts: Get, Wait and Submit return it
	// instead of a trimmed run (ADR 0050). For runs of the file tier or
	// above; it costs one blob write when the run finishes and one delete
	// when its marker expires.
	KeepOutput bool `json:"keep_output,omitempty"`
}

// RunLimits bound one run (ADR 0030): the number of steps, waits and
// containers it starts, and its total duration including waits. Zero
// means no limit.
type RunLimits struct {
	MaxSteps    int           `json:"max_steps,omitempty"`
	MaxDuration time.Duration `json:"max_duration,omitempty"`
}

// RunInfo describes a run.
type RunInfo struct {
	RunID   string          `json:"run_id"`
	Plan    string          `json:"plan"`
	Tenant  string          `json:"tenant"`
	Tier    Tier            `json:"tier"`
	Status  string          `json:"status"`
	Output  json.RawMessage `json:"output,omitempty"`
	Error   string          `json:"error,omitempty"`
	Evicted bool            `json:"evicted,omitempty"`
	Reviews []Review        `json:"reviews,omitempty"`
	// Waits lists steps waiting for a signal (approvals, webhooks, user
	// messages) while the run has nothing in flight.
	Waits []core.Wait `json:"waits,omitempty"`
	// Trimmed: only the finished-run marker is left (ADR 0027); Status and
	// FinishedAt are set, the output and other details are gone.
	Trimmed bool `json:"trimmed,omitempty"`
	// Exceptions counts steps that failed into their on_error strategy;
	// Dify reports such a run as partial-succeeded (ADR 0030).
	Exceptions int `json:"exceptions,omitempty"`
	// Vars are the run variables' current (when finished: final) values;
	// the host persists them (ADR 0033).
	Vars       json.RawMessage `json:"vars,omitempty"`
	FinishedAt time.Time       `json:"finished_at,omitzero"`
	// Input is the run's input, as it was submitted: given by GetInput
	// only (for a run not finished), not by Get.
	Input json.RawMessage `json:"input,omitempty"`
}

type Review struct {
	Act    uint32 `json:"act"`
	StepID string `json:"step_id"`
}

var (
	ErrUnknownPlan = errors.New("engine: unknown plan")
	ErrUnknownRun  = errors.New("engine: unknown run")
	ErrTier        = errors.New("engine: durability tier not available")
	ErrClosed      = errors.New("engine: closed")
)

type Engine struct {
	cfg    Config
	shards []*shard
	disp   *sched.Dispatcher
	adm    *sched.Admission
	live   *live.Hub
	obs    *obs.Stream
	blobs  blob.Store
	snaps  blob.Store
	tiers  [tierCount]bool
	io     chan func()         // fixed pool of I/O workers
	ioq    *mpsc.Queue[func()] // unbounded, never blocks the caller
	ioStop chan struct{}

	plansMu sync.RWMutex
	plans   map[string]*ir.Plan // by name and by name@hash

	waitMu   sync.Mutex
	waiters  map[string][]chan RunInfo
	finished map[string]RunInfo
	finOrder []string
	runEnd   atomic.Pointer[[]func(runID string)] // ADR 0044

	doneMax int      // markers per shard (ADR 0027)
	feed    *feedHub // nil without Config.Feeds (ADR 0034)

	execMu  sync.Mutex
	pollers []*sched.Poller
	budgets map[string]chan struct{} // in-process tasks at once, by resource (ADR 0039)
	stopped chan struct{}
	wg      sync.WaitGroup // shard loops
	ioWG    sync.WaitGroup
}

// Open creates an engine and recovers the runs found in its logs. Plans
// that recovered runs refer to must be registered in advance with
// RegisterPlan (use New + Recover for that), or passed via plans.
func New(cfg Config) (*Engine, error) {
	if cfg.Shards <= 0 {
		cfg.Shards = runtime.GOMAXPROCS(0)
	}
	if cfg.Registry == nil {
		cfg.Registry = ir.NewRegistry()
	}
	if cfg.BlobThreshold <= 0 {
		cfg.BlobThreshold = 16 << 10
	}
	if cfg.RecentRuns <= 0 {
		cfg.RecentRuns = 100_000
	}
	if cfg.IdempotencyTTL == 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	if cfg.IdempotencyMax <= 0 {
		cfg.IdempotencyMax = 10_000_000
	}
	if cfg.FeedLimit <= 0 {
		cfg.FeedLimit = 1_000_000
	}
	if cfg.CompactEvery == 0 {
		cfg.CompactEvery = 65536
	}
	if cfg.EvictAfter == 0 {
		cfg.EvictAfter = 2 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.EstimateTokens == nil {
		cfg.EstimateTokens = func(t *task.Task) int { return len(t.Input)/4 + 1 }
	}
	if cfg.Observe == nil {
		cfg.Observe = obs.Discard
	}
	e := &Engine{
		cfg:      cfg,
		disp:     sched.NewDispatcher(cfg.Limits),
		adm:      sched.NewAdmission(cfg.Admission),
		live:     live.NewHub(),
		obs:      obs.NewStream(cfg.Observe, 1<<16, 512),
		plans:    map[string]*ir.Plan{},
		waiters:  map[string][]chan RunInfo{},
		finished: map[string]RunInfo{},
		io:       make(chan func()),
		ioq:      mpsc.New[func()](),
		ioStop:   make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	var err error
	if e.blobs = cfg.Blobs; e.blobs == nil {
		if cfg.DataDir != "" {
			if e.blobs, err = blob.NewDir(filepath.Join(cfg.DataDir, "blobs"), cfg.NoSync); err != nil {
				return nil, err
			}
		} else {
			e.blobs = blob.NewMem()
		}
	}
	if e.snaps = cfg.Snapshots; e.snaps == nil {
		if cfg.DataDir != "" {
			if e.snaps, err = blob.NewDir(filepath.Join(cfg.DataDir, "snapshots"), cfg.NoSync); err != nil {
				return nil, err
			}
		} else {
			e.snaps = blob.NewMem()
		}
	}
	for i := 0; i < 16; i++ {
		e.ioWG.Add(1)
		go e.ioWorker()
	}
	go e.ioFeeder()

	sinkFor := cfg.Sinks
	if sinkFor == nil {
		sinkFor = func(t Tier, shard int) (wal.Sink, error) {
			switch t {
			case TierMemory:
				return &wal.MemSink{}, nil
			case TierFile:
				if cfg.DataDir == "" {
					return nil, nil
				}
				fs, err := wal.OpenFile(filepath.Join(cfg.DataDir, "wal"), fmt.Sprintf("shard-%03d", shard), cfg.NoSync)
				if err != nil {
					return nil, err // not a typed nil inside the interface
				}
				if cfg.SegmentSize > 0 {
					fs.SegmentSize = cfg.SegmentSize
				}
				return fs, nil
			}
			return nil, nil
		}
	}
	if cfg.DataDir != "" {
		if err := checkShardCount(cfg.DataDir, cfg.Shards); err != nil {
			return nil, err
		}
	}
	e.tiers[TierNone] = true
	sinks := make([][tierCount]wal.Sink, cfg.Shards)
	for t := TierMemory; t < tierCount; t++ {
		avail := true
		for i := 0; i < cfg.Shards; i++ {
			s, err := sinkFor(t, i)
			if err != nil {
				return nil, err
			}
			if s == nil {
				avail = false
				break
			}
			sinks[i][t] = s
		}
		e.tiers[t] = avail
	}
	if e.cfg.RealMinTier == TierNone {
		if e.tiers[TierFile] {
			e.cfg.RealMinTier = TierFile
		} else {
			e.cfg.RealMinTier = TierMemory
		}
	}
	if cfg.Keys != nil {
		for i := range sinks {
			for t, s := range sinks[i] {
				if s != nil {
					sinks[i][t] = wal.Encrypted(s, cfg.Keys, fmt.Sprintf("shard-%03d/%s", i, Tier(t)))
				}
			}
		}
		e.snaps = blob.Encrypted(e.snaps, cfg.Keys)
		e.blobs = blob.Encrypted(e.blobs, cfg.Keys)
	}
	dones, err := e.doneSinks(sinks)
	if err != nil {
		return nil, err
	}
	e.doneMax = (cfg.IdempotencyMax + cfg.Shards - 1) / cfg.Shards
	e.shards = make([]*shard, cfg.Shards)
	for i := range e.shards {
		e.shards[i] = newShard(e, i, sinks[i], dones[i])
	}
	if len(cfg.Feeds) > 0 {
		e.feed = newFeedHub(e)
		go e.feed.run()
	}
	return e, nil
}

// Start recovers runs from the logs (plans must already be registered) and
// starts the shard loops.
func (e *Engine) Start() error {
	if h := e.feed; h != nil {
		if err := h.load(); err != nil {
			return err
		}
		for _, s := range e.shards {
			s.feedBound = h.bounds[s.id]
		}
	}
	for _, s := range e.shards {
		if err := s.recover(); err != nil {
			return fmt.Errorf("shard %d: %w", s.id, err)
		}
	}
	if h := e.feed; h != nil {
		for _, s := range e.shards {
			for t := TierFile; t < tierCount; t++ {
				if l := s.logs[t]; l != nil {
					h.setStart(s.id, t, l.lsn)
				}
			}
		}
		h.mu.Lock()
		h.computeBounds(true)
		h.mu.Unlock()
	}
	for _, s := range e.shards {
		e.wg.Add(1)
		go s.loop()
	}
	return nil
}

// Close stops the engine. Runs stay in their logs and are recovered by the
// next Start.
func (e *Engine) Close() {
	select {
	case <-e.stopped:
		return
	default:
	}
	close(e.stopped)
	e.execMu.Lock()
	for _, p := range e.pollers {
		e.disp.Unpoll(p)
	}
	e.execMu.Unlock()
	e.disp.AbortAll() // running executors see their context end
	for _, s := range e.shards {
		s.inbox.Push(msg{kind: mStop})
	}
	e.wg.Wait()
	if e.feed != nil {
		close(e.feed.stop)
		<-e.feed.done
	}
	e.disp.Close()
	close(e.ioStop) // the feeder drains what is queued, then closes e.io
	e.ioWG.Wait()
	for _, s := range e.shards {
		s.close()
	}
	e.obs.Close()
}

func (e *Engine) ioWorker() {
	defer e.ioWG.Done()
	for f := range e.io {
		f()
	}
}

// ioFeeder moves queued I/O jobs to the worker pool. It may block on the
// pool; the shards that enqueue never do.
func (e *Engine) ioFeeder() {
	var buf []func()
	for {
		stopping := false
		select {
		case <-e.ioq.Ready():
		case <-e.ioStop:
			stopping = true
		}
		buf = e.ioq.Drain(buf)
		for _, f := range buf {
			e.io <- f
		}
		if stopping && e.ioq.Len() == 0 {
			close(e.io)
			return
		}
	}
}

// doIO runs f off the shard loop on the fixed I/O pool. It never blocks and
// never starts a goroutine: a burst (e.g. many runs evicted at once) only
// lengthens the queue.
func (e *Engine) doIO(f func()) { e.ioq.Push(f) }

// RegisterPlan compiles def and makes it available under its name.
func (e *Engine) RegisterPlan(def *ir.Definition) (*ir.Plan, error) {
	p, err := ir.Compile(def, e.cfg.Registry)
	if err != nil {
		return nil, err
	}
	e.plansMu.Lock()
	e.plans[p.Name] = p
	e.plans[p.Name+"@"+p.Hash] = p
	e.plansMu.Unlock()
	return p, nil
}

// AddPlan registers an already compiled plan (e.g. a thawed ir.Frozen
// recovered from storage). The latest plan added under a name is the one
// new runs use; older versions stay available to recovered runs.
func (e *Engine) AddPlan(p *ir.Plan, latest bool) {
	e.plansMu.Lock()
	if latest || e.plans[p.Name] == nil {
		e.plans[p.Name] = p
	}
	e.plans[p.Name+"@"+p.Hash] = p
	e.plansMu.Unlock()
}

func (e *Engine) plan(name string) *ir.Plan {
	e.plansMu.RLock()
	defer e.plansMu.RUnlock()
	return e.plans[name]
}

func (e *Engine) Registry() *ir.Registry { return e.cfg.Registry }

// Shards is the number of shards; a run's feed entries all carry its shard
// (Cursor.Shard).
func (e *Engine) Shards() int                   { return len(e.shards) }
func (e *Engine) Live() *live.Hub               { return e.live }
func (e *Engine) Dispatcher() *sched.Dispatcher { return e.disp }
func (e *Engine) Blobs() blob.Store             { return e.blobs }

// shardFor maps a run to its shard. The hash must be stable across
// restarts (recovered runs stay in the shard whose log holds them), so it
// is FNV-1a rather than a randomly seeded hash.
func (e *Engine) shardFor(runID string) *shard {
	h := uint64(14695981039346656037)
	for i := 0; i < len(runID); i++ {
		h ^= uint64(runID[i])
		h *= 1099511628211
	}
	return e.shards[h%uint64(len(e.shards))]
}

func newRunID() string {
	var b [12]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// SubmitResult describes an accepted submission.
type SubmitResult struct {
	RunID string `json:"run_id"`
	// Existing: the run id was already running or recently finished; no
	// new run was started (the id is an idempotency key, ADR 0023).
	Existing bool `json:"existing"`
}

var (
	// ErrNotAccepted: ctx ended while the run waited for admission; it was
	// withdrawn and never started.
	ErrNotAccepted = errors.New("engine: not accepted (still waiting for admission when the context ended)")
	// ErrUnconfirmed: the run was started but ctx ended before its start was
	// durable. It may or may not survive a crash: submit again with the
	// same RunID to find out.
	ErrUnconfirmed = errors.New("engine: started but not yet durable when the context ended; retry with the same RunID")
)

// Submit starts a run and returns once its start is durable (immediately
// for TierNone): a run Submit accepted survives a crash (ADR 0023). It
// waits for admission if the engine is at capacity, until ctx ends.
//
// RunID is an idempotency key: submitting an id that is running or
// recently finished (Config.RecentRuns, in this process) starts nothing
// and reports Existing. Without a RunID the engine assigns one and a retry
// would start a second run.
func (e *Engine) Submit(ctx context.Context, req SubmitRequest) (SubmitResult, error) {
	select {
	case <-e.stopped:
		return SubmitResult{}, ErrClosed
	default:
	}
	p := e.plan(req.Plan)
	if p == nil {
		return SubmitResult{}, fmt.Errorf("%w: %q", ErrUnknownPlan, req.Plan)
	}
	if e.cfg.MaxDepth > 0 && req.Depth > e.cfg.MaxDepth {
		return SubmitResult{}, fmt.Errorf("%w: depth %d > %d", ErrTooDeep, req.Depth, e.cfg.MaxDepth)
	}
	if req.Entry != "" && !hasEntry(p, req.Entry) {
		return SubmitResult{}, fmt.Errorf("%w: %q is not an entry of plan %s", ErrUnknownEntry, req.Entry, p.Name)
	}
	tier := e.cfg.DefaultTier
	if req.Tier != nil {
		tier = *req.Tier
	}
	if p.HasReal && tier < e.cfg.RealMinTier {
		tier = e.cfg.RealMinTier
	}
	if tier >= tierCount || !e.tiers[tier] {
		return SubmitResult{}, fmt.Errorf("%w: %s", ErrTier, tier)
	}
	id := req.RunID
	if id == "" {
		id = newRunID()
	} else {
		e.waitMu.Lock()
		_, done := e.finished[id]
		e.waitMu.Unlock()
		if !done {
			_, done = e.marker(id)
		}
		if done {
			return SubmitResult{RunID: id, Existing: true}, nil
		}
	}
	reply := make(chan startReply, 1)
	// A request's limits apply field by field; zero keeps the configured one.
	lim := e.cfg.RunLimits
	if l := req.Limits; l != nil {
		if l.MaxSteps > 0 {
			lim.MaxSteps = l.MaxSteps
		}
		if l.MaxDuration > 0 {
			lim.MaxDuration = l.MaxDuration
		}
	}
	sr := &startReq{runID: id, tenant: req.Tenant, plan: p, tier: tier, input: req.Input, entry: req.Entry, limits: lim, depth: req.Depth, vars: req.Vars,
		keep: req.KeepOutput && tier >= TierFile, reply: reply}
	sh := e.shardFor(id)
	ticket, err := e.adm.Admit(req.Tenant, func() { sh.inbox.Push(msg{kind: mStart, start: sr}) })
	if err != nil {
		return SubmitResult{}, err
	}
	select {
	case r := <-reply:
		return SubmitResult{RunID: id, Existing: r.existing}, nil
	case <-ctx.Done():
		if ticket.Cancel() {
			return SubmitResult{}, ErrNotAccepted
		}
		return SubmitResult{RunID: id}, ErrUnconfirmed
	}
}

// ErrUnknownEntry: SubmitRequest.Entry names no entry of the plan's root
// graph.
var ErrUnknownEntry = errors.New("engine: unknown entry")

// ErrTooDeep: SubmitRequest.Depth exceeds Config.MaxDepth.
var ErrTooDeep = errors.New("engine: workflow nested too deep")

func hasEntry(p *ir.Plan, id string) bool {
	root := &p.Nodes[0]
	if root.Kind != ir.KGraph || root.Sugar != ir.SugarGraph {
		return false
	}
	for _, mi := range root.Entry {
		if p.Nodes[root.Children[mi]].ID == id {
			return true
		}
	}
	return false
}

// Signal delivers an external event (webhook, approval) to a run.
func (e *Engine) Signal(runID, name string, payload json.RawMessage) {
	e.shardFor(runID).inbox.Push(msg{kind: mEvent, runID: runID, ev: core.Event{Kind: core.EvSignal, Name: name, Data: payload}})
}

// SignalStep delivers a signal to one specific waiting step (an entry of
// RunInfo.Waits). It is dropped if that step is not waiting for name.
func (e *Engine) SignalStep(runID string, act uint32, name string, payload json.RawMessage) {
	e.shardFor(runID).inbox.Push(msg{kind: mEvent, runID: runID, ev: core.Event{Kind: core.EvSignal, Act: act, Name: name, Data: payload}})
}

// Resolve settles a step that needs review: either with its output (it did
// take effect) or with an error (it did not, and the run fails).
func (e *Engine) Resolve(runID string, act uint32, output json.RawMessage, errMsg string) {
	e.shardFor(runID).inbox.Push(msg{kind: mEvent, runID: runID, ev: core.Event{Kind: core.EvResolve, Act: act, Data: output, Err: errMsg}})
}

func (e *Engine) Cancel(runID, reason string) {
	e.shardFor(runID).inbox.Push(msg{kind: mEvent, runID: runID, ev: core.Event{Kind: core.EvCancel, Err: reason}})
}

// Get returns the current state of a run.
func (e *Engine) Get(ctx context.Context, runID string) (RunInfo, error) {
	e.waitMu.Lock()
	if ri, ok := e.finished[runID]; ok {
		e.waitMu.Unlock()
		return ri, nil
	}
	e.waitMu.Unlock()
	reply := make(chan queryReply, 1)
	e.shardFor(runID).inbox.Push(msg{kind: mQuery, runID: runID, reply: reply})
	select {
	case r := <-reply:
		if !r.found {
			e.waitMu.Lock()
			ri, ok := e.finished[runID]
			e.waitMu.Unlock()
			if ok {
				return ri, nil
			}
			if ri, ok := e.trimmed(runID); ok {
				return ri, nil
			}
			return RunInfo{}, ErrUnknownRun
		}
		return r.info, nil
	case <-ctx.Done():
		return RunInfo{}, ctx.Err()
	}
}

// GetInput is Get with the run's input, for a run not finished: what the
// host submitted, given back as it was (an SDK reads its own data in it,
// such as a workflow's version, ADR 0060). An evicted run is loaded for
// it: the cost is paid by who asks, not by every run.
func (e *Engine) GetInput(ctx context.Context, runID string) (RunInfo, error) {
	e.waitMu.Lock()
	if ri, ok := e.finished[runID]; ok {
		e.waitMu.Unlock()
		return ri, nil
	}
	e.waitMu.Unlock()
	reply := make(chan queryReply, 1)
	e.shardFor(runID).inbox.Push(msg{kind: mQueryInput, runID: runID, reply: reply})
	select {
	case r := <-reply:
		if !r.found {
			return e.Get(ctx, runID)
		}
		return r.info, nil
	case <-ctx.Done():
		return RunInfo{}, ctx.Err()
	}
}

// Wait blocks until the run finishes (and its completion is durable).
func (e *Engine) Wait(ctx context.Context, runID string) (RunInfo, error) {
	ch := make(chan RunInfo, 1)
	e.waitMu.Lock()
	if ri, ok := e.finished[runID]; ok {
		e.waitMu.Unlock()
		return ri, nil
	}
	if ri, ok := e.trimmed(runID); ok {
		e.waitMu.Unlock()
		return ri, nil
	}
	e.waiters[runID] = append(e.waiters[runID], ch)
	e.waitMu.Unlock()
	select {
	case ri := <-ch:
		return ri, nil
	case <-ctx.Done():
		return RunInfo{}, ctx.Err()
	}
}

// trimmed describes a run known only by its finished-run marker: with its
// output if it was kept (ADR 0050; read here, in the caller's goroutine),
// else as trimmed.
func (e *Engine) trimmed(runID string) (RunInfo, bool) {
	en, ok := e.marker(runID)
	if !ok {
		return RunInfo{}, false
	}
	ri := RunInfo{RunID: runID, Status: en.status, Trimmed: true, FinishedAt: time.UnixMilli(en.at)}
	if en.kept {
		if out, err := e.blobs.Get(keptKey(runID, en.gen)); err == nil {
			ri.Output, ri.Trimmed = out, false
		} else if !errors.Is(err, blob.ErrNotFound) {
			log.Printf("kairo: run %s: reading its kept output: %v", runID, err)
		}
	}
	return ri, true
}

// keepOutput durably puts a finished run's output (with its blob
// references inlined) where trimmed finds it (ADR 0050). I/O: not from the
// shard loop.
func (e *Engine) keepOutput(runID string, gen uint64, out json.RawMessage) error {
	if bytes.Contains(out, []byte(`"$blob"`)) {
		var err error
		if out, err = e.ResolveInput(out); err != nil {
			return err
		}
	}
	return e.blobs.Put(keptKey(runID, gen), out)
}

// deleteKept deletes a kept output (ADR 0050); one already gone is fine.
// I/O: not from the shard loop.
func (e *Engine) deleteKept(runID string, gen uint64) error {
	if err := e.blobs.Delete(keptKey(runID, gen)); err != nil && !errors.Is(err, blob.ErrNotFound) {
		log.Printf("kairo: run %s: deleting its kept output: %v", runID, err)
		return err
	}
	return nil
}

// doneSinks opens the per-shard logs of finished-run markers (ADR 0027).
// None are needed if markers are disabled or no shard has a log tier that
// survives a restart.
func (e *Engine) doneSinks(sinks [][tierCount]wal.Sink) ([]wal.Sink, error) {
	cfg := e.cfg
	out := make([]wal.Sink, cfg.Shards)
	if cfg.IdempotencyTTL < 0 || (!e.tiers[TierFile] && !e.tiers[TierReplicated]) {
		return out, nil
	}
	if cfg.DoneLogs == nil && cfg.DataDir == "" {
		log.Printf("kairo: no DataDir and no DoneLogs: finished-run markers are kept in memory and lost on restart")
	}
	for i := range out {
		var s wal.Sink
		var err error
		switch {
		case cfg.DoneLogs != nil:
			s, err = cfg.DoneLogs(i)
		case cfg.DataDir != "":
			var fs *wal.FileSink
			if fs, err = wal.OpenFile(filepath.Join(cfg.DataDir, "wal"), fmt.Sprintf("done-%03d", i), cfg.NoSync); err == nil {
				s = fs
			}
		default:
			s = &wal.MemSink{}
		}
		if err != nil {
			return nil, err
		}
		if s == nil {
			return nil, fmt.Errorf("engine: DoneLogs returned no sink for shard %d", i)
		}
		if cfg.Keys != nil {
			s = wal.Encrypted(s, cfg.Keys, fmt.Sprintf("shard-%03d/done", i))
		}
		out[i] = s
	}
	return out, nil
}

// rememberFinished records a finished run for Get and Submit's
// idempotency without waking waiters (they are woken by finish).
func (e *Engine) rememberFinished(ri RunInfo) {
	e.waitMu.Lock()
	e.finished[ri.RunID] = ri
	e.waitMu.Unlock()
}

func (e *Engine) finish(ri RunInfo) {
	e.waitMu.Lock()
	ws := e.waiters[ri.RunID]
	delete(e.waiters, ri.RunID)
	e.finished[ri.RunID] = ri
	e.finOrder = append(e.finOrder, ri.RunID)
	if keep := e.cfg.RecentRuns; len(e.finOrder) > keep {
		old := e.finOrder[:len(e.finOrder)-keep]
		for _, id := range old {
			delete(e.finished, id)
		}
		e.finOrder = append([]string(nil), e.finOrder[len(old):]...)
	}
	e.waitMu.Unlock()
	for _, w := range ws {
		w <- ri
	}
	e.live.End(ri.RunID)
	e.disp.RunEnded(ri.RunID) // ADR 0046
	if fns := e.runEnd.Load(); fns != nil {
		for _, fn := range *fns {
			fn(ri.RunID)
		}
	}
}

// OnRunEnd registers fn to be called when a run has finished, once its
// end is durable (when Wait returns). It is called from the shard loop, so
// it must not block (ADR 0044).
func (e *Engine) OnRunEnd(fn func(runID string)) {
	e.waitMu.Lock()
	defer e.waitMu.Unlock()
	var fns []func(string)
	if old := e.runEnd.Load(); old != nil {
		fns = append(fns, *old...)
	}
	fns = append(fns, fn)
	e.runEnd.Store(&fns)
}

// Finished reports whether the run has finished, as far as the engine
// remembers (the recent runs, ADR 0023).
func (e *Engine) Finished(runID string) bool {
	e.waitMu.Lock()
	defer e.waitMu.Unlock()
	_, ok := e.finished[runID]
	return ok
}

// Stats is a snapshot of engine counters.
type Stats struct {
	Active, AdmissionQueued int
	DispatchQueued          int
	InMemory, Evicted       int
	ObsDropped              uint64
	// FeedBacklog is the most unacknowledged feed entries any subscription
	// holds (ADR 0034).
	FeedBacklog int
	// FailedLogs counts shard logs whose writes failed with an unknown
	// outcome. Their real commands and completions are held: those runs
	// cannot make progress until the engine is restarted.
	FailedLogs int
	// Dests is each destination's concurrency (ADR 0039).
	Dests []sched.DestStat `json:",omitempty"`
}

func (e *Engine) Stats() Stats {
	var st Stats
	st.Active, st.AdmissionQueued = e.adm.Stats()
	st.DispatchQueued = e.disp.Queued()
	st.Dests = e.disp.Dests()
	for _, s := range e.shards {
		st.InMemory += int(s.inMemory.Load())
		st.Evicted += int(s.evicted.Load())
	}
	st.ObsDropped = e.obs.Dropped()
	if e.feed != nil {
		st.FeedBacklog = e.feed.backlog()
	}
	for _, s := range e.shards {
		st.FailedLogs += int(s.failedLogs.Load())
	}
	return st
}

// checkShardCount pins the number of shards of a data directory: runs are
// routed by hash, so changing it would strand recovered runs.
func checkShardCount(dir string, n int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	p := filepath.Join(dir, "SHARDS")
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return os.WriteFile(p, []byte(strconv.Itoa(n)), 0o644)
	}
	if err != nil {
		return err
	}
	if got, _ := strconv.Atoi(strings.TrimSpace(string(b))); got != n {
		return fmt.Errorf("engine: %s was created with %d shards, configured %d", dir, got, n)
	}
	return nil
}
