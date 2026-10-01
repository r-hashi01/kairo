// Package engine wires the pieces into a runtime: shards own runs and apply
// events to the core, the dispatcher allocates quota and feeds pulling
// workers, the durability log, observability stream and live stream are
// three independent write paths.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"kairo/blob"
	"kairo/core"
	"kairo/ir"
	"kairo/live"
	"kairo/mpsc"
	"kairo/obs"
	"kairo/sched"
	"kairo/task"
	"kairo/wal"
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

	// EvictAfter: a run that has nothing in flight and will not be woken
	// for at least this long is snapshotted and dropped from memory.
	// Default 2s; negative disables eviction.
	EvictAfter time.Duration

	Admission sched.AdmissionConfig
	Limits    func(dest string) sched.DestLimits
	// EstimateTokens estimates a task's token use for TPM accounting.
	EstimateTokens func(t *task.Task) int

	Observe obs.Sink

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

	execMu  sync.Mutex
	pollers []*sched.Poller
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
				return wal.OpenFile(filepath.Join(cfg.DataDir, "wal"), fmt.Sprintf("shard-%03d", shard), cfg.NoSync)
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
	e.shards = make([]*shard, cfg.Shards)
	for i := range e.shards {
		e.shards[i] = newShard(e, i, sinks[i])
	}
	return e, nil
}

// Start recovers runs from the logs (plans must already be registered) and
// starts the shard loops.
func (e *Engine) Start() error {
	for _, s := range e.shards {
		if err := s.recover(); err != nil {
			return fmt.Errorf("shard %d: %w", s.id, err)
		}
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
	for _, s := range e.shards {
		s.inbox.Push(msg{kind: mStop})
	}
	e.wg.Wait()
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

func (e *Engine) Registry() *ir.Registry        { return e.cfg.Registry }
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

// Submit admits a run. It returns once the run is started or queued for
// admission; it never waits for execution.
func (e *Engine) Submit(req SubmitRequest) (string, error) {
	select {
	case <-e.stopped:
		return "", ErrClosed
	default:
	}
	p := e.plan(req.Plan)
	if p == nil {
		return "", fmt.Errorf("%w: %q", ErrUnknownPlan, req.Plan)
	}
	tier := e.cfg.DefaultTier
	if req.Tier != nil {
		tier = *req.Tier
	}
	if p.HasReal && tier < e.cfg.RealMinTier {
		tier = e.cfg.RealMinTier
	}
	if tier >= tierCount || !e.tiers[tier] {
		return "", fmt.Errorf("%w: %s", ErrTier, tier)
	}
	id := req.RunID
	if id == "" {
		id = newRunID()
	}
	sr := &startReq{runID: id, tenant: req.Tenant, plan: p, tier: tier, input: req.Input}
	sh := e.shardFor(id)
	_, err := e.adm.Admit(req.Tenant, func() { sh.inbox.Push(msg{kind: mStart, start: sr}) })
	if err != nil {
		return "", err
	}
	return id, nil
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
			return RunInfo{}, ErrUnknownRun
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
	e.waiters[runID] = append(e.waiters[runID], ch)
	e.waitMu.Unlock()
	select {
	case ri := <-ch:
		return ri, nil
	case <-ctx.Done():
		return RunInfo{}, ctx.Err()
	}
}

const finishedCache = 100_000

func (e *Engine) finish(ri RunInfo) {
	e.waitMu.Lock()
	ws := e.waiters[ri.RunID]
	delete(e.waiters, ri.RunID)
	e.finished[ri.RunID] = ri
	e.finOrder = append(e.finOrder, ri.RunID)
	if len(e.finOrder) > finishedCache {
		old := e.finOrder[:len(e.finOrder)-finishedCache]
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
}

// Stats is a snapshot of engine counters.
type Stats struct {
	Active, AdmissionQueued int
	DispatchQueued          int
	InMemory, Evicted       int
	ObsDropped              uint64
}

func (e *Engine) Stats() Stats {
	var st Stats
	st.Active, st.AdmissionQueued = e.adm.Stats()
	st.DispatchQueued = e.disp.Queued()
	for _, s := range e.shards {
		st.InMemory += int(s.inMemory.Load())
		st.Evicted += int(s.evicted.Load())
	}
	st.ObsDropped = e.obs.Dropped()
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
