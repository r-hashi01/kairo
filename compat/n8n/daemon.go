package n8n

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
)

// FeedName is the engine feed kairo-n8n reads (ADR 0034).
const FeedName = "n8n"

// Daemon is kairo-n8n: engine v2's data plane on kairo (ADR 0042). It
// answers the control plane's HTTP calls, runs the graphs on the engine,
// keeps the records of executions and steps, and tells the control plane
// what happened.
type Daemon struct {
	E         *engine.Engine
	Views     *Views
	Secret    []byte     // shared with the control plane (>= 32 characters)
	Events    *Events    // lifecycle events to the control plane (nil: none)
	Responses *Responses // execution responses on Redis (nil: none)
	Dir       string     // keeps the plans, for runs recovered after a restart
	// WorkerToken authenticates n8n's workers on /internal/ (Bearer).
	WorkerToken string
	Now         func() time.Time

	mu    sync.Mutex
	plans map[string]*ir.Plan // registered plans, by name
	runs  map[string]*runInfo // executions being followed, by id
	// Requests waiting for a step's start, by step row id (see stepData).
	waiters map[string][]chan struct{}
}

// runInfo is what translating an execution's traces needs.
type runInfo struct {
	exec     *Execution
	plan     *ir.Plan
	names    map[string]string // node id -> node name
	last     *Step             // the step that settled last
	lastName string
	// ended holds, by batch loop (plan node), the round its slice found
	// nothing left: the loop's terminal round, which has no body steps.
	ended   map[int32]int
	trigger string // the trigger node's id
	// restored: rebuilt from the records after a restart, so steps that
	// started before it are known only there.
	restored bool
	// What the execution's steps read (ADR 0042), under Daemon.mu: the
	// steps that started and the completed steps' outputs, by node and
	// round. Kept from the traces as they are read, before they are
	// recorded: the traces are durable in kairo's log already.
	started map[string]bool
	outputs map[string]map[string]json.RawMessage
	// waiting holds the steps that wait for their deadline (ADR 0045):
	// while there are some, a step's start or end sets the execution's
	// status again.
	waiting map[string]bool
}

func (d *Daemon) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// Load registers the plans kept in Dir (before the engine starts, so that
// recovered runs find them).
func (d *Daemon) Load() error {
	d.mu.Lock()
	if d.plans == nil {
		d.plans, d.runs = map[string]*ir.Plan{}, map[string]*runInfo{}
		d.waiters = map[string][]chan struct{}{}
	}
	d.mu.Unlock()
	if d.Dir == "" {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(d.Dir, "plans", "*.json"))
	if err != nil {
		return err
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var def ir.Definition
		if err := json.Unmarshal(b, &def); err != nil {
			return fmt.Errorf("kairo-n8n: plan %s: %w", f, err)
		}
		p, err := d.E.RegisterPlan(&def)
		if err != nil {
			return fmt.Errorf("kairo-n8n: plan %s: %w", f, err)
		}
		d.plans[def.Name] = p
	}
	return nil
}

// plan registers the plan of graph (once) and returns its name. Plans are
// named by the graph's content: the same graph is the same plan.
func (d *Daemon) plan(graph json.RawMessage, g *Graph) (string, error) {
	sum := sha256.Sum256(graph)
	name := "n8n~" + hex.EncodeToString(sum[:12]) + "~v" + strconv.Itoa(ConverterVersion)
	d.mu.Lock()
	known := d.plans[name] != nil
	d.mu.Unlock()
	if known {
		return name, nil
	}
	def, err := Convert(name, g)
	if err != nil {
		return "", err
	}
	p, err := d.E.RegisterPlan(def)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnsupported, err)
	}
	if d.Dir != "" {
		b, _ := json.Marshal(def)
		dir := filepath.Join(d.Dir, "plans")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
		tmp := filepath.Join(dir, name+".tmp")
		if err := os.WriteFile(tmp, b, 0o600); err != nil {
			return "", err
		}
		if err := os.Rename(tmp, filepath.Join(dir, name+".json")); err != nil {
			return "", err
		}
	}
	d.mu.Lock()
	d.plans[name] = p
	d.mu.Unlock()
	return name, nil
}

// --- HTTP: engine v2's contract -----------------------------------------------

// Handler serves GET /healthz and, authenticated, /api/workflow-executions.
func (d *Daemon) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, map[string]string{"status": "ok"})
	})
	mux.Handle("POST /api/workflow-executions", d.auth(d.start))
	mux.Handle("POST /api/workflow-executions/search", d.auth(d.search))
	mux.Handle("GET /api/workflow-executions/{id}", d.auth(d.get))
	mux.Handle("POST /internal/step-data", d.workerAuth(d.stepData))
	return mux
}

func (d *Daemon) workerAuth(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || d.WorkerToken == "" || subtle.ConstantTimeCompare([]byte(tok), []byte(d.WorkerToken)) != 1 {
			reply(w, 401, map[string]string{"error": "unauthenticated"})
			return
		}
		h(w, r)
	})
}

func (d *Daemon) auth(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || VerifyCP(d.Secret, tok, d.now()) != nil {
			reply(w, 401, map[string]string{"error": "unauthenticated"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
		h(w, r)
	})
}

func reply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// fail answers engine v2's error shape {error, reason?}.
func fail(w http.ResponseWriter, code int, kind string, reason string) {
	body := map[string]string{"error": kind}
	if reason != "" {
		body["reason"] = reason
	}
	reply(w, code, body)
}

var uuidV7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func strictDecode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// POST /api/workflow-executions: StartExecutionRequest -> 201 {executionId}.
func (d *Daemon) start(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID    string          `json:"executionId"`
		WorkflowID     string          `json:"workflowId"`
		Graph          json.RawMessage `json:"graph"`
		Workflow       json.RawMessage `json:"workflow"`
		TriggerOutputs json.RawMessage `json:"triggerOutputs"`
		Mode           string          `json:"mode"`
		CallerContext  *struct {
			UserID    *string `json:"userId,omitempty"`
			ProjectID *string `json:"projectId,omitempty"`
			HostMode  string  `json:"hostMode"`
		} `json:"callerContext"`
	}
	if err := strictDecode(r, &req); err != nil {
		fail(w, 400, "invalid_request", err.Error())
		return
	}
	switch {
	case !uuidV7.MatchString(req.ExecutionID):
		fail(w, 400, "invalid_request", "executionId must be a UUIDv7")
		return
	case req.WorkflowID == "":
		fail(w, 400, "invalid_request", "workflowId is required")
		return
	case req.CallerContext == nil || req.CallerContext.HostMode == "":
		fail(w, 400, "invalid_request", "callerContext.hostMode is required")
		return
	case len(req.Workflow) == 0 || req.Workflow[0] != '{':
		fail(w, 400, "invalid_request", "workflow must be an object")
		return
	}
	if req.Mode == "" {
		req.Mode = "production"
	}
	if req.Mode != "production" && req.Mode != "manual" {
		fail(w, 400, "invalid_request", "mode must be production or manual")
		return
	}
	trigger := json.RawMessage("[]")
	if t := strings.TrimSpace(string(req.TriggerOutputs)); t != "" && t != "null" {
		var slots []json.RawMessage
		if json.Unmarshal(req.TriggerOutputs, &slots) != nil || len(slots) < 1 || len(slots) > ir.MaxPort+1 {
			fail(w, 400, "invalid_request", "triggerOutputs must hold 1 to 101 slots")
			return
		}
		trigger = req.TriggerOutputs
	}
	var g Graph
	if err := json.Unmarshal(req.Graph, &g); err != nil {
		fail(w, 400, "invalid_graph", err.Error())
		return
	}
	name, err := d.plan(req.Graph, &g)
	if err != nil {
		if errors.Is(err, ErrUnsupported) {
			fail(w, 501, "unimplemented", err.Error())
		} else {
			fail(w, 400, "invalid_graph", err.Error())
		}
		return
	}
	cc, _ := json.Marshal(req.CallerContext)
	now := d.now().UTC()
	x := &Execution{ID: req.ExecutionID, WorkflowID: req.WorkflowID, Mode: req.Mode, HostMode: req.CallerContext.HostMode,
		Graph: req.Graph, Workflow: req.Workflow, TriggerOutputs: req.TriggerOutputs, CallerContext: cc, CreatedAt: now}
	if err := d.Views.Insert(r.Context(), x); err != nil {
		fail(w, 409, "conflict", "the execution exists already")
		return
	}
	// The trigger did not run: its step is completed from the start, with
	// the trigger's outputs.
	if err := d.recordTrigger(r.Context(), x.ID, &g, trigger, now); err != nil {
		log.Printf("kairo-n8n: execution %s: %v", x.ID, err)
	}
	in, _ := json.Marshal(map[string]json.RawMessage{TriggerInput: trigger})
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if _, err := d.E.Submit(ctx, engine.SubmitRequest{Plan: name, RunID: x.ID, Tenant: x.WorkflowID, Input: in}); err != nil {
		tx, terr := d.Views.Begin(r.Context())
		if terr == nil {
			d.Views.Finish(r.Context(), tx, x.ID, "failed", d.now().UTC())
			tx.Commit()
		}
		fail(w, 503, "not_admitted", err.Error())
		return
	}
	reply(w, 201, map[string]string{"executionId": x.ID})
}

func (d *Daemon) recordTrigger(ctx context.Context, execID string, g *Graph, outputs json.RawMessage, at time.Time) error {
	for _, n := range g.Nodes {
		if n.Type != "trigger" {
			continue
		}
		tx, err := d.Views.Begin(ctx)
		if err != nil {
			return err
		}
		s := &Step{ID: stepID(execID, n.ID, 0), NodeID: n.ID, Status: "completed", Outputs: outputs, UpdatedAt: at}
		if err := d.Views.PutStep(ctx, tx, execID, s); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	return nil
}

// GET /api/workflow-executions/{id}[?includeSteps=true]: ExecutionSnapshot.
func (d *Daemon) get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for k := range q {
		if k != "includeSteps" {
			fail(w, 400, "invalid_request", "unknown query "+k)
			return
		}
	}
	steps := false
	switch q.Get("includeSteps") {
	case "", "false":
	case "true":
		steps = true
	default:
		fail(w, 400, "invalid_request", "includeSteps must be true or false")
		return
	}
	x, ss, err := d.Views.Get(r.Context(), r.PathValue("id"), steps)
	if err != nil {
		fail(w, 500, "internal", "")
		return
	}
	if x == nil {
		fail(w, 404, "not_found", "")
		return
	}
	out := struct {
		*Execution
		Steps []Step `json:"steps,omitempty"`
	}{x, ss}
	reply(w, 200, out)
}

// POST /api/workflow-executions/search: SearchExecutionsRequest.
func (d *Daemon) search(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkflowIDs   json.RawMessage `json:"workflowIds"`
		Status        []string        `json:"status"`
		HostMode      string          `json:"hostMode"`
		CreatedAfter  *time.Time      `json:"createdAfter"`
		CreatedBefore *time.Time      `json:"createdBefore"`
		Before        *Cursor         `json:"before"`
		Limit         int             `json:"limit"`
		IncludeTotal  bool            `json:"includeTotal"`
		Order         *struct {
			Top       string `json:"top"`
			StartedAt string `json:"startedAt"`
		} `json:"order"`
	}
	if err := strictDecode(r, &req); err != nil {
		fail(w, 400, "invalid_request", err.Error())
		return
	}
	q := Search{Status: req.Status, HostMode: req.HostMode, After: req.CreatedAfter, Before: req.CreatedBefore,
		Cursor: req.Before, Limit: req.Limit, IncludeTotal: req.IncludeTotal}
	if q.Limit == 0 {
		q.Limit = 20
	}
	if q.Limit < 1 || q.Limit > 100 {
		fail(w, 400, "invalid_request", "limit must be 1..100")
		return
	}
	if string(req.WorkflowIDs) != `"all"` {
		if err := json.Unmarshal(req.WorkflowIDs, &q.WorkflowIDs); err != nil || len(q.WorkflowIDs) < 1 || len(q.WorkflowIDs) > 10000 {
			fail(w, 400, "invalid_request", `workflowIds must be "all" or 1..10000 ids`)
			return
		}
	}
	if req.Status != nil && len(req.Status) == 0 {
		fail(w, 400, "invalid_request", "status must not be empty")
		return
	}
	if req.Order != nil {
		q.Top = req.Order.Top
		if q.Top != "" && q.Cursor != nil {
			fail(w, 400, "invalid_request", "before and order.top cannot be used together")
			return
		}
	}
	items, next, total, err := d.Views.Find(r.Context(), q)
	if err != nil {
		fail(w, 500, "internal", "")
		return
	}
	out := map[string]any{"items": items, "nextCursor": next}
	if total != nil {
		out["total"] = *total
	}
	reply(w, 200, out)
}

// --- the runs' traces: records and lifecycle events ----------------------------

// Consume reads the engine's feed until ctx ends: each batch of traces is
// recorded in one transaction, then acknowledged; lifecycle events and
// responses follow the records.
func (d *Daemon) Consume(ctx context.Context) error {
	for ctx.Err() == nil {
		f, err := d.E.Subscribe(FeedName)
		if err != nil {
			return err
		}
		err = d.consume(ctx, f)
		f.Close()
		switch {
		case errors.Is(err, engine.ErrLagged):
			log.Printf("kairo-n8n: event feed lagged behind; reading again from the present")
		case errors.Is(err, engine.ErrFeedClosed), ctx.Err() != nil:
			return nil
		default:
			return err
		}
	}
	return nil
}

func (d *Daemon) consume(ctx context.Context, f *engine.Feed) error {
	// Reading and recording overlap: while a batch is recorded, the next
	// is read, and what steps read is up to date as soon as a trace is
	// read (ADR 0042). Batches are recorded, then acknowledged, in order.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	batches := make(chan *batch, 64)
	recorded := make(chan error, 1)
	go func() {
		recorded <- d.record(ctx, f, batches)
		cancel() // the reader stops too
	}()
	for {
		entries, err := f.Next(ctx, 1024)
		if err == nil {
			var b *batch
			if b, err = d.read(ctx, entries); err == nil {
				select {
				case batches <- b:
					continue
				case <-ctx.Done():
					err = ctx.Err()
				}
			}
		}
		close(batches)
		if rerr := <-recorded; rerr != nil && !errors.Is(rerr, context.Canceled) {
			return rerr
		}
		return err
	}
}

// batch is what a batch of traces records, then notifies and acknowledges.
type batch struct {
	w      writes
	events []LifecycleEvent
	ended  []map[string]any
	acks   map[[2]int]engine.Cursor
}

// read translates a batch of traces (see translate).
func (d *Daemon) read(ctx context.Context, entries []engine.FeedEntry) (*batch, error) {
	b := &batch{acks: map[[2]int]engine.Cursor{}}
	for _, en := range entries {
		if en.Cursor.LSN > 0 {
			b.acks[[2]int{en.Cursor.Shard, int(en.Cursor.Tier)}] = en.Cursor
		}
		if en.Trace == nil {
			continue // live chunks: nothing to record
		}
		ri, err := d.run(ctx, en.RunID)
		if err != nil {
			return nil, err
		}
		if ri == nil {
			continue
		}
		evs, end := d.translate(ri, en.Trace, &b.w)
		b.events = append(b.events, evs...)
		if end != nil {
			b.ended = append(b.ended, end)
		}
	}
	return b, nil
}

// record records the batches, then notifies and acknowledges them: the
// batches waiting together in one transaction, so that recording keeps up
// however small the batches read are.
func (d *Daemon) record(ctx context.Context, f *engine.Feed, batches <-chan *batch) error {
	for b := range batches {
	more:
		for len(b.w) < 4096 {
			select {
			case nb, ok := <-batches:
				if !ok {
					break more
				}
				b.w = append(b.w, nb.w...)
				b.events = append(b.events, nb.events...)
				b.ended = append(b.ended, nb.ended...)
				for k, c := range nb.acks {
					b.acks[k] = c // later in the same shard and tier
				}
			default:
				break more
			}
		}
		tx, err := d.Views.Begin(ctx)
		if err != nil {
			return err
		}
		for _, op := range b.w {
			if err := op(ctx, tx); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		d.Events.Send(b.events...)
		for _, e := range b.ended {
			if err := d.Responses.Publish(e["executionId"].(string), e); err != nil {
				log.Printf("kairo-n8n: execution %s: response not published: %v", e["executionId"], err)
			}
		}
		for _, c := range b.acks {
			if err := f.Ack(c); err != nil {
				return err
			}
		}
	}
	return nil
}

// run is what translating execution id's traces needs (nil: not an
// execution of this daemon).
func (d *Daemon) run(ctx context.Context, id string) (*runInfo, error) {
	d.mu.Lock()
	ri := d.runs[id]
	d.mu.Unlock()
	if ri != nil {
		return ri, nil
	}
	x, _, err := d.Views.Get(ctx, id, false)
	if err != nil || x == nil {
		return nil, err
	}
	info, err := d.E.Get(ctx, id)
	if err != nil {
		return nil, nil // gone: its traces come from before a restart
	}
	plan, err := d.planOf(info.Plan)
	if err != nil {
		return nil, err
	}
	var g Graph
	json.Unmarshal(x.Graph, &g)
	names := map[string]string{}
	trigger := ""
	for _, n := range g.Nodes {
		names[n.ID] = n.Name
		if n.Type == "trigger" {
			trigger = n.ID
		}
	}
	ri = &runInfo{exec: x, plan: plan, names: names, trigger: trigger}
	if x.Status != "queued" {
		// Its traces were read before a restart: what the earlier ones
		// told is in the records (ADR 0042, 0045). A new execution has no
		// steps yet, and is not read again.
		_, steps, err := d.Views.Get(ctx, id, true)
		if err != nil {
			return nil, err
		}
		ri.restore(steps)
		ri.restored = true
	}
	d.mu.Lock()
	d.runs[id] = ri
	d.mu.Unlock()
	return ri, nil
}

// restore rebuilds, from an execution's step rows, what translating its
// traces keeps in memory: the steps that wait for their deadline, the
// terminal round of each batch loop, and the step that settled last.
func (ri *runInfo) restore(steps []Step) {
	loops := map[string]int32{} // batch node -> its loop (plan node)
	for i := range ri.plan.Nodes {
		if n := &ri.plan.Nodes[i]; n.Kind == ir.KStep && n.Spec.Action == ir.ActionSlice {
			if node, ok := nodeOf(n); ok {
				loops[node] = loopOf(ri.plan, int32(i))
			}
		}
	}
	ri.started = map[string]bool{}
	ri.outputs = map[string]map[string]json.RawMessage{}
	for i := range steps {
		s := &steps[i]
		ri.started[s.ID] = true
		if s.Status == "completed" && len(s.Outputs) > 0 && string(s.Outputs) != "null" {
			if ri.outputs[s.NodeID] == nil {
				ri.outputs[s.NodeID] = map[string]json.RawMessage{}
			}
			ri.outputs[s.NodeID][strconv.Itoa(s.Iteration)] = s.Outputs
		}
		switch s.Status {
		case "waiting":
			if ri.waiting == nil {
				ri.waiting = map[string]bool{}
			}
			ri.waiting[s.ID] = true
		case "running":
		default:
			if ri.last == nil || !s.UpdatedAt.Before(ri.last.UpdatedAt) {
				ri.last, ri.lastName = s, ri.names[s.NodeID]
			}
		}
		if l, ok := loops[s.NodeID]; ok && s.Status == "completed" {
			var ports []json.RawMessage
			if json.Unmarshal(s.Outputs, &ports) == nil && len(ports) >= 2 && string(ports[1]) == "null" {
				if ri.ended == nil {
					ri.ended = map[int32]int{}
				}
				ri.ended[l] = s.Iteration
			}
		}
	}
}

func (d *Daemon) planOf(name string) (*ir.Plan, error) {
	d.mu.Lock()
	p := d.plans[name]
	d.mu.Unlock()
	if p == nil {
		return nil, fmt.Errorf("kairo-n8n: unknown plan %q", name)
	}
	return p, nil
}

// --- what a worker's step reads: the outputs before it -------------------------

// stepData answers a worker about to run a step: the execution's graph and
// every completed step's outputs (engine v2's StepData), with what the
// step's context needs, once the step's start is read from the feed. The
// engine hands the step out as soon as it starts it, and its start (and so
// everything before it in the execution) reaches the daemon a little
// later, through the feed: the answer waits for it. It comes from what the
// daemon keeps in memory, or, for an execution it does not follow (after a
// restart), from the records (ADR 0042).
func (d *Daemon) stepData(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ExecutionID string `json:"executionId"`
		NodeID      string `json:"nodeId"`
		Iteration   int    `json:"iteration"`
	}
	if err := strictDecode(r, &req); err != nil {
		fail(w, 400, "invalid_request", err.Error())
		return
	}
	id := stepID(req.ExecutionID, req.NodeID, req.Iteration)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	ri, err := d.awaitStarted(ctx, req.ExecutionID, id)
	if err != nil {
		fail(w, 503, "not_recorded", "the step's start is not known yet")
		return
	}
	var x *Execution
	byNode := map[string]map[string]json.RawMessage{}
	if ri != nil {
		x = ri.exec
		d.mu.Lock()
		for node, rounds := range ri.outputs {
			byNode[node] = maps.Clone(rounds)
		}
		d.mu.Unlock()
	} else {
		var steps []Step
		if x, steps, err = d.Views.Get(r.Context(), req.ExecutionID, true); err != nil {
			fail(w, 500, "internal", "")
			return
		}
		if x == nil {
			fail(w, 404, "not_found", "")
			return
		}
		for _, s := range steps {
			if s.Status != "completed" || string(s.Outputs) == "null" {
				continue
			}
			if byNode[s.NodeID] == nil {
				byNode[s.NodeID] = map[string]json.RawMessage{}
			}
			byNode[s.NodeID][strconv.Itoa(s.Iteration)] = s.Outputs
		}
	}
	// What the step's context needs (engine v2's StepExecutionContext).
	execution := map[string]any{"workflowId": x.WorkflowID, "mode": x.Mode, "callerContext": x.CallerContext}
	reply(w, 200, map[string]any{"graph": x.Graph, "outputsByNode": byNode, "execution": execution})
}

// remember keeps what the execution's later steps read of step s, and
// wakes the requests waiting for its start.
func (d *Daemon) remember(ri *runInfo, s *Step) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ri.started == nil {
		ri.started = map[string]bool{}
	}
	ri.started[s.ID] = true
	if s.Status == "completed" && len(s.Outputs) > 0 && string(s.Outputs) != "null" {
		if ri.outputs == nil {
			ri.outputs = map[string]map[string]json.RawMessage{}
		}
		if ri.outputs[s.NodeID] == nil {
			ri.outputs[s.NodeID] = map[string]json.RawMessage{}
		}
		ri.outputs[s.NodeID][strconv.Itoa(s.Iteration)] = s.Outputs
	}
	for _, ch := range d.waiters[s.ID] {
		close(ch)
	}
	delete(d.waiters, s.ID)
}

// awaitStarted returns once step sid of execution exec has started: the
// execution as the daemon follows it, or nil when only the records know
// the step (its start was read before a restart).
func (d *Daemon) awaitStarted(ctx context.Context, exec, sid string) (*runInfo, error) {
	started := func() *runInfo {
		if ri := d.runs[exec]; ri != nil && ri.started[sid] {
			return ri
		}
		return nil
	}
	d.mu.Lock()
	if ri := started(); ri != nil {
		d.mu.Unlock()
		return ri, nil
	}
	// An execution followed since it started: its step's start comes
	// through the feed. Otherwise it may have come before a restart, and
	// the records know it.
	ri := d.runs[exec]
	ask := ri == nil || ri.restored
	ch := make(chan struct{})
	d.waiters[sid] = append(d.waiters[sid], ch)
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		ws := d.waiters[sid]
		for i, c := range ws {
			if c == ch {
				d.waiters[sid] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(d.waiters[sid]) == 0 {
			delete(d.waiters, sid)
		}
		d.mu.Unlock()
	}()
	if ask && d.Views.HasStep(ctx, exec, sid) {
		return nil, nil
	}
	select {
	case <-ch:
		d.mu.Lock()
		defer d.mu.Unlock()
		return started(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
