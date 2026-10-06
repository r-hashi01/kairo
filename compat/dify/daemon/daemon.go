// Package daemon is kairo-dify's HTTP API (ADR 0037, 0038): Dify registers
// its workflows, submits runs, answers human input and cancels runs; its
// workers read the runs' events by partition (feed.go) and turn them into
// graphon's graph events and Dify's records (ADR 0036).
package daemon

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kairo/compat/dify"
	"kairo/core"
	"kairo/engine"
	"kairo/ir"
	"kairo/sched"
)

// FeedName is the event feed subscription the daemon consumes; the engine
// must list it in Config.Feeds.
const FeedName = "dify"

// Daemon serves the API for one engine.
// errWithdrawn: a submission cancelled before its run started.
var errWithdrawn = errors.New("kairo-dify: the run was cancelled before it started")

type Daemon struct {
	E *engine.Engine
	// Key is the shared secret clients send as "Authorization: Bearer".
	// Empty disables authentication (tests and local use only).
	Key string
	// Dir persists the registered plans, so a restarted daemon recovers
	// runs with identical plans. Empty disables persistence.
	Dir string

	mu    sync.Mutex
	plans map[string]*ir.Plan // by internal name (planName)
	feed  *feed
	// Submissions waiting for admission, by run id: a cancellation of a
	// run that has not started withdraws it.
	submits map[string]context.CancelFunc
}

// Event is one line of a run's event stream: a trace of the run (ADR
// 0034) with nodes named by their ids, or a chunk of a step's streamed
// output (kind "chunk").
type Event struct {
	// ID names the entry for acknowledgement (absent: nothing to
	// acknowledge); Run and Partition say whose it is; Gen is the
	// partition's assignment it was sent under.
	ID        string          `json:"id,omitempty"`
	Run       string          `json:"run,omitempty"`
	Partition int             `json:"partition"`
	Gen       uint64          `json:"gen,omitempty"`
	Kind      string          `json:"kind"`
	At        int64           `json:"at,omitempty"` // unix milliseconds
	Node      string          `json:"node,omitempty"`
	Step      string          `json:"step,omitempty"`
	Act       uint32          `json:"act,omitempty"`
	Attempt   int32           `json:"attempt,omitempty"`
	Index     *int32          `json:"index,omitempty"`
	Status    string          `json:"status,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	Output    json.RawMessage `json:"output,omitempty"`
	Handle    string          `json:"handle,omitempty"`
	Error     string          `json:"error,omitempty"`
	ErrorType string          `json:"error_type,omitempty"`
	Meta      json.RawMessage `json:"meta,omitempty"`
	Var       string          `json:"var,omitempty"`
	Loop      string          `json:"loop,omitempty"` // the loop of a loop variable; empty: a run variable
	Chunk     string          `json:"chunk,omitempty"`
	// hello
	Conn       uint64 `json:"conn,omitempty"`
	Partitions int    `json:"partitions,omitempty"`
}

func (d *Daemon) init() {
	if d.plans == nil {
		d.plans = map[string]*ir.Plan{}
		d.feed = newFeed(d, d.E.Shards())
	}
}

// planName is the engine's name of the plan Dify registers as name: the
// converter's version is part of it, so a newer converter converts again.
func planName(name string) string {
	return name + "~v" + strconv.Itoa(dify.ConverterVersion)
}

// preparedName is the engine's name of a plan registered with ?prepare=1
// (ADR 0038): its runs wait for their variables (dify.Options.Prepare).
func preparedName(name string) string { return planName(name) + "~p" }

func (d *Daemon) internalName(name string, prepare bool) string {
	if prepare {
		return preparedName(name)
	}
	return planName(name)
}

// Load restores the persisted plans into the engine. Call it before the
// engine starts, so recovered runs find their plans.
func (d *Daemon) Load() error {
	d.init()
	if d.Dir == "" {
		return nil
	}
	files, _ := filepath.Glob(filepath.Join(d.Dir, "plans", "*.json"))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		var fr ir.Frozen
		if err := json.Unmarshal(b, &fr); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		for _, s := range fr.Specs {
			if strings.HasPrefix(s.Action, "dify.") {
				d.E.Registry().Register(s)
			}
		}
		p, err := fr.Thaw()
		if err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
		d.E.AddPlan(p, true)
		d.plans[p.Name] = p
	}
	return nil
}

// Consume reads the engine's event feed into the partitions until ctx
// ends.
func (d *Daemon) Consume(ctx context.Context) error {
	d.init()
	for ctx.Err() == nil {
		f, err := d.E.Subscribe(FeedName)
		if err != nil {
			return err
		}
		err = d.consume(ctx, f)
		f.Close()
		switch {
		case errors.Is(err, engine.ErrLagged):
			log.Printf("kairo-dify: event feed lagged behind; events were lost")
		case errors.Is(err, engine.ErrFeedClosed), ctx.Err() != nil:
			return nil
		default:
			return err
		}
	}
	return nil
}

func (d *Daemon) consume(ctx context.Context, f *engine.Feed) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ackErr := make(chan error, 1)
	go func() { ackErr <- d.feed.ackLoop(ctx, f) }()
	for {
		entries, err := f.Next(ctx, 1024)
		if err != nil {
			return err
		}
		for _, en := range entries {
			d.feed.add(ctx, en)
		}
		select {
		case err := <-ackErr:
			return err
		default:
		}
	}
}

func (d *Daemon) event(p *ir.Plan, en engine.FeedEntry) Event {
	if en.Trace == nil {
		return Event{Kind: "chunk", Step: en.StepID, Node: nodeOf(en.StepID), Chunk: string(en.Chunk)}
	}
	t := en.Trace
	ev := Event{Kind: kinds[t.Kind], At: t.At, Step: t.StepID, Act: t.Act, Attempt: t.Attempt, Status: t.Status,
		Input: d.resolve(t.Input), Output: d.resolve(t.Output), Handle: t.Handle, Error: t.Err, ErrorType: t.ErrType,
		Meta: d.resolve(t.Meta), Var: t.Var}
	switch t.Kind {
	case core.TrNodeStart, core.TrNodeEnd, core.TrNodeSkip, core.TrNodeRetry, core.TrNodeReview, core.TrRoundStart, core.TrRoundEnd:
		ev.Node = nodeID(p, t.Node)
		if ev.Node == "" {
			ev.Node = nodeOf(t.StepID)
		}
	}
	if t.Kind == core.TrRoundStart || t.Kind == core.TrRoundEnd {
		i := t.Index
		ev.Index = &i
	}
	if t.Kind == core.TrVarUpdate && t.Loop >= 0 {
		ev.Loop = nodeID(p, t.Loop)
	}
	return ev
}

// resolve replaces blob references by their content.
func (d *Daemon) resolve(v json.RawMessage) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	out, err := d.E.ResolveInput(v)
	if err != nil {
		log.Printf("kairo-dify: resolving a blob: %v", err)
		return v
	}
	return out
}

func nodeID(p *ir.Plan, i int32) string {
	if p == nil || i < 0 || int(i) >= len(p.Nodes) {
		return ""
	}
	return p.Nodes[i].ID
}

// nodeOf is the node of a step id ("node[1,2]": node, in the rounds of
// its maps and loops).
func nodeOf(step string) string {
	if i := strings.IndexByte(step, '['); i >= 0 {
		return step[:i]
	}
	return step
}

// Handler is the API. Every route needs the key.
func (d *Daemon) Handler() http.Handler {
	d.init()
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/plans/{name}", d.putPlan)
	mux.HandleFunc("GET /v1/plans/{name}", d.getPlan)
	mux.HandleFunc("POST /v1/runs", d.postRun)
	mux.HandleFunc("GET /v1/runs/{id}", d.getRun)
	mux.HandleFunc("GET /v1/feed", d.getFeed)
	mux.HandleFunc("POST /v1/feed/ack", d.postAck)
	mux.HandleFunc("POST /v1/runs/{id}/signal", d.signal)
	mux.HandleFunc("POST /v1/runs/{id}/prepare", d.prepare)
	mux.HandleFunc("POST /v1/runs/{id}/cancel", d.cancel)
	return d.auth(mux)
}

func (d *Daemon) auth(h http.Handler) http.Handler {
	if d.Key == "" {
		return h
	}
	want := []byte("Bearer " + d.Key)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			fail(w, http.StatusUnauthorized, errors.New("unauthorized"))
			return
		}
		h.ServeHTTP(w, r)
	})
}

func reply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	reply(w, code, map[string]string{"error": err.Error()})
}

func readJSON(r *http.Request, v any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

type planInfo struct {
	Name string `json:"name"`
	Hash string `json:"hash"`
}

// putPlan converts a Dify workflow ({"graph", "conversation_variables",
// "environment_variables"}) and registers it under name.
func (d *Daemon) putPlan(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	var wf dify.Workflow
	if err := readJSON(r, &wf); err != nil {
		fail(w, 400, err)
		return
	}
	prepare := r.URL.Query().Get("prepare") == "1"
	conv, err := dify.ConvertWith(d.internalName(name, prepare), &wf, dify.Options{Prepare: prepare})
	if err != nil {
		fail(w, 422, err)
		return
	}
	for _, s := range conv.Specs {
		d.E.Registry().Register(s)
	}
	p, err := d.E.RegisterPlan(conv.Definition)
	if err != nil {
		fail(w, 422, err)
		return
	}
	if d.Dir != "" {
		if err := writeJSON(filepath.Join(d.Dir, "plans", url.PathEscape(p.Name)+".json"), p.Freeze()); err != nil {
			fail(w, 500, err)
			return
		}
	}
	d.mu.Lock()
	d.plans[p.Name] = p
	d.mu.Unlock()
	reply(w, 200, planInfo{name, p.Hash})
}

func (d *Daemon) getPlan(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	p := d.plans[d.internalName(r.PathValue("name"), r.URL.Query().Get("prepare") == "1")]
	d.mu.Unlock()
	if p == nil {
		fail(w, 404, engine.ErrUnknownPlan)
		return
	}
	reply(w, 200, planInfo{r.PathValue("name"), p.Hash})
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type submitReply struct {
	RunID    string `json:"run_id"`
	Existing bool   `json:"existing"`
}

// postRun starts a run (engine.SubmitRequest; run_id is the idempotency
// key) and answers once its start is durable.
func (d *Daemon) postRun(w http.ResponseWriter, r *http.Request) {
	var req engine.SubmitRequest
	if err := readJSON(r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	req.Plan = d.internalName(req.Plan, r.URL.Query().Get("prepare") == "1")
	d.mu.Lock()
	p := d.plans[req.Plan]
	d.mu.Unlock()
	if p == nil {
		fail(w, 404, engine.ErrUnknownPlan)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if req.RunID != "" {
		withdrawn, withdraw := context.WithCancelCause(ctx)
		defer withdraw(nil)
		d.mu.Lock()
		if d.submits == nil {
			d.submits = map[string]context.CancelFunc{}
		}
		d.submits[req.RunID] = func() { withdraw(errWithdrawn) }
		d.mu.Unlock()
		defer func() {
			d.mu.Lock()
			delete(d.submits, req.RunID)
			d.mu.Unlock()
		}()
		ctx = withdrawn
	}
	res, err := d.E.Submit(ctx, req)
	if errors.Is(err, engine.ErrNotAccepted) && errors.Is(context.Cause(ctx), errWithdrawn) {
		fail(w, 409, errWithdrawn)
		return
	}
	switch {
	case errors.Is(err, sched.ErrOverloaded):
		fail(w, 429, err)
		return
	case errors.Is(err, engine.ErrNotAccepted):
		w.Header().Set("Retry-After", "1")
		fail(w, 503, err)
		return
	case errors.Is(err, engine.ErrUnconfirmed):
		reply(w, 504, map[string]string{"error": err.Error(), "run_id": res.RunID})
		return
	case err != nil:
		fail(w, 400, err)
		return
	}
	code := 201
	if res.Existing {
		code = 200
	}
	reply(w, code, submitReply{RunID: res.RunID, Existing: res.Existing})
}

func (d *Daemon) getRun(w http.ResponseWriter, r *http.Request) {
	ri, err := d.E.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, 404, err)
		return
	}
	ri.Output = d.resolve(ri.Output)
	ri.Vars = d.resolve(ri.Vars)
	reply(w, 200, ri)
}

// signal answers a human input node: {"node_id", "handle" (the chosen
// action), "outputs" (the form's fields)}. "act" addresses one
// activation (inside an iteration).
func (d *Daemon) signal(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID  string          `json:"node_id"`
		Act     uint32          `json:"act"`
		Handle  string          `json:"handle"`
		Outputs json.RawMessage `json:"outputs"`
	}
	if err := readJSON(r, &req); err != nil || req.NodeID == "" {
		fail(w, 400, errors.New("signal needs node_id"))
		return
	}
	if len(req.Outputs) == 0 {
		req.Outputs = json.RawMessage("{}")
	}
	payload, _ := json.Marshal(map[string]any{"handle": req.Handle, "outputs": req.Outputs})
	if req.Act != 0 {
		d.E.SignalStep(r.PathValue("id"), req.Act, dify.HumanSignal(req.NodeID), payload)
	} else {
		d.E.Signal(r.PathValue("id"), dify.HumanSignal(req.NodeID), payload)
	}
	reply(w, 202, map[string]bool{"accepted": true})
}

// prepare sends a prepared run its variables: {"input": {...}, "vars":
// {...}} (dify.Options.Prepare). The run takes the first; a later one stays
// unread in its mailbox (a worker that takes over a run sends it again in
// case its earlier holder had not).
func (d *Daemon) prepare(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Input json.RawMessage `json:"input"`
		Vars  json.RawMessage `json:"vars"`
	}
	if err := readJSON(r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	if len(req.Input) == 0 {
		req.Input = json.RawMessage("{}")
	}
	if len(req.Vars) == 0 {
		req.Vars = json.RawMessage("{}")
	}
	payload, _ := json.Marshal(map[string]json.RawMessage{"input": req.Input, "vars": req.Vars})
	d.E.Signal(r.PathValue("id"), dify.PrepareSignal, payload)
	reply(w, 202, map[string]bool{"accepted": true})
}

// cancel stops a run: {"reason"}. It answers {"status"} once the
// cancellation is applied (ADR 0041): no step of the run starts after it.
// A run still waiting for admission is withdrawn ("cancelled", it never
// starts); a run that had ended answers how it ended.
func (d *Daemon) cancel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason string `json:"reason"`
	}
	readJSON(r, &req)
	if req.Reason == "" {
		req.Reason = "stopped by user"
	}
	id := r.PathValue("id")
	d.mu.Lock()
	withdraw := d.submits[id]
	d.mu.Unlock()
	if withdraw != nil {
		withdraw()
	}
	d.E.Cancel(id, req.Reason)
	ri, err := d.E.Get(r.Context(), id)
	if err != nil {
		if withdraw != nil {
			reply(w, 200, map[string]string{"status": "cancelled"})
			return
		}
		fail(w, 404, err)
		return
	}
	if ri.Status == "running" || ri.Status == "blocked" {
		// Not applied yet: the run was evicted and is being loaded to take
		// the cancellation. It ends with it.
		ctx, stop := context.WithTimeout(r.Context(), 30*time.Second)
		defer stop()
		if ri, err = d.E.Wait(ctx, id); err != nil {
			fail(w, 504, err)
			return
		}
	}
	reply(w, 200, map[string]string{"status": ri.Status})
}
