// Package api is the HTTP control API of the daemon: register node specs
// and plans, submit runs, deliver signals, resolve reviews, and stream live
// output.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/live"
	"github.com/r-hashi01/kairo/sched"
)

type API struct {
	E *engine.Engine
	// Dir persists plans and node specs so a restarted daemon can recover
	// runs with identical plans. Empty disables persistence.
	Dir string
	// OnNode is called for every node spec registered through the API.
	OnNode func(ir.NodeSpec)
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/nodes", a.postNodes)
	mux.HandleFunc("POST /v1/plans", a.postPlan)
	mux.HandleFunc("POST /v1/runs", a.postRun)
	mux.HandleFunc("GET /v1/runs", a.listRuns)
	mux.HandleFunc("GET /v1/runs/{id}", a.getRun)
	mux.HandleFunc("GET /v1/runs/{id}/wait", a.waitRun)
	mux.HandleFunc("GET /v1/runs/{id}/quiescent", a.quiescentRun)
	mux.HandleFunc("GET /v1/runs/{id}/children", a.children)
	mux.HandleFunc("GET /v1/runs/{id}/stream", a.streamRun)
	mux.HandleFunc("POST /v1/runs/{id}/signals/{name}", a.signal)
	mux.HandleFunc("POST /v1/runs/{id}/signals/{name}/apply", a.applySignal)
	mux.HandleFunc("POST /v1/runs/{id}/steps/{act}/signals/{name}", a.signal)
	mux.HandleFunc("POST /v1/runs/{id}/resolve", a.resolve)
	mux.HandleFunc("POST /v1/runs/{id}/finish", a.finish)
	mux.HandleFunc("POST /v1/runs/{id}/cancel", a.cancel)
	mux.HandleFunc("POST /v1/workflow-drives/claim", a.claimWorkflow)
	mux.HandleFunc("POST /v1/workflow-drives/{id}/renew", a.renewWorkflow)
	mux.HandleFunc("POST /v1/workflow-drives/{id}/release", a.releaseWorkflow)
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) { reply(w, 200, a.E.Stats()) })
	return mux
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

func (a *API) postNodes(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		fail(w, 400, err)
		return
	}
	var specs []ir.NodeSpec
	if len(b) > 0 && b[0] == '{' {
		var s ir.NodeSpec
		err = json.Unmarshal(b, &s)
		specs = []ir.NodeSpec{s}
	} else {
		err = json.Unmarshal(b, &specs)
	}
	if err != nil {
		fail(w, 400, err)
		return
	}
	for _, s := range specs {
		if s.Action == "" {
			fail(w, 400, errors.New("node spec without action"))
			return
		}
		a.E.Registry().Register(s)
		if a.OnNode != nil {
			a.OnNode(s)
		}
		if a.Dir != "" {
			if err := writeJSON(filepath.Join(a.Dir, "nodes", fileName(s.Action)+".json"), s); err != nil {
				fail(w, 500, err)
				return
			}
		}
	}
	reply(w, 200, map[string]int{"registered": len(specs)})
}

func (a *API) postPlan(w http.ResponseWriter, r *http.Request) {
	var def ir.Definition
	if err := readJSON(r, &def); err != nil {
		fail(w, 400, err)
		return
	}
	if def.Name == "" {
		fail(w, 400, errors.New("plan needs a name"))
		return
	}
	p, err := a.E.RegisterPlan(&def)
	if err != nil {
		fail(w, 400, err)
		return
	}
	if a.Dir != "" {
		if err := writeJSON(filepath.Join(a.Dir, "plans", fileName(p.Name)+"@"+p.Hash+".json"), p.Freeze()); err != nil {
			fail(w, 500, err)
			return
		}
	}
	effects := map[string]string{}
	for _, s := range p.Specs() {
		effects[s.Action] = s.Effect.String()
	}
	reply(w, 200, map[string]any{"name": p.Name, "hash": p.Hash, "has_real": p.HasReal, "effects": effects})
}

// postRun starts a run and answers once its start is durable (ADR 0023):
// 201 for a new run, 200 if the Idempotency-Key (the run id) was already
// running or recently finished. ?timeout= bounds the wait (default 30s).
func (a *API) postRun(w http.ResponseWriter, r *http.Request) {
	var req engine.SubmitRequest
	if err := readJSON(r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		req.RunID = k
	}
	timeout := 30 * time.Second
	if t := r.URL.Query().Get("timeout"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	res, err := a.E.Submit(ctx, req)
	switch {
	case errors.Is(err, sched.ErrOverloaded):
		w.Header().Set("Retry-After", "1")
		fail(w, 429, err)
	case errors.Is(err, engine.ErrNotAccepted):
		w.Header().Set("Retry-After", "1")
		fail(w, 503, err)
	case errors.Is(err, engine.ErrUnconfirmed):
		reply(w, 504, map[string]string{"error": err.Error(), "run_id": res.RunID})
	case errors.Is(err, engine.ErrUnknownPlan):
		fail(w, 404, err)
	case errors.Is(err, engine.ErrParentUnavailable):
		fail(w, 409, err)
	case err != nil:
		fail(w, 400, err)
	case res.Existing:
		reply(w, 200, res)
	default:
		reply(w, 201, res)
	}
}

func (a *API) listRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := engine.ListFilter{Workflow: q.Get("workflow"), Status: q.Get("status"), After: q.Get("after")}
	parseTime := func(name string) (time.Time, error) {
		raw := q.Get(name)
		if raw == "" {
			return time.Time{}, nil
		}
		ms, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s must be unix milliseconds: %w", name, err)
		}
		return time.UnixMilli(ms), nil
	}
	var err error
	if f.Since, err = parseTime("since"); err != nil {
		fail(w, 400, err)
		return
	}
	if f.Until, err = parseTime("until"); err != nil {
		fail(w, 400, err)
		return
	}
	if raw := q.Get("limit"); raw != "" {
		if f.Limit, err = strconv.Atoi(raw); err != nil {
			fail(w, 400, fmt.Errorf("limit must be an integer: %w", err))
			return
		}
	}
	rows, err := a.E.List(r.Context(), f)
	if err != nil {
		fail(w, 400, err)
		return
	}
	reply(w, 200, rows)
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	get := a.E.Get
	if r.URL.Query().Get("input") == "true" {
		// With the run's input, as it was submitted (ADR 0060).
		get = a.E.GetInput
	}
	ri, err := get(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, 404, err)
		return
	}
	if out, err := a.E.ResolveInput(ri.Output); err == nil {
		ri.Output = out // large outputs live in the blob store
	}
	if len(ri.Input) > 0 {
		if in, err := a.E.ResolveInput(ri.Input); err == nil {
			ri.Input = in
		}
	}
	reply(w, 200, ri)
}

func (a *API) waitRun(w http.ResponseWriter, r *http.Request) {
	timeout := 30 * time.Second
	if t := r.URL.Query().Get("timeout"); t != "" {
		if d, err := time.ParseDuration(t); err == nil {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	ri, err := a.E.Wait(ctx, r.PathValue("id"))
	if err != nil {
		cur, gerr := a.E.Get(r.Context(), r.PathValue("id"))
		if gerr != nil {
			fail(w, 404, gerr)
			return
		}
		reply(w, 202, cur)
		return
	}
	if out, err := a.E.ResolveInput(ri.Output); err == nil {
		ri.Output = out
	}
	reply(w, 200, ri)
}

func requestTimeout(r *http.Request, fallback time.Duration) time.Duration {
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d >= 0 {
			return d
		}
	}
	return fallback
}

func (a *API) quiescentRun(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout(r, 30*time.Second))
	defer cancel()
	ri, err := a.E.WaitQuiescent(ctx, r.PathValue("id"))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			if cur, getErr := a.E.Get(r.Context(), r.PathValue("id")); getErr == nil {
				reply(w, http.StatusAccepted, cur)
				return
			}
		}
		fail(w, http.StatusNotFound, err)
		return
	}
	if out, err := a.E.ResolveInput(ri.Output); err == nil {
		ri.Output = out
	}
	reply(w, http.StatusOK, ri)
}

type driveClaimRequest struct {
	RunID     string              `json:"run_id"`
	Owner     string              `json:"owner"`
	LeaseMS   int64               `json:"lease_ms"`
	TimeoutMS int64               `json:"timeout_ms"`
	Limit     int                 `json:"limit"`
	Workflows map[string][]string `json:"workflows"`
}

type driveClaimResponse struct {
	Claim  *engine.DriveClaim `json:"claim,omitempty"`
	NextAt int64              `json:"next_at,omitempty"`
}

func workflowVersion(input json.RawMessage) (string, string, bool) {
	var in struct {
		Workflow string `json:"workflow"`
		Version  string `json:"version"`
	}
	if json.Unmarshal(input, &in) != nil || in.Workflow == "" {
		return "", "", false
	}
	return in.Workflow, in.Version, true
}

func driveMatch(req driveClaimRequest, ri engine.RunInfo) bool {
	if req.RunID != "" && req.RunID != ri.RunID {
		return false
	}
	name, version, ok := workflowVersion(ri.Input)
	if !ok {
		return false
	}
	if len(req.Workflows) == 0 {
		return true
	}
	versions, ok := req.Workflows[name]
	if !ok {
		return false
	}
	for _, v := range versions {
		if v == version {
			return true
		}
	}
	return false
}

func (a *API) claimWorkflow(w http.ResponseWriter, r *http.Request) {
	var req driveClaimRequest
	if err := readJSON(r, &req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	if req.Owner == "" || req.LeaseMS <= 0 || req.LeaseMS > int64((24*time.Hour)/time.Millisecond) || req.TimeoutMS < 0 || req.Limit < 0 || req.Limit > 1 {
		fail(w, http.StatusBadRequest, errors.New("owner, lease_ms (1..86400000), non-negative timeout_ms, and limit 0 or 1 are required"))
		return
	}
	ctx := r.Context()
	var cancel context.CancelFunc
	if req.TimeoutMS > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutMS)*time.Millisecond)
		defer cancel()
	}
	for {
		changed := a.E.DriveChanged()
		// A process resuming a known workflow names its run. Claim that shard
		// directly; the all-candidate snapshot below is for process-level Tick.
		// If the direct claim races or is not ready, fall back so NextAt and
		// long-poll behavior stay unchanged.
		if req.RunID != "" {
			directAt := a.metricStart()
			ri, getErr := a.E.GetInput(ctx, req.RunID)
			if getErr == nil && driveMatch(req, ri) {
				claim, ok, claimErr := a.E.ClaimWorkflow(ctx, req.RunID, req.Owner, time.Duration(req.LeaseMS)*time.Millisecond)
				if claimErr != nil {
					if ctx.Err() != nil {
						a.claimMetric("direct", "cancelled", directAt)
						reply(w, http.StatusOK, driveClaimResponse{})
						return
					}
					a.claimMetric("direct", "failed", directAt)
					fail(w, http.StatusInternalServerError, claimErr)
					return
				}
				if ok {
					a.claimMetric("direct", "claimed", directAt)
					claim.Run.Input = ri.Input
					reply(w, http.StatusOK, driveClaimResponse{Claim: &claim})
					return
				}
			}
			a.claimMetric("direct", "empty", directAt)
		}
		scanAt := a.metricStart()
		rows, err := a.E.DriveCandidates(ctx)
		if err != nil {
			a.claimMetric("scan", "cancelled", scanAt)
			reply(w, http.StatusOK, driveClaimResponse{})
			return
		}
		var next int64
		for _, row := range rows {
			ri, err := a.E.GetInput(ctx, row.Run.RunID)
			if err != nil || !driveMatch(req, ri) {
				continue
			}
			if row.Until > 0 && (next == 0 || row.Until < next) {
				next = row.Until
			}
			if !row.Ready {
				continue
			}
			claim, ok, err := a.E.ClaimWorkflow(ctx, row.Run.RunID, req.Owner, time.Duration(req.LeaseMS)*time.Millisecond)
			if err != nil {
				if ctx.Err() != nil {
					a.claimMetric("scan", "cancelled", scanAt)
					reply(w, http.StatusOK, driveClaimResponse{NextAt: next})
					return
				}
				a.claimMetric("scan", "failed", scanAt)
				fail(w, http.StatusInternalServerError, err)
				return
			}
			if ok {
				a.claimMetric("scan", "claimed", scanAt)
				claim.Run.Input = ri.Input
				reply(w, http.StatusOK, driveClaimResponse{Claim: &claim})
				return
			}
		}
		if req.TimeoutMS == 0 {
			a.claimMetric("scan", "empty", scanAt)
			reply(w, http.StatusOK, driveClaimResponse{NextAt: next})
			return
		}
		var timer <-chan time.Time
		var stop *time.Timer
		if next > 0 {
			d := time.Until(time.UnixMilli(next))
			if d < 0 {
				d = 0
			}
			stop = time.NewTimer(d)
			timer = stop.C
		}
		select {
		case <-changed:
		case <-timer:
		case <-ctx.Done():
			if stop != nil {
				stop.Stop()
			}
			a.claimMetric("scan", "cancelled", scanAt)
			reply(w, http.StatusOK, driveClaimResponse{NextAt: next})
			return
		}
		if stop != nil {
			stop.Stop()
		}
	}
}

func (a *API) metricStart() time.Time {
	if !a.E.MetricsEnabled() {
		return time.Time{}
	}
	return time.Now()
}

func (a *API) claimMetric(path, outcome string, started time.Time) {
	if started.IsZero() {
		return
	}
	a.E.ObserveWorkflowClaim(path, outcome, time.Since(started))
}

type driveLeaseRequest struct {
	Owner   string `json:"owner"`
	Token   string `json:"token"`
	LeaseMS int64  `json:"lease_ms"`
	Retry   bool   `json:"retry"`
}

func (a *API) renewWorkflow(w http.ResponseWriter, r *http.Request) {
	var req driveLeaseRequest
	if err := readJSON(r, &req); err != nil || req.LeaseMS <= 0 {
		if err == nil {
			err = errors.New("positive lease_ms is required")
		}
		fail(w, http.StatusBadRequest, err)
		return
	}
	until, ok, err := a.E.RenewWorkflow(r.Context(), r.PathValue("id"), req.Owner, req.Token, time.Duration(req.LeaseMS)*time.Millisecond)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		fail(w, http.StatusConflict, errors.New("workflow drive claim is no longer held"))
		return
	}
	reply(w, http.StatusOK, map[string]int64{"until": until})
}

func (a *API) releaseWorkflow(w http.ResponseWriter, r *http.Request) {
	var req driveLeaseRequest
	if err := readJSON(r, &req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	ok, err := a.E.ReleaseWorkflow(r.Context(), r.PathValue("id"), req.Owner, req.Token, req.Retry)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	if !ok {
		fail(w, http.StatusConflict, errors.New("workflow drive claim is no longer held"))
		return
	}
	reply(w, http.StatusOK, map[string]bool{"released": true})
}

// streamRun relays live chunks as server-sent events, then a final "done"
// event carrying the run's result.
func (a *API) streamRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fl, ok := w.(http.Flusher)
	if !ok {
		fail(w, 500, errors.New("streaming unsupported"))
		return
	}
	sub := a.E.Live().Subscribe(id, 4096)
	defer sub.Close()
	if ri, err := a.E.Get(r.Context(), id); err != nil {
		fail(w, 404, err)
		return
	} else if ri.Status == "completed" || ri.Status == "failed" || ri.Status == "cancelled" {
		w.Header().Set("Content-Type", "text/event-stream")
		writeEvent(w, "done", ri)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	fl.Flush()
	go func() {
		<-r.Context().Done()
		sub.Close()
	}()
	var batch []live.Chunk
	for {
		var ok bool
		// One write and one flush per batch of chunks.
		batch, ok = sub.Next(batch)
		for _, c := range batch {
			if c.Control {
				fmt.Fprintf(w, "event: control\ndata: %s\n\n", c.Data)
				continue
			}
			writeEvent(w, "chunk", liveChunk{Step: c.StepID, Data: string(c.Data)})
		}
		fl.Flush()
		if !ok {
			break
		}
	}
	if r.Context().Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if ri, err := a.E.Wait(ctx, id); err == nil {
		writeEvent(w, "done", ri)
		fl.Flush()
	}
}

type liveChunk struct {
	Step string `json:"step_id"`
	Data string `json:"data"`
}

func writeEvent(w io.Writer, name string, v any) {
	b, _ := json.Marshal(v)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b)
}

func (a *API) signal(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		fail(w, 400, err)
		return
	}
	if len(b) == 0 {
		b = []byte("null")
	}
	if !json.Valid(b) {
		fail(w, 400, errors.New("signal payload must be JSON"))
		return
	}
	if act := r.PathValue("act"); act != "" {
		n, err := strconv.ParseUint(act, 10, 32)
		if err != nil {
			fail(w, 400, err)
			return
		}
		a.E.SignalStep(r.PathValue("id"), uint32(n), r.PathValue("name"), b)
	} else {
		a.E.Signal(r.PathValue("id"), r.PathValue("name"), b)
	}
	reply(w, 202, map[string]bool{"accepted": true})
}

func (a *API) applySignal(w http.ResponseWriter, r *http.Request) {
	var req engine.SignalRequest
	if err := readJSON(r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	req.Name = r.PathValue("name")
	res, err := a.E.ApplySignal(r.Context(), r.PathValue("id"), req)
	switch {
	case errors.Is(err, engine.ErrSignalConflict):
		fail(w, 409, err)
	case err != nil:
		fail(w, 400, err)
	default:
		reply(w, 200, res)
	}
}

func (a *API) resolve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Act    uint32          `json:"act"`
		Output json.RawMessage `json:"output"`
		Error  string          `json:"error"`
	}
	if err := readJSON(r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	a.E.Resolve(r.PathValue("id"), req.Act, req.Output, req.Error)
	reply(w, 202, map[string]bool{"accepted": true})
}

func (a *API) children(w http.ResponseWriter, r *http.Request) {
	rows, err := a.E.Children(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, 400, err)
		return
	}
	reply(w, 200, rows)
}

func (a *API) finish(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil || !json.Valid(b) {
		if err == nil {
			err = errors.New("finish payload must be JSON")
		}
		fail(w, 400, err)
		return
	}
	if err := a.E.FinishWorkflow(r.Context(), r.PathValue("id"), b); err != nil {
		if errors.Is(err, engine.ErrChildrenUnsettled) {
			fail(w, 409, err)
		} else if errors.Is(err, engine.ErrUnknownRun) {
			fail(w, 404, err)
		} else {
			fail(w, 400, err)
		}
		return
	}
	reply(w, 202, map[string]bool{"accepted": true})
}

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	if err := a.E.CancelTree(r.Context(), r.PathValue("id"), "cancelled via API"); err != nil {
		if errors.Is(err, engine.ErrChildrenUnsettled) {
			fail(w, 409, err)
			return
		}
		// Preserve repeated cancel of a finished run as accepted; the SDK
		// reads its durable outcome with /wait.
		if !errors.Is(err, engine.ErrUnknownRun) {
			fail(w, 400, err)
			return
		}
	}
	reply(w, 202, map[string]bool{"accepted": true})
}

func fileName(s string) string {
	return strings.NewReplacer("/", "_", ":", "_", "@", "_").Replace(s)
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o644); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load restores persisted node specs and plans from dir into e. Plans are
// added oldest first, so the newest version of each name becomes current.
func Load(e *engine.Engine, dir string) error {
	nodes, _ := filepath.Glob(filepath.Join(dir, "nodes", "*.json"))
	for _, n := range nodes {
		b, err := os.ReadFile(n)
		if err != nil {
			return err
		}
		var s ir.NodeSpec
		if err := json.Unmarshal(b, &s); err != nil {
			return fmt.Errorf("%s: %w", n, err)
		}
		e.Registry().Register(s)
	}
	plans, _ := filepath.Glob(filepath.Join(dir, "plans", "*.json"))
	type pf struct {
		path string
		mod  time.Time
	}
	var files []pf
	for _, p := range plans {
		st, err := os.Stat(p)
		if err != nil {
			return err
		}
		files = append(files, pf{p, st.ModTime()})
	}
	for i := range files {
		for j := i + 1; j < len(files); j++ {
			if files[j].mod.Before(files[i].mod) {
				files[i], files[j] = files[j], files[i]
			}
		}
	}
	for _, f := range files {
		b, err := os.ReadFile(f.path)
		if err != nil {
			return err
		}
		var fr ir.Frozen
		if err := json.Unmarshal(b, &fr); err != nil {
			return fmt.Errorf("%s: %w", f.path, err)
		}
		p, err := fr.Thaw()
		if err != nil {
			return fmt.Errorf("%s: %w", f.path, err)
		}
		e.AddPlan(p, true)
	}
	return nil
}
