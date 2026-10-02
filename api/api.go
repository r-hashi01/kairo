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

	"kairo/engine"
	"kairo/ir"
	"kairo/live"
	"kairo/sched"
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
	mux.HandleFunc("GET /v1/runs/{id}", a.getRun)
	mux.HandleFunc("GET /v1/runs/{id}/wait", a.waitRun)
	mux.HandleFunc("GET /v1/runs/{id}/stream", a.streamRun)
	mux.HandleFunc("POST /v1/runs/{id}/signals/{name}", a.signal)
	mux.HandleFunc("POST /v1/runs/{id}/steps/{act}/signals/{name}", a.signal)
	mux.HandleFunc("POST /v1/runs/{id}/resolve", a.resolve)
	mux.HandleFunc("POST /v1/runs/{id}/cancel", a.cancel)
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
		fail(w, 429, err)
	case errors.Is(err, engine.ErrNotAccepted):
		w.Header().Set("Retry-After", "1")
		fail(w, 503, err)
	case errors.Is(err, engine.ErrUnconfirmed):
		reply(w, 504, map[string]string{"error": err.Error(), "run_id": res.RunID})
	case errors.Is(err, engine.ErrUnknownPlan):
		fail(w, 404, err)
	case err != nil:
		fail(w, 400, err)
	case res.Existing:
		reply(w, 200, res)
	default:
		reply(w, 201, res)
	}
}

func (a *API) getRun(w http.ResponseWriter, r *http.Request) {
	ri, err := a.E.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, 404, err)
		return
	}
	if out, err := a.E.ResolveInput(ri.Output); err == nil {
		ri.Output = out // large outputs live in the blob store
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

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	a.E.Cancel(r.PathValue("id"), "cancelled via API")
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
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
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
