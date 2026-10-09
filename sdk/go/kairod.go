package kairo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/r-hashi01/kairo/protocol"
	ktask "github.com/r-hashi01/kairo/task"
)

// Workflows on kairod (ADR 0058, decision 4): the runs live in kairod and
// are reached over its HTTP API; the steps of this process's actions come
// over its worker protocol. The workflow code is the same.

// ErrNeedsEmbedded: the call needs the runtime embedded in this process
// (Open), not kairod (Connect).
var ErrNeedsEmbedded = errors.New("kairo: this needs the embedded runtime (Open), not kairod (Connect)")

// ErrResultLost: the run finished, but kairod no longer keeps its result
// (its marker outlived it, ADR 0027).
var ErrResultLost = errors.New("kairo: the run finished, but its result is no longer kept")

// runs is where workflows' and calls' runs live: the runtime embedded here
// (*runtime), or kairod (*remote).
type runs interface {
	run(ctx context.Context, plan string, input any, runID string, spec runSpec) (existing bool, err error)
	get(ctx context.Context, runID string) (RunInfo, error)
	// getInput is get with the run's input (a workflow's version is in it).
	getInput(ctx context.Context, runID string) (RunInfo, error)
	wait(ctx context.Context, runID string) (RunInfo, error)
	waitDone(ctx context.Context, runID string) (RunInfo, error)
	signal(ctx context.Context, runID, name string, payload any) error
	cancel(ctx context.Context, runID string) error
}

func (r *runtime) getInput(ctx context.Context, runID string) (RunInfo, error) {
	return r.get(ctx, runID)
}

// Connect opens kairo on kairod: its HTTP API at url (http://127.0.0.1:8420)
// and its worker socket at worker (a unix socket path, or host:port), over
// which this process runs its actions' steps. opts' Logger, Observe,
// Concurrency (steps at once, default 16), WorkerToken and Now apply;
// workflows are driven where they are run (no drive leases), and the
// embedded runtime's own features (suspend mode, List, Children, Resolve,
// Tick, signals sent ahead) return ErrNeedsEmbedded.
func Connect(ctx context.Context, url, worker string, opts Options) (*Kairo, error) {
	if opts.Store != nil || opts.Mode != Wait {
		return nil, errors.New("kairo: Connect takes no Store and no suspend mode: the runs are kairod's")
	}
	opts.Store = NewMemStore() // the local runtime's: plans compiled here, logging, observing
	k, err := Open(ctx, opts)
	if err != nil {
		return nil, err
	}
	conc := opts.Concurrency
	if conc <= 0 {
		conc = 16
	}
	k.remote = &remote{url: strings.TrimSuffix(url, "/"), worker: worker, token: opts.WorkerToken, concurrency: conc,
		client: &http.Client{Timeout: 90 * time.Second}}
	k.runs = k.remote
	return k, nil
}

// remote is kairod, over its HTTP API.
type remote struct {
	url, worker, token string
	concurrency        int
	client             *http.Client
}

type remoteError struct {
	status int
	msg    string
}

func (e *remoteError) Error() string { return fmt.Sprintf("kairod: %d %s", e.status, e.msg) }

func (m *remote) call(ctx context.Context, method, path string, body, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, m.url+path, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusGatewayTimeout {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		return resp.StatusCode, &remoteError{status: resp.StatusCode, msg: e.Error}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("kairod: %s %s: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}

func runPath(id string) string { return "/v1/runs/" + url.PathEscape(id) }

func (m *remote) run(ctx context.Context, plan string, input any, runID string, spec runSpec) (bool, error) {
	// Kept after they finish (ADR 0050): a workflow resumed after kairod
	// restarts still finds its finished calls' results.
	body := map[string]any{"plan": plan, "input": input, "tenant": "default", "run_id": runID, "tier": "file", "keep_output": true}
	if spec.vars != nil {
		body["vars"] = spec.vars
	}
	var out struct {
		RunID    string `json:"run_id"`
		Existing bool   `json:"existing"`
	}
	status, err := m.call(ctx, http.MethodPost, "/v1/runs", body, &out)
	if err != nil {
		return false, err
	}
	if status == http.StatusGatewayTimeout {
		return false, fmt.Errorf("kairod: run %s: its start was not confirmed", runID)
	}
	return out.Existing, nil
}

// remoteInfo is kairod's engine.RunInfo, as far as the SDK reads it.
type remoteInfo struct {
	RunID   string          `json:"run_id"`
	Plan    string          `json:"plan"`
	Status  string          `json:"status"`
	Output  json.RawMessage `json:"output"`
	Error   string          `json:"error"`
	Input   json.RawMessage `json:"input"`
	Trimmed bool            `json:"trimmed"`
}

func (ri remoteInfo) info() RunInfo {
	return RunInfo{RunID: ri.RunID, Plan: ri.Plan, Status: ri.Status, Output: ri.Output, Error: ri.Error, Input: ri.Input, trimmed: ri.Trimmed}
}

func (m *remote) getAs(ctx context.Context, runID, query string) (RunInfo, error) {
	var ri remoteInfo
	if _, err := m.call(ctx, http.MethodGet, runPath(runID)+query, nil, &ri); err != nil {
		var re *remoteError
		if errors.As(err, &re) && re.status == http.StatusNotFound {
			return RunInfo{}, fmt.Errorf("%w: %s", ErrUnknownRun, runID)
		}
		return RunInfo{}, err
	}
	return ri.info(), nil
}

func (m *remote) get(ctx context.Context, runID string) (RunInfo, error) {
	return m.getAs(ctx, runID, "")
}

// getInput asks kairod for the run's input too (Engine.GetInput, ADR
// 0060): a kairod before it reports none.
func (m *remote) getInput(ctx context.Context, runID string) (RunInfo, error) {
	return m.getAs(ctx, runID, "?input=true")
}

// wait: kairod answers once the run has finished.
func (m *remote) wait(ctx context.Context, runID string) (RunInfo, error) {
	return m.waitDone(ctx, runID)
}

func (m *remote) waitDone(ctx context.Context, runID string) (RunInfo, error) {
	for {
		var ri remoteInfo
		status, err := m.call(ctx, http.MethodGet, runPath(runID)+"/wait?timeout=60s", nil, &ri)
		if err != nil {
			if ctx.Err() != nil {
				return RunInfo{}, ctx.Err()
			}
			var re *remoteError
			if errors.As(err, &re) && re.status == http.StatusNotFound {
				return RunInfo{}, fmt.Errorf("%w: %s", ErrUnknownRun, runID)
			}
			return RunInfo{}, err
		}
		if status == http.StatusOK {
			return ri.info(), nil
		}
		// 504: not finished within the timeout; ask again.
	}
}

func (m *remote) signal(ctx context.Context, runID, name string, payload any) error {
	_, err := m.call(ctx, http.MethodPost, runPath(runID)+"/signals/"+url.PathEscape(name), payload, nil)
	return err
}

func (m *remote) cancel(ctx context.Context, runID string) error {
	_, err := m.call(ctx, http.MethodPost, runPath(runID)+"/cancel", nil, nil)
	return err
}

// start registers the actions' specs with kairod and serves their steps
// over the worker protocol until stop, connecting again if kairod goes.
func (m *remote) start(ctx context.Context, k *Kairo, specs []map[string]any) error {
	if _, err := m.call(ctx, http.MethodPost, "/v1/nodes", specs, nil); err != nil {
		return err
	}
	actions := make([]string, len(specs))
	for i, s := range specs {
		actions[i] = s["action"].(string)
	}
	w := &protocol.Worker{Name: "kairo-sdk-go-" + k.rt.owner[:8], Actions: actions, Concurrency: m.concurrency, Token: m.token,
		Handler: func(ctx context.Context, t *ktask.Task, emit func([]byte)) ktask.Result {
			return k.serveRemote(ctx, t, emit)
		}}
	network := "unix"
	if !strings.Contains(m.worker, "/") && strings.Contains(m.worker, ":") {
		network = "tcp"
	}
	k.bgWG.Add(1)
	go func() {
		defer k.bgWG.Done()
		for k.bgCtx.Err() == nil {
			if err := w.Run(k.bgCtx, network, m.worker); err != nil && k.bgCtx.Err() == nil {
				k.rt.logger.Warn("kairo: kairod's worker connection", "err", err)
			}
			select {
			case <-k.bgCtx.Done():
			case <-time.After(500 * time.Millisecond): // kairod went away: connect again
			}
		}
	}()
	return nil
}

func (m *remote) registerPlan(ctx context.Context, def json.RawMessage) error {
	_, err := m.call(ctx, http.MethodPost, "/v1/plans", def, nil)
	return err
}

// serveRemote runs a step kairod gave this process's worker.
func (k *Kairo) serveRemote(ctx context.Context, t *ktask.Task, emit func([]byte)) ktask.Result {
	in := task{RunID: t.RunID, StepID: t.StepID, Action: t.Action, IdemKey: t.IdemKey, Act: t.Act, Attempt: t.Attempt, Input: t.Input}
	k.rt.observe(Observation{Kind: ObsStepStarted, RunID: t.RunID, Action: t.Action, StepID: t.StepID, Attempt: t.Attempt})
	began := time.Now()
	res := k.rt.call(ctx, k.serve, in)
	_ = emit // live output goes to this process's subscribers (Subscribe)
	if res.Pending != nil {
		// An action answering later on a callback (ADR 0052) needs the
		// embedded runtime: kairod's step cannot be handed over.
		res = result{Err: "an async URL action needs the embedded runtime", Unknown: true, Retryable: true, ErrType: "unsupported"}
	}
	k.rt.observe(Observation{Kind: ObsStepFinished, RunID: t.RunID, Action: t.Action, StepID: t.StepID, Attempt: t.Attempt,
		Status: stepStatus(res), Error: res.Err, Duration: time.Since(began)})
	out := ktask.Result{RunID: t.RunID, Act: t.Act, Attempt: t.Attempt, Output: res.Output, Err: res.Err, Retryable: res.Retryable,
		Unknown: res.Unknown, ErrType: res.ErrType}
	if res.Wait != nil {
		out.Wait = &ktask.Wait{Until: res.Wait.Until, Output: res.Wait.Output}
	}
	return out
}
