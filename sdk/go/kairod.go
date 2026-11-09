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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	waitQuiescent(ctx context.Context, runID string) (RunInfo, error)
	signal(ctx context.Context, runID, name string, payload any) error
	cancel(ctx context.Context, runID string) error
}

func (r *runtime) getInput(ctx context.Context, runID string) (RunInfo, error) {
	return r.get(ctx, runID)
}

func (r *runtime) waitQuiescent(ctx context.Context, runID string) (RunInfo, error) {
	return r.get(ctx, runID)
}

// Connect opens kairo on kairod: its HTTP API at url (http://127.0.0.1:8420)
// and its worker socket at worker (a unix socket path, or host:port), over
// which this process runs its actions' steps. opts' Logger, Observe,
// Concurrency (steps at once, default 16), WorkerToken and Now apply;
// Code workflows use kairod's drive claims in both wait and suspend mode.
// List, Children, Resolve, suspend Run/Submit and Tick use its HTTP API
// (ADR 0066-0069). Signals sent ahead still need the embedded runtime.
func Connect(ctx context.Context, url, worker string, opts Options) (*Kairo, error) {
	if opts.Store != nil {
		return nil, errors.New("kairo: Connect takes no Store: the runs are kairod's")
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
	startCtx, startCancel := context.WithCancel(context.Background())
	k.remote = &remote{url: strings.TrimSuffix(url, "/"), worker: worker, token: opts.WorkerToken, concurrency: conc,
		client: &http.Client{Timeout: 90 * time.Second}, owner: k.rt.owner, lease: time.Duration(k.rt.leaseMs) * time.Millisecond,
		claims: map[string]remoteDriveClaim{}, startCtx: startCtx, startCancel: startCancel, startChanged: make(chan struct{})}
	k.runs = k.remote
	return k, nil
}

// remote is kairod, over its HTTP API.
type remote struct {
	url, worker, token string
	owner              string
	concurrency        int
	client             *http.Client
	lease              time.Duration
	mu                 sync.Mutex
	claims             map[string]remoteDriveClaim
	nextAt             int64
	workerClient       *protocol.Worker
	draining           atomic.Bool
	startCtx           context.Context
	startCancel        context.CancelFunc
	starts             int
	startChanged       chan struct{}
}

type remoteDriveClaim struct {
	Run   remoteInfo `json:"run"`
	Token string     `json:"token"`
	Until int64      `json:"until"`
}

type remoteClaimResponse struct {
	Claim  *remoteDriveClaim `json:"claim"`
	NextAt int64             `json:"next_at"`
}

type remoteError struct {
	status     int
	msg        string
	retryAfter time.Duration
}

func (e *remoteError) Error() string { return fmt.Sprintf("kairod: %d %s", e.status, e.msg) }

type remoteTransportError struct{ err error }

func (e *remoteTransportError) Error() string { return e.err.Error() }
func (e *remoteTransportError) Unwrap() error { return e.err }

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
		return 0, &remoteTransportError{err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, &remoteTransportError{err}
	}
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusGatewayTimeout {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		return resp.StatusCode, &remoteError{
			status:     resp.StatusCode,
			msg:        e.Error,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		}
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("kairod: %s %s: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func retryDelay(base, minimum time.Duration, runID string, attempt int) time.Duration {
	// Stable 75%..125% jitter spreads retries without adding random state.
	h := uint64(14695981039346656037)
	for i := range len(runID) {
		h ^= uint64(runID[i])
		h *= 1099511628211
	}
	h ^= uint64(attempt)
	h *= 1099511628211
	delay := base * time.Duration(75+h%51) / 100
	if delay < minimum {
		return minimum
	}
	return delay
}

func runPath(id string) string { return "/v1/runs/" + url.PathEscape(id) }

func (m *remote) beginStart(ctx context.Context) (context.Context, func(), error) {
	m.mu.Lock()
	if m.startCtx == nil {
		m.startCtx, m.startCancel = context.WithCancel(context.Background())
		m.startChanged = make(chan struct{})
	}
	if m.draining.Load() {
		m.mu.Unlock()
		return nil, nil, ErrStopped
	}
	stopCtx := m.startCtx
	m.starts++
	m.mu.Unlock()

	startCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(stopCtx, func() { cancel(ErrStopped) })
	return startCtx, func() {
		stop()
		cancel(context.Canceled)
		m.mu.Lock()
		m.starts--
		close(m.startChanged)
		m.startChanged = make(chan struct{})
		m.mu.Unlock()
	}, nil
}

func (m *remote) stopStarts() {
	m.mu.Lock()
	if m.startCtx == nil {
		m.startCtx, m.startCancel = context.WithCancel(context.Background())
		m.startChanged = make(chan struct{})
	}
	cancel := m.startCancel
	m.mu.Unlock()
	cancel()
}

func (m *remote) waitStarts(ctx context.Context) error {
	for {
		m.mu.Lock()
		if m.starts == 0 {
			m.mu.Unlock()
			return nil
		}
		changed := m.startChanged
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (m *remote) run(ctx context.Context, plan string, input any, runID string, spec runSpec) (bool, error) {
	ctx, end, err := m.beginStart(ctx)
	if err != nil {
		return false, err
	}
	defer end()
	// Kept after they finish (ADR 0050): a workflow resumed after kairod
	// restarts still finds its finished calls' results.
	body := map[string]any{"plan": plan, "input": input, "tenant": "default", "run_id": runID, "tier": "file", "keep_output": true}
	if spec.parent != "" {
		body["parent"] = spec.parent
	}
	if spec.vars != nil {
		body["vars"] = spec.vars
	}
	if spec.parent == "" && spec.workflow != "" {
		body["workflow"] = spec.workflow
		if len(spec.meta) > 0 {
			body["meta"] = spec.meta
		}
	}
	if spec.drive != 0 {
		body["drive"] = true
	}
	var out struct {
		RunID    string `json:"run_id"`
		Existing bool   `json:"existing"`
	}
	backoff := 50 * time.Millisecond
	attempt := 0
	for {
		out = struct {
			RunID    string `json:"run_id"`
			Existing bool   `json:"existing"`
		}{}
		status, err := m.call(ctx, http.MethodPost, "/v1/runs", body, &out)
		if err == nil && status != http.StatusGatewayTimeout {
			return out.Existing, nil
		}
		minimum := time.Duration(0)
		if err != nil {
			var re *remoteError
			if errors.As(err, &re) && re.status == http.StatusConflict {
				return false, fmt.Errorf("%w: %s", ErrParentUnavailable, re.msg)
			}
			if errors.As(err, &re) && re.status == http.StatusTooManyRequests {
				minimum = re.retryAfter
			} else {
				var transport *remoteTransportError
				if !errors.As(err, &transport) {
					return false, err
				}
			}
		} else if out.RunID != runID {
			return false, fmt.Errorf("kairod: unconfirmed run %q, want %q", out.RunID, runID)
		}
		if ctx.Err() != nil {
			return false, context.Cause(ctx)
		}
		timer := time.NewTimer(retryDelay(backoff, minimum, runID, attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, context.Cause(ctx)
		case <-timer.C:
		}
		attempt++
		backoff = min(backoff*2, time.Second)
	}
}

// remoteInfo is kairod's engine.RunInfo, as far as the SDK reads it.
type remoteInfo struct {
	RunID     string          `json:"run_id"`
	Plan      string          `json:"plan"`
	Status    string          `json:"status"`
	Parent    string          `json:"parent"`
	Workflow  string          `json:"workflow"`
	Meta      json.RawMessage `json:"meta"`
	CreatedAt int64           `json:"created_at"`
	UpdatedAt int64           `json:"updated_at"`
	Output    json.RawMessage `json:"output"`
	Error     string          `json:"error"`
	Input     json.RawMessage `json:"input"`
	Trimmed   bool            `json:"trimmed"`
	Reviews   []remoteReview  `json:"reviews"`
	NextAt    int64           `json:"next_at"`
}

type remoteReview struct {
	Act uint32 `json:"act"`
}

func (ri remoteInfo) info() RunInfo {
	out := RunInfo{RunID: ri.RunID, Parent: ri.Parent, Plan: ri.Plan, Status: ri.Status, Workflow: ri.Workflow, Meta: ri.Meta,
		Output: ri.Output, Error: ri.Error, Input: ri.Input, trimmed: ri.Trimmed, nextAt: ri.NextAt}
	if ri.CreatedAt != 0 {
		out.Created = time.UnixMilli(ri.CreatedAt)
	}
	if ri.UpdatedAt != 0 {
		out.Updated = time.UnixMilli(ri.UpdatedAt)
	}
	return out
}

func (m *remote) list(ctx context.Context, f Filter) ([]RunInfo, error) {
	q := url.Values{}
	if f.Workflow != "" {
		q.Set("workflow", f.Workflow)
	}
	if f.Status != "" {
		q.Set("status", f.Status)
	}
	if !f.Since.IsZero() {
		q.Set("since", strconv.FormatInt(f.Since.UnixMilli(), 10))
	}
	if !f.Until.IsZero() {
		q.Set("until", strconv.FormatInt(f.Until.UnixMilli(), 10))
	}
	if f.After != "" {
		q.Set("after", f.After)
	}
	if f.Limit != 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	path := "/v1/runs"
	if query := q.Encode(); query != "" {
		path += "?" + query
	}
	var rows []remoteInfo
	if _, err := m.call(ctx, http.MethodGet, path, nil, &rows); err != nil {
		return nil, err
	}
	out := make([]RunInfo, len(rows))
	for i := range rows {
		out[i] = rows[i].info()
	}
	return out, nil
}

func (m *remote) getRemote(ctx context.Context, runID, query string) (remoteInfo, error) {
	var ri remoteInfo
	if _, err := m.call(ctx, http.MethodGet, runPath(runID)+query, nil, &ri); err != nil {
		var re *remoteError
		if errors.As(err, &re) && re.status == http.StatusNotFound {
			return remoteInfo{}, fmt.Errorf("%w: %s", ErrUnknownRun, runID)
		}
		return remoteInfo{}, err
	}
	return ri, nil
}

func (m *remote) getAs(ctx context.Context, runID, query string) (RunInfo, error) {
	ri, err := m.getRemote(ctx, runID, query)
	if err != nil {
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

func (m *remote) waitQuiescent(ctx context.Context, runID string) (RunInfo, error) {
	for {
		var out remoteInfo
		status, err := m.call(ctx, http.MethodGet, runPath(runID)+"/quiescent?timeout=60s", nil, &out)
		if err != nil {
			return RunInfo{}, err
		}
		if status == http.StatusOK {
			return out.info(), nil
		}
	}
}

func (m *remote) noteNext(at int64) {
	if at == 0 {
		return
	}
	m.mu.Lock()
	if m.nextAt == 0 || at < m.nextAt {
		m.nextAt = at
	}
	m.mu.Unlock()
}

func (m *remote) nextWake() int64 {
	m.mu.Lock()
	at := m.nextAt
	m.mu.Unlock()
	return at
}

func (m *remote) clearDue(now int64) {
	m.mu.Lock()
	if m.nextAt != 0 && m.nextAt <= now {
		m.nextAt = 0
	}
	m.mu.Unlock()
}

func (m *remote) claim(ctx context.Context, runID string, workflows map[string][]string, timeout time.Duration) (*remoteDriveClaim, int64, error) {
	if m.draining.Load() {
		return nil, m.nextWake(), nil
	}
	body := map[string]any{"owner": m.owner, "lease_ms": m.lease.Milliseconds(), "timeout_ms": timeout.Milliseconds(), "limit": 1, "workflows": workflows}
	if runID != "" {
		body["run_id"] = runID
	}
	var out remoteClaimResponse
	if _, err := m.call(ctx, http.MethodPost, "/v1/workflow-drives/claim", body, &out); err != nil {
		return nil, 0, err
	}
	m.noteNext(out.NextAt)
	if out.Claim != nil && m.draining.Load() {
		_ = m.release(context.Background(), *out.Claim, true)
		out.Claim = nil
	}
	return out.Claim, out.NextAt, nil
}

func (m *remote) hold(c remoteDriveClaim) {
	m.mu.Lock()
	m.claims[c.Run.RunID] = c
	m.mu.Unlock()
}

func (m *remote) drop(id string) {
	m.mu.Lock()
	delete(m.claims, id)
	m.mu.Unlock()
}

func (m *remote) release(ctx context.Context, c remoteDriveClaim, retry bool) error {
	m.drop(c.Run.RunID)
	body := map[string]any{"owner": m.owner, "token": c.Token, "retry": retry}
	_, err := m.call(ctx, http.MethodPost, "/v1/workflow-drives/"+url.PathEscape(c.Run.RunID)+"/release", body, nil)
	var re *remoteError
	if errors.As(err, &re) && re.status == http.StatusConflict {
		return nil // terminal completion removed it, or the lease expired
	}
	return err
}

func (m *remote) renewClaims(ctx context.Context) {
	m.mu.Lock()
	claims := make([]remoteDriveClaim, 0, len(m.claims))
	for _, c := range m.claims {
		claims = append(claims, c)
	}
	m.mu.Unlock()
	for _, c := range claims {
		body := map[string]any{"owner": m.owner, "token": c.Token, "lease_ms": m.lease.Milliseconds()}
		if _, err := m.call(ctx, http.MethodPost, "/v1/workflow-drives/"+url.PathEscape(c.Run.RunID)+"/renew", body, nil); err != nil && ctx.Err() == nil {
			m.drop(c.Run.RunID)
		}
	}
}

func (m *remote) releaseAll() {
	m.mu.Lock()
	claims := make([]remoteDriveClaim, 0, len(m.claims))
	for _, claim := range m.claims {
		claims = append(claims, claim)
	}
	m.mu.Unlock()
	for _, claim := range claims {
		_ = m.release(context.Background(), claim, true)
	}
}

func (m *remote) signal(ctx context.Context, runID, name string, payload any) error {
	_, err := m.call(ctx, http.MethodPost, runPath(runID)+"/signals/"+url.PathEscape(name), payload, nil)
	return err
}

func (m *remote) cancel(ctx context.Context, runID string) error {
	_, err := m.call(ctx, http.MethodPost, runPath(runID)+"/cancel", nil, nil)
	var re *remoteError
	if errors.As(err, &re) && re.status == http.StatusConflict {
		return errChildrenUnsettled
	}
	return err
}

func (m *remote) finish(ctx context.Context, runID string, payload any) error {
	ri, err := m.getRemote(ctx, runID, "")
	if err != nil {
		return err
	}
	if doneStatus[ri.Status] {
		return nil // a retry after the terminal event was already recorded
	}
	_, err = m.call(ctx, http.MethodPost, runPath(runID)+"/finish", payload, nil)
	var re *remoteError
	if errors.As(err, &re) && re.status == http.StatusNotFound {
		// Cancellation can win between the read above and the finish request.
		if current, getErr := m.getRemote(ctx, runID, ""); getErr == nil && doneStatus[current.Status] {
			return nil
		}
	}
	if errors.As(err, &re) && re.status == http.StatusConflict {
		return errChildrenUnsettled
	}
	return err
}

func (m *remote) children(ctx context.Context, runID string) ([]RunInfo, error) {
	var rows []remoteInfo
	if _, err := m.call(ctx, http.MethodGet, runPath(runID)+"/children", nil, &rows); err != nil {
		return nil, err
	}
	out := make([]RunInfo, len(rows))
	for i := range rows {
		out[i] = rows[i].info()
	}
	return out, nil
}

// resolve settles a code-workflow call through kairod's existing review
// API, and waits for its terminal event to become durable (ADR 0066).
func (m *remote) resolve(ctx context.Context, runID string, output any, errMsg string) error {
	ri, err := m.getRemote(ctx, runID, "")
	if err != nil {
		return err
	}
	if doneStatus[ri.Status] {
		return nil // an HTTP retry after an ambiguous response
	}
	if len(ri.Reviews) == 0 {
		return fmt.Errorf("kairo: run %s has no step stopped for review", runID)
	}
	body := map[string]any{"act": ri.Reviews[0].Act, "output": output}
	if errMsg != "" {
		body["error"] = errMsg
	}
	if _, err := m.call(ctx, http.MethodPost, runPath(runID)+"/resolve", body, nil); err != nil {
		return err
	}
	_, err = m.waitDone(ctx, runID)
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
	m.mu.Lock()
	m.workerClient = w
	m.mu.Unlock()
	network := "unix"
	if !strings.Contains(m.worker, "/") && strings.Contains(m.worker, ":") {
		network = "tcp"
	}
	k.bgWG.Add(1)
	go func() {
		defer k.bgWG.Done()
		for k.bgCtx.Err() == nil && !k.draining.Load() {
			if err := w.Run(k.bgCtx, network, m.worker); err != nil && k.bgCtx.Err() == nil {
				k.rt.logger.Warn("kairo: kairod's worker connection", "err", err)
			}
			select {
			case <-k.bgCtx.Done():
			case <-time.After(500 * time.Millisecond): // kairod went away: connect again
			}
		}
	}()
	k.bgWG.Add(1)
	go func() {
		defer k.bgWG.Done()
		every := max(m.lease/3, 10*time.Millisecond)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-k.bgCtx.Done():
				return
			case <-t.C:
				m.renewClaims(k.bgCtx)
			}
		}
	}()
	if k.mode == Wait {
		// One process-level claimant takes up workflows left by stopped
		// processes. The server blocks on readiness; this is not polling.
		k.bgWG.Add(1)
		go func() {
			defer k.bgWG.Done()
			for k.bgCtx.Err() == nil {
				if k.draining.Load() {
					return
				}
				claim, _, err := m.claim(k.bgCtx, "", k.supportedWorkflows(), 60*time.Second)
				if err != nil {
					if k.bgCtx.Err() == nil {
						k.rt.logger.Warn("kairo: claiming a workflow from kairod", "err", err)
					}
					continue
				}
				if claim != nil && k.draining.Load() {
					_ = m.release(context.Background(), *claim, true)
					return
				}
				if claim != nil {
					k.driveRemoteClaim(k.bgCtx, *claim)
				}
			}
		}()
	}
	return nil
}

func (m *remote) drain(ctx context.Context) error {
	m.draining.Store(true)
	m.stopStarts()
	m.mu.Lock()
	w := m.workerClient
	m.mu.Unlock()
	if w == nil {
		return errors.New("kairo: kairod worker has not started")
	}
	if err := w.Drain(ctx); err != nil {
		return err
	}
	return m.waitStarts(ctx)
}

func (m *remote) inflight() int {
	m.mu.Lock()
	w := m.workerClient
	m.mu.Unlock()
	if w == nil {
		return 0
	}
	return w.Inflight()
}

func (k *Kairo) driveRemoteClaim(ctx context.Context, claim remoteDriveClaim) {
	name, _, input, ok := workflowOf(claim.Run.Input)
	if !ok {
		_ = k.remote.release(context.Background(), claim, false)
		return
	}
	k.remote.hold(claim)
	_, err := k.runAs(ctx, name, input, claim.Run.RunID, claim.Run.Parent, nil)
	if ctx.Err() != nil || errors.Is(err, ErrStopped) {
		_ = k.remote.release(context.Background(), claim, true)
		return
	}
	if releaseErr := k.remote.release(context.Background(), claim, retryWorkflow(err)); releaseErr != nil {
		k.rt.logger.Warn("kairo: releasing a workflow claim", "workflow", claim.Run.RunID, "err", releaseErr)
	}
	if err != nil && !errors.Is(err, ErrSuspended) && !errors.Is(err, errUnknownPlan) && !retryWorkflow(err) && !errors.Is(err, ErrCancelled) {
		k.rt.logger.Warn("kairo: driving a workflow claimed from kairod", "workflow", claim.Run.RunID, "err", err)
	}
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
