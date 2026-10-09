package kairo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/r-hashi01/kairo/httpaction"
)

// Actions over HTTP(S) (ADR 0052) and the scheduler's entry point (ADR
// 0053), as the TypeScript and Python SDKs have them: the same request
// and result bodies, the same signature (httpaction).

// HTTPOptions configure actions over HTTP(S) and /tick (Options.HTTP).
type HTTPOptions struct {
	// Secret signs the calls and the callbacks (both sides share it).
	Secret string
	// CallbackURL is where this process takes outcomes (Handler's
	// /callback).
	CallbackURL string
	// CA: certificates to trust for https, besides the system's.
	CA *x509.CertPool
	// AllowInsecure allows plain http to other machines.
	AllowInsecure bool
	// TickSecret, in suspend mode, is the bearer token a scheduler calls
	// /tick with (Vercel Cron's CRON_SECRET, say). Without it /tick is not
	// served.
	TickSecret string
}

var localHosts = map[string]bool{"localhost": true, "127.0.0.1": true, "::1": true}

// checkURL: https, or http to this machine, or http when allowed.
func checkURL(raw string, allowInsecure bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("kairo: %s: not a URL", raw)
	}
	switch {
	case u.Scheme == "https":
		return nil
	case u.Scheme == "http" && (localHosts[u.Hostname()] || allowInsecure):
		return nil
	case u.Scheme == "http":
		return fmt.Errorf("kairo: %s: plain http is only for this machine; use https, or AllowInsecure", raw)
	}
	return fmt.Errorf("kairo: %s: only http(s)", raw)
}

// URL makes an action's steps calls over HTTP(S) to url (ADR 0052): its
// handler runs there (Handler). Async: where the URL serves it, it answers
// at once (202) and sends the outcome to the callback.
func URL(url string, async bool) ActionOption {
	return func(s map[string]any) {
		s["$url"] = url
		if async {
			s["$async"] = true
		}
	}
}

func (k *Kairo) httpClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if k.http.CA != nil {
		tr.TLSClientConfig = &tls.Config{RootCAs: k.http.CA}
	}
	return &http.Client{Transport: tr}
}

// callRemote calls an action over HTTP(S).
func (k *Kairo) callRemote(ctx context.Context, t task, a *actionDef, in json.RawMessage) result {
	body, err := json.Marshal(httpaction.Request{RunID: t.RunID, StepID: t.StepID, Act: t.Act, Attempt: t.Attempt, Action: t.Action,
		Input: orNull(in), IdempotencyKey: t.IdemKey, Callback: k.http.CallbackURL})
	if err != nil {
		return result{Err: err.Error()}
	}
	lease := 15 * time.Minute
	if s, ok := a.spec["timeout"].(string); ok {
		if d, err := time.ParseDuration(s); err == nil {
			lease = d
		}
	}
	rctx, cancel := context.WithTimeout(ctx, min(lease, time.Minute))
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		return result{Err: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", t.IdemKey)
	req.Header.Set("Kairo-Signature", httpaction.Sign(k.http.Secret, body, time.Now()))
	resp, err := k.client.Do(req)
	if err != nil {
		// It may have run: unknown (invariant 5).
		return result{Err: "calling " + a.url + ": " + err.Error(), Unknown: true, Retryable: true, ErrType: "http"}
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, httpaction.MaxBody))
	switch {
	case resp.StatusCode == http.StatusAccepted:
		return result{Pending: &pending{Owner: "remote:" + a.url, LeaseMs: lease.Milliseconds()}}
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var w httpaction.Result
		if err := json.Unmarshal(data, &w); err != nil {
			return result{Err: a.url + ": the answer is not JSON", Unknown: true, Retryable: true, ErrType: "http"}
		}
		return fromWire(w)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		msg := string(data)
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return result{Err: fmt.Sprintf("%s: %d %s", a.url, resp.StatusCode, msg), ErrType: fmt.Sprintf("http_%d", resp.StatusCode)}
	}
	return result{Err: fmt.Sprintf("%s: %d", a.url, resp.StatusCode), Unknown: true, Retryable: true, ErrType: fmt.Sprintf("http_%d", resp.StatusCode)}
}

func fromWire(w httpaction.Result) result {
	if w.Error != "" {
		return result{Err: w.Error, Retryable: w.Retryable, ErrType: w.ErrorType}
	}
	if w.Wait != nil {
		return result{Wait: &waitResult{Until: w.Wait.Until, Output: w.Wait.Output}}
	}
	return result{Output: orNull(w.Output)}
}

func toWire(r result) (w httpaction.Result, unknown bool) {
	switch {
	case r.Unknown:
		return httpaction.Result{Error: r.Err, ErrorType: r.ErrType}, true
	case r.Err != "":
		return httpaction.Result{Error: r.Err, Retryable: r.Retryable, ErrorType: r.ErrType}, false
	default:
		return httpaction.Result{Output: orNull(r.Output)}, false
	}
}

// Handler serves, as an http.Handler (mount it at any base path):
//
//	POST <base>/action    runs an action's handler here (signed, ADR 0052)
//	POST <base>/callback  applies the outcome of an action that ran elsewhere
//	GET|POST <base>/tick  a scheduler's tick, with "Authorization: Bearer
//	                      <TickSecret>" (suspend mode, ADR 0053)
func (k *Kairo) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answer := func(status int, v any) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(v)
		}
		path := r.URL.Path
		if strings.HasSuffix(path, "/tick") {
			k.serveTick(w, r, answer)
			return
		}
		if r.Method != http.MethodPost {
			answer(http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		if k.http.Secret == "" {
			answer(http.StatusInternalServerError, map[string]string{"error": "no secret configured"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpaction.MaxBody))
		if err != nil {
			answer(http.StatusRequestEntityTooLarge, map[string]string{"error": err.Error()})
			return
		}
		if err := httpaction.Verify(k.http.Secret, body, r.Header.Get("Kairo-Signature"), time.Now()); err != nil {
			answer(http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		switch {
		case strings.HasSuffix(path, "/callback"):
			var cb struct {
				RunID   string            `json:"run_id"`
				Act     uint32            `json:"act"`
				Attempt int32             `json:"attempt"`
				Result  httpaction.Result `json:"result"`
			}
			if err := json.Unmarshal(body, &cb); err != nil {
				answer(http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			if err := k.rt.complete(r.Context(), cb.RunID, cb.Act, cb.Attempt, fromWire(cb.Result)); err != nil && !errors.Is(err, errUnknownPlan) {
				answer(http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			if k.mode == Suspend {
				k.settle()
				if err := k.wakeUp(r.Context()); err != nil {
					log.Printf("kairo: wake: %v", err)
				}
			}
			answer(http.StatusOK, map[string]bool{"ok": true})
		case strings.HasSuffix(path, "/action"):
			k.serveAction(w, r, body, answer)
		default:
			answer(http.StatusNotFound, map[string]string{"error": "no such path"})
		}
	})
}

func (k *Kairo) serveAction(_ http.ResponseWriter, r *http.Request, body []byte, answer func(int, any)) {
	var req httpaction.Request
	if err := json.Unmarshal(body, &req); err != nil {
		answer(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	k.mu.Lock()
	a := k.actions[req.Action]
	k.mu.Unlock()
	if a == nil {
		answer(http.StatusNotFound, map[string]string{"error": "no action " + req.Action + " here"})
		return
	}
	run := func(ctx context.Context) result {
		tc := &TaskContext{Context: ctx, RunID: req.RunID, StepID: req.StepID, IdempotencyKey: req.IdempotencyKey, Attempt: req.Attempt, emit: func([]byte) {}}
		return k.runHandler(a, tc, req.Action, req.Input)
	}
	if !a.async {
		res, unknown := toWire(run(r.Context()))
		if unknown {
			answer(http.StatusBadGateway, res)
			return
		}
		answer(http.StatusOK, res)
		return
	}
	// Answer now; run after, and send the outcome to the callback.
	go func() {
		res, unknown := toWire(run(context.Background()))
		if unknown {
			return // no callback: the caller's lease expires and the step is taken up
		}
		if err := checkURL(req.Callback, k.http.AllowInsecure); err != nil {
			log.Printf("kairo: action %s: %v", req.Action, err)
			return
		}
		cb, _ := json.Marshal(map[string]any{"run_id": req.RunID, "act": req.Act, "attempt": req.Attempt, "result": res})
		hreq, err := http.NewRequest(http.MethodPost, req.Callback, bytes.NewReader(cb))
		if err != nil {
			return
		}
		hreq.Header.Set("Content-Type", "application/json")
		hreq.Header.Set("Kairo-Signature", httpaction.Sign(k.http.Secret, cb, time.Now()))
		if resp, err := k.client.Do(hreq); err == nil {
			resp.Body.Close()
		} else {
			log.Printf("kairo: action %s: its callback: %v", req.Action, err) // the lease expires and the step is taken up
		}
	}()
	answer(http.StatusAccepted, map[string]bool{"accepted": true})
}

func (k *Kairo) serveTick(_ http.ResponseWriter, r *http.Request, answer func(int, any)) {
	if k.http.TickSecret == "" || k.mode != Suspend {
		answer(http.StatusNotFound, map[string]string{"error": "no such path"})
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		answer(http.StatusMethodNotAllowed, map[string]string{"error": "GET or POST"})
		return
	}
	got := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	want := sha256.Sum256([]byte("Bearer " + k.http.TickSecret))
	if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
		answer(http.StatusUnauthorized, map[string]string{"error": "bad token"})
		return
	}
	next, ok, err := k.Tick(r.Context())
	if err != nil {
		answer(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		answer(http.StatusOK, map[string]any{"next": nil})
		return
	}
	answer(http.StatusOK, map[string]any{"next": next.UnixMilli()})
}

// TickHandler is a function's entry for a scheduler that calls it, not
// over HTTP (AWS Lambda from EventBridge Scheduler, say; ADR 0053): each
// call opens a Kairo (started, suspend mode), ticks, and closes it.
func TickHandler(open func(ctx context.Context) (*Kairo, error)) func(ctx context.Context) (next time.Time, ok bool, err error) {
	return func(ctx context.Context) (time.Time, bool, error) {
		k, err := open(ctx)
		if err != nil {
			return time.Time{}, false, err
		}
		defer k.Close()
		return k.Tick(ctx)
	}
}
