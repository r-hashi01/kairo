// Package httpaction serves kairo actions over HTTP(S) (ADR 0052): the
// runtime embedded in an application (the TypeScript and Python SDKs)
// calls an action at its URL with a signed POST, and the answer is the
// step's outcome. This is the Go side of that contract, for action servers
// written in Go (e.g. executor/jev's kairo-jev, ADR 0055).
//
// The signature, the request body and the result body are the same as the
// SDKs' (sdk/ts/src/http.ts, sdk/python/kairo_worker/http.py):
//
//	Kairo-Signature: t=<unix ms>,v1=<hex HMAC-SHA256(secret, t + "." + body)>
//
// The handler answers at once (200 with the result); it does not accept
// for later (202 and a callback), which actions that take long need.
package httpaction

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kairo/task"
)

// Tolerance is how old (or how far ahead) a signed request may be.
const Tolerance = 5 * time.Minute

// MaxBody bounds a request body.
const MaxBody = 16 << 20

// ErrSignature is wrapped by Verify's errors.
var ErrSignature = errors.New("signature")

// Request is a step called over HTTP(S).
type Request struct {
	RunID          string          `json:"run_id"`
	StepID         string          `json:"step_id"`
	Act            uint32          `json:"act"`
	Attempt        int32           `json:"attempt"`
	Action         string          `json:"action"`
	Input          json.RawMessage `json:"input"`
	IdempotencyKey string          `json:"idempotency_key"`
	Callback       string          `json:"callback,omitempty"`
}

// Result is an outcome as the 200 answer carries it.
type Result struct {
	Output    json.RawMessage `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
	Retryable bool            `json:"retryable,omitempty"`
	ErrorType string          `json:"error_type,omitempty"`
	Wait      *task.Wait      `json:"wait,omitempty"`
}

func mac(secret string, t int64, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(strconv.FormatInt(t, 10)))
	m.Write([]byte("."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// Sign returns the Kairo-Signature header of body at time at.
func Sign(secret string, body []byte, at time.Time) string {
	t := at.UnixMilli()
	return fmt.Sprintf("t=%d,v1=%s", t, mac(secret, t, body))
}

// Verify returns an error wrapping ErrSignature unless header signs body
// with secret, within Tolerance of now.
func Verify(secret string, body []byte, header string, now time.Time) error {
	var t int64 = -1
	var v1 string
	for _, part := range strings.Split(header, ",") {
		k, v, _ := strings.Cut(part, "=")
		switch k {
		case "t":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				t = n
			}
		case "v1":
			v1 = v
		}
	}
	if t < 0 || v1 == "" {
		return fmt.Errorf("%w: missing", ErrSignature)
	}
	if d := now.Sub(time.UnixMilli(t)); d > Tolerance || d < -Tolerance {
		return fmt.Errorf("%w: too old", ErrSignature)
	}
	if !hmac.Equal([]byte(v1), []byte(mac(secret, t, body))) {
		return fmt.Errorf("%w: bad", ErrSignature)
	}
	return nil
}

// FromTask is the answer for an executor's result. An unknown outcome has
// no 200 body (the caller cannot tell it from success): Handler answers it
// with 502, which the caller takes as unknown.
func FromTask(r task.Result) (res Result, unknown bool) {
	switch {
	case r.Unknown:
		return Result{Error: r.Err, ErrorType: r.ErrType}, true
	case r.Err != "":
		return Result{Error: r.Err, Retryable: r.Retryable, ErrorType: r.ErrType}, false
	case r.Wait != nil:
		return Result{Wait: r.Wait}, false
	default:
		return Result{Output: r.Output}, false
	}
}

// Handler serves POST requests whose path ends in "/action": it checks the
// signature, then answers with serve's result. serve has an executor's
// shape (engine.Executor's Execute); it gets the request as a task (Input
// is the step's input), and its chunks go nowhere (no live stream here).
func Handler(secret string, serve func(ctx context.Context, t *task.Task, emit func([]byte)) task.Result) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answer := func(status int, v any) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			json.NewEncoder(w).Encode(v)
		}
		if !strings.HasSuffix(r.URL.Path, "/action") {
			answer(http.StatusNotFound, map[string]string{"error": "no such path"})
			return
		}
		if r.Method != http.MethodPost {
			answer(http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
		if err != nil {
			answer(http.StatusRequestEntityTooLarge, map[string]string{"error": err.Error()})
			return
		}
		if err := Verify(secret, body, r.Header.Get("Kairo-Signature"), time.Now()); err != nil {
			answer(http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		var req Request
		if err := json.Unmarshal(body, &req); err != nil {
			answer(http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		res, unknown := FromTask(serve(r.Context(), &task.Task{
			RunID: req.RunID, StepID: req.StepID, Act: req.Act, Attempt: req.Attempt,
			IdemKey: req.IdempotencyKey, Action: req.Action, Input: req.Input,
		}, func([]byte) {}))
		if unknown {
			answer(http.StatusBadGateway, res)
			return
		}
		answer(http.StatusOK, res)
	})
}
