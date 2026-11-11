package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/httpaction"
	"github.com/r-hashi01/kairo/task"
)

// ActionCallbacks is the public, capability-authenticated callback surface.
// Its handler intentionally contains none of the control API routes (ADR 0070).
type ActionCallbacks struct {
	E            *engine.Engine
	Capabilities *httpaction.CallbackCapabilities
}

func (a *ActionCallbacks) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/action-results/{capability}", a.post)
	return mux
}

func (a *ActionCallbacks) post(w http.ResponseWriter, r *http.Request) {
	answer := func(status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	claims, err := a.Capabilities.Verify(r.PathValue("capability"))
	if err != nil {
		if errors.Is(err, httpaction.ErrCapabilityExpired) {
			answer(http.StatusGone, map[string]string{"error": "callback expired"})
		} else {
			answer(http.StatusUnauthorized, map[string]string{"error": "invalid callback capability"})
		}
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpaction.MaxBody))
	if err != nil {
		answer(http.StatusRequestEntityTooLarge, map[string]string{"error": "callback body too large"})
		return
	}
	res, err := decodeCallbackResult(body)
	if err != nil {
		answer(http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := a.E.Callback(r.Context(), claims.Task(), res); err != nil {
		switch {
		case errors.Is(err, engine.ErrUnknownRun):
			answer(http.StatusNotFound, map[string]string{"error": "callback target not found"})
		case r.Context().Err() != nil:
			answer(http.StatusGatewayTimeout, map[string]string{"error": "callback durability unconfirmed"})
		default:
			answer(http.StatusInternalServerError, map[string]string{"error": "callback could not be recorded"})
		}
		return
	}
	answer(http.StatusOK, map[string]bool{"accepted": true})
}

func decodeCallbackResult(body []byte) (task.Result, error) {
	var fields map[string]json.RawMessage
	if len(body) == 0 || json.Unmarshal(body, &fields) != nil {
		return task.Result{}, errors.New("callback result must be a JSON object")
	}
	allowed := map[string]bool{"output": true, "error": true, "retryable": true, "error_type": true, "wait": true}
	for key := range fields {
		if !allowed[key] {
			return task.Result{}, errors.New("callback result has an unknown field")
		}
	}
	_, output := fields["output"]
	_, failure := fields["error"]
	_, waiting := fields["wait"]
	if btoi(output)+btoi(failure)+btoi(waiting) != 1 {
		return task.Result{}, errors.New("callback result needs exactly one of output, error, or wait")
	}
	if output {
		if fields["retryable"] != nil || fields["error_type"] != nil || !json.Valid(fields["output"]) {
			return task.Result{}, errors.New("invalid callback output")
		}
		return task.Result{Output: bytes.Clone(fields["output"])}, nil
	}
	if failure {
		var out task.Result
		if json.Unmarshal(fields["error"], &out.Err) != nil || out.Err == "" {
			return task.Result{}, errors.New("callback error must be a non-empty string")
		}
		if raw := fields["retryable"]; raw != nil && json.Unmarshal(raw, &out.Retryable) != nil {
			return task.Result{}, errors.New("callback retryable must be boolean")
		}
		if raw := fields["error_type"]; raw != nil && json.Unmarshal(raw, &out.ErrType) != nil {
			return task.Result{}, errors.New("callback error_type must be a string")
		}
		return out, nil
	}
	if fields["retryable"] != nil || fields["error_type"] != nil {
		return task.Result{}, errors.New("callback wait cannot carry error fields")
	}
	var wait task.Wait
	if json.Unmarshal(fields["wait"], &wait) != nil || wait.Until <= 0 {
		return task.Result{}, errors.New("callback wait needs a positive until")
	}
	return task.Result{Wait: &wait}, nil
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}
