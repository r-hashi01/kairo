package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
)

func exactJSONObject(t *testing.T, limit int64, prefix, suffix string) []byte {
	t.Helper()
	padding := int(limit) - len(prefix) - len(suffix)
	if padding < 0 {
		t.Fatalf("body framing exceeds %d bytes", limit)
	}
	body := make([]byte, 0, limit)
	body = append(body, prefix...)
	body = append(body, bytes.Repeat([]byte{'x'}, padding)...)
	body = append(body, suffix...)
	if int64(len(body)) != limit || !json.Valid(body) {
		t.Fatalf("made invalid exact-limit JSON: len=%d limit=%d", len(body), limit)
	}
	return body
}

func exactJSONString(t *testing.T, limit int64) []byte {
	t.Helper()
	return exactJSONObject(t, limit, `"`, `"`)
}

func postControlBody(h http.Handler, path string, body io.Reader) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, body))
	return w
}

func overLimit(body []byte) io.Reader {
	// MultiReader leaves ContentLength unknown, so this exercises the
	// limit+1 streaming check rather than only the header fast path.
	return io.MultiReader(bytes.NewReader(body), bytes.NewReader([]byte{' '}))
}

func TestControlAPIRejectsBodiesOverEndpointLimits(t *testing.T) {
	e := newRegistryEngine(t)
	for _, raw := range []string{
		`{"name":"body-limit-wait","root":{"kind":"wait","id":"wait","signal":"go"}}`,
		`{"name":"kairo.workflow","root":{"kind":"wait","id":"done","signal":"done"}}`,
	} {
		var def ir.Definition
		if err := json.Unmarshal([]byte(raw), &def); err != nil {
			t.Fatal(err)
		}
		if _, err := e.RegisterPlan(&def); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	for _, req := range []engine.SubmitRequest{
		{Plan: "body-limit-wait", RunID: "signal-over-limit"},
		{Plan: "kairo.workflow", RunID: "finish-over-limit", Workflow: "body-limit"},
	} {
		if _, err := e.Submit(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	h := (&API{E: e}).Handler()

	t.Run("shared JSON", func(t *testing.T) {
		prefix := `{"plan":"missing","run_id":"read-json-limit","padding":"`
		exact := exactJSONObject(t, jsonBodyLimit, prefix, `"}`)
		if w := postControlBody(h, "/v1/runs", bytes.NewReader(exact)); w.Code == http.StatusRequestEntityTooLarge {
			t.Fatalf("exact limit was rejected: %d %s", w.Code, w.Body.String())
		}
		if w := postControlBody(h, "/v1/runs", overLimit(exact)); w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("over limit: %d %s", w.Code, w.Body.String())
		}
	})

	t.Run("nodes", func(t *testing.T) {
		exact := exactJSONObject(t, nodesBodyLimit, `{"action":"exact-limit-node","effect":"unprotected","padding":"`, `"}`)
		if w := postControlBody(h, "/v1/nodes", bytes.NewReader(exact)); w.Code != http.StatusOK {
			t.Fatalf("exact limit: %d %s", w.Code, w.Body.String())
		}
		over := exactJSONObject(t, nodesBodyLimit, `{"action":"over-limit-node","effect":"unprotected","padding":"`, `"}`)
		if w := postControlBody(h, "/v1/nodes", overLimit(over)); w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("over limit: %d %s", w.Code, w.Body.String())
		}
		if got := e.Registry().Lookup("over-limit-node").Effect; got != ir.EffectReal {
			t.Fatalf("oversized node registration changed the registry: %s", got)
		}
	})

	event := exactJSONString(t, eventBodyLimit)
	t.Run("signal", func(t *testing.T) {
		if w := postControlBody(h, "/v1/runs/missing/signals/go", bytes.NewReader(event)); w.Code != http.StatusAccepted {
			t.Fatalf("exact limit: %d %s", w.Code, w.Body.String())
		}
		if w := postControlBody(h, "/v1/runs/signal-over-limit/signals/go", overLimit(event)); w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("over limit: %d %s", w.Code, w.Body.String())
		}
		info, err := e.Get(context.Background(), "signal-over-limit")
		if err != nil {
			t.Fatal(err)
		}
		if !info.FinishedAt.IsZero() {
			t.Fatalf("oversized signal finished the run: %+v", info)
		}
	})

	t.Run("workflow finish", func(t *testing.T) {
		if w := postControlBody(h, "/v1/runs/missing/finish", bytes.NewReader(event)); w.Code != http.StatusNotFound {
			t.Fatalf("exact limit: %d %s", w.Code, w.Body.String())
		}
		if w := postControlBody(h, "/v1/runs/finish-over-limit/finish", overLimit(event)); w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("over limit: %d %s", w.Code, w.Body.String())
		}
		info, err := e.Get(context.Background(), "finish-over-limit")
		if err != nil {
			t.Fatal(err)
		}
		if !info.FinishedAt.IsZero() {
			t.Fatalf("oversized finish changed the run: %+v", info)
		}
	})
}
