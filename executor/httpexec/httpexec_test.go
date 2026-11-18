package httpexec

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/task"
)

func TestJSONAndClassification(t *testing.T) {
	var gotKey string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		json.NewDecoder(r.Body).Decode(&gotBody)
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"text":"hi","usage":{"total_tokens":42}}`)
		case "/limited":
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(429)
		default:
			w.WriteHeader(400)
		}
	}))
	defer srv.Close()
	x := New()
	tk := &task.Task{IdemKey: "run/step", Input: json.RawMessage(`{"prompt":"p"}`),
		Params: json.RawMessage(`{"url":"` + srv.URL + `/ok","body":{"model":"m","prompt":"default"}}`)}
	res := x.Execute(context.Background(), tk, func([]byte) {})
	if res.Err != "" || res.Tokens != 42 || gotKey != "run/step" || gotBody["model"] != "m" || gotBody["prompt"] != "p" {
		t.Fatalf("%+v key=%q body=%v", res, gotKey, gotBody)
	}
	tk.Params = json.RawMessage(`{"url":"` + srv.URL + `/limited"}`)
	if res := x.Execute(context.Background(), tk, nil); !res.Retryable || res.RetryAfter != 2000 {
		t.Fatalf("429 not retryable: %+v", res)
	}
	tk.Params = json.RawMessage(`{"url":"` + srv.URL + `/bad"}`)
	if res := x.Execute(context.Background(), tk, nil); res.Retryable || res.Err == "" {
		t.Fatalf("400: %+v", res)
	}
	srv.Close()
	tk.Params = json.RawMessage(`{"url":"` + srv.URL + `/ok"}`)
	if res := x.Execute(context.Background(), tk, nil); !res.Unknown {
		t.Fatalf("transport error should be unknown: %+v", res)
	}
}

func TestSSEStreaming(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: {\"delta\":\"t%d\"}\n\n", i)
			w.(http.Flusher).Flush()
		}
		fmt.Fprint(w, "data: {\"usage\":{\"total_tokens\":7}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	var chunks []string
	res := New().Execute(context.Background(), &task.Task{Params: json.RawMessage(`{"url":"` + srv.URL + `","stream":true}`)},
		func(b []byte) { chunks = append(chunks, string(b)) })
	if res.Err != "" || res.Tokens != 7 || len(chunks) != 4 || !strings.Contains(string(res.Output), `t2`) {
		t.Fatalf("%+v %v", res, chunks)
	}
}

func TestSuccessfulResponseBodyLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/json":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"text":%q}`, strings.Repeat("x", 80))
		case "/text":
			fmt.Fprint(w, strings.Repeat("x", 80))
		}
	}))
	defer srv.Close()

	for _, path := range []string{"/json", "/text"} {
		t.Run(path[1:], func(t *testing.T) {
			x := New()
			x.MaxResponseBytes = 64
			res := x.Execute(context.Background(), &task.Task{Params: json.RawMessage(`{"url":"` + srv.URL + path + `"}`)}, nil)
			if !res.Unknown || !strings.Contains(res.Err, "exceeds 64 bytes") || len(res.Output) != 0 {
				t.Fatalf("over-limit response treated as success: %+v", res)
			}
		})
	}
}

func TestSSERetainedOutputLimit(t *testing.T) {
	tests := []struct {
		name   string
		events []string
	}{
		{name: "one oversized event", events: []string{`{"delta":"` + strings.Repeat("x", 80) + `"}`}},
		{name: "many events", events: []string{`{"delta":"one"}`, `{"delta":"two"}`, `{"delta":"three"}`, `{"delta":"four"}`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				for _, event := range tt.events {
					fmt.Fprintf(w, "data: %s\n\n", event)
					w.(http.Flusher).Flush()
				}
				fmt.Fprint(w, "data: [DONE]\n\n")
			}))
			defer srv.Close()

			x := New()
			x.MaxResponseBytes = 48
			var chunks []string
			res := x.Execute(context.Background(), &task.Task{Params: json.RawMessage(`{"url":"` + srv.URL + `","stream":true}`)},
				func(b []byte) { chunks = append(chunks, string(b)) })
			if !res.Unknown || !strings.Contains(res.Err, "exceeds 48 bytes") || len(res.Output) != 0 {
				t.Fatalf("over-limit SSE treated as success: %+v", res)
			}
			if len(chunks) != len(tt.events) {
				t.Fatalf("live relay stopped at retained-output limit: got %d chunks, want %d", len(chunks), len(tt.events))
			}
		})
	}
}

func TestResponseLimitValidation(t *testing.T) {
	for _, limit := range []int64{-1, MaximumMaxResponseBytes + 1} {
		if _, err := NormalizeMaxResponseBytes(limit); err == nil {
			t.Fatalf("NormalizeMaxResponseBytes(%d) succeeded", limit)
		}
	}
	if got, err := NormalizeMaxResponseBytes(0); err != nil || got != DefaultMaxResponseBytes {
		t.Fatalf("default = %d, %v", got, err)
	}
}

func TestUsesHTTP2(t *testing.T) {
	var proto string
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proto = r.Proto
		fmt.Fprint(w, `{}`)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	x := New()
	x.Client.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	if res := x.Execute(context.Background(), &task.Task{Params: json.RawMessage(`{"url":"` + srv.URL + `"}`)}, nil); res.Err != "" {
		t.Fatal(res.Err)
	}
	if proto != "HTTP/2.0" {
		t.Fatalf("protocol %s", proto)
	}
}
