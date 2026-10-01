package httpexec

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kairo/task"
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
	if res := x.Execute(context.Background(), tk, nil); !res.Retryable {
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
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	var chunks []string
	res := New().Execute(context.Background(), &task.Task{Params: json.RawMessage(`{"url":"` + srv.URL + `","stream":true}`)},
		func(b []byte) { chunks = append(chunks, string(b)) })
	if res.Err != "" || len(chunks) != 3 || !strings.Contains(string(res.Output), `t2`) {
		t.Fatalf("%+v %v", res, chunks)
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
