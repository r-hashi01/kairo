package kairo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/r-hashi01/kairo/httpaction"
)

func actionResultBody(size int) []byte {
	prefix := []byte(`{"output":"`)
	suffix := []byte(`"}`)
	if size < len(prefix)+len(suffix) {
		panic("result body is too small")
	}
	body := make([]byte, 0, size)
	body = append(body, prefix...)
	body = append(body, strings.Repeat("x", size-len(prefix)-len(suffix))...)
	body = append(body, suffix...)
	return body
}

func TestRemoteActionHTTPBodiesAreBounded(t *testing.T) {
	exact := actionResultBody(httpaction.MaxBody)
	over := append(append([]byte(nil), exact...), ' ')
	var requestReached atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/exact":
			_, _ = w.Write(exact)
		case "/over":
			_, _ = w.Write(over)
		case "/truncated":
			w.Header().Set("Content-Length", "10")
			w.Header().Set("Connection", "close")
			_, _ = w.Write([]byte("x"))
		case "/request":
			requestReached.Store(true)
			_, _ = w.Write([]byte(`{"output":null}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	k := &Kairo{http: HTTPOptions{Secret: "s"}, client: srv.Client()}
	call := func(path string, input json.RawMessage) result {
		return k.callRemote(context.Background(), task{RunID: "r", Action: "x", IdemKey: "r/1"}, &actionDef{url: srv.URL + path}, input)
	}
	if got := call("/exact", json.RawMessage(`null`)); got.Err != "" || got.Unknown || len(got.Output) == 0 {
		t.Fatalf("exact limit: %+v", got)
	}
	if got := call("/over", json.RawMessage(`null`)); !got.Unknown || !strings.Contains(got.Err, "exceeds "+strconv.Itoa(httpaction.MaxBody)) {
		t.Fatalf("over limit: %+v", got)
	}
	if got := call("/truncated", json.RawMessage(`null`)); !got.Unknown || !strings.Contains(got.Err, "truncated") {
		t.Fatalf("truncated: %+v", got)
	}

	largeInput := json.RawMessage(fmt.Sprintf("%q", strings.Repeat("x", httpaction.MaxBody)))
	if got := call("/request", largeInput); got.Unknown || got.ErrType != "http_body_too_large" {
		t.Fatalf("oversized request: %+v", got)
	}
	if requestReached.Load() {
		t.Fatal("an oversized request reached the action server")
	}
}
