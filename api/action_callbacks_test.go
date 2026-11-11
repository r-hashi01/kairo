package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/httpaction"
	"github.com/r-hashi01/kairo/task"
)

func TestDecodeCallbackResultIsStrict(t *testing.T) {
	valid := []string{
		`{"output":{"ok":true}}`,
		`{"error":"busy","retryable":true,"error_type":"upstream"}`,
		`{"wait":{"until":123,"output":null}}`,
	}
	for _, body := range valid {
		if _, err := decodeCallbackResult([]byte(body)); err != nil {
			t.Fatalf("valid %s: %v", body, err)
		}
	}
	invalid := []string{
		`{}`,
		`{"output":1,"error":"bad"}`,
		`{"output":1,"retryable":true}`,
		`{"error":""}`,
		`{"wait":{"until":0}}`,
		`{"output":1,"capability":"leak"}`,
	}
	for _, body := range invalid {
		if _, err := decodeCallbackResult([]byte(body)); err == nil {
			t.Fatalf("invalid body accepted: %s", body)
		}
	}
}

func TestActionCallbackRejectsInvalidAndExpiredCapabilities(t *testing.T) {
	now := time.Unix(100, 0)
	caps, err := httpaction.NewCallbackCapabilities("https://callbacks.example", [][]byte{[]byte(strings.Repeat("k", 32))}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	h := (&ActionCallbacks{Capabilities: caps}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/v1/action-results/not-a-token", strings.NewReader(`{"output":1}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("invalid status = %d", w.Code)
	}
	callback := caps.Issue(&task.Task{RunID: "r", StepID: "s", Action: "a", Act: 1, Attempt: 1}, now.Add(time.Millisecond).UnixMilli())
	now = now.Add(time.Millisecond)
	req = httptest.NewRequest(http.MethodPost, callback.URL, strings.NewReader(`{"output":1}`))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusGone {
		t.Fatalf("expired status = %d", w.Code)
	}
}
