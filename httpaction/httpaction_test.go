package httpaction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/task"
)

func TestSignAndVerify(t *testing.T) {
	body := []byte(`{"a":1}`)
	now := time.UnixMilli(1_700_000_000_000)
	h := Sign("s", body, now)
	if err := Verify("s", body, h, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		secret, header string
		body           []byte
		now            time.Time
	}{
		"missing":         {"s", "", body, now},
		"other secret":    {"other", h, body, now},
		"changed body":    {"s", h, []byte(`{"a":2}`), now},
		"too old":         {"s", h, body, now.Add(10 * time.Minute)},
		"from the future": {"s", h, body, now.Add(-10 * time.Minute)},
		"bad number":      {"s", "t=x,v1=00", body, now},
	} {
		if err := Verify(c.secret, c.body, c.header, c.now); !errors.Is(err, ErrSignature) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// The SDKs sign the same way: a signature made by the TypeScript SDK is
// accepted here, and one made here is accepted there.
func TestSignaturesAcrossLanguages(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not found")
	}
	_, file, _, _ := runtime.Caller(0)
	httpTS := filepath.Join(filepath.Dir(file), "..", "sdk", "ts", "src", "http.ts")
	body := `{"run_id":"r","act":1}`
	now := time.Now()
	script := `import { sign, verify } from ` + jsonString(httpTS) + `;
const [secret, body, theirs] = process.argv.slice(2);
verify(secret, body, theirs);
process.stdout.write(sign(secret, body));`
	f := filepath.Join(t.TempDir(), "sig.ts")
	if err := os.WriteFile(f, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, f, "secret", body, Sign("secret", []byte(body), now)).Output()
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	if err := Verify("secret", []byte(body), strings.TrimSpace(string(out)), time.Now()); err != nil {
		t.Fatalf("the TypeScript signature %q: %v", out, err)
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestHandler(t *testing.T) {
	var got *task.Task
	h := Handler("s", func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		got = tk
		switch string(tk.Input) {
		case `"ok"`:
			return task.Result{Output: json.RawMessage(`{"yes":0.9}`)}
		case `"busy"`:
			return task.Result{Err: "busy", Retryable: true, ErrType: "rate_limited"}
		default:
			return task.Result{Err: "lost", Unknown: true}
		}
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	post := func(path, input, sig string) (int, map[string]any) {
		body, _ := json.Marshal(Request{RunID: "r", Act: 3, Attempt: 2, Action: "jev.noul", Input: json.RawMessage(input), IdempotencyKey: "r/s"})
		if sig == "" {
			sig = Sign("s", body, time.Now())
		}
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(body))
		req.Header.Set("Kairo-Signature", sig)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	if code, m := post("/kairo/action", `"ok"`, ""); code != 200 || m["output"].(map[string]any)["yes"] != 0.9 {
		t.Fatalf("%d %v", code, m)
	}
	if got.RunID != "r" || got.Act != 3 || got.Attempt != 2 || got.IdemKey != "r/s" || got.Action != "jev.noul" {
		t.Fatalf("task %+v", got)
	}
	if code, m := post("/kairo/action", `"busy"`, ""); code != 200 || m["error"] != "busy" || m["retryable"] != true || m["error_type"] != "rate_limited" {
		t.Fatalf("%d %v", code, m)
	}
	if code, _ := post("/kairo/action", `"x"`, ""); code != http.StatusBadGateway {
		t.Fatalf("unknown: %d", code)
	}
	if code, _ := post("/kairo/action", `"ok"`, "t=1,v1=00"); code != http.StatusUnauthorized {
		t.Fatalf("bad signature: %d", code)
	}
	if code, _ := post("/kairo/other", `"ok"`, ""); code != http.StatusNotFound {
		t.Fatalf("other path: %d", code)
	}
}

func TestHandlerBoundsRequestAndResponseBodies(t *testing.T) {
	var runs int
	responseOutput := func(size int) json.RawMessage {
		const envelope = `{"output":}`
		rawSize := size - len(envelope)
		return json.RawMessage(`"` + strings.Repeat("x", rawSize-2) + `"`)
	}
	h := Handler("s", func(_ context.Context, tk *task.Task, _ func([]byte)) task.Result {
		runs++
		switch string(tk.Input) {
		case `"exact-response"`:
			return task.Result{Output: responseOutput(MaxBody)}
		case `"over-response"`:
			return task.Result{Output: responseOutput(MaxBody + 1)}
		default:
			return task.Result{Output: json.RawMessage(`1`)}
		}
	})
	srv := httptest.NewServer(h)
	defer srv.Close()
	post := func(body []byte) (int, []byte) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/kairo/action", bytes.NewReader(body))
		req.Header.Set("Kairo-Signature", Sign("s", body, time.Now()))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, data
	}
	requestBody := func(size int) []byte {
		prefix := []byte(`{"run_id":"r","action":"x","input":"`)
		suffix := []byte(`"}`)
		body := make([]byte, 0, size)
		body = append(body, prefix...)
		body = append(body, strings.Repeat("x", size-len(prefix)-len(suffix))...)
		return append(body, suffix...)
	}
	if code, _ := post(requestBody(MaxBody)); code != http.StatusOK || runs != 1 {
		t.Fatalf("exact request: %d, ran %d", code, runs)
	}
	if code, _ := post(requestBody(MaxBody + 1)); code != http.StatusRequestEntityTooLarge || runs != 1 {
		t.Fatalf("oversized request: %d, ran %d", code, runs)
	}
	for input, want := range map[string]struct {
		code int
		size int
	}{
		`"exact-response"`: {http.StatusOK, MaxBody},
		`"over-response"`:  {http.StatusBadGateway, len(`{"error":"response body too large"}`)},
	} {
		body, _ := json.Marshal(Request{RunID: "r", Action: "x", Input: json.RawMessage(input)})
		code, response := post(body)
		if code != want.code || len(response) != want.size {
			t.Errorf("%s response: %d, %d bytes", input, code, len(response))
		}
	}
}
