package httpaction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
