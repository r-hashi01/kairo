package kairo_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/httpaction"
	kairo "github.com/r-hashi01/kairo/sdk/go"
)

const secret = "test-secret"

// callbackServer fronts whichever process is "running" now: a callback may
// reach a process other than the one that called.
type callbackServer struct {
	mu  sync.Mutex
	cur *kairo.Kairo
	srv *httptest.Server
}

func newCallbackServer(t *testing.T) *callbackServer {
	cs := &callbackServer{}
	cs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs.mu.Lock()
		k := cs.cur
		cs.mu.Unlock()
		if k == nil {
			http.Error(w, "no process", http.StatusServiceUnavailable)
			return
		}
		k.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(cs.srv.Close)
	return cs
}

func (cs *callbackServer) url() string { return cs.srv.URL + "/kairo/callback" }

func (cs *callbackServer) to(k *kairo.Kairo) {
	cs.mu.Lock()
	cs.cur = k
	cs.mu.Unlock()
}

func open(t *testing.T, store kairo.Store, mode kairo.Mode, h kairo.HTTPOptions) *kairo.Kairo {
	t.Helper()
	k, err := kairo.Open(context.Background(), kairo.Options{Store: store, Mode: mode, HTTP: h})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestActionAnswersAtOnce(t *testing.T) {
	var runs atomic.Int32
	remote := open(t, kairo.NewMemStore(), kairo.Wait, kairo.HTTPOptions{Secret: secret})
	kairo.Action(remote, "shout", kairo.Real, func(_ *kairo.TaskContext, s string) (string, error) {
		runs.Add(1)
		return strings.ToUpper(s), nil
	})
	srv := httptest.NewServer(remote.Handler())
	defer srv.Close()

	cb := newCallbackServer(t)
	k := open(t, kairo.NewMemStore(), kairo.Wait, kairo.HTTPOptions{Secret: secret, CallbackURL: cb.url()})
	defer k.Close()
	kairo.Action(k, "shout", kairo.Real, func(*kairo.TaskContext, string) (string, error) {
		t.Error("runs at the URL")
		return "", nil
	}, kairo.URL(srv.URL+"/kairo/action", false))
	kairo.Workflow(k, "w", func(ctx *kairo.Context, s string) (string, error) { return kairo.Call[string](ctx, "shout", s) })
	if err := k.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	cb.to(k)
	out, err := kairo.Run[string](context.Background(), k, "w", "hi", kairo.WithID("sync-1"))
	if err != nil || out != "HI" || runs.Load() != 1 {
		t.Fatalf("%q %v, ran %d", out, err, runs.Load())
	}
}

func TestActionAnswersLaterToAnotherProcess(t *testing.T) {
	var runs atomic.Int32
	release := make(chan struct{})
	remote := open(t, kairo.NewMemStore(), kairo.Wait, kairo.HTTPOptions{Secret: secret, AllowInsecure: true})
	kairo.Action(remote, "render", kairo.Real, func(_ *kairo.TaskContext, n int) (int, error) {
		runs.Add(1)
		<-release
		return n * 2, nil
	}, kairo.URL("http://unused", true))
	srv := httptest.NewServer(remote.Handler())
	defer srv.Close()

	cb := newCallbackServer(t)
	store := kairo.NewMemStore()
	invocation := func() *kairo.Kairo {
		k := open(t, store, kairo.Suspend, kairo.HTTPOptions{Secret: secret, CallbackURL: cb.url()})
		kairo.Action(k, "render", kairo.Real, func(*kairo.TaskContext, int) (int, error) { return 0, nil }, kairo.URL(srv.URL+"/kairo/action", true))
		kairo.Workflow(k, "w", func(ctx *kairo.Context, n int) (int, error) { return kairo.Call[int](ctx, "render", n) })
		if err := k.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		cb.to(k)
		return k
	}
	k := invocation()
	if _, err := kairo.Run[int](context.Background(), k, "w", 21, kairo.WithID("async-1")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("%v", err)
	}
	k.Close()

	k = invocation()
	defer k.Close()
	close(release)
	for i := 0; ; i++ {
		out, err := kairo.Run[int](context.Background(), k, "w", 21, kairo.WithID("async-1"))
		if err == nil {
			if out != 42 {
				t.Fatalf("%d", out)
			}
			break
		}
		if !errors.Is(err, kairo.ErrSuspended) || i > 400 {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if runs.Load() != 1 {
		t.Fatalf("ran %d times", runs.Load())
	}
}

func TestSignaturesRefused(t *testing.T) {
	var runs atomic.Int32
	k := open(t, kairo.NewMemStore(), kairo.Wait, kairo.HTTPOptions{Secret: secret})
	kairo.Action(k, "x", kairo.Real, func(*kairo.TaskContext, any) (int, error) { runs.Add(1); return 1, nil })
	srv := httptest.NewServer(k.Handler())
	defer srv.Close()
	body := []byte(`{"run_id":"r","act":1,"attempt":1,"action":"x","input":null}`)
	post := func(path, sig string) int {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(body))
		if sig != "" {
			req.Header.Set("Kairo-Signature", sig)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for _, path := range []string{"/kairo/action", "/kairo/callback"} {
		for _, sig := range []string{"", httpaction.Sign("other", body, time.Now()), httpaction.Sign(secret, append(body, ' '), time.Now()),
			httpaction.Sign(secret, body, time.Now().Add(-10*time.Minute))} {
			if code := post(path, sig); code != http.StatusUnauthorized {
				t.Errorf("%s with %q: %d", path, sig, code)
			}
		}
	}
	if runs.Load() != 0 {
		t.Fatal("ran without a signature")
	}
	if code := post("/kairo/action", httpaction.Sign(secret, body, time.Now())); code != http.StatusOK || runs.Load() != 1 {
		t.Fatalf("%d, ran %d", code, runs.Load())
	}
}

func TestPlainHTTPOnlyHere(t *testing.T) {
	k := open(t, kairo.NewMemStore(), kairo.Wait, kairo.HTTPOptions{Secret: secret, CallbackURL: "http://10.0.0.5/cb"})
	defer k.Close()
	kairo.Action(k, "a", kairo.Real, func(*kairo.TaskContext, any) (any, error) { return nil, nil }, kairo.URL("https://example.com/a", false))
	if err := k.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "plain http") {
		t.Fatalf("a plain http callback to another machine: %v", err)
	}
	k2 := open(t, kairo.NewMemStore(), kairo.Wait, kairo.HTTPOptions{Secret: secret, CallbackURL: "http://10.0.0.5/cb", AllowInsecure: true})
	defer k2.Close()
	kairo.Action(k2, "a", kairo.Real, func(*kairo.TaskContext, any) (any, error) { return nil, nil }, kairo.URL("http://10.0.0.6/a", false))
	if err := k2.Start(context.Background()); err != nil {
		t.Fatalf("allowed: %v", err)
	}
}

// A scheduler ticks over HTTP with its token; Wake is told when; the
// workflow goes on.
func TestTickOverHTTP(t *testing.T) {
	var wakes []time.Time
	k, err := kairo.Open(context.Background(), kairo.Options{Store: kairo.NewMemStore(), Mode: kairo.Suspend,
		HTTP: kairo.HTTPOptions{TickSecret: "cron"}, Wake: func(at time.Time) error { wakes = append(wakes, at); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	kairo.Action(k, "llm", kairo.Unprotected, func(_ *kairo.TaskContext, q string) (string, error) { return strings.ToUpper(q), nil })
	kairo.Workflow(k, "w", func(ctx *kairo.Context, q string) (string, error) {
		if err := ctx.Sleep(200 * time.Millisecond); err != nil {
			return "", err
		}
		return kairo.Call[string](ctx, "llm", q)
	})
	k.Start(context.Background())
	srv := httptest.NewServer(k.Handler())
	defer srv.Close()
	if _, err := kairo.Run[string](context.Background(), k, "w", "hi", kairo.WithID("tick-1")); !errors.Is(err, kairo.ErrSuspended) {
		t.Fatalf("%v", err)
	}
	if len(wakes) != 1 {
		t.Fatalf("wakes %v", wakes)
	}
	tick := func(token string) (int, map[string]any) {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/kairo/tick", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	if code, _ := tick(""); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := tick("nope"); code != http.StatusUnauthorized {
		t.Fatalf("a wrong token: %d", code)
	}
	if code, m := tick("cron"); code != http.StatusOK || int64(m["next"].(float64)) != wakes[0].UnixMilli() {
		t.Fatalf("too early: %d %v", code, m)
	}
	time.Sleep(time.Until(wakes[0]) + 20*time.Millisecond)
	if code, m := tick("cron"); code != http.StatusOK || m["next"] != nil {
		t.Fatalf("after: %d %v", code, m)
	}
	if out, err := kairo.Run[string](context.Background(), k, "w", "hi", kairo.WithID("tick-1")); err != nil || out != "HI" {
		t.Fatalf("%q %v", out, err)
	}
}
