package kairo

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentTicksAreSerialized(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var active atomic.Int32
	var maximum atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/workflow-drives/claim" {
			http.NotFound(w, r)
			return
		}
		n := active.Add(1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(remoteClaimResponse{})
	}))
	defer srv.Close()

	k := &Kairo{
		remote:    &remote{url: srv.URL, client: srv.Client(), owner: "driver", lease: time.Minute, claims: map[string]remoteDriveClaim{}},
		nowFn:     time.Now,
		workflows: map[string]*workflowDef{},
		tickSlot:  make(chan struct{}, 1),
	}
	k.tickSlot <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make(chan error, 2)
	go func() { _, _, err := k.Tick(ctx); errs <- err }()
	<-entered
	go func() { _, _, err := k.Tick(ctx); errs <- err }()
	select {
	case <-entered:
		t.Fatal("second Tick entered while the first was running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent claims = %d", maximum.Load())
	}
}

func TestRemoteRunRetriesUnconfirmedStartWithSameID(t *testing.T) {
	var mu sync.Mutex
	var measured []Metric
	emitter := newMetricEmitter(MetricSinkFunc(func(batch []Metric) error {
		mu.Lock()
		measured = append(measured, batch...)
		mu.Unlock()
		return nil
	}))
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n := calls.Add(1); n == 1 {
			w.WriteHeader(http.StatusGatewayTimeout)
			json.NewEncoder(w).Encode(map[string]string{"run_id": "same"})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"run_id": "same", "existing": true})
	}))
	defer srv.Close()
	m := &remote{url: srv.URL, client: srv.Client(), metrics: emitter}
	existing, err := m.run(context.Background(), "plan", nil, "same", runSpec{})
	emitter.close()
	if err != nil || !existing || calls.Load() != 2 {
		t.Fatalf("existing=%v calls=%d err=%v", existing, calls.Load(), err)
	}
	mu.Lock()
	defer mu.Unlock()
	seen := map[string]int{}
	for _, metric := range measured {
		seen[metric.Name]++
		if metric.Labels.Cause == "" && metric.Labels.Outcome == "" {
			t.Fatalf("metric has no bounded cause/outcome: %+v", metric)
		}
	}
	if seen["kairo_run_start_unconfirmed_total"] != 1 || seen["kairo_run_start_confirmation_retries_total"] != 1 || seen["kairo_run_start_confirmation_seconds"] != 1 {
		t.Fatalf("measurements = %#v", measured)
	}
}

func TestRemoteRunRetriesOverloadWithSameID(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RunID string `json:"run_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RunID != "same" {
			t.Errorf("body=%+v err=%v", body, err)
		}
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{"error": "busy"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"run_id": "same", "existing": false})
	}))
	defer srv.Close()
	m := &remote{url: srv.URL, client: srv.Client()}
	existing, err := m.run(context.Background(), "plan", nil, "same", runSpec{})
	if err != nil || existing || calls.Load() != 2 {
		t.Fatalf("existing=%v calls=%d err=%v", existing, calls.Load(), err)
	}
}

func TestRemoteRunStopsRetryingWhenContextCancelled(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 2 {
			cancel()
		}
		if n > 2 {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]any{"run_id": "same", "existing": true})
			return
		}
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]string{"run_id": "same"})
	}))
	defer srv.Close()
	m := &remote{url: srv.URL, client: srv.Client()}
	_, err := m.run(ctx, "plan", nil, "same", runSpec{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
	time.Sleep(80 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls after cancellation = %d, want 2", got)
	}
	existing, err := m.run(context.Background(), "plan", nil, "same", runSpec{})
	if err != nil || !existing || calls.Load() != 3 {
		t.Fatalf("later drive: existing=%v calls=%d err=%v", existing, calls.Load(), err)
	}
}

func TestRemoteStopStartsWaitsForUnconfirmedStart(t *testing.T) {
	entered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		w.WriteHeader(http.StatusGatewayTimeout)
		json.NewEncoder(w).Encode(map[string]string{"run_id": "drained"})
	}))
	defer srv.Close()
	m := &remote{url: srv.URL, client: srv.Client()}
	done := make(chan error, 1)
	go func() {
		_, err := m.run(context.Background(), "plan", nil, "drained", runSpec{})
		done <- err
	}()
	<-entered
	m.draining.Store(true)
	m.stopStarts()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.waitStarts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v", err)
	}
}
