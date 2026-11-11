package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
	opmetrics "github.com/r-hashi01/kairo/metrics"
	"github.com/r-hashi01/kairo/sched"
	"github.com/r-hashi01/kairo/wal"
)

func TestWriteJSONConcurrentAtomicReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans", "same.json")
	const writers = 100
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			errs <- writeJSON(path, map[string]int{"writer": n})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]int
	if err := json.Unmarshal(b, &got); err != nil || got["writer"] < 0 || got["writer"] >= writers {
		t.Fatalf("final file: %s: %v", b, err)
	}
	left, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".same.json.tmp-*"))
	if err != nil || len(left) != 0 {
		t.Fatalf("temporary files: %v: %v", left, err)
	}
}

func TestClaimWorkflowByRunID(t *testing.T) {
	measured := make(chan opmetrics.Measurement, 4)
	e, err := engine.New(engine.Config{Shards: 2, NoSync: true, Metrics: opmetrics.SinkFunc(func(batch []opmetrics.Measurement) error {
		for _, metric := range batch {
			measured <- metric
		}
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	var def ir.Definition
	if err := json.Unmarshal([]byte(`{"name":"kairo.workflow","root":{"kind":"wait","id":"done","signal":"done"}}`), &def); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterPlan(&def); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), engine.SubmitRequest{
		Plan: "kairo.workflow", RunID: "target", Drive: true,
		Input: json.RawMessage(`{"workflow":"orders","version":"1"}`),
	}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"run_id":"target","owner":"driver","lease_ms":60000,"workflows":{"orders":["1"]}}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow-drives/claim", bytes.NewReader(body))
	w := httptest.NewRecorder()
	(&API{E: e}).Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	var got driveClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Claim == nil || got.Claim.Run.RunID != "target" || got.Claim.Until <= time.Now().UnixMilli() {
		t.Fatalf("claim: %+v", got)
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case metric := <-measured:
			if metric.Labels.Path != "direct" || metric.Labels.Outcome != "claimed" {
				t.Fatalf("claim measurement = %+v", metric)
			}
			seen[metric.Name] = true
		case <-time.After(time.Second):
			t.Fatalf("claim measurements = %v", seen)
		}
	}
	if !seen[opmetrics.WorkflowClaim] || !seen[opmetrics.WorkflowClaimDuration] {
		t.Fatalf("claim measurements = %v", seen)
	}
}

func TestStartAdmissionOverloadReturnsRetryAfterBeforeCreatingRun(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	e, err := engine.New(engine.Config{
		Shards:      1,
		DefaultTier: engine.TierMemory,
		Admission:   sched.AdmissionConfig{MaxStarting: 1},
		Sinks: func(tier engine.Tier, _ int) (wal.Sink, error) {
			if tier != engine.TierMemory {
				return nil, nil
			}
			return &wal.MemSink{Delay: func() {
				once.Do(func() { close(entered) })
				<-release
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseOnce.Do(func() { close(release) })
		e.Close()
	}()
	var def ir.Definition
	if err := json.Unmarshal([]byte(`{"name":"wait","root":{"kind":"wait","id":"w","signal":"go"}}`), &def); err != nil {
		t.Fatal(err)
	}
	if _, err := e.RegisterPlan(&def); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	h := (&API{E: e}).Handler()
	post := func(id, tenant string) *httptest.ResponseRecorder {
		body := fmt.Sprintf(`{"plan":"wait","run_id":%q,"tenant":%q}`, id, tenant)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/runs", bytes.NewBufferString(body)))
		return w
	}
	done := make(chan *httptest.ResponseRecorder, 5)
	go func() { done <- post("start-0", "A") }()
	<-entered
	for i := 1; i <= 4; i++ {
		i := i
		go func() { done <- post(fmt.Sprintf("start-%d", i), string(rune('A'+i%2))) }()
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		starts := e.Stats().Starts
		if len(starts) == 1 && starts[0].Queued == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("starts never queued: %+v", starts)
		}
		time.Sleep(time.Millisecond)
	}
	w := post("rejected", "C")
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d Retry-After=%q body=%s", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
	if _, err := e.Get(context.Background(), "rejected"); err == nil {
		t.Fatal("rejected run exists")
	}
	releaseOnce.Do(func() { close(release) })
	for range 5 {
		if w := <-done; w.Code != http.StatusCreated {
			t.Fatalf("admitted status=%d body=%s", w.Code, w.Body.String())
		}
	}
}
