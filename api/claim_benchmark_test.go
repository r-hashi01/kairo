package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
)

// BenchmarkProcessWorkflowClaimScan measures the process-level Tick path: an
// unmatched claim snapshots and sorts every active code workflow, then reads
// every input to check its workflow version. The probe is an unrelated shard
// query issued while each wave of claim scans is running.
//
// Run it on an otherwise idle machine with scripts/bench-claims.sh. These are
// measurements, not time-based test assertions (ADR 0056).
func BenchmarkProcessWorkflowClaimScan(b *testing.B) {
	for _, active := range []int{100, 1_000, 5_000, 10_000} {
		b.Run(fmt.Sprintf("active=%d", active), func(b *testing.B) {
			e, h, probeID := claimBenchmarkEngine(b, active)

			for _, claimants := range []int{1, 8} {
				b.Run(fmt.Sprintf("claimants=%d", claimants), func(b *testing.B) {
					body := []byte(`{"owner":"benchmark","lease_ms":60000,"workflows":{"missing":["1"]}}`)
					claimLatency := make([]time.Duration, 0, b.N*claimants)
					probe := make([]time.Duration, 0, b.N)
					b.ReportAllocs()
					b.ResetTimer()
					started := time.Now()
					cpuStarted := processCPUTime()
					for range b.N {
						entered := make(chan struct{}, claimants)
						done := make(chan claimBenchResult, claimants)
						for range claimants {
							go func() {
								entered <- struct{}{}
								at := time.Now()
								done <- claimBenchResult{err: serveEmptyClaim(h, body), latency: time.Since(at)}
							}()
						}
						for range claimants {
							<-entered
						}
						runtime.Gosched()
						probeStarted := time.Now()
						if _, err := e.Get(context.Background(), probeID); err != nil {
							b.Fatal(err)
						}
						probe = append(probe, time.Since(probeStarted))
						for range claimants {
							result := <-done
							if result.err != nil {
								b.Fatal(result.err)
							}
							claimLatency = append(claimLatency, result.latency)
						}
					}
					cpu := processCPUTime() - cpuStarted
					elapsed := time.Since(started)
					b.StopTimer()

					claims := float64(b.N * claimants)
					b.ReportMetric(float64(cpu.Microseconds())/claims, "cpu-us/claim")
					b.ReportMetric(float64(elapsed.Microseconds())/claims, "wall-us/claim")
					slices.Sort(claimLatency)
					b.ReportMetric(float64(claimLatency[len(claimLatency)*50/100].Nanoseconds())/1e3, "claim-p50-us")
					b.ReportMetric(float64(claimLatency[len(claimLatency)*99/100].Nanoseconds())/1e3, "claim-p99-us")
					slices.Sort(probe)
					b.ReportMetric(float64(probe[len(probe)*50/100].Nanoseconds()), "probe-p50-ns")
					b.ReportMetric(float64(probe[len(probe)*99/100].Nanoseconds()), "probe-p99-ns")
				})
			}
		})
	}
}

type claimBenchResult struct {
	err     error
	latency time.Duration
}

func claimBenchmarkEngine(b *testing.B, active int) (*engine.Engine, http.Handler, string) {
	b.Helper()
	e, err := engine.New(engine.Config{Shards: 8, NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(e.Close)
	var def ir.Definition
	if err := json.Unmarshal([]byte(`{"name":"kairo.workflow","root":{"kind":"wait","id":"done","signal":"done"}}`), &def); err != nil {
		b.Fatal(err)
	}
	if _, err := e.RegisterPlan(&def); err != nil {
		b.Fatal(err)
	}
	if err := e.Start(); err != nil {
		b.Fatal(err)
	}
	for i := range active {
		id := fmt.Sprintf("workflow-%08d", i)
		if _, err := e.Submit(context.Background(), engine.SubmitRequest{
			Plan: "kairo.workflow", RunID: id, Drive: true,
			Input: json.RawMessage(`{"workflow":"orders","version":"1"}`),
		}); err != nil {
			b.Fatalf("submit %s: %v", id, err)
		}
	}
	return e, (&API{E: e}).Handler(), "workflow-00000000"
}

func serveEmptyClaim(h http.Handler, body []byte) error {
	req := httptest.NewRequest(http.MethodPost, "/v1/workflow-drives/claim", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		return fmt.Errorf("claim status %d: %s", w.Code, w.Body.String())
	}
	var got driveClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		return err
	}
	if got.Claim != nil {
		return fmt.Errorf("unexpected claim of %s", got.Claim.Run.RunID)
	}
	return nil
}

func processCPUTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
