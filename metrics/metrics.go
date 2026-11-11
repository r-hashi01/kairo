// Package metrics defines kairo's bounded-cardinality operational
// measurements (ADR 0076). Measurements are live and non-durable.
package metrics

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
)

type Kind string

const (
	Counter   Kind = "counter"
	Gauge     Kind = "gauge"
	Histogram Kind = "histogram"
)

const (
	RunStartUnconfirmed   = "kairo_run_start_unconfirmed_total"
	RunStartRetries       = "kairo_run_start_confirmation_retries_total"
	RunStartConfirmation  = "kairo_run_start_confirmation_seconds"
	WorkflowClaim         = "kairo_workflow_claim_total"
	WorkflowClaimDuration = "kairo_workflow_claim_seconds"
	DurableStartInflight  = "kairo_durable_start_inflight"
	DurableStartQueued    = "kairo_durable_start_queued"
	DurableStartRejected  = "kairo_durable_start_rejected_total"
	DurableStartQueue     = "kairo_durable_start_queue_seconds"
	DurableStartInbox     = "kairo_durable_start_inbox_seconds"
	DurableStartWALAck    = "kairo_durable_start_wal_ack_seconds"
	Drain                 = "kairo_drain_total"
	DrainDuration         = "kairo_drain_seconds"
	DrainStartingTasks    = "kairo_drain_starting_tasks"
	DrainStartingDrives   = "kairo_drain_starting_drives"
	WorkerDisconnect      = "kairo_worker_disconnect_total"
	WorkerDisconnectTasks = "kairo_worker_disconnect_tasks"
	RunBlocked            = "kairo_run_blocked_total"
)

// Labels contains only the finite dimensions reserved by ADR 0076. Shard is
// the decimal form of a member of the data directory's fixed shard set.
type Labels struct {
	Cause       string `json:"cause,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Path        string `json:"path,omitempty"`
	Shard       string `json:"shard,omitempty"`
	Tier        string `json:"tier,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Outstanding string `json:"outstanding,omitempty"`
}

// Measurement is one counter increment, gauge value, or raw histogram
// observation. Value is in Unit; duration measurements use seconds.
type Measurement struct {
	Name   string  `json:"name"`
	Kind   Kind    `json:"kind"`
	Unit   string  `json:"unit"`
	Value  float64 `json:"value"`
	Labels Labels  `json:"labels,omitzero"`
}

// Sink receives batches outside the producer's execution path.
type Sink interface {
	WriteMetrics([]Measurement) error
}

type SinkFunc func([]Measurement) error

func (f SinkFunc) WriteMetrics(batch []Measurement) error { return f(batch) }

// Stream is a bounded, non-blocking handoff to one background writer.
type Stream struct {
	ch      chan Measurement
	sink    Sink
	dropped atomic.Uint64
	failed  atomic.Uint64
	closed  atomic.Bool
	max     int
	wg      sync.WaitGroup
	stop    chan struct{}
}

func NewStream(sink Sink, buffer, maxBatch int) *Stream {
	if sink == nil {
		return nil
	}
	if buffer <= 0 {
		buffer = 4096
	}
	if maxBatch <= 0 {
		maxBatch = 256
	}
	s := &Stream{ch: make(chan Measurement, buffer), sink: sink, max: maxBatch, stop: make(chan struct{})}
	s.wg.Add(1)
	go s.loop()
	return s
}

// Emit never blocks. A full stream drops the measurement.
func (s *Stream) Emit(m Measurement) {
	if s == nil {
		return
	}
	if s.closed.Load() {
		return
	}
	select {
	case s.ch <- m:
	default:
		s.dropped.Add(1)
	}
}

func (s *Stream) Dropped() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}

func (s *Stream) Failed() uint64 {
	if s == nil {
		return 0
	}
	return s.failed.Load()
}

func (s *Stream) Close() {
	if s == nil {
		return
	}
	if s.closed.CompareAndSwap(false, true) {
		close(s.stop)
		s.wg.Wait()
	}
}

func (s *Stream) loop() {
	defer s.wg.Done()
	batch := make([]Measurement, 0, s.max)
	for {
		var m Measurement
		select {
		case m = <-s.ch:
		case <-s.stop:
			s.flushRemaining(batch[:0])
			return
		}
		batch = append(batch[:0], m)
	fill:
		for len(batch) < s.max {
			select {
			case m := <-s.ch:
				batch = append(batch, m)
			default:
				break fill
			}
		}
		if err := s.sink.WriteMetrics(batch); err != nil {
			s.failed.Add(uint64(len(batch)))
		}
	}
}

func (s *Stream) flushRemaining(batch []Measurement) {
	for {
		select {
		case m := <-s.ch:
			batch = append(batch, m)
			if len(batch) == s.max {
				if err := s.sink.WriteMetrics(batch); err != nil {
					s.failed.Add(uint64(len(batch)))
				}
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				if err := s.sink.WriteMetrics(batch); err != nil {
					s.failed.Add(uint64(len(batch)))
				}
			}
			return
		}
	}
}

// JSONLines writes one measurement per line.
type JSONLines struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func NewJSONLines(w io.Writer) *JSONLines { return &JSONLines{w: bufio.NewWriterSize(w, 64<<10)} }

func (j *JSONLines) WriteMetrics(batch []Measurement) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	enc := json.NewEncoder(j.w)
	for i := range batch {
		if err := enc.Encode(&batch[i]); err != nil {
			return err
		}
	}
	return j.w.Flush()
}
