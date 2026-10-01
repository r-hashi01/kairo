// Package obs is the observability stream: detailed records (node inputs and
// outputs, timings) shipped asynchronously in batches. It may drop and may
// lag; it never slows down or blocks execution.
package obs

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
)

type Record struct {
	At      int64           `json:"at"`
	Type    string          `json:"type"`
	RunID   string          `json:"run_id"`
	Tenant  string          `json:"tenant,omitempty"`
	StepID  string          `json:"step_id,omitempty"`
	Action  string          `json:"action,omitempty"`
	Effect  string          `json:"effect,omitempty"`
	Attempt int32           `json:"attempt,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Err     string          `json:"error,omitempty"`
}

// Sink receives batches. Implementations: a database writer, an OTEL
// exporter, a JSON-lines file.
type Sink interface {
	Write(batch []Record) error
}

type SinkFunc func([]Record) error

func (f SinkFunc) Write(b []Record) error { return f(b) }

// Stream buffers records in a bounded channel and ships them from one
// background goroutine. When the buffer is full, records are dropped and
// counted.
type Stream struct {
	ch      chan Record
	sink    Sink
	dropped atomic.Uint64
	failed  atomic.Uint64
	wg      sync.WaitGroup
	max     int
}

func NewStream(sink Sink, buffer, maxBatch int) *Stream {
	if buffer <= 0 {
		buffer = 1 << 16
	}
	if maxBatch <= 0 {
		maxBatch = 512
	}
	s := &Stream{ch: make(chan Record, buffer), sink: sink, max: maxBatch}
	s.wg.Add(1)
	go s.loop()
	return s
}

// Emit never blocks.
func (s *Stream) Emit(r Record) {
	if s == nil {
		return
	}
	select {
	case s.ch <- r:
	default:
		s.dropped.Add(1)
	}
}

func (s *Stream) Dropped() uint64 { return s.dropped.Load() }

// Close flushes what is buffered and stops the stream.
func (s *Stream) Close() {
	close(s.ch)
	s.wg.Wait()
}

func (s *Stream) loop() {
	defer s.wg.Done()
	batch := make([]Record, 0, s.max)
	for r := range s.ch {
		batch = append(batch[:0], r)
	fill:
		for len(batch) < s.max {
			select {
			case r, ok := <-s.ch:
				if !ok {
					break fill
				}
				batch = append(batch, r)
			default:
				break fill
			}
		}
		if err := s.sink.Write(batch); err != nil {
			s.failed.Add(uint64(len(batch)))
		}
	}
}

// JSONLines writes one JSON object per line.
type JSONLines struct {
	mu sync.Mutex
	w  *bufio.Writer
}

func NewJSONLines(w io.Writer) *JSONLines { return &JSONLines{w: bufio.NewWriterSize(w, 64<<10)} }

func (j *JSONLines) Write(batch []Record) error {
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

// Discard drops everything.
var Discard Sink = SinkFunc(func([]Record) error { return nil })
