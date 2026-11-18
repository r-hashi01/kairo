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
	ch        chan Record
	sink      Sink
	dropped   atomic.Uint64
	failed    atomic.Uint64
	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup
	max       int
	stop      chan struct{}
}

func NewStream(sink Sink, buffer, maxBatch int) *Stream {
	if buffer <= 0 {
		buffer = 1 << 16
	}
	if maxBatch <= 0 {
		maxBatch = 512
	}
	s := &Stream{ch: make(chan Record, buffer), sink: sink, max: maxBatch, stop: make(chan struct{})}
	s.wg.Add(1)
	go s.loop()
	return s
}

// Emit never blocks.
func (s *Stream) Emit(r Record) {
	if s == nil {
		return
	}
	if s.closed.Load() {
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
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.stop)
		s.wg.Wait()
	})
}

func (s *Stream) loop() {
	defer s.wg.Done()
	batch := make([]Record, 0, s.max)
	for {
		var r Record
		select {
		case r = <-s.ch:
		case <-s.stop:
			s.flushRemaining(batch[:0])
			return
		}
		batch = append(batch[:0], r)
	fill:
		for len(batch) < s.max {
			select {
			case r := <-s.ch:
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

func (s *Stream) flushRemaining(batch []Record) {
	for {
		select {
		case r := <-s.ch:
			batch = append(batch, r)
			if len(batch) == s.max {
				if err := s.sink.Write(batch); err != nil {
					s.failed.Add(uint64(len(batch)))
				}
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				if err := s.sink.Write(batch); err != nil {
					s.failed.Add(uint64(len(batch)))
				}
			}
			return
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
