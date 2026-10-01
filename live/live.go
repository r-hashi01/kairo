// Package live relays streaming output (LLM tokens and similar chunks) from
// executors to connected clients. The engine does not record chunks and
// does not turn them into events: a chunk that nobody is listening to is
// simply discarded.
package live

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

type Chunk struct {
	RunID  string `json:"run_id"`
	StepID string `json:"step_id"`
	Data   []byte `json:"data"`
	// Control chunks come from the engine rather than an executor: Data is
	// a JSON object such as {"type":"waiting","waits":[...]}.
	Control bool `json:"control,omitempty"`
}

const hubShards = 64

type Hub struct {
	seed   maphash.Seed
	shards [hubShards]hubShard
}

type hubShard struct {
	mu   sync.RWMutex
	subs map[string][]*Sub
}

type Sub struct {
	hub     *Hub
	runID   string
	ch      chan Chunk
	dropped atomic.Uint64
	closed  atomic.Bool
}

func NewHub() *Hub {
	h := &Hub{seed: maphash.MakeSeed()}
	for i := range h.shards {
		h.shards[i].subs = map[string][]*Sub{}
	}
	return h
}

func (h *Hub) shard(runID string) *hubShard {
	return &h.shards[maphash.String(h.seed, runID)%hubShards]
}

// Subscribe starts receiving chunks for runID. buffer bounds how far a slow
// client may fall behind before chunks are dropped for it.
func (h *Hub) Subscribe(runID string, buffer int) *Sub {
	if buffer <= 0 {
		buffer = 1024
	}
	s := &Sub{hub: h, runID: runID, ch: make(chan Chunk, buffer)}
	sh := h.shard(runID)
	sh.mu.Lock()
	sh.subs[runID] = append(sh.subs[runID], s)
	sh.mu.Unlock()
	return s
}

// Publish delivers a chunk to current subscribers without blocking.
func (h *Hub) Publish(c Chunk) {
	sh := h.shard(c.RunID)
	sh.mu.RLock()
	subs := sh.subs[c.RunID]
	for _, s := range subs {
		select {
		case s.ch <- c:
		default:
			s.dropped.Add(1)
		}
	}
	sh.mu.RUnlock()
}

// End closes every subscription of runID.
func (h *Hub) End(runID string) {
	sh := h.shard(runID)
	sh.mu.Lock()
	subs := sh.subs[runID]
	delete(sh.subs, runID)
	sh.mu.Unlock()
	for _, s := range subs {
		if s.closed.CompareAndSwap(false, true) {
			close(s.ch)
		}
	}
}

// Next blocks for at least one chunk and then returns everything else that
// is already buffered, so the client writes in batches. ok is false once
// the stream has ended.
func (s *Sub) Next(buf []Chunk) (batch []Chunk, ok bool) {
	buf = buf[:0]
	c, ok := <-s.ch
	if !ok {
		return buf, false
	}
	buf = append(buf, c)
	for {
		select {
		case c, ok := <-s.ch:
			if !ok {
				return buf, true
			}
			buf = append(buf, c)
		default:
			return buf, true
		}
	}
}

// C exposes the raw channel (closed at the end of the run).
func (s *Sub) C() <-chan Chunk { return s.ch }

func (s *Sub) Dropped() uint64 { return s.dropped.Load() }

// Close unsubscribes.
func (s *Sub) Close() {
	sh := s.hub.shard(s.runID)
	sh.mu.Lock()
	subs := sh.subs[s.runID]
	for i, x := range subs {
		if x == s {
			subs = append(subs[:i], subs[i+1:]...)
			break
		}
	}
	if len(subs) == 0 {
		delete(sh.subs, s.runID)
	} else {
		sh.subs[s.runID] = subs
	}
	sh.mu.Unlock()
	if s.closed.CompareAndSwap(false, true) {
		close(s.ch)
	}
}
