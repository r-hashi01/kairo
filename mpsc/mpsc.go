// Package mpsc provides an unbounded multi-producer / single-consumer queue.
//
// Producers never block (Push only takes a short critical section to append
// to a slice), and the consumer is woken through a 1-buffered channel, so a
// consumer that has nothing to do costs nothing: no polling, no spinning.
package mpsc

import "sync"

type Queue[T any] struct {
	mu     sync.Mutex
	items  []T
	notify chan struct{}
}

func New[T any]() *Queue[T] {
	return &Queue[T]{notify: make(chan struct{}, 1)}
}

// Push appends v and wakes the consumer if it is waiting.
func (q *Queue[T]) Push(v T) {
	q.mu.Lock()
	q.items = append(q.items, v)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Ready returns a channel that receives a value whenever the queue may have
// become non-empty. The consumer selects on it and then calls Drain.
func (q *Queue[T]) Ready() <-chan struct{} { return q.notify }

// Drain moves all queued items into buf (which is reset) and returns it.
// The consumer passes the previous buffer back so steady state is allocation
// free: the two slices are swapped between producer side and consumer side.
func (q *Queue[T]) Drain(buf []T) []T {
	var zero T
	for i := range buf {
		buf[i] = zero
	}
	buf = buf[:0]
	q.mu.Lock()
	buf, q.items = q.items, buf
	q.mu.Unlock()
	return buf
}

// Len reports the current queue length (for metrics only).
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	n := len(q.items)
	q.mu.Unlock()
	return n
}
