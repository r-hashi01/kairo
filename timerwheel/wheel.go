// Package timerwheel implements a hierarchical timing wheel with O(1)
// insertion and cancellation.
//
// Timers are placed by their absolute expiry tick (1 tick = 1 ms by
// convention of the caller), in the lowest level whose 6-bit group differs
// from the current tick. Each level keeps a 64-bit occupancy bitmap, so the
// next point at which anything can happen is found with a handful of bit
// operations. The owner arms exactly one OS timer for that point; an empty
// wheel arms nothing, so an idle shard is never woken (no polling).
//
// The wheel is not safe for concurrent use: it is owned by one shard loop.
package timerwheel

import "math/bits"

const (
	slotBits = 6
	slots    = 1 << slotBits
	slotMask = slots - 1
	levels   = 8 // 48 bits of ticks
)

// Handle identifies a scheduled timer. The zero Handle is never valid.
type Handle uint64

type entry[T any] struct {
	expiry     int64
	prev, next int32
	level      int8
	slot       int8
	live       bool
	gen        uint32
	val        T
}

type Wheel[T any] struct {
	now     int64
	heads   [levels][slots]int32
	occ     [levels]uint64
	entries []entry[T]
	free    []int32
	n       int
}

// New creates a wheel whose current tick is now.
func New[T any](now int64) *Wheel[T] {
	w := &Wheel[T]{now: now}
	for l := range w.heads {
		for s := range w.heads[l] {
			w.heads[l][s] = -1
		}
	}
	return w
}

func (w *Wheel[T]) Len() int   { return w.n }
func (w *Wheel[T]) Now() int64 { return w.now }

// Schedule adds a timer that expires at tick expiry. Expiries at or before
// the current tick fire on the next Advance.
func (w *Wheel[T]) Schedule(expiry int64, v T) Handle {
	var idx int32
	if n := len(w.free); n > 0 {
		idx = w.free[n-1]
		w.free = w.free[:n-1]
	} else {
		w.entries = append(w.entries, entry[T]{})
		idx = int32(len(w.entries) - 1)
	}
	e := &w.entries[idx]
	e.gen++
	if e.gen == 0 {
		e.gen = 1
	}
	e.expiry = expiry
	e.val = v
	e.live = true
	w.n++
	w.place(idx)
	return Handle(uint64(e.gen)<<32 | uint64(uint32(idx)))
}

// Cancel removes a timer. It reports false if the timer already fired or
// was cancelled.
func (w *Wheel[T]) Cancel(h Handle) bool {
	idx := int32(uint32(h))
	gen := uint32(h >> 32)
	if idx < 0 || int(idx) >= len(w.entries) {
		return false
	}
	e := &w.entries[idx]
	if !e.live || e.gen != gen {
		return false
	}
	w.unlink(idx)
	w.release(idx)
	return true
}

func (w *Wheel[T]) place(idx int32) {
	e := &w.entries[idx]
	exp := e.expiry
	if exp < w.now {
		exp = w.now
	}
	var level int
	if x := uint64(exp ^ w.now); x != 0 {
		level = (bits.Len64(x) - 1) / slotBits
	}
	if level >= levels {
		level = levels - 1
	}
	slot := int((exp >> (slotBits * level)) & slotMask)
	e.level, e.slot = int8(level), int8(slot)
	e.prev = -1
	e.next = w.heads[level][slot]
	if e.next >= 0 {
		w.entries[e.next].prev = idx
	}
	w.heads[level][slot] = idx
	w.occ[level] |= 1 << slot
}

func (w *Wheel[T]) unlink(idx int32) {
	e := &w.entries[idx]
	l, s := e.level, e.slot
	if e.prev >= 0 {
		w.entries[e.prev].next = e.next
	} else {
		w.heads[l][s] = e.next
	}
	if e.next >= 0 {
		w.entries[e.next].prev = e.prev
	}
	if w.heads[l][s] < 0 {
		w.occ[l] &^= 1 << s
	}
}

func (w *Wheel[T]) release(idx int32) {
	var zero T
	e := &w.entries[idx]
	e.live = false
	e.val = zero
	w.free = append(w.free, idx)
	w.n--
}

// next returns the earliest tick at which something happens (a level-0 slot
// fires, or a higher-level slot must cascade down).
func (w *Wheel[T]) next() (tick int64, level, slot int, ok bool) {
	for l := 0; l < levels; l++ {
		occ := w.occ[l]
		if occ == 0 {
			continue
		}
		shift := uint(slotBits * l)
		cur := int((w.now >> shift) & slotMask)
		var mask uint64
		if l == 0 {
			mask = ^uint64(0) << cur
		} else {
			if cur == slotMask {
				mask = 0
			} else {
				mask = ^uint64(0) << (cur + 1)
			}
		}
		m := occ & mask
		if m == 0 {
			// Can only happen for the top level wrapping around; treat
			// remaining entries as due at the next block.
			m = occ
		}
		s := bits.TrailingZeros64(m)
		base := (w.now >> (shift + slotBits)) << (shift + slotBits)
		t := base | int64(s)<<shift
		if t < w.now {
			t = w.now
		}
		return t, l, s, true
	}
	return 0, 0, 0, false
}

// NextDeadline reports the tick at which Advance should next be called.
func (w *Wheel[T]) NextDeadline() (int64, bool) {
	t, _, _, ok := w.next()
	return t, ok
}

// Advance moves the wheel to tick now and appends the values of all expired
// timers to out.
func (w *Wheel[T]) Advance(now int64, out []T) []T {
	for {
		t, l, s, ok := w.next()
		if !ok || t > now {
			if now > w.now {
				w.now = now
			}
			return out
		}
		w.now = t
		head := w.heads[l][s]
		w.heads[l][s] = -1
		w.occ[l] &^= 1 << s
		for idx := head; idx >= 0; {
			e := &w.entries[idx]
			nxt := e.next
			if l == 0 || e.expiry <= w.now {
				out = append(out, e.val)
				w.release(idx)
			} else {
				w.place(idx)
			}
			idx = nxt
		}
	}
}
