package core

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"slices"
)

// Snapshot encoding: a compact, deterministic binary form of State. Equal
// states encode to equal bytes, which is what the replay tests compare.

// Version 2 adds graph state (ADR 0029). Version 1 snapshots still decode;
// their seq, par and cond activations are migrated on first use. Version 3
// adds run limits and counters (ADR 0030). Version 4 has the same layout:
// an activation may be a step waiting for its deadline (ADR 0045), which
// older code would not understand, so it refuses version 4 snapshots
// instead of leaving such a run stuck.
const codecVersion = 4

var errCorrupt = errors.New("core: corrupt snapshot")

type enc struct{ b []byte }

func (e *enc) u(v uint64)            { e.b = binary.AppendUvarint(e.b, v) }
func (e *enc) i(v int64)             { e.b = binary.AppendVarint(e.b, v) }
func (e *enc) bytes(v []byte)        { e.u(uint64(len(v))); e.b = append(e.b, v...) }
func (e *enc) str(v string)          { e.u(uint64(len(v))); e.b = append(e.b, v...) }
func (e *enc) raw(v json.RawMessage) { e.bytes(v) }

// Encode appends the encoding of s to b.
func (s *State) Encode(b []byte) []byte {
	e := enc{b: b}
	e.u(codecVersion)
	e.str(s.RunID)
	e.u(uint64(s.Status))
	e.raw(s.Input)
	e.raw(s.Output)
	e.str(s.Error)
	e.u(uint64(s.NextAct))
	e.u(uint64(s.NextScope))
	e.u(uint64(s.NextTimer))
	e.i(int64(s.Inflight))
	e.i(int64(s.Steps))
	e.i(int64(s.MaxSteps))
	e.i(int64(s.Exceptions))
	e.i(s.Deadline)
	e.u(uint64(s.DeadlineTimer))
	e.i(int64(s.Depth))

	ids := sortedActs(s)
	e.u(uint64(len(ids)))
	for _, id := range ids {
		a := s.Acts[id]
		e.u(uint64(id))
		e.i(int64(a.Node))
		e.u(uint64(a.Parent))
		e.u(uint64(a.Scope))
		e.i(int64(a.Idx))
		e.i(int64(a.Pos))
		e.i(int64(a.Pending))
		e.i(int64(a.Attempt))
		e.u(uint64(a.Flags))
		e.u(uint64(a.Timer))
		e.i(a.TimerAt)
		e.u(uint64(len(a.Results)))
		for _, r := range a.Results {
			e.raw(r)
		}
		e.u(uint64(len(a.Items)))
		for _, r := range a.Items {
			e.raw(r)
		}
		if a.G == nil {
			e.u(0)
		} else {
			e.u(1)
			e.bytes(a.G.Members)
			e.bytes(a.G.Edges)
		}
	}

	sids := make([]uint32, 0, len(s.Scopes))
	for id := range s.Scopes {
		sids = append(sids, id)
	}
	slices.Sort(sids)
	e.u(uint64(len(sids)))
	for _, id := range sids {
		sc := s.Scopes[id]
		e.u(uint64(id))
		e.u(uint64(sc.Parent))
		e.i(int64(sc.Map))
		e.i(int64(sc.Index))
		e.raw(sc.Item)
		nodes := make([]int32, 0, len(sc.Vals))
		for n := range sc.Vals {
			nodes = append(nodes, n)
		}
		slices.Sort(nodes)
		e.u(uint64(len(nodes)))
		for _, n := range nodes {
			e.i(int64(n))
			e.raw(sc.Vals[n])
		}
	}

	names := make([]string, 0, len(s.Mailbox))
	for n := range s.Mailbox {
		names = append(names, n)
	}
	slices.Sort(names)
	e.u(uint64(len(names)))
	for _, n := range names {
		e.str(n)
		q := s.Mailbox[n]
		e.u(uint64(len(q)))
		for _, p := range q {
			e.raw(p)
		}
	}
	return e.b
}

type dec struct {
	b   []byte
	err error
}

func (d *dec) u() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) i() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) bytes() []byte {
	n := d.u()
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.err = errCorrupt
		return nil
	}
	if n == 0 {
		return nil
	}
	v := make([]byte, n)
	copy(v, d.b[:n])
	d.b = d.b[n:]
	return v
}

func (d *dec) str() string { return string(d.bytes()) }

func (d *dec) count() int {
	n := d.u()
	if n > uint64(len(d.b)) {
		d.err = errCorrupt
		return 0
	}
	return int(n)
}

// DecodeState decodes a snapshot produced by Encode.
func DecodeState(b []byte) (*State, error) {
	d := &dec{b: b}
	version := d.u()
	if version < 1 || version > codecVersion {
		return nil, errCorrupt
	}
	s := &State{Acts: map[uint32]*Act{}, Scopes: map[uint32]*Scope{}}
	s.RunID = d.str()
	s.Status = RunStatus(d.u())
	s.Input = d.bytes()
	s.Output = d.bytes()
	s.Error = d.str()
	s.NextAct = uint32(d.u())
	s.NextScope = uint32(d.u())
	s.NextTimer = uint32(d.u())
	s.Inflight = int32(d.i())
	if version >= 3 {
		s.Steps = int32(d.i())
		s.MaxSteps = int32(d.i())
		s.Exceptions = int32(d.i())
		s.Deadline = d.i()
		s.DeadlineTimer = uint32(d.u())
		s.Depth = int32(d.i())
	}
	for n := d.count(); n > 0 && d.err == nil; n-- {
		id := uint32(d.u())
		a := &Act{}
		a.Node = int32(d.i())
		a.Parent = uint32(d.u())
		a.Scope = uint32(d.u())
		a.Idx = int32(d.i())
		a.Pos = int32(d.i())
		a.Pending = int32(d.i())
		a.Attempt = int32(d.i())
		a.Flags = uint8(d.u())
		a.Timer = uint32(d.u())
		a.TimerAt = d.i()
		if k := d.count(); k > 0 {
			a.Results = make([]json.RawMessage, k)
			for j := range a.Results {
				a.Results[j] = d.bytes()
			}
		}
		if k := d.count(); k > 0 {
			a.Items = make([]json.RawMessage, k)
			for j := range a.Items {
				a.Items[j] = d.bytes()
			}
		}
		if version >= 2 && d.u() == 1 {
			a.G = &Graph{Members: d.bytes(), Edges: d.bytes()}
		}
		s.Acts[id] = a
	}
	for n := d.count(); n > 0 && d.err == nil; n-- {
		id := uint32(d.u())
		sc := &Scope{}
		sc.Parent = uint32(d.u())
		sc.Map = int32(d.i())
		sc.Index = int32(d.i())
		sc.Item = d.bytes()
		if k := d.count(); k > 0 {
			sc.Vals = make(map[int32]json.RawMessage, k)
			for ; k > 0 && d.err == nil; k-- {
				node := int32(d.i())
				sc.Vals[node] = d.bytes()
			}
		}
		s.Scopes[id] = sc
	}
	if k := d.count(); k > 0 {
		s.Mailbox = make(map[string][]json.RawMessage, k)
		for ; k > 0 && d.err == nil; k-- {
			name := d.str()
			q := make([]json.RawMessage, d.count())
			for j := range q {
				q[j] = d.bytes()
			}
			s.Mailbox[name] = q
		}
	}
	if d.err != nil {
		return nil, d.err
	}
	return s, nil
}
