package engine

import (
	"encoding/binary"
	"errors"

	"kairo/core"
)

// Log records. The log of a shard is the concatenation of the events applied
// to its runs, in the order they were applied, plus one metadata record per
// run. Replaying the events of a run from its start (or from a snapshot)
// reproduces its state exactly.

const (
	recStart byte = 1 // run metadata; followed by the EvStart event record
	recEvent byte = 2
)

var errBadRecord = errors.New("engine: bad log record")

type startMeta struct {
	RunID    string
	Plan     string
	PlanHash string
	Tenant   string
	Tier     Tier
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func encodeStart(b []byte, m *startMeta) []byte {
	b = append(b, recStart)
	b = appendStr(b, m.RunID)
	b = appendStr(b, m.Plan)
	b = appendStr(b, m.PlanHash)
	b = appendStr(b, m.Tenant)
	return append(b, byte(m.Tier))
}

func encodeEvent(b []byte, runID string, ev *core.Event) []byte {
	b = append(b, recEvent)
	b = appendStr(b, runID)
	b = append(b, byte(ev.Kind))
	b = binary.AppendVarint(b, ev.At)
	b = binary.AppendUvarint(b, uint64(ev.Act))
	b = binary.AppendVarint(b, int64(ev.Attempt))
	b = binary.AppendUvarint(b, uint64(ev.Timer))
	b = appendStr(b, ev.Name)
	b = binary.AppendUvarint(b, uint64(len(ev.Data)))
	b = append(b, ev.Data...)
	b = appendStr(b, ev.Err)
	var flags byte
	if ev.Retryable {
		flags |= 1
	}
	if ev.Unknown {
		flags |= 2
	}
	return append(b, flags)
}

type rdr struct {
	b   []byte
	err error
}

func (r *rdr) u() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errBadRecord
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *rdr) i() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = errBadRecord
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *rdr) byte1() byte {
	if r.err != nil || len(r.b) == 0 {
		r.err = errBadRecord
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *rdr) bytes() []byte {
	n := r.u()
	if r.err != nil || n > uint64(len(r.b)) {
		r.err = errBadRecord
		return nil
	}
	if n == 0 {
		return nil
	}
	v := make([]byte, n)
	copy(v, r.b)
	r.b = r.b[n:]
	return v
}

func (r *rdr) str() string { return string(r.bytes()) }

// decodeRecord returns the record kind, run id, and either meta or event.
func decodeRecord(rec []byte) (kind byte, meta *startMeta, runID string, ev *core.Event, err error) {
	r := &rdr{b: rec}
	kind = r.byte1()
	switch kind {
	case recStart:
		m := &startMeta{}
		m.RunID = r.str()
		m.Plan = r.str()
		m.PlanHash = r.str()
		m.Tenant = r.str()
		m.Tier = Tier(r.byte1())
		return kind, m, m.RunID, nil, r.err
	case recEvent:
		runID = r.str()
		e := &core.Event{}
		e.Kind = core.EventKind(r.byte1())
		e.At = r.i()
		e.Act = uint32(r.u())
		e.Attempt = int32(r.i())
		e.Timer = uint32(r.u())
		e.Name = r.str()
		e.Data = r.bytes()
		e.Err = r.str()
		flags := r.byte1()
		e.Retryable = flags&1 != 0
		e.Unknown = flags&2 != 0
		return kind, nil, runID, e, r.err
	}
	return kind, nil, "", nil, errBadRecord
}

// Snapshot objects: uvarint LSN of the last event included, then the state.
func encodeSnapshot(lsn uint64, st *core.State) []byte {
	b := binary.AppendUvarint(make([]byte, 0, 256), lsn)
	return st.Encode(b)
}

func decodeSnapshot(b []byte) (uint64, *core.State, error) {
	lsn, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, errBadRecord
	}
	st, err := core.DecodeState(b[n:])
	return lsn, st, err
}
