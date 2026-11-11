package engine

import (
	"encoding/binary"
	"errors"

	"github.com/r-hashi01/kairo/core"
)

// Log records. The log of a shard is the concatenation of the events applied
// to its runs, in the order they were applied, plus one metadata record per
// run. Replaying the events of a run from its start (or from a snapshot)
// reproduces its state exactly.

const (
	recStart      byte = 1 // run metadata; followed by the EvStart event record
	recEvent      byte = 2
	recCheckpoint byte = 3 // run metadata + LSN of a stored snapshot (ADR 0016)
	// recEventV2 makes binaries predating ADR 0070 reject records with its
	// event kinds instead of decoding and silently ignoring them.
	recEventV2 byte = 4
	// recInitialStart keeps an SDK wait's start and initial signal in one WAL
	// frame, so recovery cannot expose an empty early wait (ADR 0071).
	recInitialStart byte = 5
)

var errBadRecord = errors.New("engine: bad log record")

type startMeta struct {
	RunID    string
	Parent   string
	Plan     string
	PlanHash string
	Tenant   string
	Workflow string
	Meta     []byte
	Created  int64
	Updated  int64
	Tier     Tier
	// Keep: the run's output is kept after it finishes, for IdempotencyTTL
	// (SubmitRequest.KeepOutput, ADR 0050). Stored in the tier byte's high
	// bit: records written before it read as false.
	Keep bool
}

// keepBit, parentBit and infoBit extend the tier byte (tiers are small
// numbers). infoBit carries the code-workflow listing metadata (ADR 0068).
const (
	keepBit   = 0x80
	parentBit = 0x40
	infoBit   = 0x20
)

func tierByte(m *startMeta) byte {
	b := byte(m.Tier)
	if m.Keep {
		b |= keepBit
	}
	if m.Parent != "" {
		b |= parentBit
	}
	if m.Workflow != "" {
		b |= infoBit
	}
	return b
}

func appendMeta(b []byte, m *startMeta) []byte {
	if m.Parent != "" {
		b = appendStr(b, m.Parent)
	}
	if m.Workflow != "" {
		b = appendStr(b, m.Workflow)
		b = appendStr(b, string(m.Meta))
		b = binary.AppendVarint(b, m.Created)
		b = binary.AppendVarint(b, m.Updated)
	}
	return b
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
	b = append(b, tierByte(m))
	return appendMeta(b, m)
}

func encodeInitialStart(b []byte, m *startMeta, start, signal *core.Event) []byte {
	b = append(b, recInitialStart)
	for _, rec := range [][]byte{
		encodeStart(nil, m),
		encodeEvent(nil, m.RunID, start),
		encodeEvent(nil, m.RunID, signal),
	} {
		b = binary.AppendUvarint(b, uint64(len(rec)))
		b = append(b, rec...)
	}
	return b
}

// encodeCheckpoint records that the snapshot of m.RunID covering the log up
// to snapLSN is in the snapshot store. Once it is durable the run no longer
// needs its earlier records, including its start record: recovery finds the
// run through this record instead.
func encodeCheckpoint(b []byte, m *startMeta, snapLSN uint64) []byte {
	b = append(b, recCheckpoint)
	b = appendStr(b, m.RunID)
	b = appendStr(b, m.Plan)
	b = appendStr(b, m.PlanHash)
	b = appendStr(b, m.Tenant)
	b = append(b, tierByte(m))
	b = appendMeta(b, m)
	return binary.AppendUvarint(b, snapLSN)
}

func encodeEvent(b []byte, runID string, ev *core.Event) []byte {
	kind := recEvent
	if ev.Kind >= core.EvStepPending {
		kind = recEventV2
	}
	b = append(b, kind)
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
	ext := ev.ErrType != "" || ev.MaxSteps != 0 || ev.Deadline != 0 || ev.Depth != 0 || len(ev.Vars) > 0 || len(ev.Meta) > 0
	if !ext {
		return append(b, flags)
	}
	// Extension fields, added after v0 (ADR 0030). Records without the
	// flag decode as before. Later fields go after these, under a higher
	// extension version.
	b = append(b, flags|flagExt)
	b = binary.AppendUvarint(b, extVersion)
	b = appendStr(b, ev.ErrType)
	b = binary.AppendVarint(b, int64(ev.MaxSteps))
	b = binary.AppendVarint(b, ev.Deadline)
	b = binary.AppendVarint(b, int64(ev.Depth))
	// Version 2 (ADR 0033).
	b = binary.AppendUvarint(b, uint64(len(ev.Vars)))
	b = append(b, ev.Vars...)
	// Version 3 (ADR 0034).
	b = binary.AppendUvarint(b, uint64(len(ev.Meta)))
	return append(b, ev.Meta...)
}

const (
	flagExt    byte = 4
	extVersion      = 3
)

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

type record struct {
	kind    byte
	runID   string
	meta    *startMeta  // start, checkpoint
	ev      *core.Event // event
	events  []*core.Event
	snapLSN uint64 // checkpoint
}

func decodeMeta(r *rdr) *startMeta {
	m := &startMeta{}
	m.RunID = r.str()
	m.Plan = r.str()
	m.PlanHash = r.str()
	m.Tenant = r.str()
	t := r.byte1()
	m.Tier, m.Keep = Tier(t&^(keepBit|parentBit|infoBit)), t&keepBit != 0
	if t&parentBit != 0 {
		m.Parent = r.str()
	}
	if t&infoBit != 0 {
		m.Workflow = r.str()
		m.Meta = r.bytes()
		m.Created = r.i()
		m.Updated = r.i()
	}
	return m
}

// decodeRecord decodes one log record.
func decodeRecord(rec []byte) (record, error) {
	r := &rdr{b: rec}
	out := record{kind: r.byte1()}
	switch out.kind {
	case recStart:
		out.meta = decodeMeta(r)
		out.runID = out.meta.RunID
	case recInitialStart:
		parts := make([]record, 3)
		for i := range parts {
			part, err := decodeRecord(r.bytes())
			if err != nil {
				return out, err
			}
			parts[i] = part
		}
		if parts[0].kind != recStart || parts[1].ev == nil || parts[1].ev.Kind != core.EvStart ||
			parts[2].ev == nil || parts[2].ev.Kind != core.EvSignal ||
			parts[1].runID != parts[0].runID || parts[2].runID != parts[0].runID {
			return out, errBadRecord
		}
		out.meta, out.runID = parts[0].meta, parts[0].runID
		out.events = []*core.Event{parts[1].ev, parts[2].ev}
	case recCheckpoint:
		out.meta = decodeMeta(r)
		out.runID = out.meta.RunID
		out.snapLSN = r.u()
	case recEvent, recEventV2:
		out.runID = r.str()
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
		if flags&flagExt != 0 {
			v := r.u()
			if v >= 1 {
				e.ErrType = r.str()
				e.MaxSteps = int32(r.i())
				e.Deadline = r.i()
				e.Depth = int32(r.i())
			}
			if v >= 2 {
				e.Vars = r.bytes()
			}
			if v >= 3 {
				e.Meta = r.bytes()
			}
		}
		out.ev = e
	default:
		return out, errBadRecord
	}
	return out, r.err
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
