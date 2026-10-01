package wal

import (
	"encoding/binary"
	"fmt"

	"kairo/seal"
)

// Encrypted wraps a sink so every record is sealed (ADR 0021): encrypted,
// and bound to logID and its LSN. ReadAll fails with seal.ErrTampered if a
// record does not authenticate or the LSNs are not consecutive (a record
// was removed or moved in the middle of the log). Retirement of a prefix
// is legitimate and not detected; neither is rolling the whole log back.
//
// It must only be used from the committer's goroutine, like any sink.
func Encrypted(inner Sink, keys seal.Keys, logID string) Sink {
	return &encSink{inner: inner, keys: keys, logID: logID}
}

type encSink struct {
	inner Sink
	keys  seal.Keys
	logID string
	buf   []byte
}

func (s *encSink) aad(dst []byte, lsn uint64) []byte {
	dst = append(dst, "kairo-wal\x00"...)
	dst = append(dst, s.logID...)
	dst = append(dst, 0)
	return binary.BigEndian.AppendUint64(dst, lsn)
}

func (s *encSink) Append(batch []byte) error {
	b, err := seal.NewBatch(s.keys)
	if err != nil {
		return err
	}
	lsn := s.inner.Next()
	out := s.buf[:0]
	var aad, env []byte
	if _, err := Scan(batch, func(rec []byte) error {
		aad = s.aad(aad[:0], lsn)
		env = b.Seal(env[:0], rec, aad)
		out = Frame(out, env)
		lsn++
		return nil
	}); err != nil {
		return err
	}
	s.buf = out
	return s.inner.Append(out)
}

func (s *encSink) ReadAll(fn func(uint64, []byte) error) error {
	o := seal.NewOpener(s.keys)
	var prev uint64
	var aad []byte
	return s.inner.ReadAll(func(lsn uint64, rec []byte) error {
		if prev != 0 && lsn != prev+1 {
			return fmt.Errorf("%w: log %s jumps from LSN %d to %d", seal.ErrTampered, s.logID, prev, lsn)
		}
		prev = lsn
		aad = s.aad(aad[:0], lsn)
		plain, err := o.Open(rec, aad)
		if err != nil {
			return fmt.Errorf("log %s LSN %d: %w", s.logID, lsn, err)
		}
		return fn(lsn, plain)
	})
}

func (s *encSink) Next() uint64 { return s.inner.Next() }

func (s *encSink) Retire(lsn uint64) error {
	if r, ok := s.inner.(Retirer); ok {
		return r.Retire(lsn)
	}
	return nil
}

func (s *encSink) Close() error { return s.inner.Close() }
