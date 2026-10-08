package wal_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/r-hashi01/kairo/seal"
	"github.com/r-hashi01/kairo/wal"
	"github.com/r-hashi01/kairo/wal/waltest"
)

func keys(t testing.TB) seal.Keys {
	k, err := seal.NewStatic(1, map[uint32][]byte{1: bytes.Repeat([]byte{7}, 32)}, bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryptedSinkConformance(t *testing.T) {
	waltest.Run(t, func(t testing.TB, dir string) wal.Sink {
		s, err := wal.OpenFile(dir, "shard-000", true)
		if err != nil {
			t.Fatal(err)
		}
		s.SegmentSize = 512
		return wal.Encrypted(s, keys(t), "shard-000/file")
	}, waltest.Options{Durable: true})
}

func readAll(s wal.Sink) error {
	return s.ReadAll(func(uint64, []byte) error { return nil })
}

// Tampering with the stored log is detected: flipped bits, records moved
// to another log, and records removed from the middle.
func TestEncryptedSinkDetectsTampering(t *testing.T) {
	write := func(t *testing.T, logID string) (string, []byte) {
		dir := t.TempDir()
		s, _ := wal.OpenFile(dir, "x", true)
		e := wal.Encrypted(s, keys(t), logID)
		var b []byte
		for _, r := range []string{"intent: pay 100", "result: ok", "done"} {
			b = wal.Frame(b, []byte(r))
		}
		e.Append(b)
		e.Close()
		segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
		data, _ := os.ReadFile(segs[0])
		if bytes.Contains(data, []byte("pay 100")) {
			t.Fatal("plaintext on disk")
		}
		return segs[0], data
	}
	reopen := func(t *testing.T, path, logID string) wal.Sink {
		s, err := wal.OpenFile(filepath.Dir(path), "x", true)
		if err != nil {
			t.Fatal(err)
		}
		return wal.Encrypted(s, keys(t), logID)
	}
	t.Run("bit flip", func(t *testing.T) {
		path, data := write(t, "a")
		data[len(data)-3] ^= 0x40 // inside the last record
		// Re-frame with valid CRCs, so only the AEAD can notice.
		var out []byte
		for _, r := range rawRecords(t, data) {
			out = wal.Frame(out, r)
		}
		os.WriteFile(path, out, 0o644)
		if err := readAll(reopen(t, path, "a")); !errors.Is(err, seal.ErrTampered) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("moved to another log", func(t *testing.T) {
		path, _ := write(t, "a")
		if err := readAll(reopen(t, path, "b")); !errors.Is(err, seal.ErrTampered) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("record removed from the middle", func(t *testing.T) {
		path, data := write(t, "a")
		recs := rawRecords(t, data)
		var out []byte
		out = wal.Frame(out, recs[0])
		out = wal.Frame(out, recs[2]) // now at LSN 2, sealed for LSN 3
		os.WriteFile(path, out, 0o644)
		if err := readAll(reopen(t, path, "a")); !errors.Is(err, seal.ErrTampered) {
			t.Fatalf("got %v", err)
		}
	})
}

// rawRecords splits a segment into payloads, ignoring CRCs (the tamperer
// can recompute those).
func rawRecords(t *testing.T, data []byte) [][]byte {
	var out [][]byte
	for off := 0; off < len(data); {
		n, k := uvarint(data[off:])
		out = append(out, append([]byte(nil), data[off+k+4:off+k+4+int(n)]...))
		off += k + 4 + int(n)
	}
	return out
}

func uvarint(b []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, c := range b {
		if c < 0x80 {
			return x | uint64(c)<<s, i + 1
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0
}
