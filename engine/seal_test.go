package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/seal"
	"github.com/r-hashi01/kairo/wal"
)

func frame(b, p []byte) []byte { return wal.Frame(b, p) }

func testKeys(t testing.TB, b byte) seal.Keys {
	k, err := seal.NewStatic(1, map[uint32][]byte{1: bytes.Repeat([]byte{b}, 32)}, bytes.Repeat([]byte{b + 1}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

const twoWaits = `{"name":"two","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"s","action":"llm","input":{"secret":"$input.secret"}},
  {"kind":"wait","id":"a","signal":"a"},
  {"kind":"wait","id":"b","signal":"b"}]}}`

func startSealed(t *testing.T, dir string, keys seal.Keys) (*Engine, error) {
	e := newEngine(t, Config{Shards: 1, DataDir: dir, NoSync: true, EvictAfter: time.Millisecond, Keys: keys})
	mustPlan(t, e, twoWaits)
	e.RegisterExecutor([]string{"llm"}, 2, echoExec())
	return e, e.Start()
}

func filesContain(t *testing.T, dir, s string) []string {
	var hits []string
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			if b, _ := os.ReadFile(p); bytes.Contains(b, []byte(s)) {
				hits = append(hits, p)
			}
		}
		return nil
	})
	return hits
}

func TestEncryptedEngine(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	e1, err := startSealed(t, dir, testKeys(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := submit(e1, SubmitRequest{Plan: "two", Tenant: "t", Tier: &ft, RunID: "run-visible-name", Input: json.RawMessage(`{"secret":"TOPSECRET-123"}`)})
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted && len(ri.Waits) == 1 })
	e1.Close()
	if hits := filesContain(t, dir, "TOPSECRET-123"); len(hits) > 0 {
		t.Fatalf("plaintext input stored in %v", hits)
	}
	if hits := filesContain(t, dir, "run-visible-name"); len(hits) > 0 {
		t.Fatalf("run id stored in clear in %v", hits)
	}

	// Wrong keys: refuse to start.
	if _, err := startSealed(t, dir, testKeys(t, 5)); err == nil {
		t.Fatal("started with the wrong keys")
	}
	// Right keys: resume.
	e2, err := startSealed(t, dir, testKeys(t, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	e2.Signal(id, "a", nil)
	e2.Signal(id, "b", nil)
	if ri := wait(t, e2, id); ri.Status != "completed" {
		t.Fatalf("%+v", ri)
	}
}

func TestTamperedLogRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	ft := TierFile
	e1, _ := startSealed(t, dir, testKeys(t, 1))
	id, _ := submit(e1, SubmitRequest{Plan: "two", Tenant: "t", Tier: &ft, Input: json.RawMessage(`{"secret":"x"}`)})
	waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return len(ri.Waits) == 1 })
	e1.Close()
	// Flip a payload bit and recompute nothing else: CRC catches it as a
	// torn tail at best, so re-frame with a valid CRC like an attacker would.
	segs := walSegments(t, dir)
	var target string
	for _, s := range segs {
		if fi, _ := os.Stat(s); fi.Size() > 0 {
			target = s
		}
	}
	data, _ := os.ReadFile(target)
	recs := splitFrames(data)
	recs[len(recs)/2][len(recs[len(recs)/2])-1] ^= 1
	var out []byte
	for _, r := range recs {
		out = frame(out, r)
	}
	os.WriteFile(target, out, 0o644)
	if _, err := startSealed(t, dir, testKeys(t, 1)); !errors.Is(err, seal.ErrTampered) {
		t.Fatalf("tampered log: Start returned %v", err)
	}
}

// Putting an older snapshot back is caught by the checkpoint in the log,
// with or without encryption.
func TestStaleSnapshotRefusesToStart(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "sealed"}[keyed], func(t *testing.T) {
			dir := t.TempDir()
			var keys seal.Keys
			if keyed {
				keys = testKeys(t, 1)
			}
			ft := TierFile
			e1, _ := startSealed(t, dir, keys)
			id, _ := submit(e1, SubmitRequest{Plan: "two", Tenant: "t", Tier: &ft, Input: json.RawMessage(`{"secret":"x"}`)})
			snapFile := func() string {
				var f string
				filepath.Walk(filepath.Join(dir, "snapshots"), func(p string, fi os.FileInfo, err error) error {
					if err == nil && !fi.IsDir() && !strings.HasPrefix(fi.Name(), ".tmp") {
						f = p
					}
					return nil
				})
				return f
			}
			waitFor(t, func() bool { ri, _ := e1.Get(context.Background(), id); return ri.Evicted && snapFile() != "" })
			// The snapshot's checkpoint is in the log (issue #89: not a fixed wait).
			waitFor(t, func() bool { return checkpointed(t, dir, keys) > 0 })
			first := checkpointed(t, dir, keys)
			old, _ := os.ReadFile(snapFile())
			e1.Signal(id, "a", nil)
			waitFor(t, func() bool {
				ri, _ := e1.Get(context.Background(), id)
				if !ri.Evicted || len(ri.Waits) != 1 || ri.Waits[0].Signal != "b" {
					return false
				}
				cur, _ := os.ReadFile(snapFile())
				return !bytes.Equal(cur, old)
			})
			// The newer snapshot's checkpoint is in the log before the engine
			// stops: that is what refuses the old one put back.
			waitFor(t, func() bool { return checkpointed(t, dir, keys) > first })
			e1.Close()
			os.WriteFile(snapFile(), old, 0o644)
			if _, err := startSealed(t, dir, keys); !errors.Is(err, seal.ErrTampered) {
				t.Fatalf("stale snapshot: Start returned %v", err)
			}
		})
	}
}

// checkpointed is the newest snapshot LSN that a checkpoint record in dir's
// file-tier log (shard 0) points at, 0 if none. It reads the segments as
// they are, while the engine writes: a record still being written is left
// out. keys decrypt a sealed log.
func checkpointed(t *testing.T, dir string, keys seal.Keys) uint64 {
	t.Helper()
	const prefix = "shard-000"
	paths, _ := filepath.Glob(filepath.Join(dir, "wal", prefix+".L*.wal"))
	sort.Strings(paths) // the 20-digit first LSN orders them
	var src logRecords
	for _, p := range paths {
		first, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), prefix+".L"), ".wal"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		lsn := first
		wal.Scan(data, func(rec []byte) error {
			src = append(src, logRecord{lsn: lsn, rec: append([]byte(nil), rec...)})
			lsn++
			return nil
		})
	}
	var log wal.Sink = src
	if keys != nil {
		log = wal.Encrypted(src, keys, prefix+"/"+TierFile.String())
	}
	var newest uint64
	if err := log.ReadAll(func(_ uint64, rec []byte) error {
		if r, err := decodeRecord(rec); err == nil && r.kind == recCheckpoint && r.snapLSN > newest {
			newest = r.snapLSN
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return newest
}

type logRecord struct {
	lsn uint64
	rec []byte
}

// logRecords is a read-only wal.Sink over records already read.
type logRecords []logRecord

func (l logRecords) Append([]byte) error { return errors.New("read only") }
func (l logRecords) Next() uint64 {
	if len(l) == 0 {
		return 1
	}
	return l[len(l)-1].lsn + 1
}
func (l logRecords) Close() error { return nil }
func (l logRecords) ReadAll(fn func(uint64, []byte) error) error {
	for _, r := range l {
		if err := fn(r.lsn, r.rec); err != nil {
			return err
		}
	}
	return nil
}

func splitFrames(data []byte) [][]byte {
	var out [][]byte
	for off := 0; off < len(data); {
		var n uint64
		var k int
		for i, c := range data[off:] {
			n |= uint64(c&0x7f) << (7 * i)
			if c < 0x80 {
				k = i + 1
				break
			}
		}
		out = append(out, append([]byte(nil), data[off+k+4:off+k+4+int(n)]...))
		off += k + 4 + int(n)
	}
	return out
}
