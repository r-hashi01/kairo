package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kairo/seal"
	"kairo/wal"
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
	id, _ := e1.Submit(SubmitRequest{Plan: "two", Tenant: "t", Tier: &ft, RunID: "run-visible-name", Input: json.RawMessage(`{"secret":"TOPSECRET-123"}`)})
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
	id, _ := e1.Submit(SubmitRequest{Plan: "two", Tenant: "t", Tier: &ft, Input: json.RawMessage(`{"secret":"x"}`)})
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
			id, _ := e1.Submit(SubmitRequest{Plan: "two", Tenant: "t", Tier: &ft, Input: json.RawMessage(`{"secret":"x"}`)})
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
			time.Sleep(50 * time.Millisecond) // checkpoint durable
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
			time.Sleep(50 * time.Millisecond)
			e1.Close()
			os.WriteFile(snapFile(), old, 0o644)
			if _, err := startSealed(t, dir, keys); !errors.Is(err, seal.ErrTampered) {
				t.Fatalf("stale snapshot: Start returned %v", err)
			}
		})
	}
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
