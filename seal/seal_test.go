package seal

import (
	"bytes"
	"errors"
	"testing"
)

func testKeys(t *testing.T) *Static {
	k, err := NewStatic(2, map[uint32][]byte{1: bytes.Repeat([]byte{1}, 32), 2: bytes.Repeat([]byte{2}, 32)}, bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestSealOpen(t *testing.T) {
	keys := testKeys(t)
	b, _ := NewBatch(keys)
	e1 := b.Seal(nil, []byte("one"), []byte("aad-1"))
	e2 := b.Seal(nil, []byte("two"), []byte("aad-2"))
	o := NewOpener(keys)
	if p, err := o.Open(e1, []byte("aad-1")); err != nil || string(p) != "one" {
		t.Fatal(p, err)
	}
	if p, err := o.Open(e2, []byte("aad-2")); err != nil || string(p) != "two" {
		t.Fatal(p, err)
	}
	// Wrong place, flipped bit, truncated, swapped index: all rejected.
	if _, err := o.Open(e1, []byte("aad-2")); !errors.Is(err, ErrTampered) {
		t.Fatal("moved envelope accepted")
	}
	bad := append([]byte(nil), e2...)
	bad[len(bad)-1] ^= 1
	if _, err := o.Open(bad, []byte("aad-2")); !errors.Is(err, ErrTampered) {
		t.Fatal("flipped bit accepted")
	}
	if _, err := o.Open(e2[:10], []byte("aad-2")); !errors.Is(err, ErrTampered) {
		t.Fatal("truncated accepted")
	}
	if bytes.Contains(e1, []byte("one")) {
		t.Fatal("plaintext visible")
	}
}

func TestKeyRotation(t *testing.T) {
	old, _ := NewStatic(1, map[uint32][]byte{1: bytes.Repeat([]byte{1}, 32)}, bytes.Repeat([]byte{9}, 32))
	b, _ := NewBatch(old)
	env := b.Seal(nil, []byte("before rotation"), nil)
	// The new provider writes with key 2 and still reads key 1.
	p, err := NewOpener(testKeys(t)).Open(env, nil)
	if err != nil || string(p) != "before rotation" {
		t.Fatal(p, err)
	}
}

func TestDistinctSaltsPerBatch(t *testing.T) {
	keys := testKeys(t)
	a, _ := NewBatch(keys)
	b, _ := NewBatch(keys)
	if a.salt == b.salt {
		t.Fatal("two batches share a salt")
	}
	n1, _ := Name(keys, "snap/run-1")
	n2, _ := Name(keys, "snap/run-1")
	if n1 != n2 || bytes.Contains([]byte(n1), []byte("run-1")) {
		t.Fatalf("names %s %s", n1, n2)
	}
}
