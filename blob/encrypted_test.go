package blob_test

import (
	"bytes"
	"errors"
	"testing"

	"kairo/blob"
	"kairo/blob/blobtest"
	"kairo/seal"
)

func keys(t testing.TB) seal.Keys {
	k, err := seal.NewStatic(1, map[uint32][]byte{1: bytes.Repeat([]byte{7}, 32)}, bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryptedConformance(t *testing.T) {
	blobtest.Run(t, blob.Encrypted(blob.NewMem(), keys(t)), blobtest.Options{ArbitraryNames: true})
}

func TestEncryptedHidesAndAuthenticates(t *testing.T) {
	inner := blob.NewMem()
	s := blob.Encrypted(inner, keys(t))
	s.Put("snap/run-a", []byte("state of a"))
	s.Put("snap/run-b", []byte("state of b"))
	// Names and contents are hidden from the inner store.
	if _, err := inner.Get("snap/run-a"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal("name visible in the inner store")
	}
	na, _ := seal.Name(keys(t), "snap/run-a")
	nb, _ := seal.Name(keys(t), "snap/run-b")
	ea, _ := inner.Get(na)
	if bytes.Contains(ea, []byte("state of a")) {
		t.Fatal("content visible")
	}
	// Swapping objects between names is detected.
	eb, _ := inner.Get(nb)
	inner.Put(na, eb)
	if _, err := s.Get("snap/run-a"); !errors.Is(err, seal.ErrTampered) {
		t.Fatalf("swapped object accepted: %v", err)
	}
	_ = ea
}
