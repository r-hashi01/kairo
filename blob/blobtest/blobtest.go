// Package blobtest is a conformance suite for blob.Store implementations.
package blobtest

import (
	"bytes"
	"errors"
	"testing"

	"kairo/blob"
)

// Options select optional checks.
type Options struct {
	// ArbitraryNames: any string is a valid object name (quotes, SQL,
	// non-ASCII, path-like). Every store in this repository passes it.
	ArbitraryNames bool
}

func Run(t *testing.T, s blob.Store, o Options) {
	if _, err := s.Get("snap/none"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Get of a missing key: %v, want ErrNotFound", err)
	}
	if err := s.Put("snap/a", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("snap/a", []byte("two")); err != nil { // replace
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte{7}, 1<<20)
	key, err := blob.PutContent(s, big)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("snap/a"); string(got) != "two" {
		t.Fatalf("Get after replace: %q", got)
	}
	if got, _ := s.Get(key); !bytes.Equal(got, big) {
		t.Fatal("large object changed")
	}
	if err := s.Delete("snap/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("snap/a"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Get after Delete: %v", err)
	}
	if err := s.Delete("snap/a"); err != nil {
		t.Fatalf("Delete of a missing key: %v", err)
	}

	// Large objects (split into parts by SQL stores), replaced by a
	// smaller one: no stale parts remain.
	huge := bytes.Repeat([]byte("0123456789abcdef"), 220_000) // 3.5 MB
	if err := s.Put("snap/huge", huge); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("snap/huge"); !bytes.Equal(got, huge) {
		t.Fatal("large object changed")
	}
	if err := s.Put("snap/huge", []byte("small")); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("snap/huge"); string(got) != "small" {
		t.Fatalf("after shrinking: %d bytes", len(got))
	}
	// Empty values round-trip (Oracle stores an empty BLOB as NULL).
	if err := s.Put("snap/empty", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get("snap/empty"); err != nil || len(got) != 0 {
		t.Fatalf("empty: %q %v", got, err)
	}

	if o.ArbitraryNames {
		arbitraryNames(t, s)
	}
}

// arbitraryNames: names are data, never SQL or paths.
func arbitraryNames(t *testing.T, s blob.Store) {
	odd := []string{`snap/'; DROP TABLE kairo_blob; --`, `snap/"quoted"`, "snap/日本語", "snap/a%_b", `snap/..\..\x`, "snap/../../escape"}
	for _, k := range odd {
		if err := s.Put(k, []byte(k)); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	for _, k := range odd {
		if got, err := s.Get(k); err != nil || string(got) != k {
			t.Fatalf("Get(%q) = %q, %v", k, got, err)
		}
	}
	if _, err := s.Get("snap/a_b"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal("LIKE-style wildcard matched another name")
	}
}
