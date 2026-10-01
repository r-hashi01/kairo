package blob_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kairo/blob"
	"kairo/blob/blobtest"
)

func TestDirConformance(t *testing.T) {
	d, err := blob.NewDir(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	blobtest.Run(t, d, blobtest.Options{ArbitraryNames: true})
}

func TestMemConformance(t *testing.T) {
	blobtest.Run(t, blob.NewMem(), blobtest.Options{ArbitraryNames: true})
}

// Objects written by the previous blob.Dir format are read, moved on the
// next Put, and removed by Delete (ADR 0022).
func TestDirReadsPreviousFormat(t *testing.T) {
	root := t.TempDir()
	// The previous format: ":" and "/" replaced by "_", in a subdirectory
	// named after the last two characters.
	legacy := filepath.Join(root, "un", "snap_run-legacy-run")
	os.MkdirAll(filepath.Dir(legacy), 0o755)
	os.WriteFile(legacy, []byte("old state"), 0o644)
	d, _ := blob.NewDir(root, true)
	if got, err := d.Get("snap/run-legacy-run"); err != nil || string(got) != "old state" {
		t.Fatalf("previous format not read: %q %v", got, err)
	}
	if err := d.Put("snap/run-legacy-run", []byte("new state")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatal("stale copy in the previous format left behind")
	}
	if got, _ := d.Get("snap/run-legacy-run"); string(got) != "new state" {
		t.Fatalf("after Put: %q", got)
	}
	os.WriteFile(legacy, []byte("old again"), 0o644)
	d.Delete("snap/run-legacy-run")
	if _, err := d.Get("snap/run-legacy-run"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Delete left a copy: %v", err)
	}
	// Names are no longer visible in file names.
	filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err == nil && strings.Contains(p, "legacy") {
			t.Errorf("name visible in %s", p)
		}
		return nil
	})
}
