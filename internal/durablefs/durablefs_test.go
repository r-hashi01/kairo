package durablefs

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMkdirAllSyncsEachCreatedEntry(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "one", "two")
	var synced []string
	if err := MkdirAll(path, 0o755, false, func(dir string) error {
		synced = append(synced, filepath.Clean(dir))
		return SyncDir(dir)
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Clean(base), filepath.Join(base, "one")}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
}

func TestMkdirAllResyncsExistingFinalEntry(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "store")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	var synced []string
	if err := MkdirAll(path, 0o755, false, func(dir string) error {
		synced = append(synced, filepath.Clean(dir))
		return SyncDir(dir)
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Clean(base)}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
}

func TestMkdirAllPropagatesDirectorySyncFailure(t *testing.T) {
	injected := errors.New("injected directory sync failure")
	path := filepath.Join(t.TempDir(), "store")
	if err := MkdirAll(path, 0o755, false, func(string) error { return injected }); !errors.Is(err, injected) {
		t.Fatalf("MkdirAll error = %v", err)
	}
}

func TestMkdirAllNoSyncSkipsDirectorySync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "one", "two")
	if err := MkdirAll(path, 0o755, true, func(string) error {
		t.Fatal("directory sync called with noSync")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
