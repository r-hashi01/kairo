package blob

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDirSyncsEveryGroupedDirectoryAndRename(t *testing.T) {
	root := t.TempDir()
	s, err := NewDir(root, false)
	if err != nil {
		t.Fatal(err)
	}
	var synced []string
	s.dirSync = func(dir string) error {
		synced = append(synced, filepath.Clean(dir))
		return syncDir(dir)
	}
	key := "run/a/sha256:one"
	if err := s.Put(key, []byte("one")); err != nil {
		t.Fatal(err)
	}
	groupDir := s.groupDir(RunGroup("a"))
	want := []string{
		filepath.Clean(root),
		filepath.Join(root, "g"),
		filepath.Dir(groupDir),
		groupDir,
	}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
}

func TestDirPropagatesDirectoryDurabilityFailures(t *testing.T) {
	injected := errors.New("injected directory failure")

	t.Run("root creation", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "blobs")
		if _, err := newDir(root, false, func(string) error { return injected }); !errors.Is(err, injected) {
			t.Fatalf("NewDir error = %v", err)
		}
	})

	t.Run("parent creation", func(t *testing.T) {
		s, err := NewDir(t.TempDir(), false)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		s.dirSync = func(dir string) error {
			calls++
			if calls == 2 {
				return injected
			}
			return syncDir(dir)
		}
		key := "run/a/sha256:one"
		if err := s.Put(key, []byte("one")); !errors.Is(err, injected) {
			t.Fatalf("Put error = %v", err)
		}
		if _, err := s.Get(key); !errors.Is(err, ErrNotFound) {
			t.Fatalf("object exists after parent sync failure: %v", err)
		}
		// The failed directory was not marked durable; a retry syncs its
		// parent again before writing the object.
		s.dirSync = syncDir
		if err := s.Put(key, []byte("one")); err != nil {
			t.Fatalf("Put retry: %v", err)
		}
	})

	t.Run("object deletion", func(t *testing.T) {
		s, err := NewDir(t.TempDir(), false)
		if err != nil {
			t.Fatal(err)
		}
		key := "run/a/sha256:one"
		if err := s.Put(key, []byte("one")); err != nil {
			t.Fatal(err)
		}
		s.dirSync = func(string) error { return injected }
		if err := s.Delete(key); !errors.Is(err, injected) {
			t.Fatalf("Delete error = %v", err)
		}
		var retriedDir string
		s.dirSync = func(dir string) error {
			retriedDir = filepath.Clean(dir)
			return syncDir(dir)
		}
		if err := s.Delete(key); err != nil {
			t.Fatalf("Delete retry: %v", err)
		}
		if retriedDir != filepath.Dir(s.path(key)) {
			t.Fatalf("Delete retry synced %q, want %q", retriedDir, filepath.Dir(s.path(key)))
		}
	})

	t.Run("group deletion", func(t *testing.T) {
		s, err := NewDir(t.TempDir(), false)
		if err != nil {
			t.Fatal(err)
		}
		key := "run/a/sha256:one"
		if err := s.Put(key, []byte("one")); err != nil {
			t.Fatal(err)
		}
		s.dirSync = func(string) error { return injected }
		if err := s.DeleteGroup(RunGroup("a")); !errors.Is(err, injected) {
			t.Fatalf("DeleteGroup error = %v", err)
		}
	})
}
