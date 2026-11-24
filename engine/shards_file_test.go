package engine

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCheckShardCountDurabilityOrder(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "data")
	defaults := defaultShardCountFileOps()
	var order []string
	ops := shardCountFileOps{
		syncFile: func(f *os.File) error {
			order = append(order, "file-sync")
			return defaults.syncFile(f)
		},
		rename: func(old, new string) error {
			order = append(order, "rename")
			return defaults.rename(old, new)
		},
		syncDir: func(path string) error {
			order = append(order, "dir-sync:"+filepath.Clean(path))
			return defaults.syncDir(path)
		},
	}
	if err := checkShardCountWithOps(dir, 4, false, ops); err != nil {
		t.Fatal(err)
	}
	want := []string{"dir-sync:" + filepath.Clean(base), "file-sync", "rename", "dir-sync:" + filepath.Clean(dir)}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("durability order = %v, want %v", order, want)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "SHARDS")); err != nil || string(b) != "4" {
		t.Fatalf("SHARDS = %q, err=%v", b, err)
	}
}

func TestCheckShardCountPropagatesDurabilityFailures(t *testing.T) {
	injected := errors.New("injected durability failure")

	t.Run("data directory", func(t *testing.T) {
		defaults := defaultShardCountFileOps()
		ops := defaults
		ops.syncDir = func(string) error { return injected }
		dir := filepath.Join(t.TempDir(), "data")
		if err := checkShardCountWithOps(dir, 2, false, ops); !errors.Is(err, injected) {
			t.Fatalf("checkShardCount error = %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, "SHARDS")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("SHARDS exists after directory sync failure: %v", err)
		}
	})

	t.Run("shard file", func(t *testing.T) {
		dir := t.TempDir()
		ops := defaultShardCountFileOps()
		ops.syncFile = func(*os.File) error { return injected }
		if err := checkShardCountWithOps(dir, 2, false, ops); !errors.Is(err, injected) {
			t.Fatalf("checkShardCount error = %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, "SHARDS")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("SHARDS exists after file sync failure: %v", err)
		}
	})

	t.Run("rename directory", func(t *testing.T) {
		dir := t.TempDir()
		defaults := defaultShardCountFileOps()
		ops := defaults
		calls := 0
		ops.syncDir = func(path string) error {
			calls++
			if calls == 2 {
				return injected
			}
			return defaults.syncDir(path)
		}
		if err := checkShardCountWithOps(dir, 2, false, ops); !errors.Is(err, injected) {
			t.Fatalf("checkShardCount error = %v", err)
		}
		// The rename happened, but its durability was unknown. A retry syncs
		// both the existing file and its directory and accepts the same pin.
		if err := checkShardCount(dir, 2, false); err != nil {
			t.Fatalf("checkShardCount retry: %v", err)
		}
	})
}

func TestCheckShardCountNoSyncSkipsSyncs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	ops := defaultShardCountFileOps()
	ops.syncFile = func(*os.File) error {
		t.Fatal("file sync called with NoSync")
		return nil
	}
	ops.syncDir = func(string) error {
		t.Fatal("directory sync called with NoSync")
		return nil
	}
	if err := checkShardCountWithOps(dir, 3, true, ops); err != nil {
		t.Fatal(err)
	}
}
