// Package durablefs contains the small filesystem primitives needed before
// a file-backed store can safely acknowledge durable data.
package durablefs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// SyncDir makes directory-entry changes durable.
func SyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// MkdirAll creates path and makes every newly created directory entry
// durable before returning. The final path's parent is synced even when the
// directory already exists, so retrying after an uncertain parent sync repairs
// the initialization. noSync keeps mkdir behavior but skips durability work.
func MkdirAll(path string, perm fs.FileMode, noSync bool, syncDir func(string) error) error {
	if noSync {
		return os.MkdirAll(path, perm)
	}
	return mkdirAll(filepath.Clean(path), perm, syncDir, true)
}

func mkdirAll(path string, perm fs.FileMode, syncDir func(string) error, final bool) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
		}
		if final {
			return syncDir(filepath.Dir(path))
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	parent := filepath.Dir(path)
	if parent == path {
		return err
	}
	if err := mkdirAll(parent, perm, syncDir, false); err != nil {
		return err
	}
	if err := os.Mkdir(path, perm); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err = os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}
	return syncDir(parent)
}
