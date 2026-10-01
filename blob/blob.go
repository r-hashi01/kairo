// Package blob stores large payloads and snapshots outside the run state.
//
// The only write primitive is an atomic replace of a whole object. Payloads
// are content-addressed (the key is their hash), so a put is idempotent and
// the run state only carries the reference.
package blob

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("blob: not found")

type Store interface {
	// Put atomically replaces the object at key.
	Put(key string, data []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

// ContentKey returns the content-addressed key for data.
func ContentKey(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

// PutContent stores data under its content hash and returns the key.
func PutContent(s Store, data []byte) (string, error) {
	k := ContentKey(data)
	return k, s.Put(k, data)
}

type Mem struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func NewMem() *Mem { return &Mem{m: map[string][]byte{}} }

func (s *Mem) Put(key string, data []byte) error {
	c := append([]byte(nil), data...)
	s.mu.Lock()
	s.m[key] = c
	s.mu.Unlock()
	return nil
}

func (s *Mem) Get(key string) ([]byte, error) {
	s.mu.RLock()
	d, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return d, nil
}

func (s *Mem) Delete(key string) error {
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
	return nil
}

func (s *Mem) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Dir stores objects as files; Put writes a temp file, fsyncs and renames
// it over the target, which is atomic on POSIX filesystems.
type Dir struct {
	root   string
	noSync bool
}

func NewDir(root string, noSync bool) (*Dir, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &Dir{root: root, noSync: noSync}, nil
}

func (s *Dir) path(key string) string {
	k := strings.NewReplacer(":", "_", "/", "_").Replace(key)
	sub := k
	if len(k) > 2 {
		sub = k[len(k)-2:]
	}
	return filepath.Join(s.root, sub, k)
}

func (s *Dir) Put(key string, data []byte) error {
	p := s.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if !s.noSync {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return err
	}
	if s.noSync {
		return nil
	}
	// Make the rename (and a newly created subdirectory) durable: callers
	// such as log compaction rely on the object surviving a power loss.
	if err := syncDir(filepath.Dir(p)); err != nil {
		return err
	}
	return syncDir(s.root)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *Dir) Get(key string) ([]byte, error) {
	d, err := os.ReadFile(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return d, err
}

func (s *Dir) Delete(key string) error {
	err := os.Remove(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
