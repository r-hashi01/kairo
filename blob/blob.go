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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/r-hashi01/kairo/internal/durablefs"
)

var ErrNotFound = errors.New("blob: not found")

type Store interface {
	// Put atomically replaces the object at key.
	Put(key string, data []byte) error
	Get(key string) ([]byte, error)
	// Delete removes key. Durable stores return only after the deletion is
	// durable; callers may retire the object's last reference after that.
	Delete(key string) error
}

// Grouper is implemented by stores that can delete a group of objects at
// once (ADR 0024). The engine stores each run's blobs under the group
// "run/<run id>" and deletes the group when the run is finished.
type Grouper interface {
	// DeleteGroup durably deletes every object whose key starts with group
	// + "/" before returning for a durable store.
	DeleteGroup(group string) error
}

// RunGroup is the group of a run's blobs.
func RunGroup(runID string) string { return "run/" + runID }

// GroupOf returns the group of key: "run/<id>" for keys "run/<id>/<rest>".
// Other keys (snapshots, blobs of the previous format) have no group.
func GroupOf(key string) (string, bool) {
	if !strings.HasPrefix(key, "run/") {
		return "", false
	}
	i := strings.LastIndex(key, "/")
	if i <= len("run/") {
		return "", false
	}
	return key[:i], true
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

// PutRunContent stores data in the run's group under its content hash
// (deduplicated within the run only, ADR 0024) and returns the key.
func PutRunContent(s Store, runID string, data []byte) (string, error) {
	k := RunGroup(runID) + "/" + ContentKey(data)
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

func (s *Mem) DeleteGroup(group string) error {
	s.mu.Lock()
	for k := range s.m {
		if strings.HasPrefix(k, group+"/") {
			delete(s.m, k)
		}
	}
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
	root        string
	noSync      bool
	dirSync     func(string) error
	dirMu       sync.Mutex
	durableDirs map[string]struct{}
}

func NewDir(root string, noSync bool) (*Dir, error) {
	return newDir(root, noSync, syncDir)
}

func newDir(root string, noSync bool, dirSync func(string) error) (*Dir, error) {
	if err := durablefs.MkdirAll(root, 0o755, noSync, dirSync); err != nil {
		return nil, err
	}
	return &Dir{
		root: root, noSync: noSync, dirSync: dirSync,
		durableDirs: map[string]struct{}{filepath.Clean(root): {}},
	}, nil
}

// path is where key is stored: the SHA-256 of the name, so any name maps
// to a valid, fixed-length file name (ADR 0022).
func (s *Dir) path(key string) string {
	h := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(h[:])
	if g, ok := GroupOf(key); ok {
		return filepath.Join(s.groupDir(g), name)
	}
	return filepath.Join(s.root, name[:2], name)
}

// groupDir holds a group's objects, so the group is deleted as a directory
// (ADR 0024).
func (s *Dir) groupDir(group string) string {
	h := sha256.Sum256([]byte(group))
	name := hex.EncodeToString(h[:])
	return filepath.Join(s.root, "g", name[:2], name)
}

func (s *Dir) DeleteGroup(group string) error {
	d := s.groupDir(group)
	if err := os.RemoveAll(d); err != nil {
		return err
	}
	s.dirMu.Lock()
	delete(s.durableDirs, filepath.Clean(d))
	s.dirMu.Unlock()
	if s.noSync {
		return nil
	}
	if err := s.dirSync(filepath.Dir(d)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// legacyPath is where the previous format stored key, if that was a valid
// path at all (it was not for, e.g., names ending in a split multi-byte
// character). Objects there are still read, and moved on the next write.
func (s *Dir) legacyPath(key string) (string, bool) {
	if _, grouped := GroupOf(key); grouped {
		return "", false // group keys only exist in the current format
	}
	k := strings.NewReplacer(":", "_", "/", "_").Replace(key)
	sub := k
	if len(k) > 2 {
		sub = k[len(k)-2:]
	}
	if !utf8.ValidString(k) || !utf8.ValidString(sub) || len(k) > 255 || k == "." || k == ".." || sub == "." || sub == ".." ||
		strings.ContainsAny(k, "\\\x00") {
		return "", false
	}
	return filepath.Join(s.root, sub, k), true
}

func (s *Dir) Put(key string, data []byte) error {
	p := s.path(key)
	if err := s.ensureDir(filepath.Dir(p)); err != nil {
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
		os.Remove(tmp.Name())
		return err
	}
	if !s.noSync {
		// Make the rename durable. ensureDir made each directory entry from
		// root through the leaf durable before the file was created.
		if err := s.dirSync(filepath.Dir(p)); err != nil {
			return err
		}
	}
	// The new copy is in place: a copy in the previous format is stale.
	if lp, ok := s.legacyPath(key); ok {
		if err := s.removeDurable(lp); err != nil {
			return err
		}
	}
	return nil
}

// ensureDir creates each directory below root separately and makes its
// entry durable before using it. Directories whose entry has already been
// synced are remembered, avoiding repeated parent fsyncs on later puts.
func (s *Dir) ensureDir(dir string) error {
	if s.noSync {
		return os.MkdirAll(dir, 0o755)
	}
	root, dir := filepath.Clean(s.root), filepath.Clean(dir)
	rel, err := filepath.Rel(root, dir)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("blob: directory %s is outside root %s", dir, root)
	}
	if rel == "." {
		return nil
	}
	s.dirMu.Lock()
	defer s.dirMu.Unlock()
	parent := root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		next := filepath.Join(parent, part)
		if _, ok := s.durableDirs[next]; ok {
			parent = next
			continue
		}
		if err := os.Mkdir(next, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := s.dirSync(parent); err != nil {
			return err
		}
		s.durableDirs[next] = struct{}{}
		parent = next
	}
	return nil
}

func syncDir(dir string) error {
	return durablefs.SyncDir(dir)
}

func (s *Dir) Get(key string) ([]byte, error) {
	d, err := os.ReadFile(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		if lp, ok := s.legacyPath(key); ok {
			d, err = os.ReadFile(lp)
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return d, err
}

func (s *Dir) Delete(key string) error {
	if err := s.removeDurable(s.path(key)); err != nil {
		return err
	}
	if lp, ok := s.legacyPath(key); ok {
		if err := s.removeDurable(lp); err != nil {
			return err
		}
	}
	return nil
}

func (s *Dir) removeDurable(path string) error {
	if err := os.Remove(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// A prior attempt may have removed the file but failed to sync its
		// parent. Sync again so retrying a deletion cannot report success
		// while its directory entry is still not known to be durable.
		if s.noSync {
			return nil
		}
		if err := s.dirSync(filepath.Dir(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if s.noSync {
		return nil
	}
	return s.dirSync(filepath.Dir(path))
}
