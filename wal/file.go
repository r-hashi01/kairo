package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FileSink appends to local segment files. Every open starts a new segment,
// so a torn tail from a crash is never overwritten or truncated: files are
// only ever appended to.
type FileSink struct {
	dir    string
	prefix string
	f      *os.File
	noSync bool
}

// OpenFile opens the log for prefix (e.g. "shard-003") in dir. If noSync
// is set, Append returns after write(2) without fsync (use for a tmpfs /
// shared-memory directory, where the failure domain is the machine).
func OpenFile(dir, prefix string, noSync bool) (*FileSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	segs, err := segments(dir, prefix)
	if err != nil {
		return nil, err
	}
	next := 1
	if len(segs) > 0 {
		var last int
		fmt.Sscanf(strings.TrimPrefix(filepath.Base(segs[len(segs)-1]), prefix+"."), "%d", &last)
		next = last + 1
	}
	name := filepath.Join(dir, fmt.Sprintf("%s.%08d.wal", prefix, next))
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if !noSync {
		if d, err := os.Open(dir); err == nil {
			d.Sync()
			d.Close()
		}
	}
	return &FileSink{dir: dir, prefix: prefix, f: f, noSync: noSync}, nil
}

func segments(dir, prefix string) ([]string, error) {
	segs, err := filepath.Glob(filepath.Join(dir, prefix+".*.wal"))
	if err != nil {
		return nil, err
	}
	sort.Strings(segs)
	return segs, nil
}

func (s *FileSink) Append(batch []byte) error {
	if _, err := s.f.Write(batch); err != nil {
		return err
	}
	if s.noSync {
		return nil
	}
	return s.f.Sync()
}

func (s *FileSink) ReadAll(fn func([]byte) error) error {
	segs, err := segments(s.dir, s.prefix)
	if err != nil {
		return err
	}
	for _, name := range segs {
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := Scan(data, fn); err != nil && err != errTorn {
			return err
		}
	}
	return nil
}

func (s *FileSink) Close() error { return s.f.Close() }
