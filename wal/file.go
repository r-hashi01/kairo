package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/r-hashi01/kairo/internal/durablefs"
)

// DefaultSegmentSize is the size after which a FileSink starts a new
// segment.
const DefaultSegmentSize = 64 << 20

// FileSink appends to local segment files named after the LSN of their
// first record: "<prefix>.L<20-digit LSN>.wal" (ADR 0016). A new segment is
// started on every open and when the current one exceeds SegmentSize.
// Segments are only ever appended to; Retire deletes whole segments whose
// records are all no longer needed.
//
// Segments written by v0 ("<prefix>.<8-digit sequence>.wal") are still
// read; their LSNs continue from the previous segment.
type FileSink struct {
	dir     string
	prefix  string
	noSync  bool
	dirSync func(string) error
	remove  func(string) error
	// SegmentSize bounds a segment (default DefaultSegmentSize).
	SegmentSize int64

	f      *os.File
	size   int64
	broken error     // set after an unsafe durability failure: later appends are refused
	next   uint64    // LSN of the next record appended
	segs   []segment // all segments, oldest first; the last is current
}

type segment struct {
	path   string
	first  uint64 // LSN of its first record
	legacy bool
}

// OpenFile opens the log for prefix (e.g. "shard-003") in dir. If noSync
// is set, Append returns after write(2) without fsync (use for a tmpfs /
// shared-memory directory, where the failure domain is the machine).
func OpenFile(dir, prefix string, noSync bool) (*FileSink, error) {
	return openFile(dir, prefix, noSync, syncDirectory)
}

func openFile(dir, prefix string, noSync bool, dirSync func(string) error) (*FileSink, error) {
	if err := durablefs.MkdirAll(dir, 0o755, noSync, dirSync); err != nil {
		return nil, err
	}
	s := &FileSink{
		dir: dir, prefix: prefix, noSync: noSync, SegmentSize: DefaultSegmentSize,
		dirSync: dirSync, remove: os.Remove,
	}
	segs, err := s.list()
	if err != nil {
		return nil, err
	}
	// Find the LSN after the last complete record. Named segments state
	// their first LSN; v0 segments continue from the previous one, so they
	// (and the last segment) are counted.
	next := uint64(1)
	for i, sg := range segs {
		if sg.legacy {
			segs[i].first = next
		} else {
			next = sg.first
		}
		if sg.legacy || i == len(segs)-1 {
			data, err := os.ReadFile(sg.path)
			if err != nil {
				return nil, err
			}
			next += count(data)
		}
	}
	s.segs, s.next = segs, next
	if err := s.startSegment(); err != nil {
		if s.f != nil {
			_ = s.f.Close()
		}
		return nil, err
	}
	return s, nil
}

// list returns the segments of the log, oldest first. v0 names sort before
// LSN-named ones ('0'-'9' < 'L'), which is also their age order.
func (s *FileSink) list() ([]segment, error) {
	paths, err := filepath.Glob(filepath.Join(s.dir, s.prefix+".*.wal"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	var segs []segment
	for _, p := range paths {
		mid := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), s.prefix+"."), ".wal")
		if strings.HasPrefix(mid, "L") {
			first, err := strconv.ParseUint(mid[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("wal: bad segment name %s", p)
			}
			segs = append(segs, segment{path: p, first: first})
			continue
		}
		if _, err := strconv.Atoi(mid); err != nil {
			continue // not ours
		}
		segs = append(segs, segment{path: p, legacy: true})
	}
	return segs, nil
}

func (s *FileSink) segmentPath(first uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%s.L%020d.wal", s.prefix, first))
}

// startSegment opens a new segment beginning at s.next. An empty segment
// with that name (left by an open without appends) is reused.
func (s *FileSink) startSegment() error {
	p := s.segmentPath(s.next)
	if n := len(s.segs); n > 0 && s.segs[n-1].path == p {
		s.segs = s.segs[:n-1]
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		if s.f != nil {
			return s.fail("segment rotation", err)
		}
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		if s.f != nil {
			return s.fail("segment rotation", err)
		}
		return err
	}
	if st.Size() != 0 {
		f.Close()
		err := fmt.Errorf("wal: segment %s already holds data", p)
		if s.f != nil {
			return s.fail("segment rotation", err)
		}
		return err
	}
	if s.f != nil {
		if err := s.f.Close(); err != nil {
			f.Close()
			return s.fail("close", err)
		}
	}
	s.f, s.size = f, 0
	s.segs = append(s.segs, segment{path: p, first: s.next})
	if !s.noSync {
		if err := s.dirSync(s.dir); err != nil {
			return s.fail("directory sync", err)
		}
	}
	return nil
}

func syncDirectory(dir string) error {
	return durablefs.SyncDir(dir)
}

func (s *FileSink) fail(op string, err error) error {
	s.broken = fmt.Errorf("wal: segment broken by %s error: %w", op, err)
	return s.broken
}

// Append writes and syncs batch. After a failed write or sync the sink is
// broken: what reached the file is unknown, so LSNs could no longer be kept
// in step with the engine, and every later Append fails too.
func (s *FileSink) Append(batch []byte) error {
	if s.broken != nil {
		return s.broken
	}
	if _, err := s.f.Write(batch); err != nil {
		s.broken = fmt.Errorf("wal: segment broken by write error: %w", err)
		return err
	}
	if !s.noSync {
		if err := s.f.Sync(); err != nil {
			s.broken = fmt.Errorf("wal: segment broken by sync error: %w", err)
			return err
		}
	}
	s.next += countFrames(batch)
	s.size += int64(len(batch))
	if s.size >= s.SegmentSize {
		return s.startSegment()
	}
	return nil
}

func (s *FileSink) ReadAll(fn func(uint64, []byte) error) error {
	for _, sg := range s.segs {
		data, err := os.ReadFile(sg.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		lsn := sg.first
		if _, err := Scan(data, func(rec []byte) error {
			err := fn(lsn, rec)
			lsn++
			return err
		}); err != nil && err != errTorn {
			return err
		}
	}
	return nil
}

// Retire deletes every segment, except the current one, whose records all
// have LSN < lsn. A segment's last LSN is the next segment's first minus one.
func (s *FileSink) Retire(lsn uint64) error {
	if s.broken != nil {
		return s.broken
	}
	i := 0
	var removeErr error
	for i+1 < len(s.segs) && s.segs[i+1].first <= lsn {
		if err := s.remove(s.segs[i].path); err != nil && !errors.Is(err, os.ErrNotExist) {
			removeErr = err
			break
		}
		i++
	}
	if i > 0 {
		s.segs = append([]segment(nil), s.segs[i:]...)
		if !s.noSync {
			if err := s.dirSync(s.dir); err != nil {
				return s.fail("directory sync after retirement", err)
			}
		}
	}
	return removeErr
}

func (s *FileSink) Next() uint64 { return s.next }

// Segments reports the number of segment files (for tests and metrics).
func (s *FileSink) Segments() int { return len(s.segs) }

func (s *FileSink) Close() error { return s.f.Close() }
