// Package sqlitenotify wakes waits in other processes that share a file-backed
// SQLite store. Notifications are best-effort hints; SQLite remains the source
// of truth and the runtime's lease-period sweep is the fallback (ADR 0079).
package sqlitenotify

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

const maxMessage = 64 << 10

type message struct {
	Version   int    `json:"v"`
	Namespace string `json:"namespace"`
	RunID     string `json:"run_id"`
}

// Store adds same-host process notifications to a SQLite SQLStore.
type Store struct {
	*kairo.SQLStore
	path string

	mu     sync.Mutex
	dir    string
	socket string
	ln     net.Listener
	done   chan struct{}
}

// Wrap adds local notifications for path. Path must name the file opened by
// store; in-memory SQLite cannot be shared between processes.
func Wrap(store *kairo.SQLStore, path string) *Store {
	return &Store{SQLStore: store, path: path}
}

// WithRun publishes a settlement hint only after SQLite committed it.
func (s *Store) WithRun(ctx context.Context, id string, fn func(*kairo.RunRow) (*kairo.Changes, error)) error {
	notify := false
	err := s.SQLStore.WithRun(ctx, id, func(row *kairo.RunRow) (*kairo.Changes, error) {
		ch, err := fn(row)
		if err == nil && ch != nil {
			notify = ch.Notify
		}
		return ch, err
	})
	if err == nil && notify {
		s.publish(id)
	}
	return err
}

// Listen receives settlement hints until the returned function is called.
func (s *Store) Listen(_ context.Context, settled func(string)) (func(), error) {
	path, err := filepath.Abs(s.path)
	if err != nil {
		return nil, err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(path + "\x00" + s.Prefix()))
	tmp := os.TempDir()
	if resolved, err := filepath.EvalSymlinks(tmp); err == nil {
		tmp = resolved
	}
	dir := filepath.Join(tmp, ".kairo-n-"+hex.EncodeToString(sum[:8]))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	var id [4]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	socket := filepath.Join(dir, hex.EncodeToString(id[:])+".sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	s.mu.Lock()
	s.dir, s.socket, s.ln, s.done = dir, socket, ln, done
	s.mu.Unlock()
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			receive(conn, s.Prefix(), settled)
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = ln.Close()
			<-done
			_ = os.Remove(socket)
			_ = os.Remove(dir)
		})
	}, nil
}

func receive(conn net.Conn, namespace string, settled func(string)) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	b, err := io.ReadAll(io.LimitReader(conn, maxMessage+1))
	if err != nil || len(b) == 0 || len(b) > maxMessage {
		return
	}
	var m message
	if json.Unmarshal(b, &m) == nil && m.Version == 1 && m.Namespace == namespace && m.RunID != "" {
		settled(m.RunID)
	}
}

func (s *Store) publish(runID string) {
	s.mu.Lock()
	dir, own := s.dir, s.socket
	s.mu.Unlock()
	if dir == "" {
		return
	}
	b, err := json.Marshal(message{Version: 1, Namespace: s.Prefix(), RunID: runID})
	if err != nil || len(b) > maxMessage {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sock") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if path == own {
			continue
		}
		conn, err := net.DialTimeout("unix", path, 100*time.Millisecond)
		if err != nil {
			_ = os.Remove(path)
			continue
		}
		_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = conn.Write(b)
		_ = conn.Close()
	}
}
