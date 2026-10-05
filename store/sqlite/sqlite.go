// Package sqlite is the SQLite backend of kairo (ADR 0018, 0020): the
// shared SQL implementation (store/sqlstore) with the SQLite dialect and
// the pure Go driver modernc.org/sqlite. It lives in its own module so the
// runtime stays free of dependencies (ADR 0014); the default storage
// remains the file format.
//
//	st, _ := sqlite.OpenStore(filepath.Join(dir, "objects.db"))
//	engine.New(engine.Config{
//		Sinks:     sqlite.Sinks(filepath.Join(dir, "wal")),
//		DoneLogs:  sqlite.DoneLogs(filepath.Join(dir, "wal")),
//		Snapshots: st, Blobs: st,
//	})
package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"kairo/engine"
	"kairo/store/sqlstore"
	"kairo/wal"
)

// Dialect is the SQLite dialect.
var Dialect = sqlstore.SQLite

// Open opens a database file with one connection (SQLite has a single
// writer) and durability matching wal.FileSink.
func Open(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	// synchronous=FULL syncs every commit; fullfsync makes that sync reach
	// the drive on macOS too (F_FULLFSYNC), matching wal.FileSink.
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=fullfsync(1)&_pragma=checkpoint_fullfsync(1)" +
		"&_pragma=auto_vacuum(INCREMENTAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Sink is a log in its own database file.
type Sink struct {
	*sqlstore.Sink
	db *sql.DB
}

// OpenSink opens the log database at path.
func OpenSink(path string) (*Sink, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	s, err := sqlstore.OpenSink(db, Dialect, "log", sqlstore.Options{})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Sink{Sink: s, db: db}, nil
}

// Retire returns freed pages to the file after deleting.
func (s *Sink) Retire(lsn uint64) error {
	if err := s.Sink.Retire(lsn); err != nil {
		return err
	}
	_, err := s.db.Exec(`PRAGMA incremental_vacuum`)
	return err
}

func (s *Sink) Close() error { return s.db.Close() }

// Sinks returns an engine.Config.Sinks function that stores the file tier
// of each shard in dir/shard-NNN.db (one file per shard, so shards write
// in parallel). The memory tier keeps wal.MemSink.
func Sinks(dir string) func(engine.Tier, int) (wal.Sink, error) {
	return func(t engine.Tier, shard int) (wal.Sink, error) {
		switch t {
		case engine.TierMemory:
			return &wal.MemSink{}, nil
		case engine.TierFile:
			s, err := OpenSink(filepath.Join(dir, fmt.Sprintf("shard-%03d.db", shard)))
			if err != nil {
				return nil, err // not a typed nil inside the interface
			}
			return s, nil
		}
		return nil, nil
	}
}

// DoneLogs returns an engine.Config.DoneLogs function that stores each
// shard's finished-run markers (ADR 0027) in dir/done-NNN.db.
func DoneLogs(dir string) func(int) (wal.Sink, error) {
	return func(shard int) (wal.Sink, error) {
		s, err := OpenSink(filepath.Join(dir, fmt.Sprintf("done-%03d.db", shard)))
		if err != nil {
			return nil, err
		}
		return s, nil
	}
}

// Store is a blob.Store in its own database file.
type Store struct {
	*sqlstore.Store
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	s, err := sqlstore.OpenStore(db, Dialect, sqlstore.Options{})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{Store: s, db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }
