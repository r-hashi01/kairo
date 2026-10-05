package sqlstore

import (
	"database/sql"
	"fmt"

	"kairo/engine"
	"kairo/wal"
)

// Sinks returns an engine.Config.Sinks function storing each shard's file
// tier log in db as "shard-NNN/file". The memory tier keeps wal.MemSink.
// Use the database only where it is at least as durable as a local file;
// it is not reported as the replicated tier.
func Sinks(db *sql.DB, d Dialect, o Options) func(engine.Tier, int) (wal.Sink, error) {
	return func(t engine.Tier, shard int) (wal.Sink, error) {
		switch t {
		case engine.TierMemory:
			return &wal.MemSink{}, nil
		case engine.TierFile:
			s, err := OpenSink(db, d, fmt.Sprintf("shard-%03d/file", shard), o)
			if err != nil {
				return nil, err // not a typed nil inside the interface
			}
			return s, nil
		}
		return nil, nil
	}
}

// DoneLogs returns an engine.Config.DoneLogs function storing each shard's
// finished-run markers (ADR 0027) in db as "shard-NNN/done".
func DoneLogs(db *sql.DB, d Dialect, o Options) func(int) (wal.Sink, error) {
	return func(shard int) (wal.Sink, error) {
		s, err := OpenSink(db, d, fmt.Sprintf("shard-%03d/done", shard), o)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
}

// Backend bundles what engine.Config needs: Sinks and DoneLogs for the
// logs and one Store for snapshots and blobs.
type Backend struct {
	Sinks    func(engine.Tier, int) (wal.Sink, error)
	DoneLogs func(int) (wal.Sink, error)
	Store    *Store
}

// Configure sets the storage fields of cfg to this backend.
func (b *Backend) Configure(cfg *engine.Config) {
	cfg.Sinks, cfg.DoneLogs, cfg.Snapshots, cfg.Blobs = b.Sinks, b.DoneLogs, b.Store, b.Store
}

// NewBackend creates the tables (unless Options.SkipCreate) and returns
// the backend for db.
func NewBackend(db *sql.DB, d Dialect, o Options) (*Backend, error) {
	st, err := OpenStore(db, d, o)
	if err != nil {
		return nil, err
	}
	return &Backend{Sinks: Sinks(db, d, o), DoneLogs: DoneLogs(db, d, o), Store: st}, nil
}
