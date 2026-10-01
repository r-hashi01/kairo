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

// Backend bundles what engine.Config needs: Sinks for the logs and one
// Store for snapshots and blobs.
type Backend struct {
	Sinks func(engine.Tier, int) (wal.Sink, error)
	Store *Store
}

// NewBackend creates the tables (unless Options.SkipCreate) and returns
// the backend for db.
func NewBackend(db *sql.DB, d Dialect, o Options) (*Backend, error) {
	st, err := OpenStore(db, d, o)
	if err != nil {
		return nil, err
	}
	return &Backend{Sinks: Sinks(db, d, o), Store: st}, nil
}
