// Package pgnotify wakes the waits of kairo's Go SDK when runs settle in
// other processes, through PostgreSQL's LISTEN/NOTIFY (ADR 0058). The SQL
// store sends the notifications (pg_notify, in the transaction that
// settles a run); this listens for them on a connection of its own. It is a
// module of its own for the driver (pgx): kairo itself depends on none.
//
//	store, _ := kairo.NewSQLStore(db, kairo.Postgres)
//	k, _ := kairo.Open(ctx, kairo.Options{Store: pgnotify.Wrap(store, dsn)})
package pgnotify

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// Store is a SQL store that also hears of runs settled in other processes.
type Store struct {
	*kairo.SQLStore
	dsn string
}

// Wrap listens on dsn (the same database as store's) for runs that settle.
func Wrap(store *kairo.SQLStore, dsn string) *Store { return &Store{SQLStore: store, dsn: dsn} }

// Listen calls settled with the id of each run that settles in any process,
// until stop is called.
func (s *Store) Listen(ctx context.Context, settled func(runID string)) (stop func(), err error) {
	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return nil, err
	}
	channel := pgx.Identifier{s.Prefix() + "settled"}.Sanitize()
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		conn.Close(context.Background())
		return nil, err
	}
	lctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			n, err := conn.WaitForNotification(lctx)
			if err != nil {
				if lctx.Err() == nil {
					log.Printf("kairo: pgnotify: %v", err)
				}
				return
			}
			settled(n.Payload)
		}
	}()
	return func() {
		cancel()
		<-done
		conn.Close(context.Background())
	}, nil
}
