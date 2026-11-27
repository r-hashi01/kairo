package kairo

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Liveness tells owners' liveness, where a store can (ADR 0086): a mark a
// process holds while it lives, which the database or the OS takes away
// when it ends. Another process takes a gone owner's leases up without
// waiting for them to expire. Gone is said only on proof (the mark could
// be taken); on any doubt, the leases expire by time, as before. The marks
// are the TypeScript and Python SDKs', so processes of each language see
// each other.
type Liveness interface {
	// Hold takes owner's mark, held until Close.
	Hold(ctx context.Context, owner string) error
	// Gone says whether owner's mark is provably gone (false on any doubt).
	Gone(ctx context.Context, owner string) bool
	// Lost says whether this process lost its own mark: it must stop.
	Lost() bool
	Close() error
}

// LivenessStore is a Store that can tell owners' liveness (ADR 0086).
type LivenessStore interface {
	// Liveness is the store's liveness, or nil if it cannot tell.
	Liveness() Liveness
	// LeaseOwners are the owners of leases not expired at now.
	LeaseOwners(ctx context.Context, now int64) ([]string, error)
}

// liveness is the liveness of store, or of the store it wraps, or nil.
func liveness(store Store) (Liveness, LivenessStore) {
	switch r := store.(type) {
	case retryStore:
		store = r.Store
	case retryNotifierStore:
		store = r.Store
	}
	ls, ok := store.(LivenessStore)
	if !ok {
		return nil, nil
	}
	l := ls.Liveness()
	if l == nil {
		return nil, nil
	}
	return l, ls
}

// Liveness: a lock file of each owner's for SQLite, a session advisory
// lock of each owner's for PostgreSQL.
func (s *SQLStore) Liveness() Liveness {
	switch s.dialect {
	case Postgres:
		return &pgLiveness{db: s.db}
	case SQLite:
		var file string
		if err := s.db.QueryRow(`SELECT file FROM pragma_database_list WHERE name = 'main'`).Scan(&file); err != nil || file == "" {
			return nil // in memory
		}
		return &sqliteLiveness{db: s.db, dir: file + ".owners"}
	}
	return nil
}

// LeaseOwners are the owners of leases not expired at now (ADR 0086).
func (s *SQLStore) LeaseOwners(ctx context.Context, now int64) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT DISTINCT owner FROM `+s.p+`lease WHERE until >= ? AND owner <> ''`), now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var o string
		if err := rows.Scan(&o); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// sqliteLiveness: a lock file of the owner's, beside the database, held in
// an exclusive transaction for the process's life. SQLite's lock is the
// OS's, which it takes away when the process ends. The lock file is opened
// with the store's own driver.
type sqliteLiveness struct {
	db       *sql.DB
	dir      string
	held     *sql.DB
	heldConn *sql.Conn
	heldPath string
}

func (l *sqliteLiveness) file(owner string) string {
	h := sha256.Sum256([]byte(owner))
	return filepath.Join(l.dir, hex.EncodeToString(h[:])[:32]+".lock")
}

func (l *sqliteLiveness) open(path string) (*sql.DB, error) {
	var c driver.Connector = nameConnector{l.db.Driver(), path}
	if dc, ok := l.db.Driver().(driver.DriverContext); ok {
		var err error
		if c, err = dc.OpenConnector(path); err != nil {
			return nil, err
		}
	}
	db := sql.OpenDB(c)
	db.SetMaxOpenConns(1)
	return db, nil
}

// nameConnector opens a driver's connections by name, for a driver that
// has no connectors of its own.
type nameConnector struct {
	d    driver.Driver
	name string
}

func (c nameConnector) Connect(context.Context) (driver.Conn, error) { return c.d.Open(c.name) }
func (c nameConnector) Driver() driver.Driver                        { return c.d }

func (l *sqliteLiveness) Hold(ctx context.Context, owner string) error {
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		return err
	}
	path := l.file(owner)
	db, err := l.open(path)
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err == nil {
		_, err = conn.ExecContext(ctx, "BEGIN EXCLUSIVE")
	}
	if err != nil {
		db.Close()
		return err
	}
	l.held, l.heldConn, l.heldPath = db, conn, path
	return nil
}

func (l *sqliteLiveness) Gone(ctx context.Context, owner string) bool {
	path := l.file(owner)
	// No mark: an owner that never held one (an older SDK), not proof.
	if _, err := os.Stat(path); err != nil {
		return false
	}
	db, err := l.open(path)
	if err != nil {
		return false
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return false
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		return false // its owner lives (or doubt)
	}
	_, _ = conn.ExecContext(ctx, "ROLLBACK")
	return true
}

func (l *sqliteLiveness) Lost() bool { return false } // a process that ends loses it with its life

func (l *sqliteLiveness) Close() error {
	if l.held == nil {
		return nil
	}
	// Its own file goes with it; one of a process that stopped stays, as a
	// gone owner's (another's is never removed: an owner of that name may
	// be taking it again).
	_ = os.Remove(l.heldPath)
	l.heldConn.Close()
	err := l.held.Close()
	l.held = nil
	return err
}

// pgKey is the server's hash of an owner: the same in the three SDKs.
const pgKey = "hashtextextended('kairo.owner:' || $1, 0)"

// pgLiveness: a session advisory lock of the owner's, on a connection of
// its own, which the server takes away when the session ends. The same
// connection tries others' locks (and gives back one it took).
type pgLiveness struct {
	db     *sql.DB
	conn   *sql.Conn
	owner  string
	broken atomic.Bool
}

func (l *pgLiveness) Hold(ctx context.Context, owner string) error {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock("+pgKey+")", owner); err != nil {
		conn.Close()
		return err
	}
	l.conn, l.owner = conn, owner
	return nil
}

func (l *pgLiveness) Gone(ctx context.Context, owner string) bool {
	if l.conn == nil || l.broken.Load() || owner == l.owner {
		return false
	}
	// Advisory locks nest in a session: one with our own key (a hash
	// collision) would be taken here, so it is never taken as gone.
	var same, took bool
	err := l.conn.QueryRowContext(ctx,
		"SELECT hashtextextended('kairo.owner:' || $1, 0) = hashtextextended('kairo.owner:' || $2, 0), pg_try_advisory_lock("+pgKey+")",
		owner, l.owner).Scan(&same, &took)
	if err != nil {
		l.broken.Store(true)
		return false
	}
	if took {
		if _, err := l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock("+pgKey+")", owner); err != nil {
			l.broken.Store(true)
		}
	}
	return took && !same
}

func (l *pgLiveness) Lost() bool { return l.broken.Load() }

func (l *pgLiveness) Close() error {
	if l.conn == nil {
		return nil
	}
	if !l.broken.Load() {
		_, _ = l.conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock("+pgKey+")", l.owner)
	}
	err := l.conn.Close()
	l.conn = nil
	return err
}
