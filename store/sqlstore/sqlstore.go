// Package sqlstore stores kairo's log, snapshots and blobs in a SQL
// database through database/sql (ADR 0020). Product modules provide the
// Dialect and the driver; this package depends on nothing else.
//
// Security (ADR 0020):
//   - Every value is a bind parameter. All SQL is built once, in
//     buildQueries, from the dialect and the validated table prefix; the
//     only statements executed are those fields (checked by a test).
//   - Pass a *sql.DB you configured (credentials, IAM auth, TLS); this
//     package never sees or logs a DSN.
//   - Every call has a statement timeout. A transaction that times out may
//     or may not have committed, so the sink then refuses further appends.
//   - Fencing: opening a log bumps its owner epoch; appends and retires
//     check it under a row lock, so only the latest opener can write.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"kairo/blob"
	"kairo/wal"
)

type Options struct {
	// Prefix of the table names (default "kairo_"): [a-z][a-z0-9_]{0,30}.
	Prefix string
	// Namespace separates engines sharing the tables, within one trust
	// boundary (default "default"): [A-Za-z0-9_.-]{1,64}.
	Namespace string
	// StatementTimeout bounds every database call (default 30s).
	StatementTimeout time.Duration
	// SkipCreate: do not create the tables (they were created with DDL by
	// a more privileged role).
	SkipCreate bool
}

// MaxKeyLen bounds log ids and object names, in bytes.
const MaxKeyLen = 512

var (
	prefixRe    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)
	namespaceRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

	// ErrFenced: another process opened the same log after this one.
	ErrFenced = errors.New("sqlstore: log taken over by a newer owner")
)

func (o *Options) normalize() error {
	if o.Prefix == "" {
		o.Prefix = "kairo_"
	}
	if o.Namespace == "" {
		o.Namespace = "default"
	}
	if o.StatementTimeout <= 0 {
		o.StatementTimeout = 30 * time.Second
	}
	if !prefixRe.MatchString(o.Prefix) {
		return fmt.Errorf("sqlstore: invalid table prefix %q", o.Prefix)
	}
	if !namespaceRe.MatchString(o.Namespace) {
		return fmt.Errorf("sqlstore: invalid namespace %q", o.Namespace)
	}
	return nil
}

// insertSizes are the row counts of the prepared multi-row INSERTs; a
// batch is split greedily into them.
var insertSizes = []int{64, 16, 4, 1}

type queries struct {
	create      []string
	metaSelect  string // epoch, next for a log (locks the row)
	metaInsert  string
	metaClaim   string
	metaAdvance string
	insert      map[int]string
	readAll     string
	retire      string
	blobDelete  string
	blobInsert  string
	blobGet     string
}

func buildQueries(d *Dialect, prefix string) *queries {
	log, meta, obj := prefix+"log", prefix+"log_meta", prefix+"blob"
	p := d.Placeholder
	q := &queries{insert: map[int]string{}}
	create := "CREATE TABLE IF NOT EXISTS "
	if d.NoCreateIfNotExists {
		create = "CREATE TABLE "
	}
	q.create = []string{
		fmt.Sprintf("%s%s (log_id %s NOT NULL, lsn %s NOT NULL, rec %s, PRIMARY KEY (log_id, lsn))", create, log, d.IDType, d.BigInt, d.BytesType),
		fmt.Sprintf("%s%s (log_id %s NOT NULL, next_lsn %s NOT NULL, owner_epoch %s NOT NULL, PRIMARY KEY (log_id))", create, meta, d.IDType, d.BigInt, d.BigInt),
		fmt.Sprintf("%s%s (k %s NOT NULL, part %s NOT NULL, data %s, PRIMARY KEY (k, part))", create, obj, d.IDType, d.BigInt, d.BytesType),
	}
	q.metaSelect = fmt.Sprintf("SELECT owner_epoch, next_lsn FROM %s WHERE log_id = %s%s", meta, p(1), d.ForUpdate)
	q.metaInsert = fmt.Sprintf("INSERT INTO %s (log_id, next_lsn, owner_epoch) VALUES (%s, 1, 1)", meta, p(1))
	q.metaClaim = fmt.Sprintf("UPDATE %s SET owner_epoch = owner_epoch + 1 WHERE log_id = %s", meta, p(1))
	q.metaAdvance = fmt.Sprintf("UPDATE %s SET next_lsn = %s WHERE log_id = %s", meta, p(1), p(2))
	for _, n := range insertSizes {
		switch {
		case d.InsertRows != nil:
			q.insert[n] = d.InsertRows(log, "log_id, lsn, rec", n, 3, p)
		case n == 1 || d.MultiRowInsert:
			q.insert[n] = fmt.Sprintf("INSERT INTO %s (log_id, lsn, rec) VALUES %s", log, d.values(n, 3, 1))
		}
	}
	q.readAll = fmt.Sprintf("SELECT lsn, rec FROM %s WHERE log_id = %s ORDER BY lsn", log, p(1))
	q.retire = d.DeleteLimited(log, fmt.Sprintf("log_id = %s AND lsn < %s", p(1), p(2)), d.RetireBatch)
	q.blobDelete = fmt.Sprintf("DELETE FROM %s WHERE k = %s", obj, p(1))
	q.blobInsert = fmt.Sprintf("INSERT INTO %s (k, part, data) VALUES (%s, %s, %s)", obj, p(1), p(2), p(3))
	q.blobGet = fmt.Sprintf("SELECT data FROM %s WHERE k = %s ORDER BY part", obj, p(1))
	return q
}

// DDL returns the statements that create the tables, for running with a
// role that may create tables (then use Options.SkipCreate).
func DDL(d Dialect, o Options) ([]string, error) {
	if err := o.normalize(); err != nil {
		return nil, err
	}
	return buildQueries(&d, o.Prefix).create, nil
}

// Migrate creates the tables if they do not exist.
func Migrate(db *sql.DB, d Dialect, o Options) error {
	if err := o.normalize(); err != nil {
		return err
	}
	q := buildQueries(&d, o.Prefix)
	ctx, cancel := context.WithTimeout(context.Background(), o.StatementTimeout)
	defer cancel()
	for _, stmt := range q.create {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if d.NoCreateIfNotExists && d.TableExists != nil && d.TableExists(err) {
				continue
			}
			return fmt.Errorf("sqlstore: creating tables: %w", err)
		}
	}
	return nil
}

func prepare(db *sql.DB, d *Dialect, o *Options) (*queries, error) {
	if err := o.normalize(); err != nil {
		return nil, err
	}
	if d.Placeholder == nil || d.DeleteLimited == nil || d.PartSize <= 0 || d.RetireBatch <= 0 {
		return nil, errors.New("sqlstore: incomplete dialect")
	}
	if !o.SkipCreate {
		if err := Migrate(db, *d, *o); err != nil {
			return nil, err
		}
	}
	return buildQueries(d, o.Prefix), nil
}

// Pool applies connection limits to db: at most max open connections
// (default 16) and idle connections recycled after 5 minutes. Product
// modules' Open uses it; call it on a *sql.DB you open yourself too.
func Pool(db *sql.DB, max int) *sql.DB {
	if max <= 0 {
		max = 16
	}
	db.SetMaxOpenConns(max)
	db.SetMaxIdleConns(max)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db
}

// Sink is a wal.Sink and wal.Retirer for one log.
type Sink struct {
	db     *sql.DB
	q      *queries
	o      Options
	logID  string
	epoch  int64
	next   uint64
	broken error
}

var _ wal.Sink = (*Sink)(nil)
var _ wal.Retirer = (*Sink)(nil)

// OpenSink opens the log logID (within Options.Namespace) and becomes its
// owner: a sink that opened it earlier can no longer write.
func OpenSink(db *sql.DB, d Dialect, logID string, o Options) (*Sink, error) {
	q, err := prepare(db, &d, &o)
	if err != nil {
		return nil, err
	}
	full := o.Namespace + "/" + logID
	if len(full) > MaxKeyLen {
		return nil, fmt.Errorf("sqlstore: log id longer than %d bytes", MaxKeyLen)
	}
	s := &Sink{db: db, q: q, o: o, logID: full}
	// A concurrent first open may lose the INSERT race; retry once.
	for attempt := 0; ; attempt++ {
		err = s.claim()
		if err == nil || attempt == 2 {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("sqlstore: opening log: %w", err)
	}
	return s, nil
}

func (s *Sink) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.o.StatementTimeout)
}

func (s *Sink) claim() error {
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, s.q.metaClaim, s.logID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, err := tx.ExecContext(ctx, s.q.metaInsert, s.logID); err != nil {
			return err
		}
	}
	if err := tx.QueryRowContext(ctx, s.q.metaSelect, s.logID).Scan(&s.epoch, &s.next); err != nil {
		return err
	}
	return tx.Commit()
}

// own locks the meta row and checks this sink still owns the log.
func (s *Sink) own(ctx context.Context, tx *sql.Tx) error {
	var epoch int64
	var next uint64
	if err := tx.QueryRowContext(ctx, s.q.metaSelect, s.logID).Scan(&epoch, &next); err != nil {
		return err
	}
	if epoch != s.epoch {
		return ErrFenced
	}
	return nil
}

func (s *Sink) fail(err error) error {
	s.broken = fmt.Errorf("sqlstore: log %s unusable after an earlier error: %w", s.logID, err)
	return err
}

// Append stores the framed records of batch in one transaction.
func (s *Sink) Append(batch []byte) error {
	if s.broken != nil {
		return s.broken
	}
	var recs [][]byte
	wal.Scan(batch, func(r []byte) error { recs = append(recs, r); return nil })
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		// No transaction began, so nothing was written: retryable.
		return fmt.Errorf("%w: %v", wal.ErrTransient, err)
	}
	defer tx.Rollback()
	if err := s.own(ctx, tx); err != nil {
		return s.fail(err)
	}
	lsn := s.next
	args := make([]any, 0, 3*insertSizes[0])
	for len(recs) > 0 {
		n := 1
		for _, size := range insertSizes {
			if _, ok := s.q.insert[size]; ok && size <= len(recs) {
				n = size
				break
			}
		}
		args = args[:0]
		for _, r := range recs[:n] {
			args = append(args, s.logID, int64(lsn), r)
			lsn++
		}
		if _, err := tx.ExecContext(ctx, s.q.insert[n], args...); err != nil {
			return s.fail(err)
		}
		recs = recs[n:]
	}
	if _, err := tx.ExecContext(ctx, s.q.metaAdvance, int64(lsn), s.logID); err != nil {
		return s.fail(err)
	}
	if err := tx.Commit(); err != nil {
		return s.fail(err)
	}
	s.next = lsn
	return nil
}

func (s *Sink) ReadAll(fn func(uint64, []byte) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*s.o.StatementTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.q.readAll, s.logID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var lsn int64
		var rec []byte
		if err := rows.Scan(&lsn, &rec); err != nil {
			return err
		}
		if err := fn(uint64(lsn), rec); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Sink) Next() uint64 { return s.next }

// ReadLog reads a log without taking ownership of it (for inspection and
// tools; it does not fence the current owner).
func ReadLog(db *sql.DB, d Dialect, logID string, o Options, fn func(uint64, []byte) error) error {
	if err := o.normalize(); err != nil {
		return err
	}
	s := &Sink{db: db, q: buildQueries(&d, o.Prefix), o: o, logID: o.Namespace + "/" + logID}
	return s.ReadAll(fn)
}

// Retire deletes the log's rows below lsn, RetireBatch rows per
// transaction (products such as TiDB limit transaction size).
func (s *Sink) Retire(lsn uint64) error {
	if s.broken != nil {
		return s.broken
	}
	for {
		n, err := s.retireBatch(lsn)
		if errors.Is(err, wal.ErrTransient) {
			return err // nothing happened; the next compaction retries
		}
		if err != nil {
			return s.fail(err)
		}
		if n == 0 {
			return nil
		}
	}
}

func (s *Sink) retireBatch(lsn uint64) (int64, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", wal.ErrTransient, err)
	}
	defer tx.Rollback()
	if err := s.own(ctx, tx); err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, s.q.retire, s.logID, int64(lsn))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

// Close does not close the *sql.DB, which belongs to the caller.
func (s *Sink) Close() error { return nil }

// Store is a blob.Store. Objects larger than the dialect's PartSize are
// split into rows; Put replaces an object in one transaction.
type Store struct {
	db   *sql.DB
	q    *queries
	o    Options
	part int
}

var _ blob.Store = (*Store)(nil)

func OpenStore(db *sql.DB, d Dialect, o Options) (*Store, error) {
	q, err := prepare(db, &d, &o)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, q: q, o: o, part: d.PartSize}, nil
}

func (s *Store) name(key string) (string, error) {
	full := s.o.Namespace + "/" + key
	if len(full) > MaxKeyLen {
		return "", fmt.Errorf("sqlstore: object name longer than %d bytes", MaxKeyLen)
	}
	return full, nil
}

func (s *Store) Put(key string, data []byte) error {
	name, err := s.name(key)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.o.StatementTimeout)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, s.q.blobDelete, name); err != nil {
		return err
	}
	for part := 0; part == 0 || part*s.part < len(data); part++ {
		end := min((part+1)*s.part, len(data))
		if _, err := tx.ExecContext(ctx, s.q.blobInsert, name, int64(part), data[part*s.part:end]); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Get(key string) ([]byte, error) {
	name, err := s.name(key)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.o.StatementTimeout)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, s.q.blobGet, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []byte
	found := false
	for rows.Next() {
		var part []byte // NULL (Oracle's empty BLOB) scans as nil
		if err := rows.Scan(&part); err != nil {
			return nil, err
		}
		found = true
		out = append(out, part...)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !found {
		return nil, blob.ErrNotFound
	}
	if out == nil {
		out = []byte{}
	}
	return out, nil
}

func (s *Store) Delete(key string) error {
	name, err := s.name(key)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.o.StatementTimeout)
	defer cancel()
	_, err = s.db.ExecContext(ctx, s.q.blobDelete, name)
	return err
}
