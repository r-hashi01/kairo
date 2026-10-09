package kairo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Dialect is the SQL product behind a SQLStore.
type Dialect int

const (
	// SQLite (e.g. modernc.org/sqlite). The store uses one connection:
	// SQLite takes one writer at a time.
	SQLite Dialect = iota
	// Postgres (e.g. github.com/jackc/pgx/v5/stdlib).
	Postgres
)

// SQLStore keeps runs in SQLite or PostgreSQL through database/sql: the
// driver is the application's (kairo depends on none). The tables are the
// TypeScript and Python SDKs' (sdk/ts/src/store.ts), so their processes
// and Go's may share a database.
type SQLStore struct {
	db      *sql.DB
	dialect Dialect
	p       string // table prefix
}

var prefixRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// NewSQLStore keeps runs in db. prefix names the tables (default
// "kairo_"). For SQLite it limits db to one connection.
func NewSQLStore(db *sql.DB, dialect Dialect, prefix ...string) (*SQLStore, error) {
	p := "kairo_"
	if len(prefix) > 0 && prefix[0] != "" {
		p = prefix[0]
	}
	if !prefixRE.MatchString(p) {
		return nil, fmt.Errorf("kairo: table prefix %s: letters, digits and _ only", p)
	}
	if dialect == SQLite {
		db.SetMaxOpenConns(1)
	}
	return &SQLStore{db: db, dialect: dialect, p: p}, nil
}

// DB is the database the store keeps runs in.
func (s *SQLStore) DB() *sql.DB { return s.db }

// Prefix is the store's table prefix.
func (s *SQLStore) Prefix() string { return s.p }

// q rewrites ? placeholders for the dialect.
func (s *SQLStore) q(query string) string {
	if s.dialect != Postgres {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

const doneSQL = `('completed', 'failed', 'cancelled')`

func (s *SQLStore) Init(ctx context.Context) error {
	blob, big := "BLOB", "INTEGER"
	if s.dialect == Postgres {
		blob, big = "BYTEA", "BIGINT"
	}
	p := s.p
	ddl := []string{
		`CREATE TABLE IF NOT EXISTS ` + p + `run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state ` + blob + ` NOT NULL,
		  input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq ` + big + ` NOT NULL, created_at ` + big + ` NOT NULL, updated_at ` + big + ` NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS ` + p + `event (run TEXT NOT NULL, seq ` + big + ` NOT NULL, body TEXT NOT NULL, PRIMARY KEY (run, seq))`,
		`CREATE TABLE IF NOT EXISTS ` + p + `timer (run TEXT NOT NULL, timer ` + big + ` NOT NULL, act ` + big + ` NOT NULL, at ` + big + ` NOT NULL, PRIMARY KEY (run, timer))`,
		`CREATE INDEX IF NOT EXISTS ` + p + `timer_at ON ` + p + `timer (at)`,
		`CREATE TABLE IF NOT EXISTS ` + p + `lease (run TEXT NOT NULL, act ` + big + ` NOT NULL, attempt ` + big + ` NOT NULL, owner TEXT NOT NULL,
		  until ` + big + ` NOT NULL, PRIMARY KEY (run, act))`,
		`CREATE INDEX IF NOT EXISTS ` + p + `lease_until ON ` + p + `lease (until)`,
		`CREATE INDEX IF NOT EXISTS ` + p + `lease_owner ON ` + p + `lease (owner)`,
	}
	if s.dialect == SQLite {
		ddl = append([]string{`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`}, ddl...)
	}
	for _, q := range ddl {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("kairo: %s: %w", firstLine(q), err)
		}
	}
	// Columns added to tables made before them: parent (ADR 0054),
	// workflow and meta (ADR 0059).
	for _, col := range []string{"parent", "workflow", "meta"} {
		if s.dialect == Postgres {
			if _, err := s.db.ExecContext(ctx, `ALTER TABLE `+p+`run ADD COLUMN IF NOT EXISTS `+col+` TEXT`); err != nil {
				return err
			}
			continue
		}
		has, err := s.hasColumn(ctx, p+"run", col)
		if err != nil {
			return err
		}
		if !has {
			if _, err := s.db.ExecContext(ctx, `ALTER TABLE `+p+`run ADD COLUMN `+col+` TEXT`); err != nil {
				return err
			}
		}
	}
	for _, q := range []string{
		`CREATE INDEX IF NOT EXISTS ` + p + `run_parent ON ` + p + `run (parent)`,
		`CREATE INDEX IF NOT EXISTS ` + p + `run_done ON ` + p + `run (updated_at) WHERE status IN ` + doneSQL,
		`CREATE INDEX IF NOT EXISTS ` + p + `run_roots ON ` + p + `run (created_at, id) WHERE parent IS NULL`,
	} {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func (s *SQLStore) hasColumn(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, err
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return false, err
		}
		for i, c := range cols {
			if c == "name" && fmt.Sprint(asString(vals[i])) == column {
				return true, nil
			}
		}
	}
	return false, rows.Err()
}

func asString(v any) string {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

const runCols = `id, plan, hash, state, input, status, output, error, seq, created_at, updated_at, parent, workflow, meta`

type scanner interface{ Scan(dest ...any) error }

func scanRun(sc scanner) (*RunRow, error) {
	var r RunRow
	var input, output, errMsg, parent, workflow, meta sql.NullString
	if err := sc.Scan(&r.ID, &r.Plan, &r.Hash, &r.State, &input, &r.Status, &output, &errMsg, &r.Seq, &r.CreatedAt, &r.UpdatedAt, &parent, &workflow, &meta); err != nil {
		return nil, err
	}
	r.Workflow = workflow.String
	if meta.Valid {
		r.Meta = json.RawMessage(meta.String)
	}
	if input.Valid {
		r.Input = json.RawMessage(input.String)
	}
	if output.Valid {
		r.Output = json.RawMessage(output.String)
	}
	r.Error, r.Parent = errMsg.String, parent.String
	return &r, nil
}

func nullable(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// tx runs fn in a transaction that writes: SQLite's BEGIN IMMEDIATE (the
// write lock up front), PostgreSQL's ordinary one (rows locked FOR UPDATE).
func (s *SQLStore) tx(ctx context.Context, fn func(ex execer) error) error {
	if s.dialect == SQLite {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		// The transaction ends however ctx does: a connection back in the
		// pool with a transaction open would hold SQLite's write lock from
		// every other process (database is locked).
		end := context.Background()
		if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
			_, _ = conn.ExecContext(end, `ROLLBACK`) // if it began after all
			return err
		}
		if err := fn(conn); err != nil {
			_, _ = conn.ExecContext(end, `ROLLBACK`)
			return err
		}
		if _, err := conn.ExecContext(end, `COMMIT`); err != nil {
			_, _ = conn.ExecContext(end, `ROLLBACK`)
			return err
		}
		return nil
	}
	t, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(t); err != nil {
		_ = t.Rollback()
		return err
	}
	return t.Commit()
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *SQLStore) WithRun(ctx context.Context, id string, fn func(*RunRow) (*Changes, error)) error {
	p := s.p
	return s.tx(ctx, func(ex execer) error {
		lock := ""
		if s.dialect == Postgres {
			lock = " FOR UPDATE"
		}
		before, err := scanRun(ex.QueryRowContext(ctx, s.q(`SELECT `+runCols+` FROM `+p+`run WHERE id = ?`+lock), id))
		if err == sql.ErrNoRows {
			before, err = nil, nil
		}
		if err != nil {
			return err
		}
		ch, err := fn(before)
		if err != nil || ch == nil {
			return err
		}
		var seq int64
		if before != nil {
			seq = before.Seq
		}
		for _, ev := range ch.Events {
			if _, err := ex.ExecContext(ctx, s.q(`INSERT INTO `+p+`event (run, seq, body) VALUES (?, ?, ?)`), id, seq, string(ev)); err != nil {
				return err
			}
			seq++
		}
		if r := ch.Row; r != nil {
			// A new run's row: two submissions racing for one id insert it
			// once; the loser's transaction fails and rolls back.
			if _, err := ex.ExecContext(ctx, s.q(`INSERT INTO `+p+`run (`+runCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT (id) DO UPDATE SET state = excluded.state, status = excluded.status, output = excluded.output,
				error = excluded.error, seq = excluded.seq, updated_at = excluded.updated_at`),
				r.ID, r.Plan, r.Hash, r.State, nullable(r.Input), r.Status, nullable(r.Output), nullStr(r.Error), seq, r.CreatedAt, r.UpdatedAt, nullStr(r.Parent),
				nullStr(r.Workflow), nullable(r.Meta)); err != nil {
				return err
			}
		}
		if ch.Clear {
			for _, t := range []string{"timer", "lease"} {
				if _, err := ex.ExecContext(ctx, s.q(`DELETE FROM `+p+t+` WHERE run = ?`), id); err != nil {
					return err
				}
			}
		}
		for _, t := range ch.DeleteTimers {
			if _, err := ex.ExecContext(ctx, s.q(`DELETE FROM `+p+`timer WHERE run = ? AND timer = ?`), id, t); err != nil {
				return err
			}
		}
		for _, t := range ch.SetTimers {
			if _, err := ex.ExecContext(ctx, s.q(`INSERT INTO `+p+`timer (run, timer, act, at) VALUES (?, ?, ?, ?)
				ON CONFLICT (run, timer) DO UPDATE SET at = excluded.at`), t.Run, t.Timer, t.Act, t.At); err != nil {
				return err
			}
		}
		for _, a := range ch.EndLeases {
			if _, err := ex.ExecContext(ctx, s.q(`DELETE FROM `+p+`lease WHERE run = ? AND act = ?`), id, a); err != nil {
				return err
			}
		}
		for _, l := range ch.SetLeases {
			if _, err := ex.ExecContext(ctx, s.q(`INSERT INTO `+p+`lease (run, act, attempt, owner, until) VALUES (?, ?, ?, ?, ?)
				ON CONFLICT (run, act) DO UPDATE SET attempt = excluded.attempt, owner = excluded.owner, until = excluded.until`),
				l.Run, l.Act, l.Attempt, l.Owner, l.Until); err != nil {
				return err
			}
		}
		if ch.Notify && s.dialect == Postgres {
			if _, err := ex.ExecContext(ctx, `SELECT pg_notify($1, $2)`, p+"settled", id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *SQLStore) Get(ctx context.Context, id string) (*RunRow, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx, s.q(`SELECT `+runCols+` FROM `+s.p+`run WHERE id = ?`), id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return r, err
}

func (s *SQLStore) Children(ctx context.Context, parent string) ([]RunRow, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+runCols+` FROM `+s.p+`run WHERE parent = ? ORDER BY created_at, id`), parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunRow
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *SQLStore) DueTimers(ctx context.Context, now int64, limit int) ([]TimerRow, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT run, timer, act, at FROM `+s.p+`timer WHERE at <= ? ORDER BY at LIMIT ?`), now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TimerRow
	for rows.Next() {
		var t TimerRow
		if err := rows.Scan(&t.Run, &t.Timer, &t.Act, &t.At); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *SQLStore) ExpiredLeases(ctx context.Context, now int64, limit int) ([]LeaseRow, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT run, act, attempt, owner, until FROM `+s.p+`lease WHERE until < ? ORDER BY until LIMIT ?`), now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LeaseRow
	for rows.Next() {
		var l LeaseRow
		if err := rows.Scan(&l.Run, &l.Act, &l.Attempt, &l.Owner, &l.Until); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (s *SQLStore) RenewLeases(ctx context.Context, owner string, until int64) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE `+s.p+`lease SET until = ? WHERE owner = ?`), until, owner)
	return err
}

func (s *SQLStore) ExpireLeases(ctx context.Context, owner string, now int64) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE `+s.p+`lease SET until = ? WHERE owner = ?`), now-1, owner)
	return err
}

func (s *SQLStore) HandOver(ctx context.Context, l LeaseRow) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE `+s.p+`lease SET owner = ?, until = ? WHERE run = ? AND act = ? AND attempt = ?`),
		l.Owner, l.Until, l.Run, l.Act, l.Attempt)
	return err
}

func (s *SQLStore) NextWake(ctx context.Context) (int64, bool, error) {
	var t, l sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(at) FROM `+s.p+`timer`).Scan(&t); err != nil {
		return 0, false, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(until) FROM `+s.p+`lease`).Scan(&l); err != nil {
		return 0, false, err
	}
	switch {
	case t.Valid && l.Valid:
		return min(t.Int64, l.Int64), true, nil
	case t.Valid:
		return t.Int64, true, nil
	case l.Valid:
		return l.Int64, true, nil
	}
	return 0, false, nil
}

func (s *SQLStore) RemoveFinished(ctx context.Context, cutoff int64, limit int) (int, error) {
	p := s.p
	notParented := ` AND (parent IS NULL OR NOT EXISTS (SELECT 1 FROM ` + p + `run x WHERE x.id = r.parent))`
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT id FROM `+p+`run r WHERE status IN `+doneSQL+` AND updated_at < ?`+notParented+
		` ORDER BY updated_at LIMIT ?`), cutoff, limit)
	if err != nil {
		return 0, err
	}
	var roots []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		roots = append(roots, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	tree := `WITH RECURSIVE tree(id) AS (SELECT CAST(? AS TEXT) UNION SELECT r.id FROM ` + p + `run r JOIN tree t ON r.parent = t.id) SELECT id FROM tree`
	n := 0
	for _, id := range roots {
		err := s.tx(ctx, func(ex execer) error {
			lock := ""
			if s.dialect == Postgres {
				lock = " FOR UPDATE"
			}
			var x int
			if err := ex.QueryRowContext(ctx, s.q(`SELECT 1 FROM `+p+`run WHERE id = ?`+lock), id).Scan(&x); err != nil {
				if err == sql.ErrNoRows {
					return nil // removed by another process
				}
				return err
			}
			// Still a finished root (not resubmitted, not removed meanwhile)?
			if err := ex.QueryRowContext(ctx, s.q(`SELECT 1 FROM `+p+`run r WHERE id = ? AND status IN `+doneSQL+` AND updated_at < ?`+notParented), id, cutoff).Scan(&x); err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
				return err
			}
			// The tree's rows first, the runs (which the tree is found by) last.
			for _, t := range []string{"event", "timer", "lease"} {
				if _, err := ex.ExecContext(ctx, s.q(`DELETE FROM `+p+t+` WHERE run IN (`+tree+`)`), id); err != nil {
					return err
				}
			}
			if _, err := ex.ExecContext(ctx, s.q(`DELETE FROM `+p+`run WHERE id IN (`+tree+`)`), id); err != nil {
				return err
			}
			n++
			return nil
		})
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s *SQLStore) ClaimDrive(ctx context.Context, l LeaseRow, now int64) (bool, error) {
	p := s.p
	// One statement: two processes claiming at once, one of them wins.
	res, err := s.db.ExecContext(ctx, s.q(`INSERT INTO `+p+`lease (run, act, attempt, owner, until) VALUES (?, 0, ?, ?, ?)
		ON CONFLICT (run, act) DO UPDATE SET attempt = excluded.attempt, owner = excluded.owner, until = excluded.until
		WHERE `+p+`lease.owner = excluded.owner OR `+p+`lease.until < ?`), l.Run, l.Attempt, l.Owner, l.Until, now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *SQLStore) EndDrive(ctx context.Context, run, owner string, token int32, now int64, resume bool) error {
	where := ` WHERE run = ? AND act = 0 AND owner = ? AND attempt = ?`
	var err error
	if resume {
		_, err = s.db.ExecContext(ctx, s.q(`UPDATE `+s.p+`lease SET until = ?, owner = ''`+where), now-1, run, owner, token)
	} else {
		_, err = s.db.ExecContext(ctx, s.q(`DELETE FROM `+s.p+`lease`+where), run, owner, token)
	}
	return err
}

func (s *SQLStore) List(ctx context.Context, f ListFilter) ([]RunRow, error) {
	q := `SELECT ` + runCols + ` FROM ` + s.p + `run WHERE parent IS NULL`
	var args []any
	if f.Workflow != "" {
		q += ` AND workflow = ?`
		args = append(args, f.Workflow)
	}
	if f.Status != "" {
		q += ` AND status = ?`
		args = append(args, f.Status)
	}
	if f.Since != 0 {
		q += ` AND created_at >= ?`
		args = append(args, f.Since)
	}
	if f.Until != 0 {
		q += ` AND created_at < ?`
		args = append(args, f.Until)
	}
	if f.After != "" {
		q += ` AND (created_at, id) > (SELECT created_at, id FROM ` + s.p + `run WHERE id = ?)`
		args = append(args, f.After)
	}
	q += ` ORDER BY created_at, id`
	if f.Limit > 0 {
		q += ` LIMIT ?`
		args = append(args, f.Limit)
	}
	rows, err := s.db.QueryContext(ctx, s.q(q), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunRow
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// Close does not close the database: it is the application's.
func (s *SQLStore) Close() error { return nil }
