package kairo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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
		`CREATE TABLE IF NOT EXISTS ` + p + `rate (destination TEXT PRIMARY KEY, rpm ` + big + ` NOT NULL, wpm ` + big + ` NOT NULL,
		  next_at ` + big + ` NOT NULL, weight REAL NOT NULL, weight_at ` + big + ` NOT NULL, pause_until ` + big + ` NOT NULL, expires_at ` + big + ` NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS ` + p + `rate_expires ON ` + p + `rate (expires_at)`,
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
		if ch.GuardParent != "" {
			// SQLite has no SELECT FOR UPDATE. A no-op write takes its write
			// lock before the check; PostgreSQL locks only the parent row.
			lock := ""
			if s.dialect == Postgres {
				lock = " FOR UPDATE"
			} else if _, err := ex.ExecContext(ctx, s.q(`UPDATE `+p+`run SET id = id WHERE id = ?`), ch.GuardParent); err != nil {
				return err
			}
			parent, err := scanRun(ex.QueryRowContext(ctx, s.q(`SELECT `+runCols+` FROM `+p+`run WHERE id = ?`+lock), ch.GuardParent))
			if err == sql.ErrNoRows {
				parent, err = nil, nil
			}
			if err != nil {
				return err
			}
			if err := checkParent(parent, ch.GuardParent, id); err != nil {
				return err
			}
		}
		if ch.GuardTreeSettled {
			if s.dialect != Postgres {
				// Pair with a child's GuardParent no-op write.
				if _, err := ex.ExecContext(ctx, s.q(`UPDATE `+p+`run SET id = id WHERE id = ?`), id); err != nil {
					return err
				}
			}
			q := `WITH RECURSIVE tree(id) AS (
				SELECT id FROM ` + p + `run WHERE parent = ?
				UNION SELECT r.id FROM ` + p + `run r JOIN tree t ON r.parent = t.id
			) SELECT 1 FROM ` + p + `run WHERE id IN (SELECT id FROM tree) AND status NOT IN ('completed','failed','cancelled') LIMIT 1`
			var one int
			err := ex.QueryRowContext(ctx, s.q(q), id).Scan(&one)
			if err == nil {
				return errChildrenUnsettled
			}
			if err != sql.ErrNoRows {
				return err
			}
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
		if len(ch.SharedRates) > 0 {
			settlements := append([]SharedRateSettlement(nil), ch.SharedRates...)
			sort.Slice(settlements, func(i, j int) bool { return settlements[i].Destination < settlements[j].Destination })
			now, err := s.storeNow(ctx, ex)
			if err != nil {
				return err
			}
			for _, settlement := range settlements {
				if err := s.settleDestination(ctx, ex, now, settlement); err != nil {
					return err
				}
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

type sharedRateRow struct {
	destination                         string
	rpm, wpm, nextAt, weightAt, pauseAt int64
	weight                              float64
	expiresAt                           int64
}

func (s *SQLStore) storeNow(ctx context.Context, ex execer) (int64, error) {
	q := `SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`
	if s.dialect == Postgres {
		q = `SELECT CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)`
	}
	var now int64
	err := ex.QueryRowContext(ctx, q).Scan(&now)
	return now, err
}

func (s *SQLStore) readRate(ctx context.Context, ex execer, destination string) (sharedRateRow, bool, error) {
	if s.dialect == Postgres {
		if _, err := ex.ExecContext(ctx, s.q(`INSERT INTO `+s.p+`rate
			(destination, rpm, wpm, next_at, weight, weight_at, pause_until, expires_at)
			VALUES (?, 0, 0, 0, 0, 0, 0, 0) ON CONFLICT (destination) DO NOTHING`), destination); err != nil {
			return sharedRateRow{}, false, err
		}
	}
	lock := ""
	if s.dialect == Postgres {
		lock = " FOR UPDATE"
	}
	var r sharedRateRow
	err := ex.QueryRowContext(ctx, s.q(`SELECT destination, rpm, wpm, next_at, weight, weight_at, pause_until, expires_at
		FROM `+s.p+`rate WHERE destination = ?`+lock), destination).Scan(
		&r.destination, &r.rpm, &r.wpm, &r.nextAt, &r.weight, &r.weightAt, &r.pauseAt, &r.expiresAt)
	if err == sql.ErrNoRows {
		return sharedRateRow{destination: destination}, false, nil
	}
	return r, err == nil, err
}

func rateCap(wpm int64) float64 {
	if wpm <= 0 {
		return 0
	}
	return max(1, float64(wpm)/6)
}

func configureRate(r *sharedRateRow, now int64, rpm, wpm int) {
	if r.expiresAt <= now {
		*r = sharedRateRow{destination: r.destination}
	}
	if r.wpm > 0 && r.weightAt > 0 && now > r.weightAt {
		r.weight = min(rateCap(r.wpm), r.weight+float64(now-r.weightAt)*float64(r.wpm)/60_000)
	}
	oldWPM := r.wpm
	r.rpm = int64(stricter(int(r.rpm), rpm))
	r.wpm = int64(stricter(int(r.wpm), wpm))
	if r.wpm <= 0 {
		r.weight = 0
	} else if oldWPM <= 0 {
		r.weight = rateCap(r.wpm)
	} else {
		r.weight = min(r.weight, rateCap(r.wpm))
	}
	r.weightAt = now
}

func rateExpiry(r sharedRateRow, now int64) int64 {
	expires := max(r.nextAt, r.pauseAt)
	if r.wpm > 0 && r.weight < rateCap(r.wpm) {
		full := now + int64(math.Ceil((rateCap(r.wpm)-r.weight)*60_000/float64(r.wpm)))
		expires = max(expires, full)
	}
	return expires
}

func (s *SQLStore) writeRate(ctx context.Context, ex execer, r sharedRateRow, now int64) error {
	r.expiresAt = rateExpiry(r, now)
	if r.expiresAt <= now {
		_, err := ex.ExecContext(ctx, s.q(`DELETE FROM `+s.p+`rate WHERE destination = ?`), r.destination)
		return err
	}
	_, err := ex.ExecContext(ctx, s.q(`INSERT INTO `+s.p+`rate
		(destination, rpm, wpm, next_at, weight, weight_at, pause_until, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (destination) DO UPDATE SET rpm = excluded.rpm, wpm = excluded.wpm, next_at = excluded.next_at,
		weight = excluded.weight, weight_at = excluded.weight_at, pause_until = excluded.pause_until, expires_at = excluded.expires_at`),
		r.destination, r.rpm, r.wpm, r.nextAt, r.weight, r.weightAt, r.pauseAt, r.expiresAt)
	return err
}

// AdmitDestination atomically reserves a shared request start. A positive
// wait means no reservation was made; the caller waits and asks again.
func (s *SQLStore) AdmitDestination(ctx context.Context, req SharedRateRequest) (time.Duration, error) {
	if req.Destination == "" {
		return 0, errors.New("kairo: shared rate destination is empty")
	}
	if req.RequestsPerMin < 0 || req.WeightPerMin < 0 {
		return 0, errors.New("kairo: shared destination rates must be non-negative")
	}
	var wait time.Duration
	err := s.tx(ctx, func(ex execer) error {
		now, err := s.storeNow(ctx, ex)
		if err != nil {
			return err
		}
		r, _, err := s.readRate(ctx, ex, req.Destination)
		if err != nil {
			return err
		}
		configureRate(&r, now, req.RequestsPerMin, req.WeightPerMin)
		at := max(r.nextAt, r.pauseAt)
		if r.wpm > 0 && r.weight < 0 {
			at = max(at, now+int64(math.Ceil(-r.weight*60_000/float64(r.wpm))))
		}
		if at > now {
			wait = time.Duration(at-now) * time.Millisecond
		} else if r.rpm > 0 {
			r.nextAt = now + int64(math.Ceil(60_000/float64(r.rpm)))
		}
		return s.writeRate(ctx, ex, r, now)
	})
	return wait, err
}

func (s *SQLStore) settleDestination(ctx context.Context, ex execer, now int64, v SharedRateSettlement) error {
	if v.Destination == "" {
		return errors.New("kairo: shared rate destination is empty")
	}
	if v.RequestsPerMin < 0 || v.WeightPerMin < 0 || v.Weight < 0 || v.RetryAfter < 0 {
		return errors.New("kairo: shared rate settlement must be non-negative")
	}
	r, _, err := s.readRate(ctx, ex, v.Destination)
	if err != nil {
		return err
	}
	configureRate(&r, now, v.RequestsPerMin, v.WeightPerMin)
	if v.Weight > 0 && r.wpm > 0 {
		r.weight -= float64(v.Weight)
	}
	if v.RetryAfter > 0 {
		r.pauseAt = max(r.pauseAt, now+v.RetryAfter.Milliseconds())
	}
	return s.writeRate(ctx, ex, r, now)
}

// PruneDestinationRates removes neutral shared-limit rows in bounded batches.
func (s *SQLStore) PruneDestinationRates(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	now := `CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)`
	if s.dialect == Postgres {
		now = `CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)`
	}
	res, err := s.db.ExecContext(ctx, s.q(`DELETE FROM `+s.p+`rate WHERE destination IN
		(SELECT destination FROM `+s.p+`rate WHERE expires_at <= `+now+` ORDER BY expires_at LIMIT ?)`), limit)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
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

func (s *SQLStore) Recoverable(ctx context.Context, run string, now int64) ([]LeaseRow, []TimerRow, error) {
	leaseRows, err := s.db.QueryContext(ctx, s.q(`SELECT run, act, attempt, owner, until FROM `+s.p+`lease WHERE run = ? AND act <> 0 AND until < ? ORDER BY until`), run, now)
	if err != nil {
		return nil, nil, err
	}
	var leases []LeaseRow
	for leaseRows.Next() {
		var l LeaseRow
		if err := leaseRows.Scan(&l.Run, &l.Act, &l.Attempt, &l.Owner, &l.Until); err != nil {
			leaseRows.Close()
			return nil, nil, err
		}
		leases = append(leases, l)
	}
	if err := leaseRows.Close(); err != nil {
		return nil, nil, err
	}
	timerRows, err := s.db.QueryContext(ctx, s.q(`SELECT run, timer, act, at FROM `+s.p+`timer WHERE run = ? AND at <= ? ORDER BY at`), run, now)
	if err != nil {
		return nil, nil, err
	}
	defer timerRows.Close()
	var timers []TimerRow
	for timerRows.Next() {
		var t TimerRow
		if err := timerRows.Scan(&t.Run, &t.Timer, &t.Act, &t.At); err != nil {
			return nil, nil, err
		}
		timers = append(timers, t)
	}
	return leases, timers, timerRows.Err()
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
