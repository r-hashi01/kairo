package storetest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// oldSchema is a run table from before the columns added later (parent,
// workflow, meta, created_order).
const oldSchema = `CREATE TABLE kairo_run (id TEXT PRIMARY KEY, plan TEXT NOT NULL, hash TEXT NOT NULL, state BLOB NOT NULL,
	input TEXT, status TEXT NOT NULL, output TEXT, error TEXT, seq INTEGER NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`

// hookConnector opens the SQLite driver's connections, calling before
// ahead of each statement the store executes.
type hookConnector struct {
	drv    driver.Driver
	path   string
	before func(query string)
}

func (c hookConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.path)
	if err != nil {
		return nil, err
	}
	return hookConn{conn, c.before}, nil
}

func (c hookConnector) Driver() driver.Driver { return c.drv }

type hookConn struct {
	driver.Conn
	before func(query string)
}

func (c hookConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.before(query)
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c hookConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.before(query)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c hookConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c hookConn) ResetSession(ctx context.Context) error {
	if r, ok := c.Conn.(driver.SessionResetter); ok {
		return r.ResetSession(ctx)
	}
	return nil
}

func runColumns(t *testing.T, path string) map[string]bool {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM pragma_table_info('kairo_run')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		cols[name] = true
	}
	return cols
}

// Two processes initialize a database whose run table is from before the
// added columns. Another process's initialization comes between this
// one's look at the columns and its adding them, the moment the two would
// both add the same column (duplicate column name). The look and the
// change are one transaction that holds the write lock: the other process
// cannot change the schema in between (it is told the database is locked,
// with no wait here), and both initializations succeed.
func TestSQLiteInitIsSerializedWithAnotherProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo.db")
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	old.Close()

	probe, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	var once sync.Once
	var interleaved error
	before := func(query string) {
		if !strings.HasPrefix(query, "ALTER TABLE kairo_run ADD COLUMN parent") {
			return
		}
		// Between the look and the change: what another process's
		// initialization does, without waiting for a lock.
		once.Do(func() {
			conn, err := probe.Conn(context.Background())
			if err != nil {
				interleaved = err
				return
			}
			defer conn.Close()
			if _, err := conn.ExecContext(context.Background(), `PRAGMA busy_timeout=0`); err != nil {
				interleaved = err
				return
			}
			_, interleaved = conn.ExecContext(context.Background(), `ALTER TABLE kairo_run ADD COLUMN parent TEXT`)
		})
	}
	drv := probe.Driver()
	db := sql.OpenDB(hookConnector{drv: drv, path: path, before: before})
	defer db.Close()
	s, err := kairo.NewSQLStore(db, kairo.SQLite)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("initialization with another process's between its look and its change: %v", err)
	}
	if interleaved == nil {
		t.Fatal("another process changed the schema between the look and the change")
	}
	if !strings.Contains(interleaved.Error(), "locked") && !strings.Contains(interleaved.Error(), "busy") {
		t.Fatalf("the other process was refused for another reason: %v", interleaved)
	}
	// The other process, initializing after: nothing left to add.
	if err := openSQLite(t, path).Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	cols := runColumns(t, path)
	for _, c := range []string{"parent", "workflow", "meta", "created_order"} {
		if !cols[c] {
			t.Fatalf("column %s not added: %v", c, cols)
		}
	}
}

// A table from before the added columns still gets them, and the store
// works on it.
func TestSQLiteInitUpgradesAnOlderTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kairo.db")
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`INSERT INTO kairo_run (id, plan, hash, state, status, seq, created_at, updated_at) VALUES ('old', 'p', 'h', x'00', 'running', 0, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	old.Close()
	s := openSQLite(t, path)
	ctx := context.Background()
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	r, err := s.Get(ctx, "old")
	if err != nil || r == nil || r.Parent != "" || r.Workflow != "" || r.CreationOrder != 0 {
		t.Fatalf("the older run: %+v, %v", r, err)
	}
	cols := runColumns(t, path)
	for _, c := range []string{"parent", "workflow", "meta", "created_order"} {
		if !cols[c] {
			t.Fatalf("column %s not added: %v", c, cols)
		}
	}
}

// schemaLockWaiters counts the sessions waiting for the advisory lock that
// initialization takes for prefix.
const schemaLockWaiters = `SELECT count(*) FROM pg_locks l, (SELECT hashtextextended('kairo.schema:' || $1, 0) AS k) h
	WHERE l.locktype = 'advisory' AND NOT l.granted AND l.objsubid = 1
	AND l.classid::bigint = ((h.k >> 32) & 4294967295) AND l.objid::bigint = (h.k & 4294967295)`

// PostgreSQL: two sessions making the same table at once can both find it
// missing, so initialization takes an advisory lock of the prefix's first.
// While another session holds it, initialization waits for it (seen in
// pg_locks), and goes on once it is let go. Several initializations at
// once, of a table from before the added columns, all succeed.
func TestPostgresInitTakesTheSchemaLock(t *testing.T) {
	dsn := os.Getenv("KAIRO_SDK_PG_DSN")
	if dsn == "" {
		t.Skip("KAIRO_SDK_PG_DSN not set")
	}
	ctx := context.Background()
	prefix := fmt.Sprintf("kgoinit%d_", os.Getpid())
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	drop := func() {
		for _, tbl := range []string{"run", "counter", "event", "timer", "lease", "rate"} {
			db.ExecContext(ctx, "DROP TABLE IF EXISTS "+prefix+tbl)
		}
	}
	drop()
	t.Cleanup(drop)
	if _, err := db.ExecContext(ctx, strings.ReplaceAll(strings.ReplaceAll(oldSchema, "kairo_run", prefix+"run"), "BLOB", "BYTEA")); err != nil {
		t.Fatal(err)
	}
	open := func() kairo.Store {
		d, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { d.Close() })
		s, err := kairo.NewSQLStore(d, kairo.Postgres, prefix)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_lock(hashtextextended('kairo.schema:' || $1, 0))`, prefix); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- open().Init(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := db.QueryRowContext(ctx, schemaLockWaiters, prefix).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("initialization did not wait for the schema lock: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("initialization never asked for the schema lock")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := holder.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtextextended('kairo.schema:' || $1, 0))`, prefix); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		s := open()
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = s.Init(ctx) }()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var cols int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = $1
		AND column_name IN ('parent', 'workflow', 'meta', 'created_order')`, prefix+"run").Scan(&cols); err != nil {
		t.Fatal(err)
	}
	if cols != 4 {
		t.Fatalf("added columns: %d of 4", cols)
	}
}
