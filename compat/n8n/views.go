package n8n

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// The records of executions and their steps (ADR 0042): what engine v2's
// GET and search answer from. kairo forgets an execution's state once it
// ended, so the records live in PostgreSQL. Only the statements prepared
// here are run (ADR 0020).

// Execution is the record of one execution (engine v2's ExecutionSnapshot
// without its steps).
type Execution struct {
	ID             string          `json:"id"`
	WorkflowID     string          `json:"workflowId"`
	Status         string          `json:"status"` // queued | running | waiting | completed | failed | cancelled
	Mode           string          `json:"mode"`   // production | manual
	HostMode       string          `json:"hostMode"`
	Graph          json.RawMessage `json:"graph,omitempty"`
	Workflow       json.RawMessage `json:"workflow,omitempty"`
	TriggerOutputs json.RawMessage `json:"-"`
	CallerContext  json.RawMessage `json:"-"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
	FinishedAt     *time.Time      `json:"finishedAt"`
}

// Step is the record of one step (engine v2's StepDetail).
type Step struct {
	ID        string          `json:"id"`
	NodeID    string          `json:"nodeId"`
	Iteration int             `json:"iteration"`
	Status    string          `json:"status"` // running | waiting | completed | failed | skipped | cancelled
	Outputs   json.RawMessage `json:"outputs"`
	Error     json.RawMessage `json:"error"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// Search is engine v2's SearchExecutionsRequest.
type Search struct {
	WorkflowIDs  []string // nil: all
	Status       []string
	HostMode     string
	After        *time.Time
	Before       *time.Time
	Cursor       *Cursor
	Limit        int
	IncludeTotal bool
	Top          string // a status to list first
}

// Cursor pages a search (engine v2's {createdAt, id}).
type Cursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

// Views stores the records.
type Views struct {
	db                                                                         *sql.DB
	insert, running, finish, get, steps, putStep, search, count, hasStep, live *sql.Stmt
}

var prefixRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}$`)

// ViewsDDL creates the tables (prefix: letters, digits and _).
func ViewsDDL(prefix string) ([]string, error) {
	if !prefixRE.MatchString(prefix) {
		return nil, fmt.Errorf("n8n: invalid table prefix %q", prefix)
	}
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + prefix + `execution (
			id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, status TEXT NOT NULL, mode TEXT NOT NULL,
			host_mode TEXT NOT NULL, graph JSONB NOT NULL, workflow JSONB NOT NULL, trigger_outputs JSONB,
			caller_context JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
			finished_at TIMESTAMPTZ)`,
		`CREATE INDEX IF NOT EXISTS ` + prefix + `execution_created ON ` + prefix + `execution (created_at DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS ` + prefix + `execution_workflow ON ` + prefix + `execution (workflow_id, created_at DESC, id DESC)`,
		`CREATE TABLE IF NOT EXISTS ` + prefix + `step (
			id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, node_id TEXT NOT NULL, iteration INTEGER NOT NULL,
			status TEXT NOT NULL, outputs JSONB, error JSONB, created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL, UNIQUE (execution_id, node_id, iteration))`,
	}, nil
}

// OpenViews creates the tables if needed and prepares the statements.
func OpenViews(db *sql.DB, prefix string) (*Views, error) {
	ddl, err := ViewsDDL(prefix)
	if err != nil {
		return nil, err
	}
	for _, q := range ddl {
		if _, err := db.Exec(q); err != nil {
			return nil, err
		}
	}
	e, s := prefix+"execution", prefix+"step"
	cols := `id, workflow_id, status, mode, host_mode, created_at, updated_at, finished_at`
	where := `($1::text[] IS NULL OR workflow_id = ANY($1)) AND ($2::text[] IS NULL OR status = ANY($2))
		AND ($3::text = '' OR host_mode = $3) AND ($4::timestamptz IS NULL OR created_at > $4)
		AND ($5::timestamptz IS NULL OR created_at < $5)`
	v := &Views{db: db}
	for _, p := range []struct {
		st **sql.Stmt
		q  string
	}{
		{&v.insert, `INSERT INTO ` + e + ` (id, workflow_id, status, mode, host_mode, graph, workflow, trigger_outputs,
			caller_context, created_at, updated_at) VALUES ($1, $2, 'queued', $3, $4, $5, $6, $7, $8, $9, $9)`},
		{&v.running, `UPDATE ` + e + ` SET status = 'running', updated_at = $2 WHERE id = $1 AND status = 'queued'`},
		{&v.finish, `UPDATE ` + e + ` SET status = $2, updated_at = $3, finished_at = $3 WHERE id = $1 AND finished_at IS NULL`},
		{&v.get, `SELECT ` + cols + `, graph, workflow, caller_context FROM ` + e + ` WHERE id = $1`},
		{&v.steps, `SELECT id, node_id, iteration, status, outputs, error, created_at, updated_at FROM ` + s +
			` WHERE execution_id = $1 ORDER BY created_at, node_id, iteration`},
		{&v.putStep, `INSERT INTO ` + s + ` (id, execution_id, node_id, iteration, status, outputs, error, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
			ON CONFLICT (execution_id, node_id, iteration) DO UPDATE SET status = EXCLUDED.status,
			outputs = EXCLUDED.outputs, error = EXCLUDED.error, updated_at = EXCLUDED.updated_at`},
		{&v.search, `SELECT ` + cols + ` FROM ` + e + ` WHERE ` + where + `
			AND ($6::timestamptz IS NULL OR (created_at, id) < ($6, $7::text))
			ORDER BY CASE WHEN $8::text <> '' AND status = $8 THEN 0 ELSE 1 END, created_at DESC, id DESC LIMIT $9`},
		{&v.count, `SELECT count(*) FROM ` + e + ` WHERE ` + where},
		{&v.hasStep, `SELECT 1 FROM ` + s + ` WHERE execution_id = $1 AND id = $2`},
		// As engine v2: waiting while a step waits and none runs (ADR 0045).
		{&v.live, `UPDATE ` + e + ` SET status = CASE WHEN EXISTS (SELECT 1 FROM ` + s + `
			WHERE execution_id = $1 AND status = 'waiting') AND NOT EXISTS (SELECT 1 FROM ` + s + `
			WHERE execution_id = $1 AND status = 'running') THEN 'waiting' ELSE 'running' END, updated_at = $2
			WHERE id = $1 AND status IN ('running', 'waiting')`},
	} {
		if *p.st, err = db.Prepare(p.q); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func jsonOrNull(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// Insert records a new execution (queued).
func (v *Views) Insert(ctx context.Context, x *Execution) error {
	_, err := v.insert.ExecContext(ctx, x.ID, x.WorkflowID, x.Mode, x.HostMode, string(x.Graph), string(x.Workflow),
		jsonOrNull(x.TriggerOutputs), string(x.CallerContext), x.CreatedAt)
	return err
}

// Tx applies records from one batch of events at once.
type Tx struct{ tx *sql.Tx }

func (v *Views) Begin(ctx context.Context) (*Tx, error) {
	tx, err := v.db.BeginTx(ctx, nil)
	return &Tx{tx}, err
}

func (t *Tx) Commit() error   { return t.tx.Commit() }
func (t *Tx) Rollback() error { return t.tx.Rollback() }

func (v *Views) Running(ctx context.Context, t *Tx, id string, at time.Time) error {
	_, err := t.tx.StmtContext(ctx, v.running).ExecContext(ctx, id, at)
	return err
}

// Live sets a running execution's status from its steps: waiting while a
// step waits and none runs, running otherwise (ADR 0045).
func (v *Views) Live(ctx context.Context, t *Tx, id string, at time.Time) error {
	_, err := t.tx.StmtContext(ctx, v.live).ExecContext(ctx, id, at)
	return err
}

func (v *Views) Finish(ctx context.Context, t *Tx, id, status string, at time.Time) error {
	_, err := t.tx.StmtContext(ctx, v.finish).ExecContext(ctx, id, status, at)
	return err
}

func (v *Views) PutStep(ctx context.Context, t *Tx, execID string, s *Step) error {
	_, err := t.tx.StmtContext(ctx, v.putStep).ExecContext(ctx, s.ID, execID, s.NodeID, s.Iteration, s.Status,
		jsonOrNull(s.Outputs), jsonOrNull(s.Error), s.UpdatedAt)
	return err
}

func scanExecution(row interface{ Scan(...any) error }, full bool) (*Execution, error) {
	x := &Execution{}
	var fin sql.NullTime
	dest := []any{&x.ID, &x.WorkflowID, &x.Status, &x.Mode, &x.HostMode, &x.CreatedAt, &x.UpdatedAt, &fin}
	var graph, workflow, caller []byte
	if full {
		dest = append(dest, &graph, &workflow, &caller)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	if fin.Valid {
		x.FinishedAt = &fin.Time
	}
	x.Graph, x.Workflow, x.CallerContext = graph, workflow, caller
	return x, nil
}

// Get returns an execution (nil when there is none) and, with steps, its
// steps.
func (v *Views) Get(ctx context.Context, id string, steps bool) (*Execution, []Step, error) {
	x, err := scanExecution(v.get.QueryRowContext(ctx, id), true)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil || !steps {
		return x, nil, err
	}
	rows, err := v.steps.QueryContext(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	out := []Step{}
	for rows.Next() {
		var s Step
		var outputs, errj []byte
		if err := rows.Scan(&s.ID, &s.NodeID, &s.Iteration, &s.Status, &outputs, &errj, &s.CreatedAt, &s.UpdatedAt); err != nil {
			return nil, nil, err
		}
		s.Outputs, s.Error = nullJSON(outputs), nullJSON(errj)
		out = append(out, s)
	}
	return x, out, rows.Err()
}

func nullJSON(b []byte) json.RawMessage {
	if b == nil {
		return json.RawMessage("null")
	}
	return b
}

// Find answers a search: a page of executions, the cursor of the next
// page, and (when asked) the total.
func (v *Views) Find(ctx context.Context, q Search) ([]Execution, *Cursor, *int, error) {
	var wf, st any
	if q.WorkflowIDs != nil {
		wf = q.WorkflowIDs
	}
	if q.Status != nil {
		st = q.Status
	}
	var curAt, curID any
	if q.Cursor != nil {
		curAt, curID = q.Cursor.CreatedAt, q.Cursor.ID
	}
	after, before := timeOrNil(q.After), timeOrNil(q.Before)
	rows, err := v.search.QueryContext(ctx, wf, st, q.HostMode, after, before, curAt, curID, q.Top, q.Limit+1)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()
	out := []Execution{}
	for rows.Next() {
		x, err := scanExecution(rows, false)
		if err != nil {
			return nil, nil, nil, err
		}
		out = append(out, *x)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	var next *Cursor
	if len(out) > q.Limit {
		out = out[:q.Limit]
		last := out[len(out)-1]
		next = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	var total *int
	if q.IncludeTotal {
		var n int
		if err := v.count.QueryRowContext(ctx, wf, st, q.HostMode, after, before).Scan(&n); err != nil {
			return nil, nil, nil, err
		}
		total = &n
	}
	return out, next, total, nil
}

func timeOrNil(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// HasStep reports whether step id of execution exec is recorded.
func (v *Views) HasStep(ctx context.Context, exec, id string) bool {
	var one int
	return v.hasStep.QueryRowContext(ctx, exec, id).Scan(&one) == nil
}
