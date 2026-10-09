package kairo

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
)

// MemStore keeps runs in memory: for tests, and for runs that need not
// outlive the process. One lock for everything: a transaction's function
// only computes.
type MemStore struct {
	mu     sync.Mutex
	runs   map[string]*RunRow
	events map[string][]json.RawMessage
	timers map[string]map[uint32]TimerRow
	leases map[string]map[uint32]LeaseRow
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{runs: map[string]*RunRow{}, events: map[string][]json.RawMessage{},
		timers: map[string]map[uint32]TimerRow{}, leases: map[string]map[uint32]LeaseRow{}}
}

func (m *MemStore) Init(context.Context) error { return nil }
func (m *MemStore) Close() error               { return nil }

func copyRow(r *RunRow) *RunRow {
	if r == nil {
		return nil
	}
	c := *r
	c.State = append([]byte(nil), r.State...)
	return &c
}

func (m *MemStore) WithRun(_ context.Context, id string, fn func(*RunRow) (*Changes, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	before := copyRow(m.runs[id])
	ch, err := fn(before)
	if err != nil || ch == nil {
		return err
	}
	m.events[id] = append(m.events[id], ch.Events...)
	if ch.Row != nil {
		r := copyRow(ch.Row)
		r.Seq = int64(len(m.events[id]))
		if before != nil {
			r.CreatedAt, r.Input, r.Parent, r.Workflow, r.Meta = before.CreatedAt, before.Input, before.Parent, before.Workflow, before.Meta
		}
		m.runs[id] = r
	}
	if ch.Clear {
		delete(m.timers, id)
		delete(m.leases, id)
	}
	for _, t := range ch.DeleteTimers {
		delete(m.timers[id], t)
	}
	for _, t := range ch.SetTimers {
		if m.timers[t.Run] == nil {
			m.timers[t.Run] = map[uint32]TimerRow{}
		}
		m.timers[t.Run][t.Timer] = t
	}
	for _, a := range ch.EndLeases {
		delete(m.leases[id], a)
	}
	for _, l := range ch.SetLeases {
		if m.leases[l.Run] == nil {
			m.leases[l.Run] = map[uint32]LeaseRow{}
		}
		m.leases[l.Run][l.Act] = l
	}
	return nil
}

func (m *MemStore) Get(_ context.Context, id string) (*RunRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyRow(m.runs[id]), nil
}

func (m *MemStore) Children(_ context.Context, parent string) ([]RunRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []RunRow
	for _, r := range m.runs {
		if r.Parent == parent {
			out = append(out, *copyRow(r))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (m *MemStore) DueTimers(_ context.Context, now int64, limit int) ([]TimerRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []TimerRow
	for _, ts := range m.timers {
		for _, t := range ts {
			if t.At <= now {
				out = append(out, t)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At < out[j].At })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) ExpiredLeases(_ context.Context, now int64, limit int) ([]LeaseRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []LeaseRow
	for _, ls := range m.leases {
		for _, l := range ls {
			if l.Until < now {
				out = append(out, l)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Until < out[j].Until })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *MemStore) setUntil(owner string, until int64) {
	for run, ls := range m.leases {
		for act, l := range ls {
			if l.Owner == owner {
				l.Until = until
				m.leases[run][act] = l
			}
		}
	}
}

func (m *MemStore) RenewLeases(_ context.Context, owner string, until int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setUntil(owner, until)
	return nil
}

func (m *MemStore) ExpireLeases(_ context.Context, owner string, now int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setUntil(owner, now-1)
	return nil
}

func (m *MemStore) HandOver(_ context.Context, l LeaseRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.leases[l.Run][l.Act]; ok && cur.Attempt == l.Attempt {
		m.leases[l.Run][l.Act] = l
	}
	return nil
}

func (m *MemStore) NextWake(context.Context) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var at int64
	ok := false
	for _, ts := range m.timers {
		for _, t := range ts {
			if !ok || t.At < at {
				at, ok = t.At, true
			}
		}
	}
	for _, ls := range m.leases {
		for _, l := range ls {
			if !ok || l.Until < at {
				at, ok = l.Until, true
			}
		}
	}
	return at, ok, nil
}

func (m *MemStore) RemoveFinished(_ context.Context, cutoff int64, limit int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var roots []*RunRow
	for _, r := range m.runs {
		if !doneStatus[r.Status] || r.UpdatedAt >= cutoff {
			continue
		}
		if r.Parent != "" && m.runs[r.Parent] != nil {
			continue
		}
		roots = append(roots, r)
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].UpdatedAt < roots[j].UpdatedAt })
	if len(roots) > limit {
		roots = roots[:limit]
	}
	for _, root := range roots {
		tree := []string{root.ID}
		for i := 0; i < len(tree); i++ {
			for id, r := range m.runs {
				if r.Parent == tree[i] {
					tree = append(tree, id)
				}
			}
		}
		for _, id := range tree {
			delete(m.runs, id)
			delete(m.events, id)
			delete(m.timers, id)
			delete(m.leases, id)
		}
	}
	return len(roots), nil
}

func (m *MemStore) ClaimDrive(_ context.Context, l LeaseRow, now int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.leases[l.Run][0]; ok && cur.Owner != l.Owner && cur.Until >= now {
		return false, nil
	}
	if m.leases[l.Run] == nil {
		m.leases[l.Run] = map[uint32]LeaseRow{}
	}
	l.Act = 0
	m.leases[l.Run][0] = l
	return true, nil
}

func (m *MemStore) EndDrive(_ context.Context, run, owner string, token int32, now int64, resume bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.leases[run][0]
	if !ok || cur.Owner != owner || cur.Attempt != token {
		return nil
	}
	if resume {
		cur.Until, cur.Owner = now-1, ""
		m.leases[run][0] = cur
	} else {
		delete(m.leases[run], 0)
	}
	return nil
}

func (m *MemStore) List(_ context.Context, f ListFilter) ([]RunRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []RunRow
	for _, r := range m.runs {
		if r.Parent != "" || (f.Workflow != "" && r.Workflow != f.Workflow) || (f.Status != "" && r.Status != f.Status) ||
			(f.Since != 0 && r.CreatedAt < f.Since) || (f.Until != 0 && r.CreatedAt >= f.Until) {
			continue
		}
		out = append(out, *copyRow(r))
	}
	before := func(a, b *RunRow) bool {
		if a.CreatedAt != b.CreatedAt {
			return a.CreatedAt < b.CreatedAt
		}
		return a.ID < b.ID
	}
	sort.Slice(out, func(i, j int) bool { return before(&out[i], &out[j]) })
	if f.After != "" {
		if a := m.runs[f.After]; a != nil {
			i := sort.Search(len(out), func(i int) bool { return before(a, &out[i]) })
			out = out[i:]
		}
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// Events returns a run's recorded events (tests).
func (m *MemStore) Events(id string) []json.RawMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]json.RawMessage(nil), m.events[id]...)
}
