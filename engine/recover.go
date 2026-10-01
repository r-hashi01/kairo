package engine

import (
	"fmt"

	"kairo/blob"
	"kairo/core"
	"kairo/timerwheel"
)

type lsnEvent struct {
	lsn uint64
	ev  *core.Event
}

type recovered struct {
	meta   *startMeta
	tier   Tier
	events []lsnEvent
}

// recover rebuilds the shard's runs from its logs and snapshots, then
// applies EvRecover to each unfinished run so in-flight work is re-issued
// (or stopped for review, for real steps whose outcome is unknown).
func (s *shard) recover() error {
	runs := map[string]*recovered{}
	var order []string
	for t, l := range s.logs {
		if l == nil {
			continue
		}
		var lsn uint64
		err := l.sink.ReadAll(func(rec []byte) error {
			lsn++
			kind, meta, id, ev, err := decodeRecord(rec)
			if err != nil {
				return err
			}
			switch kind {
			case recStart:
				runs[id] = &recovered{meta: meta, tier: Tier(t)}
				order = append(order, id)
			case recEvent:
				if x := runs[id]; x != nil {
					x.events = append(x.events, lsnEvent{lsn: lsn, ev: ev})
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		l.lsn, l.durable = lsn, lsn
	}
	s.now = s.e.cfg.Now().UnixMilli()
	for _, id := range order {
		x := runs[id]
		p := s.e.plan(x.meta.Plan + "@" + x.meta.PlanHash)
		if p == nil {
			return fmt.Errorf("run %s: %w: %s@%s", id, errPlanMissing, x.meta.Plan, x.meta.PlanHash)
		}
		st := core.NewState(id)
		var from uint64
		if x.tier >= TierFile {
			if data, err := s.e.snaps.Get("snap/" + id); err == nil {
				if lsn, snap, err := decodeSnapshot(data); err == nil {
					st, from = snap, lsn
				}
			} else if err != blob.ErrNotFound {
				return err
			}
		}
		var last uint64
		for _, le := range x.events {
			last = le.lsn
			if le.lsn <= from {
				continue
			}
			core.Apply(p, st, le.ev, nil)
		}
		if st.Status.Done() {
			if x.tier >= TierFile {
				s.e.snaps.Delete("snap/" + id)
			}
			continue
		}
		r := &run{id: id, plan: p, tenant: x.meta.Tenant, tier: x.tier, st: st, timers: map[uint32]timerwheel.Handle{}, lastLSN: last, status: st.Status}
		s.runs[id] = r
		s.inMemory.Add(1)
		if st.Status == core.StatusBlocked {
			r.reviews = reviews(r)
		}
		if st.Quiescent() {
			r.waits = core.Waits(p, st)
		}
		s.e.adm.Readmit(r.tenant)
		s.event(r, core.Event{Kind: core.EvRecover})
	}
	s.flush()
	s.rearm()
	return nil
}
