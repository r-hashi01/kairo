package engine

import (
	"fmt"
	"log"

	"kairo/blob"
	"kairo/core"
	"kairo/seal"
	"kairo/timerwheel"
)

type lsnEvent struct {
	lsn uint64
	ev  *core.Event
}

type recovered struct {
	meta     *startMeta
	tier     Tier
	startLSN uint64 // 0 if the start record was retired
	cpLSN    uint64 // snapshot LSN of the latest checkpoint record
	cpRec    uint64 // LSN of that record
	events   []lsnEvent
}

// recover rebuilds the shard's runs from its logs and snapshots, then
// applies EvRecover to each unfinished run so in-flight work is re-issued
// (or stopped for review, for real steps whose outcome is unknown).
//
// A run is found through its start record or, once that was retired,
// through a checkpoint record (ADR 0016). Its state is the snapshot (if
// any) plus its events after the snapshot's LSN, which retention keeps for
// every live run (and for finished runs until their snapshot is deleted).
// A run found without a start record and without a snapshot finished, and
// is skipped.
func (s *shard) recover() error {
	runs := map[string]*recovered{}
	var order []string
	var first [tierCount]uint64 // first retained LSN per tier log
	get := func(t Tier, m *startMeta) *recovered {
		x := runs[m.RunID]
		if x == nil {
			x = &recovered{meta: m, tier: t}
			runs[m.RunID] = x
			order = append(order, m.RunID)
		}
		return x
	}
	for t, l := range s.logs {
		if l == nil {
			continue
		}
		err := l.sink.ReadAll(func(lsn uint64, rec []byte) error {
			if first[t] == 0 {
				first[t] = lsn
			}
			r, err := decodeRecord(rec)
			if err != nil {
				return err
			}
			switch r.kind {
			case recStart:
				// A start record begins a new run even if an earlier run
				// used the same id: forget that run's records.
				x := get(Tier(t), r.meta)
				*x = recovered{meta: r.meta, tier: Tier(t), startLSN: lsn}
			case recCheckpoint:
				x := get(Tier(t), r.meta)
				if r.snapLSN >= x.cpLSN {
					x.cpLSN, x.cpRec = r.snapLSN, lsn
				}
			case recEvent:
				if x := runs[r.runID]; x != nil {
					x.events = append(x.events, lsnEvent{lsn: lsn, ev: r.ev})
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		next := l.sink.Next()
		if first[t] == 0 {
			first[t] = next
		}
		l.lsn, l.durable, l.retired = next-1, next-1, first[t]
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
		haveSnap := false
		if x.tier >= TierFile {
			if data, err := s.e.snaps.Get("snap/" + id); err == nil {
				if lsn, snap, err := decodeSnapshot(data); err == nil {
					st, from, haveSnap = snap, lsn, true
				}
			} else if err != blob.ErrNotFound {
				return err
			}
		}
		if haveSnap && from < x.cpLSN {
			// The log says a newer snapshot was stored. Snapshot writes only
			// ever move forward, so this one was put back: refuse to run on
			// state older than the log.
			return fmt.Errorf("run %s: snapshot covers LSN %d but the log checkpointed LSN %d: %w", id, from, x.cpLSN, seal.ErrTampered)
		}
		if x.startLSN == 0 && !haveSnap {
			// Its history is gone: it finished and its records were retired.
			log.Printf("kairo: shard %d: run %s has no start record and no snapshot; treating it as finished", s.id, id)
			continue
		}
		last := from
		for _, le := range x.events {
			if le.lsn <= from {
				continue
			}
			last = le.lsn
			core.Apply(p, st, le.ev, nil)
		}
		if st.Status.Done() {
			if x.tier >= TierFile {
				s.e.snaps.Delete("snap/" + id)
			}
			continue
		}
		r := &run{id: id, plan: p, tenant: x.meta.Tenant, tier: x.tier, st: st, timers: map[uint32]timerwheel.Handle{},
			lastLSN: last, startLSN: x.startLSN, status: st.Status}
		if x.cpRec > 0 {
			r.cpLSN, r.cpRec = x.cpLSN, x.cpRec
			for _, le := range x.events {
				if le.lsn > x.cpLSN {
					r.postSnap = le.lsn
					break
				}
			}
		}
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
