// Package sched allocates scarce resources. The scarce resource in an LLM
// workflow runtime is not CPU but external quota: requests and tokens per
// minute at each provider, shared fairly between tenants.
package sched

import (
	"errors"
	"sync"
)

var ErrOverloaded = errors.New("sched: overloaded")

// AdmissionConfig bounds how many runs are active at once.
type AdmissionConfig struct {
	MaxActive int // across all tenants; 0 = unlimited
	// MaxActivePerTenant returns the per-tenant cap (0 = unlimited).
	MaxActivePerTenant func(tenant string) int
	MaxQueued          int // runs waiting for admission; beyond this Submit is rejected
}

// Admission is admission control with round-robin fairness between tenants
// for the runs that have to wait. Its methods do no I/O; the critical
// sections are a few map operations.
type Admission struct {
	cfg       AdmissionConfig
	mu        sync.Mutex
	active    int
	perTenant map[string]int
	queues    map[string][]func()
	ring      []string
	rr        int
	queued    int
}

func NewAdmission(cfg AdmissionConfig) *Admission {
	return &Admission{cfg: cfg, perTenant: map[string]int{}, queues: map[string][]func(){}}
}

func (a *Admission) tenantCap(t string) int {
	if a.cfg.MaxActivePerTenant == nil {
		return 0
	}
	return a.cfg.MaxActivePerTenant(t)
}

func (a *Admission) canStart(t string) bool {
	if a.cfg.MaxActive > 0 && a.active >= a.cfg.MaxActive {
		return false
	}
	if c := a.tenantCap(t); c > 0 && a.perTenant[t] >= c {
		return false
	}
	return true
}

// Admit runs start now if capacity allows, queues it otherwise, or rejects
// with ErrOverloaded when the queue is full. start is called without any
// lock held.
func (a *Admission) Admit(tenant string, start func()) (queued bool, err error) {
	a.mu.Lock()
	if len(a.queues[tenant]) == 0 && a.canStart(tenant) {
		a.active++
		a.perTenant[tenant]++
		a.mu.Unlock()
		start()
		return false, nil
	}
	if a.cfg.MaxQueued > 0 && a.queued >= a.cfg.MaxQueued {
		a.mu.Unlock()
		return false, ErrOverloaded
	}
	if len(a.queues[tenant]) == 0 {
		a.ring = append(a.ring, tenant)
	}
	a.queues[tenant] = append(a.queues[tenant], start)
	a.queued++
	a.mu.Unlock()
	return true, nil
}

// Readmit counts a run that is already active (recovered after restart)
// without queueing it.
func (a *Admission) Readmit(tenant string) {
	a.mu.Lock()
	a.active++
	a.perTenant[tenant]++
	a.mu.Unlock()
}

// Release is called when a run finishes. It starts queued runs, visiting
// tenants round-robin.
func (a *Admission) Release(tenant string) {
	var starts []func()
	a.mu.Lock()
	a.active--
	if a.perTenant[tenant]--; a.perTenant[tenant] <= 0 {
		delete(a.perTenant, tenant)
	}
	for len(a.ring) > 0 {
		progressed := false
		for i := 0; i < len(a.ring); i++ {
			idx := (a.rr + i) % len(a.ring)
			t := a.ring[idx]
			if !a.canStart(t) {
				continue
			}
			q := a.queues[t]
			starts = append(starts, q[0])
			a.queued--
			a.active++
			a.perTenant[t]++
			if len(q) == 1 {
				delete(a.queues, t)
				a.ring = append(a.ring[:idx], a.ring[idx+1:]...)
				if len(a.ring) > 0 {
					a.rr = idx % len(a.ring)
				} else {
					a.rr = 0
				}
			} else {
				a.queues[t] = q[1:]
				a.rr = (idx + 1) % len(a.ring)
			}
			progressed = true
			break
		}
		if !progressed {
			break
		}
	}
	a.mu.Unlock()
	for _, s := range starts {
		s()
	}
}

func (a *Admission) Stats() (active, queued int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.active, a.queued
}
