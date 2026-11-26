package kairo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ErrDiverged means a workflow's replay left its record: it was about to
// make a call with an effect while a recorded call with an effect that it
// would have reached first was never reached (ADR 0084). Nothing was made;
// the workflow is not failed, and goes on once code that replays as it
// first ran drives it.
var ErrDiverged = errors.New("kairo: the workflow's replay left its record")

// DivergedError says where a replay left its record (ADR 0084).
type DivergedError struct {
	Workflow  string   // the workflow's run id
	New       string   // the call it was about to make
	Unreached []string // recorded calls it did not reach first
}

func (e *DivergedError) Error() string {
	return fmt.Sprintf("kairo: workflow %s left its record: it was about to make %s, but has not reached %s (ADR 0084)",
		e.Workflow, e.New, strings.Join(e.Unreached, ", "))
}

func (e *DivergedError) Is(target error) bool { return target == ErrDiverged }

// divergence checks one drive of a workflow (ADR 0084). Its record, the
// calls with an effect that the workflow made before, is read once, when
// the drive first makes a call with an effect that is not recorded. Before
// such a call is made, the calls with an effect recorded in its branch of
// Parallel (its lane) or in the branches under it must all have been
// reached: within a branch, calls are made one after another.
type divergence struct {
	mu       sync.Mutex
	reached  map[string]bool
	recorded map[string]string // run id -> lane, once read
	err      error
}

func newDivergence() *divergence { return &divergence{reached: map[string]bool{}} }

// reach notes that the drive made call id (found or new).
func (d *divergence) reach(id string) {
	d.mu.Lock()
	d.reached[id] = true
	d.mu.Unlock()
}

// diverged is the drive's divergence, once found: every later call, and
// the drive's end, report it, even if the workflow caught it.
func (d *divergence) diverged() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

// check is called before c makes call id, which has an effect. It returns
// an ErrDiverged if the call is new and something recorded in its lane
// was not reached first.
func (d *divergence) check(ctx context.Context, c *Context, id string) error {
	d.mu.Lock()
	err, loaded := d.err, d.recorded != nil
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if !loaded {
		kids, err := c.k.Children(ctx, c.id)
		if err != nil {
			return err
		}
		recorded := map[string]string{}
		for _, r := range kids {
			if lane, ok := callLane(c.id, r.RunID); ok && c.k.hasEffect(r) {
				recorded[r.RunID] = lane
			}
		}
		d.mu.Lock()
		if d.recorded == nil {
			d.recorded = recorded
		}
		d.mu.Unlock()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	if _, ok := d.recorded[id]; ok {
		return nil // recorded: reached now
	}
	var unreached []string
	for rid, lane := range d.recorded {
		if !d.reached[rid] && underLane(lane, c.lane) {
			unreached = append(unreached, rid)
		}
	}
	if len(unreached) == 0 {
		d.recorded[id] = c.lane // made by this drive from here on
		return nil
	}
	sort.Strings(unreached)
	d.err = &DivergedError{Workflow: c.id, New: id, Unreached: unreached}
	return d.err
}

// callLane is the lane of call id that workflow made, from the id's form
// (<workflow>/<24 hex>.<n> or <workflow>/<24 hex>.<lane>.<n>); ok is false
// for a run not made by the workflow's calls (one attached from outside).
func callLane(workflow, id string) (string, bool) {
	rest, ok := strings.CutPrefix(id, workflow+"/")
	if !ok || len(rest) < 26 || rest[24] != '.' {
		return "", false
	}
	for _, ch := range rest[:24] {
		if !strings.ContainsRune("0123456789abcdef", ch) {
			return "", false
		}
	}
	tail := rest[25:]
	i := strings.LastIndexByte(tail, '.')
	if i < 0 {
		return "", true
	}
	return tail[:i], true
}

// underLane says whether lane is lane parent or a branch under it.
func underLane(lane, parent string) bool {
	return parent == "" || lane == parent || strings.HasPrefix(lane, parent+".")
}

// hasEffect says whether recorded run r is a call with an effect: a child
// workflow, or a call of an action that is not unprotected (or not
// declared here). Waits and sleeps are not (ADR 0084, decision 4).
func (k *Kairo) hasEffect(r RunInfo) bool {
	if r.Plan == planWorkflow {
		return !externallyAttached(r.Input)
	}
	action, ok := strings.CutPrefix(r.Plan, planCall)
	if !ok {
		return false
	}
	return k.actionHasEffect(action)
}

// actionHasEffect: an action not unprotected, or not declared here.
func (k *Kairo) actionHasEffect(action string) bool {
	switch action {
	case builtinNow, builtinRand, builtinSleep:
		return false
	}
	k.mu.Lock()
	a := k.actions[action]
	k.mu.Unlock()
	return a == nil || a.spec["effect"] != string(Unprotected)
}
