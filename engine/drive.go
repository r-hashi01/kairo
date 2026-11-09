package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"time"
)

// DriveCandidate is an unfinished code workflow and its ephemeral driver
// state. Input is intentionally fetched separately by GetInput.
type DriveCandidate struct {
	Run   RunInfo `json:"run"`
	Ready bool    `json:"ready"`
	Until int64   `json:"until,omitempty"`
}

// DriveClaim fences one process's attempt to replay a code workflow.
type DriveClaim struct {
	Run   RunInfo `json:"run"`
	Token string  `json:"token"`
	Until int64   `json:"until"`
}

type driveReply struct {
	ok    bool
	until int64
}

func (e *Engine) notifyDrive() {
	e.driveMu.Lock()
	close(e.driveChanged)
	e.driveChanged = make(chan struct{})
	e.driveMu.Unlock()
}

// DriveChanged returns a generation channel closed when driver eligibility
// may have changed. Take it before reading DriveCandidates to avoid a lost
// wake-up.
func (e *Engine) DriveChanged() <-chan struct{} {
	e.driveMu.Lock()
	ch := e.driveChanged
	e.driveMu.Unlock()
	return ch
}

// DriveCandidates returns all active code workflows. Ready candidates may be
// claimed now; Until is a current claim's lease deadline.
func (e *Engine) DriveCandidates(ctx context.Context) ([]DriveCandidate, error) {
	replies := make([]chan []DriveCandidate, len(e.shards))
	for i, s := range e.shards {
		replies[i] = make(chan []DriveCandidate, 1)
		s.inbox.Push(msg{kind: mDriveSnapshot, driveList: replies[i]})
	}
	var out []DriveCandidate
	for _, reply := range replies {
		select {
		case rows := <-reply:
			out = append(out, rows...)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Run.CreatedAt != out[j].Run.CreatedAt {
			return out[i].Run.CreatedAt < out[j].Run.CreatedAt
		}
		return out[i].Run.RunID < out[j].Run.RunID
	})
	return out, nil
}

func driveToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ClaimWorkflow atomically claims a ready code workflow.
func (e *Engine) ClaimWorkflow(ctx context.Context, runID, owner string, lease time.Duration) (DriveClaim, bool, error) {
	if owner == "" || lease <= 0 {
		return DriveClaim{}, false, errors.New("engine: drive owner and positive lease are required")
	}
	token := driveToken()
	until := e.cfg.Now().Add(lease).UnixMilli()
	reply := make(chan driveReply, 1)
	e.shardFor(runID).inbox.Push(msg{kind: mDriveClaim, runID: runID, driveToken: token, driveOwner: owner, driveUntil: until, driveReply: reply})
	select {
	case r := <-reply:
		if !r.ok {
			return DriveClaim{}, false, nil
		}
		ri, err := e.GetInput(ctx, runID)
		if err != nil {
			return DriveClaim{}, false, err
		}
		return DriveClaim{Run: ri, Token: token, Until: r.until}, true, nil
	case <-ctx.Done():
		return DriveClaim{}, false, ctx.Err()
	}
}

// RenewWorkflow extends an unexpired claim held by owner and token.
func (e *Engine) RenewWorkflow(ctx context.Context, runID, owner, token string, lease time.Duration) (int64, bool, error) {
	until := e.cfg.Now().Add(lease).UnixMilli()
	reply := make(chan driveReply, 1)
	e.shardFor(runID).inbox.Push(msg{kind: mDriveRenew, runID: runID, driveToken: token, driveOwner: owner, driveUntil: until, driveReply: reply})
	select {
	case r := <-reply:
		return r.until, r.ok, nil
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
}

// ReleaseWorkflow ends a claim. retry makes the workflow ready again.
func (e *Engine) ReleaseWorkflow(ctx context.Context, runID, owner, token string, retry bool) (bool, error) {
	reply := make(chan driveReply, 1)
	e.shardFor(runID).inbox.Push(msg{kind: mDriveRelease, runID: runID, driveToken: token, driveOwner: owner, driveRetry: retry, driveReply: reply})
	select {
	case r := <-reply:
		return r.ok, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// ReadyWorkflow makes an unfinished code workflow eligible for replay.
func (e *Engine) ReadyWorkflow(runID string) {
	e.shardFor(runID).inbox.Push(msg{kind: mDriveReady, runID: runID})
}

// WaitQuiescent waits until a run is terminal or has no executor work in
// flight. It does not poll and reports its earliest timer deadline.
func (e *Engine) WaitQuiescent(ctx context.Context, runID string) (RunInfo, error) {
	if ri, err := e.Get(ctx, runID); err != nil || ri.FinishedAt != (time.Time{}) || ri.Trimmed {
		return ri, err
	}
	id := e.quietID.Add(1)
	reply := make(chan queryReply, 1)
	s := e.shardFor(runID)
	s.inbox.Push(msg{kind: mQueryQuiescent, runID: runID, quietID: id, reply: reply})
	select {
	case r := <-reply:
		if !r.found {
			return e.Get(ctx, runID)
		}
		return r.info, nil
	case <-ctx.Done():
		s.inbox.Push(msg{kind: mCancelQuiescent, runID: runID, quietID: id})
		return RunInfo{}, ctx.Err()
	}
}
