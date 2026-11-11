package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"unicode/utf8"

	"github.com/r-hashi01/kairo/core"
	"github.com/r-hashi01/kairo/ir"
)

// ErrSignalConflict means one logical signal ID was reused with different
// content or delivery semantics.
var ErrSignalConflict = errors.New("engine: logical signal id conflicts with its recorded outcome")

type signalMeta struct {
	ID            string `json:"id"`
	Early         bool   `json:"early"`
	OnlyIfWaiting bool   `json:"only_if_waiting"`
}

type signalOutcome struct {
	Payload json.RawMessage `json:"payload"`
	Signal  *signalMeta     `json:"_kairo_signal"`
}

type signalDecision struct {
	applied   bool
	duplicate bool
	early     bool
}

func validateSignalRequest(req *SignalRequest) error {
	if req == nil || req.Name == "" {
		return errors.New("engine: signal name is required")
	}
	if req.ID == "" || len(req.ID) > 256 || !utf8.ValidString(req.ID) {
		return errors.New("engine: signal_id must be non-empty UTF-8 and at most 256 bytes")
	}
	if len(req.Payload) == 0 || !json.Valid(req.Payload) {
		return errors.New("engine: signal payload must be JSON")
	}
	return nil
}

func validateInitialSignal(p *ir.Plan, parent string, req *SignalRequest) error {
	if err := validateSignalRequest(req); err != nil {
		return err
	}
	if parent == "" || len(p.Nodes) == 0 || p.Nodes[0].Kind != ir.KWait || p.Nodes[0].Signal != req.Name || p.Name != "kairo.wait/"+req.Name {
		return errors.New("engine: initial_signal is only valid for a parented SDK wait plan")
	}
	if req.OnlyIfWaiting {
		return errors.New("engine: only-if-waiting cannot create an initial signal")
	}
	return nil
}

func encodeSignalMeta(req *SignalRequest, early bool) json.RawMessage {
	b, _ := json.Marshal(signalMeta{ID: req.ID, Early: early, OnlyIfWaiting: req.OnlyIfWaiting})
	return b
}

// ApplySignal applies one logical signal to one SDK-owned wait and waits for
// its durable decision. A duplicate returns the recorded decision (ADR 0071).
func (e *Engine) ApplySignal(ctx context.Context, runID string, req SignalRequest) (SignalResult, error) {
	if err := validateSignalRequest(&req); err != nil {
		return SignalResult{}, err
	}
	appliedReply := make(chan signalDecision, 1)
	errReply := make(chan error, 1)
	ev := core.Event{Kind: core.EvSignal, Name: req.Name, Data: req.Payload, Meta: encodeSignalMeta(&req, false)}
	e.shardFor(runID).inbox.Push(msg{kind: mApplySignal, runID: runID, ev: ev, appliedReply: appliedReply, errReply: errReply})

	var decision signalDecision
	select {
	case decision = <-appliedReply:
	case <-ctx.Done():
		return SignalResult{}, ctx.Err()
	}
	select {
	case err := <-errReply:
		if err != nil {
			return SignalResult{}, err
		}
	case <-ctx.Done():
		return SignalResult{}, ctx.Err()
	}
	if decision.applied {
		return SignalResult{Applied: true, Duplicate: decision.duplicate, Early: decision.early}, nil
	}

	ri, err := e.Get(ctx, runID)
	if errors.Is(err, ErrUnknownRun) {
		return SignalResult{}, nil
	}
	if err != nil {
		return SignalResult{}, err
	}
	match, conflict, early := matchSignalOutcome(ri.Output, req.Payload, signalMeta{ID: req.ID, OnlyIfWaiting: req.OnlyIfWaiting})
	if conflict {
		return SignalResult{}, fmt.Errorf("%w: %s", ErrSignalConflict, req.ID)
	}
	if !match {
		return SignalResult{}, nil
	}
	return SignalResult{Applied: true, Duplicate: true, Early: early}, nil
}

func matchSignalEvent(output json.RawMessage, ev core.Event) (match, conflict, early bool) {
	var meta signalMeta
	if json.Unmarshal(ev.Meta, &meta) != nil || meta.ID == "" {
		return false, false, false
	}
	return matchSignalOutcome(output, ev.Data, meta)
}

func matchSignalOutcome(output, payload json.RawMessage, meta signalMeta) (match, conflict, early bool) {
	var out signalOutcome
	if len(output) == 0 || json.Unmarshal(output, &out) != nil || out.Signal == nil || out.Signal.ID != meta.ID {
		return false, false, false
	}
	if out.Signal.OnlyIfWaiting != meta.OnlyIfWaiting || !sameJSON(out.Payload, payload) {
		return false, true, false
	}
	return true, false, out.Signal.Early
}

func sameJSON(a, b []byte) bool {
	if bytes.Equal(bytes.TrimSpace(a), bytes.TrimSpace(b)) {
		return true
	}
	decode := func(raw []byte) (any, error) {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var v any
		err := d.Decode(&v)
		return v, err
	}
	av, err := decode(a)
	if err != nil {
		return false
	}
	bv, err := decode(b)
	return err == nil && reflect.DeepEqual(av, bv)
}
