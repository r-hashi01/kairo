package metrics

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestStreamEmitsCounterAndHistogram(t *testing.T) {
	var mu sync.Mutex
	var got []Measurement
	s := NewStream(SinkFunc(func(batch []Measurement) error {
		mu.Lock()
		got = append(got, batch...)
		mu.Unlock()
		return nil
	}), 4, 4)
	s.Emit(Measurement{Name: RunStartRetries, Kind: Counter, Unit: "{event}", Value: 1, Labels: Labels{Cause: "unconfirmed"}})
	s.Emit(Measurement{Name: RunStartConfirmation, Kind: Histogram, Unit: "s", Value: .25, Labels: Labels{Outcome: "confirmed"}})
	s.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0].Name != RunStartRetries || got[1].Value != .25 {
		t.Fatalf("measurements = %#v", got)
	}
}

func TestStreamCountsSinkFailures(t *testing.T) {
	s := NewStream(SinkFunc(func([]Measurement) error { return errors.New("down") }), 1, 1)
	s.Emit(Measurement{Name: Drain, Kind: Counter, Value: 1})
	s.Close()
	if got := s.Failed(); got != 1 {
		t.Fatalf("failed = %d, want 1", got)
	}
}

func TestNilStreamCostsNoWork(t *testing.T) {
	var s *Stream
	if allocs := testing.AllocsPerRun(1000, func() { s.Emit(Measurement{Name: RunBlocked}) }); allocs != 0 {
		t.Fatalf("disabled stream allocations = %g", allocs)
	}
	if s.Dropped() != 0 || s.Failed() != 0 {
		t.Fatal("nil stream reported work")
	}
	s.Close()
}

func TestLabelsContainOnlyBoundedDimensions(t *testing.T) {
	want := map[string]bool{"Cause": true, "Outcome": true, "Path": true, "Shard": true, "Tier": true, "Reason": true, "Outstanding": true}
	typ := reflect.TypeFor[Labels]()
	for i := range typ.NumField() {
		delete(want, typ.Field(i).Name)
	}
	if len(want) != 0 || typ.NumField() != 7 {
		t.Fatalf("Labels fields changed; review cardinality: %v (%d fields)", want, typ.NumField())
	}
}
