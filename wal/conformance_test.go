package wal_test

import (
	"testing"

	"kairo/wal"
	"kairo/wal/waltest"
)

func TestFileSinkConformance(t *testing.T) {
	waltest.Run(t, func(t testing.TB, dir string) wal.Sink {
		s, err := wal.OpenFile(dir, "shard-000", true)
		if err != nil {
			t.Fatal(err)
		}
		s.SegmentSize = 256 // rotate often so Retire has segments to drop
		return s
	}, waltest.Options{Durable: true})
}

func TestMemSinkConformance(t *testing.T) {
	waltest.Run(t, func(testing.TB, string) wal.Sink { return &wal.MemSink{} }, waltest.Options{})
}
