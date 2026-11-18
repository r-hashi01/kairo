package obs

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestStreamCloseFlushesBufferedRecords(t *testing.T) {
	var got atomic.Int64
	s := NewStream(SinkFunc(func(batch []Record) error {
		got.Add(int64(len(batch)))
		return nil
	}), 16, 4)
	for i := 0; i < 10; i++ {
		s.Emit(Record{RunID: "run"})
	}
	s.Close()
	if n := got.Load(); n != 10 {
		t.Fatalf("flushed %d records, want 10", n)
	}
}

func TestStreamCloseIsSafeWithConcurrentEmit(t *testing.T) {
	s := NewStream(SinkFunc(func([]Record) error { return nil }), 64, 8)
	started := make(chan struct{}, 1)
	stop := make(chan struct{})
	var producers sync.WaitGroup
	for i := 0; i < 8; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for {
				s.Emit(Record{RunID: "run"})
				select {
				case started <- struct{}{}:
				default:
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	<-started
	var closers sync.WaitGroup
	for i := 0; i < 16; i++ {
		closers.Add(1)
		go func() {
			defer closers.Done()
			s.Close()
		}()
	}
	closers.Wait()
	close(stop)
	producers.Wait()
	s.Emit(Record{RunID: "after-close"})
	s.Close()
}
