//go:build unix

package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/r-hashi01/kairo/compat/dify/daemon"
	"github.com/r-hashi01/kairo/engine"
)

// logStatsOnSignal logs the engine's counters and the partitions
// holding entries on SIGUSR1, for looking
// into a daemon that seems stuck without stopping it.
func logStatsOnSignal(e *engine.Engine, d *daemon.Daemon) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			b, _ := json.Marshal(e.Stats())
			log.Printf("kairo-dify: stats %s", b)
			b, _ = json.Marshal(d.FeedState())
			log.Printf("kairo-dify: partitions %s", b)
		}
	}()
}
