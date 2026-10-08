//go:build !unix

package main

import (
	"github.com/r-hashi01/kairo/compat/dify/daemon"
	"github.com/r-hashi01/kairo/engine"
)

func logStatsOnSignal(*engine.Engine, *daemon.Daemon) {}
