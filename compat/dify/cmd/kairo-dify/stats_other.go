//go:build !unix

package main

import (
	"kairo/compat/dify/daemon"
	"kairo/engine"
)

func logStatsOnSignal(*engine.Engine, *daemon.Daemon) {}
