//go:build !(darwin || freebsd || netbsd || openbsd || dragonfly)

package engine

// machineMemory is unknown here (Linux reads /proc/meminfo).
func machineMemory() int64 { return 0 }
