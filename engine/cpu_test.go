package engine

import (
	"syscall"
	"time"
)

// cpuTime is the process's user+system CPU time.
func cpuTime() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
