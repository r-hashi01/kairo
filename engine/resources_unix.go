//go:build unix

package engine

import "syscall"

// fileLimit is the process's soft limit on open files.
func fileLimit() int {
	var l syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &l); err != nil || l.Cur == 0 {
		return 1024
	}
	return int(min(l.Cur, 1<<20))
}
