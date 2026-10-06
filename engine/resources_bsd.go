//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package engine

import (
	"encoding/binary"
	"syscall"
)

// machineMemory is the physical memory, from sysctl.
func machineMemory() int64 {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		s, err = syscall.Sysctl("hw.physmem")
	}
	if err != nil || len(s) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64([]byte(s[:8])))
}
