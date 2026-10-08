package engine

import (
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/r-hashi01/kairo/ir"
)

// The budgets of in-process executors (ADR 0039): how many of their tasks
// run at once, engine-wide, when the caller does not say. CPU tasks: one
// per CPU. IO tasks (waiting for the outside, each holding a connection):
// half the open file limit, and a quarter of the memory at ioTaskBytes per
// task, whichever is lower.
const ioTaskBytes = 64 << 10

func resourceOf(reg *ir.Registry, actions []string) string {
	for _, a := range actions {
		if s := reg.Lookup(a); s != nil && s.Resource == ir.ResourceCPU {
			return ir.ResourceCPU
		}
	}
	return ir.ResourceIO
}

func budget(resource string) int {
	cpus := runtime.GOMAXPROCS(0)
	if resource == ir.ResourceCPU {
		return cpus
	}
	byFiles := fileLimit() / 2
	byMemory := int(min(memoryLimit()/4/ioTaskBytes, 1<<30))
	return max(min(byFiles, byMemory), cpus)
}

// memoryLimit is the memory the process may use: its cgroup's limit, else
// the machine's (4 GiB if neither is known).
func memoryLimit() int64 {
	for _, path := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		if b, err := os.ReadFile(path); err == nil {
			if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && v > 0 && v < 1<<60 {
				return v
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if f := strings.Fields(line); len(f) >= 2 && f[0] == "MemTotal:" {
				if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil {
					return kb << 10
				}
			}
		}
	}
	if m := machineMemory(); m > 0 {
		return m
	}
	return 4 << 30
}
