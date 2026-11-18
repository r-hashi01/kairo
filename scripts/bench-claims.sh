#!/usr/bin/env bash
# Measures process-level workflow claim scans (GitHub Issue #46). Run on an
# otherwise idle machine and compare only results from the same machine.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$(uname)" = Darwin ]; then
  cpu=$(sysctl -n machdep.cpu.brand_string); cores=$(sysctl -n hw.ncpu)
else
  cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//'); cores=$(nproc)
fi
echo "== machine: $cpu, $cores CPUs, $(go env GOVERSION), load $(uptime | sed 's/.*load averages*: //')"
go test ./api -run '^$' -bench '^BenchmarkProcessWorkflowClaimScan$' -benchmem -benchtime=2s -count=3
