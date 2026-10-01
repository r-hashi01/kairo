#!/usr/bin/env bash
# Measures the non-functional budgets of the requirements and fails if one
# is exceeded. Numbers depend on the machine; budgets are the targets of
# the requirements (32-core server), so a laptop passing them has headroom.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
check() { # name value budget unit
  if awk -v v="$2" -v b="$3" 'BEGIN{exit !(v<=b)}'; then
    printf '  ok    %-40s %10s %s  (budget %s)\n' "$1" "$2" "$4" "$3"
  else
    printf '  OVER  %-40s %10s %s  (budget %s)\n' "$1" "$2" "$4" "$3"; fail=1
  fi
}
metric() { # output metric-name -> value
  awk -v m="$2" '{for(i=1;i<NF;i++) if($(i+1)==m){print $i; exit}}' <<<"$1"
}

echo "== budgets"
out=$(go test ./core -run '^$' -bench '^BenchmarkTransition$' -benchtime 50000x)
check "transition cost" "$(metric "$out" ns/transition)" 20000 ns

out=$(go test ./engine -run '^$' -bench '^BenchmarkFiveNodeRun/none$' -benchtime 20000x)
check "runtime CPU per 5-node run" "$(metric "$out" cpu-µs/run)" 1000 µs
echo "  info  throughput: $(metric "$out" runs/s) runs/s"

out=$(go test -count=1 ./engine -run '^TestHandoffLatency$' -v)
p99=$(grep -o 'p99=[^ ]*' <<<"$out" | head -1 | cut -d= -f2)
us=$(awk -v d="$p99" 'BEGIN{ if (d ~ /ms$/) {sub(/ms$/,"",d); print d*1000} else if (d ~ /µs$/) {sub(/µs$/,"",d); print d} else {sub(/s$/,"",d); print d*1e6} }')
check "node hand-off latency p99" "$us" 1000 µs

out=$(go test -count=1 ./engine -run '^TestWaitingRunMemory$' -v)
mem=$(grep -o 'heap per waiting run: [0-9]*' <<<"$out" | head -1 | awk '{print $NF}')
check "memory per waiting run (in memory)" "$mem" 20480 bytes

out=$(go test -count=1 ./wal -run '^TestFileCommitLatency$' -v)
p99=$(grep -o 'p99=[^ ]*' <<<"$out" | head -1 | cut -d= -f2)
us=$(awk -v d="$p99" 'BEGIN{ if (d ~ /ms$/) {sub(/ms$/,"",d); print d*1000} else if (d ~ /µs$/) {sub(/µs$/,"",d); print d} else {sub(/s$/,"",d); print d*1e6} }')
if [ "$(uname)" = Darwin ]; then
  # macOS fsync is F_FULLFSYNC (flushes the drive cache): not comparable.
  echo "  info  durable ack p99 (fsync, macOS F_FULLFSYNC): ${us} µs (budget 5000 on Linux)"
else
  check "durable ack p99 (group commit, fsync)" "$us" 5000 µs
fi

exit $fail
