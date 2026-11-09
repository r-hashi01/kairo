#!/usr/bin/env bash
# Runs many Go, Python and TypeScript suspend drivers against one durable
# kairod. This is an opt-in qualification, not a timing assertion in CI.
set -euo pipefail
cd "$(dirname "$0")/.."

processes=${KAIRO_QUALIFY_PROCESSES:-2}
runs=${KAIRO_QUALIFY_RUNS:-10}
inflight=${KAIRO_QUALIFY_INFLIGHT:-25}
timeout_seconds=${KAIRO_QUALIFY_TIMEOUT_SECONDS:-1800}
settle_seconds=${KAIRO_QUALIFY_SETTLE_SECONDS:-5}
case "$processes:$runs:$inflight:$timeout_seconds:$settle_seconds" in
  *[!0-9:]*|0:*|*:0:*|*:*:0:*|*:*:*:0:*) echo "qualification counts and timeout must be positive integers; KAIRO_QUALIFY_SETTLE_SECONDS must be non-negative" >&2; exit 2 ;;
esac
for command in go python3 node curl; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

work=$(mktemp -d /tmp/kairo-qualify.XXXXXX)
out=${KAIRO_QUALIFY_OUT:-$work/results}
mkdir -p "$out" "$work/markers" "$work/data" "$work/barrier"
sock="$work/worker.sock"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
url="http://127.0.0.1:$port"
kairod_pid=
sampler_pid=

cleanup() {
  if [ -n "$sampler_pid" ]; then kill "$sampler_pid" 2>/dev/null || true; fi
  if [ -n "$kairod_pid" ]; then
    kill -TERM "$kairod_pid" 2>/dev/null || true
    wait "$kairod_pid" 2>/dev/null || true
  fi
  if [ -z "${KAIRO_QUALIFY_OUT:-}" ]; then rm -rf "$work"; fi
}
trap cleanup EXIT INT TERM

go build -o "$work/kairod" ./cmd/kairod
go build -o "$work/go-driver" ./scripts/qualification/go-driver
"$work/kairod" -data "$work/data" -http "127.0.0.1:$port" -socket "$sock" -tier file -obs "$out/kairod-obs.jsonl" >"$out/kairod.log" 2>&1 &
kairod_pid=$!

ready=0
for _ in $(seq 1 200); do
  if curl -fsS "$url/v1/stats" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$kairod_pid" 2>/dev/null; then break; fi
  sleep 0.05
done
if [ "$ready" != 1 ]; then
  echo "kairod did not start" >&2
  tail -100 "$out/kairod.log" >&2
  exit 1
fi

(
  while kill -0 "$kairod_pid" 2>/dev/null; do
    sampled_at=$(date +%s)
    printf '%s ' "$sampled_at" >>"$out/kairod-rss-kib.txt"
    ps -o rss= -p "$kairod_pid" | tr -d ' ' >>"$out/kairod-rss-kib.txt"
    if stats=$(curl -fsS "$url/v1/stats" 2>/dev/null); then
      printf '{"sampled_at":%s,"stats":%s}\n' "$sampled_at" "$stats" >>"$out/kairod-stats.jsonl"
    fi
    sleep 1
  done
) &
sampler_pid=$!

started=$(date +%s)
pids=()
labels=()
for i in $(seq 1 "$processes"); do
  prefix="p$i"
  KAIRO_QUALIFY_URL="$url" KAIRO_QUALIFY_SOCKET="$sock" KAIRO_QUALIFY_MARKERS="$work/markers" KAIRO_QUALIFY_PREFIX="$prefix" KAIRO_QUALIFY_RUNS="$runs" KAIRO_QUALIFY_INFLIGHT="$inflight" KAIRO_QUALIFY_TIMEOUT_SECONDS="$timeout_seconds" KAIRO_QUALIFY_BARRIER="$work/barrier" KAIRO_QUALIFY_DRIVER="go-$prefix" \
    "$work/go-driver" >"$out/go-$prefix.log" 2>&1 &
  pids+=("$!"); labels+=("go-$prefix")
  PYTHONPATH="$(pwd)/sdk/python${PYTHONPATH:+:$PYTHONPATH}" KAIRO_QUALIFY_URL="$url" KAIRO_QUALIFY_SOCKET="$sock" KAIRO_QUALIFY_MARKERS="$work/markers" KAIRO_QUALIFY_PREFIX="$prefix" KAIRO_QUALIFY_RUNS="$runs" KAIRO_QUALIFY_INFLIGHT="$inflight" KAIRO_QUALIFY_TIMEOUT_SECONDS="$timeout_seconds" KAIRO_QUALIFY_BARRIER="$work/barrier" KAIRO_QUALIFY_DRIVER="python-$prefix" \
    python3 scripts/qualification/python-driver.py >"$out/python-$prefix.log" 2>&1 &
  pids+=("$!"); labels+=("python-$prefix")
  KAIRO_QUALIFY_URL="$url" KAIRO_QUALIFY_SOCKET="$sock" KAIRO_QUALIFY_MARKERS="$work/markers" KAIRO_QUALIFY_PREFIX="$prefix" KAIRO_QUALIFY_RUNS="$runs" KAIRO_QUALIFY_INFLIGHT="$inflight" KAIRO_QUALIFY_TIMEOUT_SECONDS="$timeout_seconds" KAIRO_QUALIFY_BARRIER="$work/barrier" KAIRO_QUALIFY_DRIVER="typescript-$prefix" \
    node scripts/qualification/ts-driver.ts >"$out/typescript-$prefix.log" 2>&1 &
  pids+=("$!"); labels+=("typescript-$prefix")
done

failed=0
participants=$((processes * 3))
barrier_deadline=$((started + timeout_seconds + 30))
while true; do
  ready=$(find "$work/barrier" -type f -name '*.done' | wc -l | tr -d ' ')
  if [ "$ready" = "$participants" ]; then break; fi
  for pid in "${pids[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then failed=1; break 2; fi
  done
  if [ "$(date +%s)" -ge "$barrier_deadline" ]; then
    echo "qualification barrier timed out: $ready/$participants drivers ready" >&2
    failed=1
    break
  fi
  sleep 0.05
done
: >"$work/barrier/release"
for i in "${!pids[@]}"; do
  if ! wait "${pids[$i]}"; then
    echo "${labels[$i]} failed; see $out/${labels[$i]}.log" >&2
    failed=1
  fi
done
finished=$(date +%s)
expected=$((processes * runs * 3))
actual=$(find "$work/markers" -type f -name '*.done' | wc -l | tr -d ' ')
if [ "$actual" != "$expected" ]; then
  echo "real-action markers: got $actual, want $expected" >&2
  failed=1
fi
if ! kill -0 "$kairod_pid" 2>/dev/null; then
  echo "kairod exited during qualification" >&2
  failed=1
fi

rss_at_finish=$(awk 'NF==2 {v=$2} END {print v}' "$out/kairod-rss-kib.txt")
if [ "$settle_seconds" -gt 0 ]; then sleep "$settle_seconds"; fi
rss_first=$(awk 'NF==2 {print $2; exit}' "$out/kairod-rss-kib.txt")
rss_last=$(awk 'NF==2 {v=$2} END {print v}' "$out/kairod-rss-kib.txt")
rss_max=$(awk 'NF==2 && $2>m {m=$2} END {print m}' "$out/kairod-rss-kib.txt")
{
  echo "machine: $(uname -sm), $(go env GOVERSION), node $(node --version), $(python3 --version 2>&1)"
  echo "processes_per_sdk: $processes"
  echo "runs_per_process: $runs"
  echo "inflight_per_process: $inflight"
  echo "timeout_seconds: $timeout_seconds"
  echo "expected_workflows: $expected"
  echo "real_action_markers: $actual"
  echo "elapsed_seconds: $((finished - started))"
  echo "settle_seconds: $settle_seconds"
  echo "kairod_rss_kib_first: ${rss_first:-unknown}"
  echo "kairod_rss_kib_at_finish: ${rss_at_finish:-unknown}"
  echo "kairod_rss_kib_last: ${rss_last:-unknown}"
  echo "kairod_rss_kib_max: ${rss_max:-unknown}"
  echo "data_sync: enabled"
} | tee "$out/summary.txt"
echo "results: $out"
exit "$failed"
