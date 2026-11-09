#!/usr/bin/env bash
# Runs scripts/bench.sh repeatedly in a baseline checkout and this checkout,
# alternating their order. It records raw outputs and reports median/min/max;
# it does not turn timing budgets into test assertions (ADR 0056).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: scripts/bench-compare.sh BASELINE_DIR [OUT_DIR]" >&2
  exit 2
fi
baseline=$(cd "$1" && pwd)
candidate=$(pwd)
out=${2:-/tmp/kairo-bench-compare-$(date +%Y%m%d-%H%M%S)}
repetitions=${KAIRO_BENCH_REPETITIONS:-5}
case "$repetitions" in
  ''|*[!0-9]*|0) echo "KAIRO_BENCH_REPETITIONS must be a positive integer" >&2; exit 2 ;;
esac
for root in "$baseline" "$candidate"; do
  [ -x "$root/scripts/bench.sh" ] || { echo "$root/scripts/bench.sh is missing or not executable" >&2; exit 2; }
done
mkdir -p "$out"
metrics="$out/metrics.tsv"
printf 'side\trepetition\ttransition_ns\tcpu_us_per_run\tthroughput_runs_per_s\thandoff_p99_us\twaiting_bytes\n' >"$metrics"

run_one() {
  side=$1 rep=$2 root=$3
  raw="$out/$side-$(printf '%02d' "$rep").txt"
  (cd "$root" && scripts/bench.sh) >"$raw" 2>&1
  awk -v side="$side" -v rep="$rep" '
    index($0, "transition cost") { for (i=1; i<=NF; i++) if ($i=="ns") transition=$(i-1) }
    index($0, "runtime CPU per 5-node run") { for (i=1; i<=NF; i++) if ($i=="µs") cpu=$(i-1) }
    index($0, "throughput:") { for (i=1; i<=NF; i++) if ($i=="throughput:") throughput=$(i+1) }
    index($0, "node hand-off latency p99") { for (i=1; i<=NF; i++) if ($i=="µs") handoff=$(i-1) }
    index($0, "memory per waiting run") { for (i=1; i<=NF; i++) if ($i=="bytes") waiting=$(i-1) }
    END {
      if (transition=="" || cpu=="" || throughput=="" || handoff=="" || waiting=="") exit 1
      printf "%s\t%d\t%s\t%s\t%s\t%s\t%s\n", side, rep, transition, cpu, throughput, handoff, waiting
    }
  ' "$raw" >>"$metrics"
}

for rep in $(seq 1 "$repetitions"); do
  if [ $((rep % 2)) -eq 1 ]; then
    run_one baseline "$rep" "$baseline"
    run_one candidate "$rep" "$candidate"
  else
    run_one candidate "$rep" "$candidate"
    run_one baseline "$rep" "$baseline"
  fi
done

stats() {
  side=$1 column=$2 name=$3 unit=$4
  awk -F '\t' -v side="$side" -v column="$column" 'NR>1 && $1==side {print $column}' "$metrics" | sort -n | \
    awk -v side="$side" -v name="$name" -v unit="$unit" '
      { value[NR]=$1; sum+=$1 }
      END {
        if (NR%2) median=value[(NR+1)/2]; else median=(value[NR/2]+value[NR/2+1])/2
        printf "%-10s %-28s median=%-12g min=%-12g max=%-12g %s\n", side, name, median, value[1], value[NR], unit
      }
    '
}

{
  echo "baseline: $baseline"
  echo "candidate: $candidate"
  echo "repetitions: $repetitions"
  echo "raw outputs include each machine/load header"
  stats baseline 3 transition ns
  stats candidate 3 transition ns
  stats baseline 4 cpu_per_5_node_run us
  stats candidate 4 cpu_per_5_node_run us
  stats baseline 5 throughput runs/s
  stats candidate 5 throughput runs/s
  stats baseline 6 handoff_p99 us
  stats candidate 6 handoff_p99 us
  stats baseline 7 waiting_memory bytes
  stats candidate 7 waiting_memory bytes
} | tee "$out/summary.txt"
echo "results: $out"
