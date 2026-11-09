#!/usr/bin/env bash
# Reproducible process-level fault injection for kairod. More scenarios can
# be added under scripts/qualification without making long tests part of CI.
set -euo pipefail
cd "$(dirname "$0")/.."

work=$(mktemp -d /tmp/kairo-fault.XXXXXX)
out=${KAIRO_FAULT_OUT:-$work/results}
mkdir -p "$out"
cleanup() {
  if [ -z "${KAIRO_FAULT_OUT:-}" ]; then rm -rf "$work"; fi
}
trap cleanup EXIT INT TERM

go build -o "$work/kairod" ./cmd/kairod
go build -o "$work/restart-driver" ./scripts/qualification/restart-driver
started=$(date +%s)
KAIRO_FAULT_KAIROD="$work/kairod" KAIRO_FAULT_WORK="$out" "$work/restart-driver" | tee "$out/summary.txt"
{
  echo "machine: $(uname -sm), $(go env GOVERSION)"
  echo "elapsed_seconds: $(($(date +%s) - started))"
  echo "data_sync: enabled"
} | tee -a "$out/summary.txt"
echo "results: $out"
