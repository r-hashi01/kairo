#!/usr/bin/env bash
# Full verification: formatting, vet, build, tests, runtime invariants.
#
#   scripts/check.sh           fmt + vet + build + tests
#   scripts/check.sh --race    also run the race detector
#   scripts/check.sh --quick   fmt + vet + build + short tests (used by hooks)
set -euo pipefail
cd "$(dirname "$0")/.."

race=0 quick=0
for a in "$@"; do
  case "$a" in
    --race) race=1 ;;
    --quick) quick=1 ;;
    *) echo "unknown flag $a" >&2; exit 2 ;;
  esac
done

step() { printf '\n== %s\n' "$*"; }

step "gofmt"
unformatted=$(gofmt -l . | grep -v '^vendor/' || true)
if [ -n "$unformatted" ]; then
  echo "not gofmt-ed:"; echo "$unformatted"; exit 1
fi

step "adr-lint"
scripts/adr-lint.sh

step "go vet"
go vet ./...

step "go build"
go build ./...

if [ "$quick" = 1 ]; then
  step "go test -short"
  go test -short ./...
  exit 0
fi

step "go test"
go test -count=1 ./...

# The invariants of the requirements (section 6), run by name so a rename or
# deletion shows up here instead of silently dropping coverage.
step "invariants"
go test -count=1 ./core -run '^(TestCoreIsPure|TestParallelJoinAnyOrder|TestSequence)$' -v 2>&1 | grep -E '^(--- |ok|FAIL)'
go test -count=1 ./engine -run '^(TestNoGoroutinePerRun|TestIdleEngineDoesNotWake|TestRealCommandWaitsForDurableIntent|TestWaitingRunMemory|TestHandoffLatency)$' -v 2>&1 | grep -E '^(--- |ok|FAIL)|heap per|latency'
go test -count=1 ./wal -run '^TestFileSinkAppendOnly$' -v 2>&1 | grep -E '^(--- |ok|FAIL)'

if [ "$race" = 1 ]; then
  step "go test -race"
  go test -race -count=1 ./...
fi

printf '\nall checks passed\n'
