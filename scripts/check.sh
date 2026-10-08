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

# The pure core as a WASM module for SDKs that embed it (ADR 0051).
step "go vet + build (wasip1)"
GOOS=wasip1 GOARCH=wasm go vet ./cmd/kairo-wasm ./wasmcore ./core ./ir
# Built once here; the SDK suites below take it from KAIRO_WASM instead of
# building their own (CI keeps it as an artifact when KAIRO_WASM is set).
if [ -z "${KAIRO_WASM:-}" ]; then
  wasm_dir=$(mktemp -d)
  trap 'rm -rf "$wasm_dir"' EXIT
  KAIRO_WASM="$wasm_dir/kairo.wasm"
fi
export KAIRO_WASM
GOOS=wasip1 GOARCH=wasm go build -trimpath -buildmode=c-shared -o "$KAIRO_WASM" ./cmd/kairo-wasm

# Nested modules (optional backends with their own dependencies, ADR 0018).
modules=$(find . -name go.mod -not -path ./go.mod -exec dirname {} \; | sort)

for m in $modules; do
  step "go vet ($m)"
  (cd "$m" && go vet ./...)
done

if [ "$quick" = 1 ]; then
  step "go test -short"
  go test -short ./...
  for m in $modules; do (cd "$m" && go test -short ./...); done
  exit 0
fi

step "go test"
go test -count=1 ./...
for m in $modules; do
  step "go test ($m)"
  (cd "$m" && go test -count=1 ./...)
done

# The Python worker SDK (sdk/python; its graphon runner needs graphon and
# is covered by compat/dify's end-to-end test when GRAPHON_PYTHON is set).
if command -v python3 >/dev/null; then
  step "python sdk tests"
  # The last line, or what failed when something did.
  (cd sdk/python && { out=$(python3 -m unittest discover -s tests 2>&1) && echo "$out" | tail -1; } || {
    echo "$out" | grep -E -A30 '^(.\[[0-9;]*m)*(FAIL|ERROR)(.\[[0-9;]*m)*:' | head -120; echo "$out" | tail -1; exit 1; })
fi

# The TypeScript SDK (sdk/ts): its suites build kairod and kairo.wasm.
if command -v node >/dev/null; then
  step "typescript sdk tests"
  (cd sdk/ts && node --test --test-reporter=dot --test-timeout=120000 src/*.test.ts)
fi

# The invariants of the requirements (section 6), run by name so a rename or
# deletion shows up here instead of silently dropping coverage.
step "invariants"
go test -count=1 ./core -run '^(TestCoreIsPure|TestParallelJoinAnyOrder|TestSequence)$' -v 2>&1 | grep -E '^(--- |ok|FAIL)'
go test -count=1 ./engine -run '^(TestNoGoroutinePerRun|TestIdleEngineDoesNotWake|TestRealCommandWaitsForDurableIntent|TestWaitingRunMemory)$' -v 2>&1 | grep -E '^(--- |ok|FAIL)|heap per'
go test -count=1 ./wal -run '^TestFileSinkAppendOnly$' -v 2>&1 | grep -E '^(--- |ok|FAIL)'

if [ "$race" = 1 ]; then
  step "go test -race"
  go test -race -count=1 ./...
  for m in $modules; do (cd "$m" && go test -race -count=1 ./...); done
fi

printf '\nall checks passed\n'
