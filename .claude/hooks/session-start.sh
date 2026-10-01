#!/usr/bin/env bash
# SessionStart: a short orientation added to Claude's context.
set -uo pipefail
root=${CLAUDE_PROJECT_DIR:-$(pwd)}
cd "$root" || exit 0
changed=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
branch=$(git branch --show-current 2>/dev/null)
hash=$(find . -path ./.git -prune -o \( -name '*.go' -o -name go.mod -o -name go.sum \) -type f -print | LC_ALL=C sort | xargs cat 2>/dev/null | shasum | cut -d' ' -f1)
green=unknown
[ -f .claude/.cache/last-green ] && { [ "$(cat .claude/.cache/last-green)" = "$hash" ] && green=yes || green="no (sources changed since)"; }
cat <<MSG
kairo session: branch=${branch:-none}, uncommitted paths=$changed, last quick check green for this tree: $green ($(go version | awk '{print $3}')).
Read AGENTS.md before changing code. Verify with scripts/check.sh (--race before finishing larger changes); performance budgets with scripts/bench.sh.
MSG
