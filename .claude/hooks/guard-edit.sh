#!/usr/bin/env bash
# PreToolUse (Edit|Write): refuse edits that break a runtime invariant before
# they land.
#   - core/ (non-test): no I/O, clock, goroutines or locks in the pure core
#   - log / snapshot data files: append-only data, never edited by hand
#   - go.mod: a new dependency needs the user's OK (the runtime is stdlib-only)
set -euo pipefail
input=$(cat)
file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
[ -z "$file" ] && exit 0
text=$(jq -r '[.tool_input.new_string, .tool_input.content] | map(select(. != null)) | join("\n")' <<<"$input")
root=${CLAUDE_PROJECT_DIR:-$(pwd)}
rel=${file#"$root"/}

decide() { # decision reason
  jq -n --arg d "$1" --arg r "$2" '{hookSpecificOutput:{hookEventName:"PreToolUse",permissionDecision:$d,permissionDecisionReason:$r}}'
  exit 0
}

case "$rel" in
  *.wal|*kairo-data/*|*/snapshots/*|*/blobs/*)
    decide deny "$rel is runtime data (append-only log / snapshot / blob). Do not edit it; change the code that writes it." ;;
esac

if [[ "$rel" == core/*.go && "$rel" != *_test.go ]]; then
  bad=$(grep -nE 'time\.(Now|Since|Until|Sleep|After|AfterFunc|NewTimer|NewTicker|Tick)\(|^[[:space:]]*go[[:space:]]+[A-Za-z(]|"(os|net|net/http|io|io/fs|bufio|sync|sync/atomic|syscall|log|log/slog|math/rand|math/rand/v2|crypto/rand|runtime)"|sync\.(Mutex|RWMutex|WaitGroup|Once)' <<<"$text" || true)
  if [ -n "$bad" ]; then
    decide deny "core/ is the pure transition function: no I/O, clock, goroutines, randomness or locks (AGENTS.md, invariant 1). Time must arrive inside core.Event; I/O belongs in engine/. Offending lines in the new text:
$bad"
  fi
fi

if [[ "$rel" == go.mod ]] && grep -qE '^[[:space:]]*require|^[[:space:]]+[a-z0-9.-]+\.[a-z]+/' <<<"$text"; then
  decide ask "This adds a module dependency; the runtime is standard-library only (AGENTS.md). Confirm with the user."
fi
exit 0
