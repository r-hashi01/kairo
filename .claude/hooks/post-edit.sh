#!/usr/bin/env bash
# PostToolUse (Edit|Write): gofmt the file, vet its package, and re-check the
# core purity invariant when core/ changed. Problems are fed back to Claude
# (exit 2 + stderr).
set -uo pipefail
input=$(cat)
file=$(jq -r '.tool_response.filePath // .tool_input.file_path // empty' <<<"$input")
[[ "$file" == *.go && -f "$file" ]] || exit 0
root=${CLAUDE_PROJECT_DIR:-$(pwd)}
cd "$root" || exit 0
rel=${file#"$root"/}

# The repository has more than one Go module (e.g. store/sqlite): vet from
# the module that contains the file.
moddir=$(dirname "$file")
while [ "$moddir" != "$root" ] && [ ! -f "$moddir/go.mod" ]; do
  moddir=$(dirname "$moddir")
done
pkg=./${file#"$moddir"/}
pkg=$(dirname "$pkg")

gofmt -w "$file"
if ! out=$(cd "$moddir" && go vet "$pkg" 2>&1); then
  printf 'go vet %s failed after editing %s:\n%s\n' "$pkg" "$rel" "$out" >&2
  exit 2
fi
if [[ "$rel" == core/* ]]; then
  if ! out=$(go test -count=1 ./core -run '^TestCoreIsPure$' 2>&1); then
    printf 'core purity check failed after editing %s:\n%s\n' "$rel" "$out" >&2
    exit 2
  fi
fi
exit 0
