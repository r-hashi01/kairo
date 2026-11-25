#!/usr/bin/env bash
# Lists the changes, since the sync commit in ../SKILL.md (or since $1),
# to the code that ../model/SnapshotRetention.tla describes. Empty output:
# the model still describes the code. Exit 1 when something changed.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
sync=${1:-$(sed -n 's/^sync commit: `\([0-9a-f]*\)`.*/\1/p' "$here/../SKILL.md")}
[ -n "$sync" ] || { echo "no sync commit in SKILL.md" >&2; exit 2; }

# Functions the model stands for (SKILL.md, coverage). A hunk counts if its
# header (the enclosing func) or its lines mention one of them.
funcs='record|holdAt|ack|release|pin|startSnapshot|checkpointRecord|maybeCompact|checkpointLive|done|finish|maybeEvict|writeSnapshot|load|loaded|mSnapStored|mSnapDeleted|recover|Retire|compactStale'
files=(engine/shard.go engine/recover.go engine/engine.go wal/)

# Committed and uncommitted changes alike.
diff=$(git -C "$repo" diff "$sync" -- "${files[@]}")
[ -z "$diff" ] && { echo "covered code unchanged since $sync"; exit 0; }
echo "$diff" | awk -v re="$funcs" '
	/^diff --git/ { file=$0; next }
	/^@@/ { if (hunk != "" && hit) print hunk; hunk = file "\n" $0 "\n"; hit = ($0 ~ "(func|case) .*(" re ")"); next }
	hunk != "" { hunk = hunk $0 "\n"; if ($0 ~ /^[+-]/ && $0 ~ "(" re ")") hit = 1 }
	END { if (hunk != "" && hit) print hunk }
' | tee /dev/stderr | grep -q . && exit 1
echo "changes since $sync do not touch the covered functions"
