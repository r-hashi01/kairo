#!/usr/bin/env bash
# Model-checks ../model/SnapshotRetention.tla with TLC (see ../SKILL.md).
#
#   check.sh                 # the whole check (MaxLSN from the .cfg)
#   check.sh -maxlsn 7       # a smaller bound, for a quick look
#
# Output and any counterexample: ${KAIRO_TLA_OUT:-/tmp/kairo-tla}/<stamp>/
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
model="$here/../model"
version=v1.7.4
sha=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
jar="$HOME/.cache/kairo-tla/$version/tla2tools.jar"
image=eclipse-temurin@sha256:d77e090ae1f6970c047b443c6f0a6c1358b0426d44432bb5a7753408cd43de0d

maxlsn=
if [ "${1:-}" = -maxlsn ]; then maxlsn=${2:?}; fi

if [ ! -f "$jar" ]; then
	mkdir -p "$(dirname "$jar")"
	curl -fsSL -o "$jar.tmp" "https://github.com/tlaplus/tlaplus/releases/download/$version/tla2tools.jar"
	mv "$jar.tmp" "$jar"
fi
echo "$sha  $jar" | shasum -a 256 -c - >/dev/null || { echo "tla2tools.jar checksum mismatch: $jar" >&2; exit 2; }

out="${KAIRO_TLA_OUT:-/tmp/kairo-tla}/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$out"
cp "$model/SnapshotRetention.tla" "$model/SnapshotRetention.cfg" "$out/"
if [ -n "$maxlsn" ]; then
	sed -i.bak "s/^CONSTANT MaxLSN = .*/CONSTANT MaxLSN = $maxlsn/" "$out/SnapshotRetention.cfg"
fi
{
	echo "commit: $(git -C "$here" rev-parse HEAD) ($(git -C "$here" status --porcelain | wc -l | tr -d ' ') uncommitted paths)"
	grep '^CONSTANT' "$out/SnapshotRetention.cfg"
} >"$out/run.txt"

# A JDK on the host (Homebrew's openjdk@21, or JAVA), else the pinned JRE image.
java=${JAVA:-}
if [ -z "$java" ]; then
	for j in /opt/homebrew/opt/openjdk@21/bin/java /usr/local/opt/openjdk@21/bin/java; do
		[ -x "$j" ] && java=$j && break
	done
fi
tlc=(-cp tla2tools.jar tlc2.TLC -workers auto -deadlock -cleanup -config SnapshotRetention.cfg SnapshotRetention.tla)
cp "$jar" "$out/tla2tools.jar"
set +e
if [ -n "$java" ]; then
	(cd "$out" && "$java" -XX:+UseParallelGC "${tlc[@]}") >"$out/tlc.log" 2>&1
else
	docker run --rm -v "$out:/w" -w /w "$image" java -XX:+UseParallelGC "${tlc[@]}" >"$out/tlc.log" 2>&1
fi
rc=$?
set -e
rm -f "$out/tla2tools.jar"
grep -E '^(Model checking completed|Error:|Finished in|[0-9,]+ states generated)|Invariant .* is violated' "$out/tlc.log" || true
echo "tlc exit: $rc  log: $out/tlc.log"
exit "$rc"
