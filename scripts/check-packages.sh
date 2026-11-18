#!/usr/bin/env bash
# Compile and package the SDKs with the same tools used by release.sh, without
# publishing or running the release-only installed-package quick starts.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

wasm=${KAIRO_WASM:?KAIRO_WASM must name the already-built kairo.wasm}
[ -f "$wasm" ] || { echo "package check: no $wasm" >&2; exit 1; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
out=${PACKAGE_OUT:-$tmp/out}
mkdir -p "$out/typescript" "$out/python"

printf '\n== typescript sdk compile\n'
(
  cd sdk/ts
  npx --no-install tsc -p tsconfig.json --outDir "$out/typescript"
)
[ -f "$out/typescript/index.js" ] || { echo "package check: TypeScript emitted no index.js" >&2; exit 1; }
[ -f "$out/typescript/index.d.ts" ] || { echo "package check: TypeScript emitted no index.d.ts" >&2; exit 1; }

printf '\n== python sdk wheel + sdist\n'
rsync -a --exclude __pycache__ --exclude '*.egg-info' --exclude wasm sdk/python/ "$tmp/python/"
mkdir -p "$tmp/python/kairo_sdk/wasm"
cp "$wasm" "$tmp/python/kairo_sdk/wasm/kairo.wasm"
python3 -m build --outdir "$out/python" "$tmp/python" >/dev/null

wheel=$(find "$out/python" -maxdepth 1 -name 'kairo_sdk-*-py3-none-any.whl' -print -quit)
sdist=$(find "$out/python" -maxdepth 1 -name 'kairo_sdk-*.tar.gz' -print -quit)
[ -n "$wheel" ] || { echo "package check: Python emitted no wheel" >&2; exit 1; }
[ -n "$sdist" ] || { echo "package check: Python emitted no sdist" >&2; exit 1; }

printf '\npackage checks passed\n'
