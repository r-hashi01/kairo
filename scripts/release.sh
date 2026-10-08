#!/usr/bin/env bash
# Builds everything a release ships, checks it, and puts it in OUT (ADR 0057).
# It publishes nothing: .github/workflows/release.yml publishes what this
# builds. Runs anywhere (Go 1.27+, Node 22+, Python 3.12+ with venv).
#
#   scripts/release.sh 0.1.0 [OUT]      (OUT defaults to dist/)
#
# 1. kairo.wasm, built twice: the same source must give the same bytes.
# 2. The npm package and the Python wheel and sdist, each carrying that
#    kairo.wasm (checked by sha256), with the version given.
# 3. The SDKs as installed from those packages load kairo.wasm (its ABI is
#    theirs) and run the README's quick starts.
# 4. kairod and kairo-jev for linux and darwin, amd64 and arm64.
# 5. SHA256SUMS of everything.
set -euo pipefail

version=${1:?usage: scripts/release.sh VERSION [OUT]}
root=$(cd "$(dirname "$0")/.." && pwd)
out=$(mkdir -p "${2:-$root/dist}" && cd "${2:-$root/dist}" && pwd)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

step() { printf '\n== %s\n' "$*"; }
die() { echo "release: $*" >&2; exit 1; }
sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi; }

rm -rf "${out:?}"/*
mkdir -p "$out/npm" "$out/python" "$out/bin"

step "kairo.wasm (twice: the same bytes)"
GOOS=wasip1 GOARCH=wasm go build -trimpath -buildmode=c-shared -o "$out/kairo.wasm" ./cmd/kairo-wasm
GOOS=wasip1 GOARCH=wasm go build -trimpath -buildmode=c-shared -o "$tmp/again.wasm" ./cmd/kairo-wasm
wasm_sha=$(sha256 "$out/kairo.wasm")
[ "$wasm_sha" = "$(sha256 "$tmp/again.wasm")" ] || die "kairo.wasm is not reproducible"
echo "kairo.wasm $wasm_sha"

# The SDKs are built from copies: the working tree is left as it is.
rsync -a --exclude node_modules --exclude dist --exclude wasm --exclude __pycache__ --exclude '*.egg-info' \
  sdk/ts sdk/python "$tmp/"

step "npm: kairo-sdk $version"
(
  cd "$tmp/ts"
  mkdir -p wasm && cp "$out/kairo.wasm" wasm/kairo.wasm
  npm install --no-audit --no-fund --loglevel=error
  npm pkg set version="$version"
  npx tsc -p tsconfig.json
  npm pack --pack-destination "$out/npm" --loglevel=error >/dev/null
)
tgz="$out/npm/kairo-sdk-$version.tgz"
[ -f "$tgz" ] || die "no $tgz"
[ "$(tar -xOzf "$tgz" package/wasm/kairo.wasm | { tee "$tmp/npm.wasm" >/dev/null; sha256 "$tmp/npm.wasm"; })" = "$wasm_sha" ] ||
  die "the npm package carries another kairo.wasm"

step "PyPI: kairo-sdk $version"
python3 -m venv "$tmp/venv"
"$tmp/venv/bin/pip" install --quiet build
(
  cd "$tmp/python"
  mkdir -p kairo_sdk/wasm && cp "$out/kairo.wasm" kairo_sdk/wasm/kairo.wasm
  sed -i.bak "s/^version = \".*\"/version = \"$version\"/" pyproject.toml && rm pyproject.toml.bak
  "$tmp/venv/bin/python" -m build --outdir "$out/python" . >/dev/null
)
# (Python normalizes the version: 0.1.0-rc.1 is 0.1.0rc1 in the file name.)
wheel=$(ls "$out"/python/kairo_sdk-*-py3-none-any.whl 2>/dev/null | head -1)
[ -n "$wheel" ] || die "no wheel"
"$tmp/venv/bin/python" - "$wheel" "$wasm_sha" <<'EOF' || die "the wheel carries another kairo.wasm"
import hashlib, sys, zipfile
with zipfile.ZipFile(sys.argv[1]) as z:
    got = hashlib.sha256(z.read("kairo_sdk/wasm/kairo.wasm")).hexdigest()
sys.exit(got != sys.argv[2])
EOF

step "the README's quick starts, from the packages"
# The quick starts as written, with a database in a temporary place and an
# amount that needs no approval (so the run ends without a signal).
python3 - "$root/README.md" "$tmp" <<'EOF'
import re, sys
readme, tmp = sys.argv[1], sys.argv[2]
s = open(readme).read()
ts = re.search(r"### TypeScript.*?```ts\n(.*?)```", s, re.S).group(1)
ts = ts.replace("'kairo.db'", f"'{tmp}/ts.db'").replace("amount: 250", "amount: 50") + "\nawait k.close();\n"
open(f"{tmp}/quick.ts", "w").write(ts)
py = re.search(r"### Python.*?```python\n(.*?)```", s, re.S).group(1)
py = py.replace('"kairo.db"', f'"{tmp}/py.db"').replace('id="refund-o-42"))', 'id="refund-o-42"))\n    await k.close()')
open(f"{tmp}/quick.py", "w").write(py)
EOF
mkdir -p "$tmp/smoke-ts" && mv "$tmp/quick.ts" "$tmp/smoke-ts/quick.ts"
(
  cd "$tmp/smoke-ts"
  npm init -y >/dev/null && npm pkg set type=module
  npm install --no-audit --no-fund --loglevel=error "$tgz"
  # The package is JavaScript; the quick start is TypeScript: Node strips the types.
  got=$(NO_COLOR=1 FORCE_COLOR=0 node --no-warnings quick.ts)
  [ "$got" = "{ refunded: 'o-42' }" ] || die "the TypeScript quick start printed: $got"
  echo "typescript: $got"
)
"$tmp/venv/bin/pip" install --quiet "$wheel[embedded]"
got=$(cd "$tmp" && "$tmp/venv/bin/python" quick.py)
[ "$got" = "{'refunded': 'o-42'}" ] || die "the Python quick start printed: $got"
echo "python: $got"

step "kairod and kairo-jev"
for os in linux darwin; do
  for arch in amd64 arm64; do
    CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags='-s -w' -o "$out/bin/kairod-$version-$os-$arch" ./cmd/kairod
    (cd executor/jev && CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags='-s -w' \
      -o "$out/bin/kairo-jev-$version-$os-$arch" ./cmd/kairo-jev)
  done
done
ls "$out/bin"

step "SHA256SUMS"
(cd "$out" && find . -type f ! -name SHA256SUMS | sed 's|^\./||' | sort | while read -r f; do echo "$(sha256 "$f")  $f"; done > SHA256SUMS)
cat "$out/SHA256SUMS"
