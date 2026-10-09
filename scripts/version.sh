#!/usr/bin/env bash
# One version for the repository (ADR 0057): VERSION is it. The npm and
# PyPI packages, the Go SDK's Version and the Go modules' requires of each
# other carry it; a release tag vX.Y.Z must be it.
#
#   scripts/version.sh check [X.Y.Z]   everything carries VERSION (and it is X.Y.Z)
#   scripts/version.sh set X.Y.Z       write X.Y.Z everywhere
#   scripts/version.sh tags            the tags a release pushes, at HEAD
#
# Push the nested modules' tags first, then vX.Y.Z alone: GitHub makes no
# push event when more than three tags are pushed at once, and release.yml
# runs on vX.Y.Z's.
set -euo pipefail
cd "$(dirname "$0")/.."

# The Go modules others import, tagged <dir>/vX.Y.Z beside vX.Y.Z (Go's
# rule for nested modules). Not tagged: compat/* (evaluation) and
# sdk/go/storetest (tests).
tagged="store/sqlstore store/sqlite store/postgres store/mysql store/oracle executor/jev sdk/go/pgnotify"

version=$(tr -d ' \n' < VERSION)
semver='^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$'
[[ $version =~ $semver ]] || { echo "VERSION: $version is not X.Y.Z" >&2; exit 1; }

# Each place, and what it says now.
ts_version() { sed -n 's/^  "version": "\(.*\)",$/\1/p' sdk/ts/package.json; }
ts_const() { sed -n "s/^export const version = '\(.*\)';$/\1/p" sdk/ts/src/version.ts; }
py_version() { sed -n 's/^version = "\(.*\)"$/\1/p' sdk/python/pyproject.toml; }
py_const() { sed -n 's/^__version__ = "\(.*\)"$/\1/p' sdk/python/kairo_sdk/_version.py; }
go_const() { sed -n 's/^const Version = "\(.*\)"$/\1/p' sdk/go/version.go; }
gomods() { find . -name go.mod -not -path '*/node_modules/*' | sed 's|^\./||' | sort; }
# Requires of kairo's own modules that do not carry the version.
go_stale() {
  for f in $(gomods); do
    grep -nE '^\s*(require\s+)?github\.com/r-hashi01/kairo(/[^ ]+)? v' "$f" | grep -v " v$version\b" | sed "s|^|$f:|" || true
  done
}

case "${1:-}" in
check)
  [ -z "${2:-}" ] || [ "$2" = "$version" ] || { echo "the tag says $2, VERSION says $version" >&2; exit 1; }
  bad=0
  for p in "sdk/ts/package.json:$(ts_version)" "sdk/ts/src/version.ts:$(ts_const)" "sdk/python/pyproject.toml:$(py_version)" \
    "sdk/python/kairo_sdk/_version.py:$(py_const)" "sdk/go/version.go:$(go_const)"; do
    [ "${p#*:}" = "$version" ] || { echo "${p%%:*} says ${p#*:}, VERSION says $version" >&2; bad=1; }
  done
  stale=$(go_stale)
  [ -z "$stale" ] || { echo "requires not at v$version:"; echo "$stale"; } >&2
  [ "$bad" = 0 ] && [ -z "$stale" ] || exit 1
  echo "version $version everywhere"
  ;;
set)
  v=${2:?usage: scripts/version.sh set X.Y.Z}
  [[ $v =~ $semver ]] || { echo "$v is not X.Y.Z" >&2; exit 1; }
  echo "$v" > VERSION
  sed -i.bak "s/^  \"version\": \".*\",$/  \"version\": \"$v\",/" sdk/ts/package.json
  sed -i.bak "s/^export const version = '.*';$/export const version = '$v';/" sdk/ts/src/version.ts
  sed -i.bak "s/^version = \".*\"$/version = \"$v\"/" sdk/python/pyproject.toml
  sed -i.bak "s/^__version__ = \".*\"$/__version__ = \"$v\"/" sdk/python/kairo_sdk/_version.py
  sed -i.bak "s/^const Version = \".*\"$/const Version = \"$v\"/" sdk/go/version.go
  for f in $(gomods); do
    sed -i.bak -E "s#(github\.com/r-hashi01/kairo(/[^ ]+)?) v[^ ]+#\1 v$v#" "$f"
  done
  for f in $(gomods); do rm -f "$f.bak"; done
  rm -f sdk/ts/package.json.bak sdk/ts/src/version.ts.bak sdk/python/pyproject.toml.bak sdk/python/kairo_sdk/_version.py.bak sdk/go/version.go.bak
  "$0" check
  ;;
tags)
  echo "v$version"
  for d in $tagged; do echo "$d/v$version"; done
  ;;
*)
  sed -n '2,10p' "$0" >&2
  exit 2
  ;;
esac
