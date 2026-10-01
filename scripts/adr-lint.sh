#!/usr/bin/env bash
# Checks docs/adr: file names NNNN-slug.md, unique numbers, required
# sections and a status line, and every ADR listed in the index.
set -euo pipefail
cd "$(dirname "$0")/../docs/adr"
fail=0
err() { echo "adr-lint: $*" >&2; fail=1; }
seen=""
for f in [0-9]*.md; do
  [[ "$f" =~ ^[0-9]{4}-[a-z0-9-]+\.md$ ]] || err "$f: name must be NNNN-slug.md"
  n=${f:0:4}
  case " $seen " in *" $n "*) err "$f: number $n is used twice" ;; esac
  seen="$seen $n"
  head -1 "$f" | grep -q "^# $n\. " || err "$f: first line must be '# $n. <title>'"
  grep -q '^- 状態: ' "$f" || err "$f: missing '- 状態: ' line"
  for s in '## 背景' '## 決定' '## 結果' '## 検討した代替案'; do
    grep -q "^$s" "$f" || err "$f: missing section '$s'"
  done
  grep -q "($f)" README.md || err "$f: not listed in docs/adr/README.md"
done
exit $fail
