#!/usr/bin/env bash
# Regenerates compat/dify/testdata/graphon/*.json: workflow fixtures run on
# graphon 0.7.0 with mocked external nodes (ADR 0028).
#
#   GRAPHON_PYTHON=/path/to/venv/bin/python \
#   DIFY_FIXTURES=/path/to/dify/api/tests/fixtures/workflow \
#     compat/dify/harness/gen_golden.sh
#
# The Python environment needs graphon==0.7.0. Each fixture runs with the
# default case and with every cases/<fixture>.<variant>.json (inputs,
# outputs of mocked nodes, errors). Our own fixtures are in fixtures/.
set -euo pipefail
cd "$(dirname "$0")"
out=../testdata/graphon
mkdir -p "$out"
rm -f "$out"/*.json
for f in "${DIFY_FIXTURES:?}"/*.yml fixtures/*.yml; do
  [ -f "$f" ] || continue
  name=$(basename "$f" .yml)
  "${GRAPHON_PYTHON:?}" graphon_trace.py "$f" > "$out/$name.json"
  for c in cases/"$name".*.json; do
    [ -f "$c" ] || continue
    variant=$(basename "$c" .json)
    "${GRAPHON_PYTHON:?}" graphon_trace.py "$f" "$c" > "$out/$variant.json"
  done
done
ls "$out" | wc -l
