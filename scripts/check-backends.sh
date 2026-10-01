#!/usr/bin/env bash
# Runs the SQL backend modules' tests against real databases in Docker
# (ADR 0020): starts store/compose.yaml, generates throwaway TLS
# certificates for it, exports the test DSNs and runs each module.
#
#   scripts/check-backends.sh            all products
#   scripts/check-backends.sh postgres   only some
#   KEEP=1 scripts/check-backends.sh     leave the containers running
set -euo pipefail
cd "$(dirname "$0")/.."
products=${*:-postgres mysql tidb oracle}
certs=store/.certs

if [ ! -f "$certs/server.crt" ]; then
  mkdir -p "$certs"
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=kairo-test-ca" \
    -keyout "$certs/ca.key" -out "$certs/ca.crt" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout "$certs/server.key" -out "$certs/server.csr" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > "$certs/san.ext"
  openssl x509 -req -in "$certs/server.csr" -CA "$certs/ca.crt" -CAkey "$certs/ca.key" -CAcreateserial \
    -days 30 -extfile "$certs/san.ext" -out "$certs/server.crt" 2>/dev/null
  # The container users must be able to read the key (postgres: uid 999).
  chmod 644 "$certs"/*.crt "$certs/server.key"
fi

compose() { docker compose -f store/compose.yaml "$@"; }
# shellcheck disable=SC2086
compose up -d --wait $products
[ -z "${KEEP:-}" ] && trap 'compose down -v >/dev/null 2>&1' EXIT

ca=$PWD/$certs/ca.crt
export KAIRO_TEST_CA=$ca
# Only the products started above get a DSN; the others' tests skip.
for p in $products; do
  case "$p" in
    postgres) export KAIRO_TEST_POSTGRES_DSN="postgres://kairo:kairo-test@localhost:55432/kairo?sslmode=verify-full&sslrootcert=$ca" ;;
    mysql) export KAIRO_TEST_MYSQL_DSN="kairo:kairo-test@tcp(localhost:53306)/kairo?tls=kairo-test" ;;
    # The test TiDB and Oracle have no TLS; their tests opt in to an
    # insecure transport explicitly.
    tidb) export KAIRO_TEST_TIDB_DSN="root@tcp(localhost:54000)/test" ;;
    oracle) export KAIRO_TEST_ORACLE_DSN="oracle://kairo:kairo-test@localhost:51521/FREEPDB1" ;;
  esac
done

# Map products to modules (TiDB is served by store/mysql).
mods=$(printf "%s\n" $products | sed "s/^tidb$/mysql/" | sort -u)
status=0
for m in $mods; do
  printf '\n== store/%s\n' "$m"
  (cd "store/$m" && go test -count=1 -v ./... > /tmp/kairo-backend-$m.log 2>&1) || status=1
  grep -E -- '^(--- |ok|FAIL)|^    --- |sqltest.go|measure.go|_test.go:' /tmp/kairo-backend-$m.log | grep -v '^        ---' || true
done
exit $status
