#!/usr/bin/env bash
# Drives the TypeScript SDK's embedded runtime the way a user's app does:
# separate Node processes on one SQLite file, killed with SIGKILL where a
# journey needs a crash. See ../SKILL.md.
#
#   kv.sh doctor
#   kv.sh journey happy|crash-resume|signal|unknown-outcome
#   kv.sh cleanup
#
# Evidence:  ${KAIRO_VERIFY_OUT:-/tmp/kairo-verify}/<stamp>-<journey>/  (kept)
# Scratch:   /tmp/kairo-verify-work.XXXXXX                              (removed)
set -uo pipefail

here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../../.." && pwd)
app="$here/app.ts"
out_root=${KAIRO_VERIFY_OUT:-/tmp/kairo-verify}
export KAIRO_VERIFY_LEASE_MS=${KAIRO_VERIFY_LEASE_MS:-1000}

die() { echo "kv: $*" >&2; exit 2; }

doctor() {
	local ok=0
	for c in go node python3; do command -v "$c" >/dev/null || { echo "missing: $c"; ok=1; }; done
	local nv
	nv=$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)
	[ "$nv" -ge 22 ] || { echo "node $nv: the SDK needs Node 22+ (node:sqlite, type stripping)"; ok=1; }
	node --no-warnings -e 'require("node:sqlite")' 2>/dev/null || { echo "node:sqlite is not available"; ok=1; }
	[ -f "$repo/sdk/ts/src/index.ts" ] || { echo "not a kairo checkout: $repo"; ok=1; }
	local stray
	stray=$(ls -d /tmp/kairo-verify-work.* 2>/dev/null | wc -l | tr -d ' ')
	[ "$stray" = 0 ] || echo "note: $stray scratch dir(s) from an earlier run; 'kv.sh cleanup' removes them"
	echo "repo: $repo ($(git -C "$repo" rev-parse --short HEAD), $(git -C "$repo" status --porcelain | wc -l | tr -d ' ') uncommitted paths)"
	echo "go: $(go env GOVERSION)  node: $(node --version)"
	[ "$ok" = 0 ] && echo "doctor: ok" || echo "doctor: NOT ok"
	return "$ok"
}

# --- one journey ---------------------------------------------------------

work=
ev=
verdict=0

bg() { # bg <log> <args...>: start app.ts in the background, record its pid
	local log=$1; shift
	node --no-warnings "$app" "$@" >"$log" 2>&1 &
	echo $! >>"$work/pids"
	echo $!
}
fg() { # fg <log> <args...>: run app.ts to its end; its exit code goes to <log>.exit
	local log=$1; shift
	node --no-warnings "$app" "$@" >"$log" 2>&1
	local rc=$?
	echo "$rc" >"$log.exit"
	return "$rc"
}
exited() { [ "$(cat "$1.exit" 2>/dev/null)" = "$2" ]; } # exited <log> <code>
past_lease() { # past_lease: let the dead process's lease run out
	step "wait $((KAIRO_VERIFY_LEASE_MS + 500))ms: the dead process's lease runs out"
	sleep "$(python3 -c "print(($KAIRO_VERIFY_LEASE_MS + 500) / 1000)")"
}
wait_for() { # wait_for <seconds> <description> <command...>
	local secs=$1 what=$2; shift 2
	local n=$((secs * 10))
	for _ in $(seq 1 "$n"); do
		if "$@"; then return 0; fi
		sleep 0.1
	done
	echo "timed out after ${secs}s waiting for: $what" | tee -a "$ev/journey.log"
	return 1
}
gone() { ! kill -0 "$1" 2>/dev/null; }
not() { ! "$@"; }
check() { # check <description> <command...>
	local what=$1; shift
	if "$@"; then
		echo "PASS  $what" | tee -a "$ev/journey.log"
	else
		echo "FAIL  $what" | tee -a "$ev/journey.log"
		verdict=1
	fi
}
step() { echo "--    $*" | tee -a "$ev/journey.log"; }
crash() { # crash <pid>: SIGKILL, as a host losing the process would
	kill -9 "$1" 2>/dev/null
	wait "$1" 2>/dev/null
	step "SIGKILL pid $1"
}
inspect() { # inspect <id> <name>: save the stored state of a workflow and its calls
	fg "$ev/inspect-$2.json" inspect "$work" "$1"
}
# jq-free reads of the JSON lines app.ts prints
has_event() { grep -q "\"event\":\"$2\"" "$1"; }
result_of() { python3 -c 'import json,sys
for l in open(sys.argv[1]):
    if l.startswith("{"):
        d=json.loads(l)
        if d.get("event")=="result": print(json.dumps(d["output"],sort_keys=True))' "$1"; }
call_status() { # call_status <inspect.json> <action>
	python3 -c 'import json,sys
for l in open(sys.argv[1]):
    if l.startswith("{"):
        d=json.loads(l)
        for c in d.get("children",[]):
            if c["plan"]=="kairo.call/"+sys.argv[2]: print(c["status"], c["run_id"])' "$1" "$2"
}
root_status() {
	python3 -c 'import json,sys
for l in open(sys.argv[1]):
    if l.startswith("{"):
        r=json.loads(l)["run"]; print(r["status"] if r else "none", json.dumps((r or {}).get("output"),sort_keys=True))' "$1"
}
wait_started() { # wait_started <log>: the process observed its wait for a signal begin
	grep -q '"kind":"run.started"[^}]*"plan":"kairo[.a-z/]*wait' "$1"
}
markers() { ls "$work/markers" 2>/dev/null | sort | tr '\n' ' '; }
count_done() { ls "$work/markers" 2>/dev/null | grep -c "^$1\.done$"; }

finish() {
	# Evidence first, then scratch: cleanup must never eat the proof.
	if [ -n "$work" ] && [ -d "$work" ]; then
		if [ -f "$work/pids" ]; then
			while read -r p; do kill -9 "$p" 2>/dev/null; done <"$work/pids"
		fi
		cp "$work/kairo.db"* "$ev/" 2>/dev/null
		cp -R "$work/markers" "$ev/markers" 2>/dev/null
		rm -rf "$work"
	fi
	if [ -n "$ev" ]; then
		[ "$verdict" = 0 ] && echo "RESULT: PASS" | tee -a "$ev/journey.log" || echo "RESULT: FAIL" | tee -a "$ev/journey.log"
		echo "evidence: $ev"
	fi
}

journey() {
	local name=$1
	case "$name" in happy | crash-resume | signal | unknown-outcome) ;; *) die "unknown journey: $name" ;; esac
	ev="$out_root/$(date +%Y%m%d-%H%M%S)-$name"
	mkdir -p "$ev"
	work=$(mktemp -d /tmp/kairo-verify-work.XXXXXX)
	mkdir -p "$work/markers"
	trap finish EXIT
	trap 'exit 130' INT TERM
	{
		echo "journey: $name"
		echo "commit: $(git -C "$repo" rev-parse HEAD) ($(git -C "$repo" status --porcelain | wc -l | tr -d ' ') uncommitted paths)"
		echo "go: $(go env GOVERSION)  node: $(node --version)  lease_ms: $KAIRO_VERIFY_LEASE_MS"
	} >"$ev/journey.log"
	# The core the SDK runs is this checkout's, not a stale sdk/ts/wasm.
	(cd "$repo" && GOOS=wasip1 GOARCH=wasm go build -trimpath -buildmode=c-shared -o "$work/kairo.wasm" ./cmd/kairo-wasm) || {
		echo "FAIL  build kairo.wasm" | tee -a "$ev/journey.log"
		verdict=1
		exit 1
	}
	"j_${name//-/_}"
	exit "$verdict"
}

j_happy() {
	step "run o-happy (amount 50: not risky, no approval)"
	fg "$ev/run.log" run "$work" o-happy 50 0
	check "run returns {refunded: o-happy, approved: false}" [ "$(result_of "$ev/run.log")" = '{"approved": false, "refunded": "o-happy"}' ]
	check "refund-payment ran exactly once (markers: $(markers))" [ "$(count_done o-happy)" = 1 ]
	inspect o-happy final
	check "stored: workflow completed, refund-payment completed" \
		[ "$(root_status "$ev/inspect-final.json" | cut -d' ' -f1) $(call_status "$ev/inspect-final.json" refund-payment | cut -d' ' -f1)" = "completed completed" ]
}

j_crash_resume() {
	step "process 1: run o-crash; refund-payment, then a 3s durable sleep"
	local p1
	p1=$(bg "$ev/run-1.log" run "$work" o-crash 50 3000)
	wait_for 30 "refund-payment's effect (markers/o-crash.done)" [ -f "$work/markers/o-crash.done" ] || { verdict=1; return; }
	crash "$p1"
	check "process 1 died before the workflow returned" not has_event "$ev/run-1.log" result
	inspect o-crash after-crash
	# README: "kill it at any point, and run it again: the workflow resumes".
	step "process 2: run o-crash again at once, same id, same database"
	fg "$ev/run-2.log" run "$work" o-crash 50 3000
	check "process 2 exits 0 (got $(cat "$ev/run-2.log.exit"); 13 = run() never settled)" exited "$ev/run-2.log" 0
	check "process 2 returns {refunded: o-crash, approved: false}" [ "$(result_of "$ev/run-2.log")" = '{"approved": false, "refunded": "o-crash"}' ]
	if ! has_event "$ev/run-2.log" result; then
		past_lease
		step "process 3: run o-crash again"
		fg "$ev/run-3.log" run "$work" o-crash 50 3000
		check "process 3 returns {refunded: o-crash, approved: false}" [ "$(result_of "$ev/run-3.log")" = '{"approved": false, "refunded": "o-crash"}' ]
		check "process 3 did not run refund-payment again" not has_event "$ev/run-3.log" real-action-done
	fi
	check "process 2 did not run refund-payment again" not has_event "$ev/run-2.log" real-action-done
	check "refund-payment's effect exists exactly once (markers: $(markers))" [ "$(count_done o-crash)" = 1 ]
	inspect o-crash final
}

j_signal() {
	step "process 1: run o-sig (amount 250: risky, waits for 'approve')"
	local p1
	p1=$(bg "$ev/run-1.log" run "$work" o-sig 250 0)
	# Watched through process 1's own observations, not by opening the
	# database from a third process: that would add contention a user's app
	# does not have.
	wait_for 30 "o-sig's wait for 'approve' to start" wait_started "$ev/run-1.log" || { verdict=1; return; }
	inspect o-sig waiting
	check "nothing paid while waiting (markers: $(markers))" [ "$(count_done o-sig)" = 0 ]
	step "process 2: signal o-sig 'approve'"
	fg "$ev/signal.log" signal "$work" o-sig approve
	check "signal process succeeded" has_event "$ev/signal.log" signalled
	wait_for 30 "process 1 to return" gone "$p1" || { verdict=1; return; }
	check "process 1 returns {refunded: o-sig, approved: true}" [ "$(result_of "$ev/run-1.log")" = '{"approved": true, "refunded": "o-sig"}' ]
	check "refund-payment ran exactly once (markers: $(markers))" [ "$(count_done o-sig)" = 1 ]
	inspect o-sig final
}

j_unknown_outcome() {
	step "process 1: run o-unk; refund-payment takes the request and never answers"
	local p1
	p1=$(bg "$ev/run-1.log" run "$work" o-unk 50 0 hold)
	wait_for 30 "refund-payment to start (markers/o-unk.started)" [ -f "$work/markers/o-unk.started" ] || { verdict=1; return; }
	crash "$p1"
	past_lease
	step "process 2: run o-unk again"
	fg "$ev/run-2.log" run "$work" o-unk 50 0
	check "process 2 did not run refund-payment again (markers: $(markers))" [ "$(count_done o-unk)" = 0 ]
	check "refund-payment stopped for review: run.settled status=blocked observed" \
		grep -q '"plan":"kairo.call/refund-payment".*"status":"blocked"' "$ev/run-2.log"
	inspect o-unk after-blocked
	local call
	call=$(call_status "$ev/inspect-after-blocked.json" refund-payment | cut -d' ' -f2)
	step "operator: resolve $call as taken effect (README: k.resolve settles a call stopped for review)"
	fg "$ev/resolve.log" resolve "$work" "$call"
	check "resolve accepted" has_event "$ev/resolve.log" resolved
	inspect o-unk final
	check "after resolve, the workflow completed with the resolved output (stored: $(root_status "$ev/inspect-final.json"))" \
		grep -q '"ok": true' <(root_status "$ev/inspect-final.json" | cut -d' ' -f2-)
	check "refund-payment still never ran a second time (markers: $(markers))" [ "$(count_done o-unk)" = 0 ]
}

cleanup_all() {
	local n=0
	for w in /tmp/kairo-verify-work.*; do
		[ -d "$w" ] || continue
		if [ -f "$w/pids" ]; then
			while read -r p; do kill -9 "$p" 2>/dev/null; done <"$w/pids"
		fi
		rm -rf "$w"
		n=$((n + 1))
	done
	echo "removed $n scratch dir(s); evidence under $out_root is kept"
}

case "${1:-}" in
	doctor) doctor ;;
	journey) journey "${2:-}" ;;
	cleanup) cleanup_all ;;
	*) die "usage: kv.sh doctor | journey happy|crash-resume|signal|unknown-outcome | cleanup" ;;
esac
