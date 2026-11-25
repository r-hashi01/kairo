# 結果不明の実の作用を確認に回す

実の作用（`effect: 'real'`）の最中にプロセスが落ちると、作用が起きたかどうかはわかりません。kairo はそれを成功扱いせず、再実行もせず、確認待ち（`blocked`）で止めます。人が `k.resolve(callId, output)`（起きた）または `k.resolveFailed(callId, message)`（起きなかった）で決着させると、ワークフローが続きます（SDK の README、ADR 0066）。

## Sub-features

- `unknown-no-rerun`：再実行したプロセスは `refund-payment` をもう一度実行しない。
- `unknown-blocked`：`refund-payment` の呼び出しが `blocked` になる。
- `unknown-resolve`：`k.resolve` がその呼び出しを決着させ、ワークフローが決着の出力で終わる。

## How to get to it (user POV)

- 決済 API への要求を出したあと、応答を受ける前にアプリのプロセスが落ちる。
- アプリを起こし直し、同じ `id` で `k.run` を呼ぶ。
- 運用者が決済の記録を確かめ、`k.resolve(callId, output)` を呼ぶ。

## Driving it with kv.sh

Preconditions:

- `kv.sh doctor` が `doctor: ok`。

- **作用の途中で落とす。** プロセス 1 で `o-unk` を `hold` 付きで実行する。`refund-payment` は `markers/o-unk.started` を書いて応答しない。`kv.sh journey unknown-outcome` が行う。印が出たら SIGKILL。
- **期限を待って再実行する。** 借用の期限（`KAIRO_VERIFY_LEASE_MS` + 500ms）のあと、プロセス 2 で同じ ID を実行する。`run-2.log` に `kairo.call/refund-payment` の `run.settled` `status:"blocked"`。`markers/` に `o-unk.done` がない。
- **決着させる。** `inspect-after-blocked.json` から `kairo.call/refund-payment` の `run_id` を取り、`resolve` 役で `k.resolve(callId, { refunded: 'resolved-by-operator' })`。`resolve.log` に `{"event":"resolved"}` を期待する。
- **結果を見る。** `inspect-final.json` で、ワークフローの `output.payload.ok` が `true`。`markers/` に `o-unk.done` がないまま。

## Gotchas

- **2026-11-25 時点で `unknown-resolve` は失敗します（Issue #80、修正は PR #85）。** 埋め込み実行の TS と Python では、`blocked` になった呼び出しが `ctx.call` に `CallError`（status `blocked`）として返り、ワークフローはその場で失敗します。そのあと、終わっていない子として呼び出しが `cancelled` にされ（ADR 0062）、`k.resolve` は `KairoError: run <callId> has no step stopped for review` で拒まれます。Go は ADR 0058 のとおり確認を待ちます。ADR 0058 は TS と Python もそろえると書いていますが、その整合が抜けていました。
- `unknown-no-rerun` と `unknown-blocked` は通ります。不変条件 5（結果が不明な実の命令を成功扱いせず、再実行もしない）は、利用者の道筋でも守られています。
- 期限の前に再実行すると、[クラッシュのあとで再開する](./resume-after-crash.md)の Gotchas と同じく、プロセス 2 は引き取らずに終了コード 13 で終わり、`blocked` に至りません。
