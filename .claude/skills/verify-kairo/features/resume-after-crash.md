# クラッシュのあとで再開する

利用者のプロセスが途中で落ちても、同じ ID でワークフローをもう一度実行すれば、終わった呼び出しは記録された結果を返し、続きから進みます。実の作用は二度起きません（README：「Run any of them, kill it at any point, and run it again: the workflow resumes」）。

## Sub-features

- `resume-immediate`：落ちた直後に同じ ID で `k.run` すると、再開して結果を返す。
- `resume-after-lease`：借用の期限が切れてから `k.run` すると、再開して結果を返す。
- `resume-once`：どちらでも、`refund-payment` は再実行されない。
- `resume-timer`：落ちたときに進行中だった耐久タイマー（`ctx.sleep`）が、再開後に満了する。

## How to get to it (user POV)

- アプリのプロセスが落ちる（デプロイ、OOM、ホストの停止）。
- アプリを起こし直し、同じ `id` で `k.run` を呼ぶ。

## Driving it with kv.sh

Preconditions:

- `kv.sh doctor` が `doctor: ok`。

- **落とす。** プロセス 1 で `o-crash` を実行し（`refund-payment` のあと 3 秒の `ctx.sleep`）、`markers/o-crash.done` が出たら SIGKILL。`kv.sh journey crash-resume` が行う。`run-1.log` に `result` がない。`inspect-after-crash.json` で `kairo.call/kairo.sleep` が `running`。
- **すぐ再実行する。** プロセス 2 で同じ ID を実行。終了コード 0 と `{"refunded":"o-crash","approved":false}` を期待する。
- **期限切れのあとで再実行する。** プロセス 2 が結果を返さなかったときだけ、借用の期限（`KAIRO_VERIFY_LEASE_MS` + 500ms）を待ってプロセス 3 で実行する。結果が返り、`real-action-done` が出ない。
- **作用を数える。** `markers/` に `o-crash.done` がちょうど 1 つ。

## Gotchas

- **2026-11-25 時点で `resume-immediate` は失敗します（Issue #79、修正は PR #82）。** 落ちたプロセスの借用が残っている間に同じ ID で `k.run` すると、`run()` はその借用が切れて掃除（ADR 0059）が引き取るのを待ちます。ところが掃除のタイマーは `unref()` されているので、ほかに処理がないスクリプトでは Node が先に終了コード 13（`Warning: Detected unsettled top-level await`）で終わります。保存状態は `running` のままです。期限が切れてから実行すれば再開し、作用も 1 回です（`resume-after-lease`）。サーバーのように動き続けるプロセスでは、次の掃除で再開します。
- このチェックを、待ち時間を足して通すように変えないでください。直ったら、プロセス 2 が結果を返し、プロセス 3 は走らなくなります。
