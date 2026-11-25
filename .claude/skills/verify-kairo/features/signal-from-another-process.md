# 別のプロセスからシグナルを送る

`ctx.waitFor('approve')` で待っているワークフローに、別のプロセス（API、Slack のボタン、別のワーカー）が `k.signal(id, 'approve', payload)` を送ると、待っていたプロセスで続きが走ります。待っている間、後ろの実の作用は起きません。

## Sub-features

- `signal-wait`：金額が大きいとき、ワークフローは `approve` を待ち、`refund-payment` を呼ばない。
- `signal-deliver`：別プロセスの `k.signal` が成功する。
- `signal-continue`：待っていたプロセスの `k.run` が `approved: true` の結果を返し、作用は 1 回。

## How to get to it (user POV)

- アプリが `k.run('refund', { amount: 250, ... }, { id })` を呼ぶ。
- 承認者の操作を受けた別のプロセスが `k.signal(id, 'approve', { by })` を呼ぶ。

## Driving it with kv.sh

Preconditions:

- `kv.sh doctor` が `doctor: ok`。

- **待たせる。** プロセス 1 で `o-sig` を金額 250 で実行する。`kv.sh journey signal` が行う。`run-1.log` に `kairo.wait/approve` の `run.started` 観測が出るまで待つ。`inspect-waiting.json` でワークフローが `running`、`kairo.wait/approve` が `running`。`markers/` は空。
- **送る。** プロセス 2 で `signal`。`signal.log` に `{"event":"signalled"}`、終了コード 0。
- **続きを見る。** プロセス 1 が終わり、`run-1.log` に `{"refunded":"o-sig","approved":true}`。`markers/` に `o-sig.done` がちょうど 1 つ。

## Gotchas

- 保存状態の待ちは `status: "running"` です（`waiting` ではない）。`kairo.wait/<名前>` の子で判断します。
- **TS の `SQLiteStore` は SQLite のロック待ち（`busy_timeout`）を設定していません（2026-11-25 時点。Go は 5000ms、Python は既定で 5 秒。Issue #78、修正は PR #81）。** 同じファイルを複数のプロセスが同時に開いたり書いたりすると、`WorkflowRetryError: kairo store: database is locked` で落ちることがあります。初版の手順は待ちの検出のために第三のプロセスでデータベースを繰り返し開き、プロセス 1 が起動時にこれで落ちました。今の手順は、プロセス 1 自身の観測イベントで待ちを検出します。`signal` や `inspect` の時点でこのエラーが出たら、手順の不具合ではなく、利用者の道筋で起きる競合として報告します。
