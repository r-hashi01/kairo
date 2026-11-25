# ワークフローを実行する

利用者は `k.workflow` で定義したワークフローを `k.run(name, input, { id })` で実行し、戻り値を受け取ります。途中の呼び出しはそれぞれ記録され、実の作用は 1 回だけ起きます。

## Sub-features

- `run-result`：`k.run` が戻り値を返す。
- `run-once`：実の作用（`refund-payment`）が 1 回だけ起きる。
- `run-stored`：保存状態で、ワークフローと各呼び出しが completed になる。

## How to get to it (user POV)

- アプリのコードで `await k.run('refund', { order, amount, sleepMs }, { id })` を呼ぶ。

## Driving it with kv.sh

Preconditions:

- `kv.sh doctor` が `doctor: ok`。

- **実行する。** 金額 50（承認不要）で実行する。`kv.sh journey happy`。`run.log` に `{"event":"result","output":{"refunded":"o-happy","approved":false}}`。
- **作用を数える。** `markers/` に `o-happy.done` がちょうど 1 つ。
- **保存状態を見る。** `inspect-final.json` で、ワークフローが `completed`、子の `kairo.call/classify`・`kairo.call/refund-payment`・`kairo.call/kairo.sleep` がすべて `completed`。

## Gotchas

- ワークフローの保存状態は、関数が例外を投げて失敗しても `status: "completed"` になります。成否は `output.payload.ok` で見ます。`status` だけで成功と判断しないでください。
- Node は WASI に `ExperimentalWarning` を出します。`kv.sh` は `--no-warnings` で消しています。
