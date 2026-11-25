---
name: verify-kairo
description: Drive kairo the way a user's app does - the TypeScript SDK's embedded runtime on SQLite, in separate Node processes that are killed with SIGKILL - and capture proof that a workflow resumes, a real action runs at most once, signals arrive from another process, and an unknown outcome stops for review. Use to prove a change to core/, wasmcore/, cmd/kairo-wasm/ or sdk/ts/ works in the real user path, not only in unit tests.
---

# verify-kairo

kairo の利用者が触るのは、自分のアプリに埋め込んだ SDK です（README のクイックスタート。常駐デーモンは前提にしない）。このスキルは、TypeScript SDK の埋め込み実行（`EmbeddedBackend` + `SQLiteStore`）で、利用者と同じ道筋を通して振る舞いを確かめます。対象は、ワークフローの実行、プロセスの強制終了と再実行、別プロセスからのシグナル、結果不明の実の作用です。

単体テスト（`scripts/check.sh`、`/verify`）の代わりではありません。テストが通ったあとに、「実際のプロセスを殺しても約束が守られるか」を確かめるためのものです。

## 道具

`scripts/kv.sh` から全部を呼びます。手で `app.ts` を叩く必要はありません。

| 物 | 役割 |
|---|---|
| `scripts/kv.sh` | 準備・診断・手順の実行・後片付け。プロセスは自分が起こしたものだけを pid で止める |
| `scripts/app.ts` | 利用者のアプリの見本。README の `refund` ワークフローとほぼ同じ形（`classify` は非保護、`refund-payment` は実）。役割ごとに 1 プロセスで動き、標準出力に 1 行 1 JSON を出す |

`refund-payment` は `markers/<order>.done` を `wx`（既にあれば失敗）で作ります。二度目の実行は「何もしない」ではなく、エラーとして必ず表に出ます。

## Launch

サーバーはありません。起動は、手順ごとに次を行うことです（`kv.sh journey` が自動で行う）。

1. 使い捨ての作業ディレクトリ `/tmp/kairo-verify-work.XXXXXX` を作る（`kairo.db`、`kairo.wasm`、`markers/`）。
2. **このチェックアウトのコアから** `kairo.wasm` をビルドする（`GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared ./cmd/kairo-wasm`）。`sdk/ts/wasm/kairo.wasm`（gitignore 済み、古いことがある）は使いません。
3. 各プロセスを `node --no-warnings scripts/app.ts <role> <work> ...` で起こす。準備完了の合図は、そのプロセスが出す `{"event":"run-begin"}` と観測イベント（`"obs"`）です。

借用（lease）の期限は `KAIRO_VERIFY_LEASE_MS`（既定 1000ms。SDK の既定は 30000ms）で短くしてあります。

## Doctor

何かおかしいと思ったら、最初にこれを流します。読み取りだけです。

```sh
.claude/skills/verify-kairo/scripts/kv.sh doctor
```

`go`・`node`（22 以上、`node:sqlite` が使える）・`python3` があること、ここが kairo のチェックアウトであること、コミットと未コミットの数、前回の作業ディレクトリが残っていないかを表示し、最後に `doctor: ok` を出します。

## Drive

```sh
.claude/skills/verify-kairo/scripts/kv.sh journey <name>
```

| name | 確かめること | 機能マップ |
|---|---|---|
| `happy` | 1 回の実行が結果を返し、実の作用が 1 回だけ起き、保存状態も completed になる | [run-a-workflow](features/run-a-workflow.md) |
| `crash-resume` | 実の作用のあと、耐久タイマーの途中で SIGKILL。同じ ID で再実行すると再開し、作用は 1 回のまま | [resume-after-crash](features/resume-after-crash.md) |
| `signal` | `waitFor('approve')` で待つワークフローに、別プロセスが `k.signal` を送ると続きが走る | [signal-from-another-process](features/signal-from-another-process.md) |
| `unknown-outcome` | 実の作用の最中に SIGKILL。再実行すると作用は再実行されず `blocked` で止まり、`k.resolve` で決着できる | [unknown-outcome-review](features/unknown-outcome-review.md) |

各チェックは `PASS` / `FAIL` の 1 行で出ます。1 つでも `FAIL` があれば `RESULT: FAIL` で終了コード 1 です。手順を通すためにチェックを緩めたり、待ち時間を伸ばしたりしないでください。失敗は失敗として報告します。

機能マップ（[`features/README.md`](features/README.md)）がこのスキルの保守対象です。変更が触れる機能の手順は全部流します。都合のよい 1 つだけで「確かめた」としないでください。

## Evidence

証拠は `${KAIRO_VERIFY_OUT:-/tmp/kairo-verify}/<日時>-<name>/` に残り、後片付けでは消えません。

- `journey.log`：コミット、Go と Node の版、lease、各ステップと `PASS`/`FAIL`、最後の `RESULT`
- `run-N.log`、`signal.log`、`resolve.log`：各プロセスの標準出力と標準エラー（1 行 1 JSON と、SDK のログ）。`*.exit` に終了コード
- `inspect-<時点>.json`：その時点の保存状態（ワークフローと、その呼び出しの状態と出力）
- `kairo.db*`、`markers/`：最後のデータベースと、実の作用が残した印

証明の基準：

- 利用者の道筋だけを通す。SDK の公開 API（`k.run`、`k.signal`、`k.resolve`、`k.list`、`k.children`）だけを使い、テスト用の内部 API やストアの直接操作はしない。
- 操作と、その結果の状態の両方を残す。「結果が返った」だけでなく、保存状態（`inspect-*.json`）と副作用（`markers/`）も見る。
- 作用の回数は `markers/` で数える。ログの文言では数えない。
- プロセスを止めるのは SIGKILL です（ホストがプロセスを失う状況と同じ）。SIGTERM や `k.close()` で止めた結果を、クラッシュの証明にしないでください。

## Cleanup

`kv.sh journey` は終わるとき（失敗や中断でも）、自分が起こした pid だけを止め、`kairo.db` と `markers/` を証拠のディレクトリへ写してから作業ディレクトリを消します。途中で殺されて残ったものは、次で消します。

```sh
.claude/skills/verify-kairo/scripts/kv.sh cleanup
```

これは `/tmp/kairo-verify-work.*` だけを対象にし、各ディレクトリの `pids` に記録した pid だけを止めます。プロセス名で kill しないでください。証拠（`/tmp/kairo-verify/`）は消しません。

## 別の面

- **kairod**（常駐デーモン。HTTP API とワーカーソケット）：利用者向けの既定の道筋ではありませんが、使う人もいます。既存の `scripts/qualify-kairod.sh`（Go・Python・TS のドライバを同時につなぐ）と `scripts/fault-kairod.sh`（SIGKILL と再起動をまたぐ）で駆動します。説明は `scripts/qualification/README.md` にあります。どちらも重いので、流す前に利用者に確認してください。
- **Python・Go の SDK**：このスキルの手順は TS だけです。同じ形の検証を足すなら、`app.ts` と同じ役割の見本を書き、機能マップに手順を足します。

## 2026-11-25 時点の結果（main 0290018）

`happy` と `signal` は通り、`crash-resume` と `unknown-outcome` は失敗します。理由は各機能ファイルの Gotchas にあり、修正は Issue #79（PR #82）と #80（PR #85）で進めています。`signal` の手順で起こり得る `database is locked` は #78（PR #81）です。修正がマージされたら、手順を流し直して、ここと Gotchas を書き換えてください。
