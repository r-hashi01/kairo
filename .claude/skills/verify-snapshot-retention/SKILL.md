---
name: verify-snapshot-retention
description: Model-check kairo's invariant 8 (snapshots hold only durable state; log retirement never drops a record recovery needs, including a finished run's records until its snapshot is deleted) with TLA+/TLC over every order of events, acks, snapshot writes, checkpoints, eviction, finish, deletion and crash. Use before finishing any change to maybeCompact, pin, record, release/ack, checkpointLive, checkpointRecord, maybeEvict, writeSnapshot, the mSnapStored/mSnapDeleted handling, finish's cleanup in engine/shard.go, or recover in engine/recover.go.
---

# verify-snapshot-retention

## 何を確かめるか

**性質（1 文）：** ファイル層の実行について、プロセスがどの時点で死んでも、復元（`engine/recover.go`）は耐久化したイベントをすべて含む状態で実行を戻すか、終わっていた実行なら終わったものとして扱い、改ざん扱いで止まることもない。

**破れたときの害（1 文）：** 再起動後に、イベントが抜けた状態で実行が進むか、終わったはずの実行が生き返り、実の作用をもう一度出しかねない（不変条件 8、ADR 0016）。

モデルが確かめる不変式（`model/SnapshotRetention.tla` の末尾）：

| 不変式 | 平易に言うと |
|---|---|
| `Recoverable` | どの時点で死んでも、生き残ったログの長さ k（fsync 済み以上の任意の前方部分）と、退役で消えた位置 c（1〜`retired` の任意）のすべての組で、復元が上の性質を満たす |
| `SnapshotDurable` | スナップショットのファイルは、fsync 済みの LSN までしか含まない。書き込みを始める時点で耐久化済みである |
| `LoadSeesLatest` | メモリから退避し、状態がディスクにしかない実行を読み込むと、最新の状態が返る |
| `OneWrite`、`TypeOK` | モデル自身の整合（書き込み中フラグと進行中の書き込みが一致する、LSN の大小関係） |

## 実行のしかた

```sh
.claude/skills/verify-snapshot-retention/scripts/stale.sh        # 対象のコードが同期コミットから変わったか
.claude/skills/verify-snapshot-retention/scripts/check.sh        # モデル検査（MaxLSN 8、M 系の Mac で約 50 秒）
.claude/skills/verify-snapshot-retention/scripts/check.sh -maxlsn 6   # 早く見るだけ（約 3 秒）
```

- `check.sh` は TLC v1.7.4 の `tla2tools.jar` を `~/.cache/kairo-tla/` に取得し（初回のみ。SHA-256 を固定して確かめる）、Homebrew の `openjdk@21`（`brew install openjdk@21`）か `JAVA` で動かします。JDK がなければ、ダイジェストを固定した `eclipse-temurin` の JRE イメージで Docker から動かします。
- 出力は `${KAIRO_TLA_OUT:-/tmp/kairo-tla}/<日時>/`（`tlc.log`、使ったモデルと設定、`run.txt` にコミット）。終了コード 0 が通過、12 が不変式の違反、150 がモデルの構文エラーです。
- TLC は全コアを使います。1 回は 1 分以内ですが、境界を 9 以上に上げると数分〜数十分かかります。上げるときは利用者に聞いてください。

## いつ使うか

`stale.sh` が何か出したら使います。出なければ、このモデルはまだコードを表しているので、流す必要はありません。

1. `stale.sh` が出した差分を読み、下の「対応表」で、どのアクションに当たるかを決めます。
2. その変更を `model/SnapshotRetention.tla` の該当アクションに写します。名前と順序はコードに合わせ、シャードループの 1 メッセージを 1 アクションにします（I/O の完了と、その結果のメッセージは別のアクション）。
3. `check.sh` を流します。
4. 結果を読みます。
   - **通った：** 変更はこの範囲で性質を保ちます。下の「同期コミット」を、モデルが新しいコードを表していると確かめたコミットに進めます。進めるのは、手順 2 を済ませたあとだけです。
   - **違反した：** 反例を、下の「反例の読み方」でコードの言葉に直します。製品コードの不具合なら、それを報告します（利用者が求めない限り製品コードは直さない）。モデルの写し間違いなら、写し直します。どちらか判断がつかないときは、性質やモデルを緩めて通さず、反例をそのまま利用者に見せてください。

### 反例の読み方

違反したら、pstack の `/statechart` スキル（Claude Code では `/pstack:statechart`）で `tlc.log` を読みます。違反までの最短の道筋を平易な言葉で示します。通ったときは、状態の移り方の図を描きます。手で読むときは、各 `State n: <アクション名>` を対応表でコードに置き換え、最後の状態で「どの k と c で、何が復元できないか」を `Recoverable` の 4 つの条件から特定します。

### 本物と写し間違いの見分け方

- 反例の各手が、対応表のコードで実際に起こり得る順序か（同じメッセージの中で起きることを別の手にしていないか）。
- 最後の状態の `retired`・`file`・`log` を、`recover` が読む順（ログの全記録 → `snap/<id>` → 改ざん検査 → `from` より後のイベントの適用）でたどったとき、本当に状態が欠けるか。
- 本物なら、その順序を起こす Go のテスト（`engine/engine_test.go` の `TestLogCompaction` の近く）に落とせるはずです。落とせたら、利用者に報告します。

## 対応表（カバレッジ）

| モデル | コード（同期コミット時点） |
|---|---|
| `Init` | `startRun`（`r.lastLSN, r.startLSN = l.lsn`） |
| `Event` | `event` → `record`（`pendFirst`・`postSnap`）。終わらせるイベントなら `commands` → `done` → `hold(hDone)` |
| `Other` | 同じシャードの別の実行の記録 |
| `Sync` / `Ack` | コミッタの group commit と fsync / `mAck` → `ack`（耐久化した保留をまとめて `release`） |
| `Rel`（`ReleaseAll`） | `release` の `hCheckpoint`（`pendSnap` / `cpLSN` / `snap + 1` の 3 分岐）、`hDone` → `finish`（`s.runs` から外し `s.pins` に入れる）、`hEvict` |
| `CheckpointLive` | `maybeCompact` → `checkpointLive` → `startSnapshot` |
| `Relog` | `maybeCompact` の退避中の実行：`checkpointRecord(r, r.cpLSN, true)` |
| `Compact` | `maybeCompact` の `bound`（`durable + 1`、`s.runs` の `pin()`、`s.pins` の `pin()`）と `Retire` |
| `Pin` | `run.pin` |
| `Evict` | `maybeEvict`（`hEvict` の保留、`startSnapshot`、`writeSnapshot`） |
| `Load` | `load` → `loaded`（`r.snap` からか、`snap/<id>` から） |
| `PutDone` / `SnapStored` | `snaps.Put` の完了 / `mSnapStored`（終わった実行なら `finishJob`、そうでなければ `checkpointRecord`、退避中なら `onDisk` か再書き込み） |
| `MarkerDurable` | `finish` の `addMarker` → `start`（書き込み中なら `finishJob` に回す） |
| `DeleteDone` / `SnapDeleted` | `finishJob` の `snaps.Delete` / `mSnapDeleted`（`s.pins` から外す） |
| `Kept`・`Found`・`CpLSN`・`RecoversAt` | `recover`（`engine/recover.go`）：開始記録かチェックポイント記録で実行を見つけ、`snapLSN` 最大のチェックポイント、`from < cpLSN` の改ざん検査、`startLSN == 0 && !haveSnap` を終わった扱い、`from` より後のイベントの適用 |

`stale.sh` が見るファイル：`engine/shard.go`、`engine/recover.go`、`engine/engine.go`、`wal/`。

sync commit: `0290018dbe5b8dd29f9f24b6fdfc832b2454bc97`（2026-11-25、main。確かめたのは同じ中身の 2d2c2e9）

## 範囲

**境界：** 1 シャード、1 本のファイル層のログ、検査する実行は 1 つ（ほかの実行は記録 `o` としてだけ現れる）、ログの長さ 8 まで（`MaxLSN`）。8 で、開始・2 回のスナップショット・そのチェックポイント記録・退避・再ログ・終了・削除の組み合わせに届きます。全アクションと全分岐が評価されることを、TLC のカバレッジ（`-coverage`）で確かめてあります。

**前提（検証していない）：**

- 1 つの実行のスナップショット書き込みは直列である（`writing` フラグ）。モデルは進行中の書き込みを 1 本しか持てないため、この直列化を外した変種は検出できません。
- `snaps.Put` は原子的に置き換える（ADR 0016、`blob`）。
- `Retire(bound)` は、`bound` 未満の記録を古いセグメントから消すだけで、`bound` 以上は消さない。消える位置はそれより前の任意の位置（モデルの c）。
- 生き残るログは前方部分（fsync 済みより後ろも、前から順に残り得る）。

**対象外：** 購読（`Config.Feeds`）の bound、ログ書き込みの失敗（`l.failed`）、スナップショットの読み込みと書き込みの失敗、`compactStale` と `CompactEvery` の起動条件（いつでも走り得るとしているので、振る舞いは増えるだけ）、復元のあとの続き（`recover` が作る `s.pins`）、終了ログ（`done-NNN.*.wal`、ADR 0027）とマーカーの耐久化そのもの、同じ ID の再投入、SQL の保存先、メモリ層。

## 確かめた記録（2026-11-25）

- 同期コミットのコードを写したモデルは、MaxLSN 8 で 6,616,770 状態をすべて調べて違反なし（約 50 秒）。
- 壊した変種で失敗すること：
  - 実コードの `maybeCompact` から `s.pins` のループを消し、`stale.sh` がその差分を拾い、モデルに写すと、`Recoverable` が 11 手の反例で違反しました。反例：開始（LSN 1）→ LSN 1 のスナップショットを書き始める → イベント（LSN 2）→ 書き込み完了でチェックポイント記録（LSN 3、snap 1）→ 終わらせるイベント（LSN 4）→ LSN 4 まで耐久化して `finish` → `maybeCompact` が LSN 5 未満を退役。ここで死に、LSN 1〜2 のセグメントだけが消えていると、`recover` はチェックポイント記録で実行を見つけ、LSN 1 のスナップショットに LSN 4 だけを当てる（LSN 2 が抜ける）。確かめたあと、実コードは差分なしに戻しました。
  - モデルだけの変種：`checkpointLive` から `l.durable < r.lastLSN` の検査を消す → `SnapshotDurable` 違反。`mSnapStored` で `m.lsn == r.lastLSN` を見ずに `onDisk` にする → `LoadSeesLatest` 違反。`release(hCheckpoint)` の保守的な `post := snap + 1` を `h.lsn` にする → `Recoverable` 違反。`maybeEvict` で書き込み中にも書き始める → 検出できない（上の前提）。
- 初版のモデルは「`retired` 未満はすべて消える」としていたため、`s.pins` を消した変種を見逃しました。退役の位置を任意（c）にして直しています。この種の甘さ（性質の側が都合のよい世界しか見ていない）に注意してください。
