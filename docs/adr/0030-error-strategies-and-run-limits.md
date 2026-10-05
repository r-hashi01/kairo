# 0030. ノードごとのエラー処理（retry / fail-branch / default-value）と、実行の上限を持つ

- 状態: 承認（2026-10-02）
- 日付: 2026-10-02
- 関連: FR3 / 不変条件 5 / ADR 0003, 0028, 0029, 0035

## 背景

Dify（graphon 0.7.0）は、エラー処理を**ノードの設置ごと**に設定する（`entities/base_node_data.py`）。

- `retry_config`: `retry_enabled`、`max_retries`、`retry_interval`（ミリ秒）
- `error_strategy`:
  - なし: 実行全体を失敗にする。
  - `fail-branch`: ハンドル `fail-branch` の辺へ進む。出力は `error_message` と `error_type`。
  - `default-value`: `default_value`（`[{key, type, value}]`）と `error_message` / `error_type` を出力にして、続ける。
- 例外として処理したノードの数（`exceptions_count`）を数え、1 つでもあれば、実行は `partial-succeeded` で終わる。

実行の上限もある。ステップ数（既定 500）、時間（既定 3600 秒）、ワークフローを tool として入れ子に呼ぶ深さ（既定 5）。

kairo のリトライは、アクションのノード仕様（`NodeSpec`）ごとで、エラー時の分岐はない。失敗すれば実行全体が失敗する。

## 決定

### ノードの設置ごとのリトライ

- step の定義に `retry`（`{"max_attempts": n, "interval": "500ms"}`）を足す。指定すれば、ノード仕様の `MaxAttempts` と `Backoff` より優先する。
- `interval` は固定の間隔とする（Dify と同じ）。指定がなければ、今の指数バックオフのまま。
- リトライを待つ間は、今と同じくタイマーで表す。graphon のように dispatcher を sleep で止めることはしない。

### エラー時の振る舞い（`on_error`）

- step の定義に `on_error` を足す。

  ```json
  {"strategy":"fail-branch"}
  {"strategy":"default-value","value":{"text":"(失敗)","score":0}}
  ```

- リトライを使い切った**確定した失敗**に適用する。
  - **戦略なし:** 今と同じく、実行を失敗にする。
  - **`fail-branch`:** ノードは「例外」で終わる。出力は `{"error_message":..., "error_type":...}` で、ハンドル `fail-branch` の辺を通り、他の辺はスキップする。
    - 成功したときは、今までどおり `source` の辺を通る。
    - 分岐ノード（ADR 0029）に `fail-branch` を付けたときは、分岐のハンドルに `fail-branch` を加える。
    - コンパイル時に、`fail-branch` の辺があることを要求する。
  - **`default-value`:** ノードは「例外」で終わる。出力は `value` に `error_message` と `error_type` を足したもので、`source` の辺を通る。
- **結果が不明な実の命令**（不変条件 5）には、`on_error` を適用しない。今までどおり要確認で止める。Dify との互換のための例外は、ADR 0035 で別に決める。
- `error_type` は、実行器が返すエラーの種類（`task.Result` に足す `ErrType`）とする。なければ `"error"`。

### 例外の数と終わりの状態

- 状態に、例外で終わったノードの数（`State.Exceptions`）を持つ。
- 実行の状態の種類は増やさない。`completed` のまま `RunInfo.Exceptions` で数を返し、Dify 側のアダプタが `partial-succeeded` に写す。

### 実行の上限

- `SubmitRequest.Limits{MaxSteps, MaxDuration}` と、その既定値の `Config.Limits` を足す。
- **ステップ数:**
  - 活性化を始めた数を数える。対象はステップ、待機、コンテナで、グラフ自体と cond の条件（KTest）は数えない。
  - 上限を超えるノードは、始めずに実行を失敗にする（`max steps exceeded`）。
  - graphon は、上限を超えたかどうかをノードが終わったときに判定する。kairo は、超える前に止める。
- **時間:**
  - 開始時に、実行の期限のタイマーを 1 つ張る（`State.Deadline`。既存のタイミングホイールを使う）。
  - 期限が来たら、実行中のタスクを取り消して（ADR 0026）、実行を失敗にする。
  - 待機中（HITL など）の時間も含む。Dify の上限と同じ。
- **呼び出しの深さ:**
  - `SubmitRequest.Depth` を足し、`Config.MaxDepth` を超えたら `Submit` を拒否する。
  - ワーカーがワークフローを入れ子に呼ぶときは、自分の深さ + 1 を渡す（ワーカー SDK が `task.Task.Depth` を見て行う）。

## 結果

- Dify のエラー処理を、そのまま IR で表せる。
- 実行の時間の上限で、実行中のタスクまで止まる。graphon は、実行中のノードを止めない。
- 状態に、例外の数と期限のタイマーが増える（数バイト）。`codecVersion` を上げる。
- テスト:
  - fail-branch と default-value のそれぞれで、出力、辺、例外の数を確かめる。
  - 結果が不明な実の命令は、`on_error` があっても要確認で止まる。
  - ノードごとのリトライが、ノード仕様より優先する。
  - ステップ数と時間の上限。時間の上限で、実行中のタスクの ctx が切れる。
  - `checkReplay` を付ける。
  - ADR 0028 の差分ハーネスで、Dify の fixture のエラー処理を含むものが一致する。

## 検討した代替案

- エラー処理をノード仕様（アクション）ごとに持つ: Dify は、同じアクションでも設置ごとに変える。
- 実行の状態に `partial-succeeded` を足す: `RunStatus` の種類が増え、終わりの判定（`Done`）にも触れる。数を返すだけで、アダプタが写せる。
- 上限の判定を、ノードが終わったときに行う（graphon と同じ）: 上限を超えたノードが走ってしまう。超える前に止める方が、上限の意味に合う。

## 実装で詰めた詳細（2026-10-05 追記、ADR 0017）

- **定義の形。** step の定義に `retry`（`max_attempts`、`interval`）と `on_error`（`strategy`、`value`）を足した。
  - `fail-branch` は、手書きのグラフのメンバーにしか置けない（コンパイル時に検査する）。糖衣（seq / par / cond）には、通る辺がないので。
  - `fail-branch` の辺がない場合も、コンパイル時に拒否する。
- **保護ステップのエラー。** 保護ステップ（`kairo.switch` など）の評価のエラーにも、`on_error` を適用する。
  - `error_type` は、graphon が投げる例外のクラス名（`ValueError` / `TypeError`）にした。
- **イベントのレコード。** 拡張フィールドを足した。`ErrType`、`MaxSteps`、`Deadline`、`Depth`。
  - フラグのビット（4）があるときだけ後ろに続くので、古いレコードはそのまま読める。拡張がないイベントのバイト列は、変更前と同じ。
  - 後の ADR で足すフィールドは、拡張の版を上げて後ろに続ける。
- **期限。** 期限は、シャードが開始時に絶対時刻（開始時刻 + `MaxDuration`）にして開始のイベントに入れる。リプレイしても同じ期限になる。
  - タイマーは活性化に属さない（`Act` = 0）。実行が終わるとき（完了・失敗・取り消し）に外し、回復のときに張り直す。
- **ステップ数。** 数えるのは、ステップ、待機、map、loop を始めた回数。グラフと cond の条件（KTest）は数えない。
- **深さ。** `task.Task.Depth` に、その実行の深さを入れる。ワーカーがワークフローを入れ子に呼ぶときは、それに 1 を足して `SubmitRequest.Depth` に渡す。
- **状態の形式。** `codecVersion` を 3 にした（制限とカウンタ）。バージョン 1・2 も読める。
- **テスト:**
  - `TestFailBranch`、`TestDefaultValue`、`TestOnErrorDoesNotCoverUnknownRealOutcome`、`TestMaxSteps`、`TestDeadline`、`TestErrorHandlingCompileErrors`（core）
  - `TestRunDeadlineCancelsRunningTask`、`TestErrTypeAndExceptions`、`TestMaxDepth`、`TestEventRecordExtension`（engine）
- **レビューで直したこと（2026-10-05）:**
  - 分岐ノードには `default-value` を付けられない（既定の出力ではハンドルが決まらないため。コンパイル時に拒否する）。
  - `kairo.switch` の case の ID に `fail-branch` は使えない。
  - `SubmitRequest.Limits` は、フィールドごとに `Config.RunLimits` を上書きする。0 のフィールドは設定の値のまま。空の指定で、サーバの上限を外せないようにするため。
