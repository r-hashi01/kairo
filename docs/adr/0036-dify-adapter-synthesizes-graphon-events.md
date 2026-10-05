# 0036. Dify 側のアダプタは、kairo のトレースから graphon のイベントを合成し、Dify の既存の下流をそのまま使う

- 状態: 承認（2026-10-02）
- 日付: 2026-10-02
- 関連: ADR 0028, 0029, 0034

## 背景

Dify は、graphon の graph event を受けたあと、多くの処理を行う。

- **ResponseStreamFilter**: Answer と End のテンプレートの順にチャンクを並べ直す。エッジの状態（TAKEN / SKIPPED）と、経路上の「ブロッキングエッジ」で、ストリームを始める時点を決める。
- **Layer**: 永続化（`workflow_runs` / `workflow_node_executions`）、会話変数の保存、pause 状態の保存、OTel、トリガーの後処理。
- **`workflow_app_runner._handle_event`**: queue event に変換し、さらに SSE にする（workflow / chatflow、full / simple）。

これを Go や新しいアダプタで作り直すと、互換性を保つ対象が一気に増える。特に、応答ストリームの順序制御を kairo に入れると、kairo が Dify の Answer テンプレートの意味を知る必要が出る。

## 決定

### アダプタは graphon のイベントを合成する

- Dify 側のアダプタ（Python。Dify のリポジトリに置く、ADR 0028）は、kairo の実行イベントの出口（ADR 0034）を購読する。トレースとチャンクを、**graphon 0.7.0 の graph event のオブジェクト**に変換する。
  - 対象: `GraphRunStarted/Succeeded/PartialSucceeded/Failed/Aborted/Paused`、`NodeRunStarted/Succeeded/Failed/Exception/Retry/StreamChunk`、`NodeRunIteration*`、`NodeRunLoop*`、`NodeRunVariableUpdated`、`GraphEdgeTaken/Skipped`
- 合成したイベントは、Dify の既存の経路にそのまま流す。対象は `WorkflowEntry.run` と同じフィルタ（`ResponseStreamFilter`、`HumanInputFormEventFilter`）、Layer、`_handle_event`。
  - **応答ストリームの順序制御は kairo に入れない。** Dify の `ResponseStreamFilter` がそのまま行う。そのために、トレースはエッジの状態（ノードの終わりで選んだハンドルと、スキップ）を含み、チャンクはノードの終わりより前に届く（ADR 0034）。
- graphon の RuntimeState（pause の snapshot）は使わない。kairo の実行は kairo が持つ。
  - Dify の pause・resume の経路（human-input）は、アダプタが kairo の signal（`SignalStep`）に置き換える。
  - pause 中の状態として Dify の DB に保存するものは、kairo の実行 ID だけになる。

### 何を kairo で、何をアダプタでするか

| 処理 | 場所 |
|---|---|
| グラフの実行、分岐、合流、スキップ、エラー処理、iteration / loop、変数、上限 | kairo |
| ノードの実行（LLM、tool など） | Python ワーカー（Dify のノードの実装） |
| 応答ストリームの順序、SSE への変換、実行履歴の DB、会話変数の保存、トレーシング | アダプタ（Dify の既存のコード） |
| 停止 | アダプタが `Cancel` を呼ぶ（Redis のコマンドの代わり） |

### 合成の検証

- 差分ハーネス（ADR 0028）では、2 つの段階で比べる。
  1. graphon が出す graph event の列と、アダプタが合成した列（ID と時刻を正規化）
  2. その先の SSE の列

  1 で一致すれば、2 はほぼ自動的に一致する。

## 結果

- Dify の下流（応答ストリーム、永続化、SSE、トレーシング）を書き直さずに、エンジンだけを置き換えられる。Dify の UI と公開 API の互換性は、既存のコードが保つ。
- kairo は、Dify の Answer テンプレートや SSE の形を知らなくてよい。
- 悪くなること:
  - アダプタは、graphon の graph event の型（0.7.0）に依存する。graphon の版が上がると、合成も追従する必要がある。
  - 合成したイベントの順序は、kairo のトレースの順序（シャードの中の順序）で決まる。graphon の dispatcher の順序とは、並列の枝の間で違いうる。並列の枝の間の順序は比較の対象から外し、集合として比べる（ADR 0028 と同じ）。
  - graphon の Layer の一部は、graphon の内部の状態（`graph_runtime_state`）を読む。合成したイベントだけでは足りないものは、アダプタが読み取り専用の代用品を渡す。何が要るかは、Layer ごとに調べて追記する。
- テスト: 差分ハーネスの 2 つの段階。Dify の既存の pipeline のテストを、アダプタ経由で流す。

## 検討した代替案

- 応答ストリームの順序制御を kairo（Go）に実装する: kairo が Dify の Answer / End のテンプレートと、ブロッキングエッジの規則を知る必要がある。その意味論は Dify の UI と一緒に変わる。
- Dify の queue event や SSE を、kairo が直接作る: Dify の下流（永続化、会話、Layer）を作り直すことになり、互換性を保つ対象が増える。
