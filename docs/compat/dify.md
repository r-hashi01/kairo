# Dify のワークフロー処理を kairo で置き換えられるか（調査メモ）

- 日付: 2026-10-02
- 対象: Dify 1.17.1（`~/Documents/dify/dify`、tag 1.17.1）。エンジンは外部パッケージ graphon **0.7.0** です（`api/pyproject.toml:48`）。手元の graphon の HEAD は 0.8.0 で、API の名前と container の実装が違います。
- 参考にしたもの:
  - graphon（`~/Documents/dify/graphon`）。とくに `docs/plans/native-runtime-performance.md`
  - 以前の Go の試作 ir-runtime（`~/Documents/dify/ir-runtime`）
- これは判断材料のまとめです。決定は ADR で行います。

## 1. 置き換える対象

Dify は、graphon の `GraphEngine` を次の形で使っています。

- **起動**
  - streaming の場合: API プロセスが Celery タスク（`workflow_based_app_execution`、gevent プール）を enqueue し、Redis の topic で SSE を中継します。
  - blocking の場合: API プロセス内で実行します。
- **組み込み口**
  - `api/core/workflow/workflow_entry.py`
  - `node_factory.py`: `DifyNodeFactory` が、ノードの種類と版ごとにクラスと依存を注入します。
  - `api/core/app/apps/workflow_app_runner.py`: graph event を queue event に、さらに SSE に変換します。
- **拡張点**
  - Layer（開始、イベント、終了、ノードの開始と終了）。実際に付いているのは、永続化、OTel、実行上限、タイムスライス、会話変数の保存、pause 状態の保存です。
  - CommandChannel（Redis。Abort / Pause / UpdateVariables）
  - RuntimeState の snapshot JSON（`workflow_pauses` に保存済みのものと互換が要る）
- **ノード**
  - graphon にあるもの: start, end, answer, llm, if-else, code, template-transform, question-classifier, http-request, tool, variable-aggregator, loop, iteration, parameter-extractor, assigner, document-extractor, list-operator, human-input
  - Dify にあるもの: knowledge-retrieval, agent（v1・v2）, datasource, knowledge-index, trigger-webhook / schedule / plugin

## 2. 再現が必要な意味論

| 項目 | Dify（graphon 0.7.0） | kairo の現状 | 差 |
|---|---|---|---|
| グラフ | 任意の DAG。エッジの状態は UNKNOWN / TAKEN / SKIPPED の 3 値。入力の全エッジが確定してから、TAKEN が 1 本でもあれば実行する（OR 結合）。全部 SKIPPED ならスキップし、再帰的に伝播する | 構造化 IR（seq / par / cond / map・loop / wait） | **大きい。** 任意の DAG（分岐の後の合流、ひし形、複数の root）は構造化 IR に写せない場合がある |
| 分岐 | 出力ハンドル（if-else の `case_id` / `"false"`、分類器の `category_id`、human-input の action id、`fail-branch`） | `cond` は型付きフィールドで分岐する | 分岐を「ハンドル（enum）を出すノード」として扱えば、要件（text で分岐しない）を保てる。if-else の文字列比較は、決定的なので保護ステップで計算できる |
| 変数 | `[node, key, ...]` の selector。予約名は sys / env / conversation / rag。型は Segment 型（file や secret を含む） | 状態の中の出力とパス式 | selector の解決、型、ファイル、秘密値の扱いが要る |
| エラー処理 | retry（間隔はミリ秒）、fail-branch、default-value。例外にしたノードを数え、partial-succeeded で終わる | retry とバックオフだけ | fail-branch、default-value、partial-succeeded が要る |
| 作用 | 区別しない。retry が有効なら、どのノードも再実行する | 実・保護・非保護。結果が不明な実の命令は要確認で止める | Dify ノードの作用の型への写し方を決める必要がある（下の 4.3） |
| Iteration | item ごとに親の変数プールをコピーした子フレームで実行する。`parallel_nums`（既定 10）、error mode 3 種、flatten | `map`（並列数の上限とエラーモードはない） | 並列数、エラーモード、flatten |
| Loop | break 条件を初回の前と各回の後に評価する。loop 変数を持つ。loop-end で抜ける。上限は `loop_count` | do-while と `max_iter` | 前判定と loop 変数 |
| 応答のストリーム | Answer / End のテンプレート順にチャンクを並べ直す。同時に動くセッションは 1 つ。Iteration 内の Answer は終わった時点でまとめて出す | ライブ中継はあるが、順序制御はない | ResponseStreamFilter 相当が要る。エッジの状態を見るので、コアの外（アダプタ）に置ける |
| 一時停止・HITL | ノードからの pause 要求か PauseCommand で止め、snapshot を DB に保存して後で再開する | `wait` と signal（耐久的で、待っている間はコストがほぼない）、待機のタイムアウト | kairo の方が強い。フォーム、配送、タイムアウトの分岐は Dify 側に残せる |
| 停止 | Redis のコマンド。実行中のノードは止まらない | `Cancel`。ADR 0026 で実行中のタスクも止まる | kairo の方が強い |
| 上限 | max steps 500、max time 3600 秒、呼び出しの深さ 5 | なし | 足す必要がある |
| 記録 | ノードの開始と終了ごとに `workflow_node_executions` へ同期で書く（大きな値は offload） | 可観測性のストリーム（**欠落を許す**） | **大きい。** Dify の実行履歴には、欠落しない記録の出口が要る |
| イベント | graph event 約 30 種。SSE は workflow_started / node_started / node_finished / text_chunk / iteration_* / loop_* / message など | obs のレコード | 写像を決めて変換層を作る必要がある |

## 3. kairo が効くところ、効かないところ

効くところ（graphon の弱点と重なる）:

- **クラッシュからの再開。** graphon は静止しているときにしか snapshot を取れません（実行中のチェックポイントは範囲外と明記）。Celery ワーカーが落ちると、実行中のワークフローは失われるか、RUNNING のまま残ります。kairo は意図をログに書いてから命令を出す設計で、途中から再開できます。
- **待っている実行のコスト。** Dify は実行 1 つにスレッド（gevent では greenlet）を数本使い、ワーカーの空きをポーリングしています。kairo は待っている実行を状態のバイト数だけで持ちます。
- **外部の割り当ての配分。** RPM/TPM をテナント間で公平に分ける仕組みは、Dify にはありません。
- **固定のコスト。** graphon の実測（同期の chain）では、1 回の実行の固定コストが約 10ms で、ほぼ shutdown のポーリングです。ノード 1 つあたりは約 170µs かかります。
- **性能のリスク。** コードを読んで見つけたもので、実測はしていません。
  - Iteration / Loop の 1 回ごとにグラフ全体を作り直す（ノードの再生成、モデル設定、memory の取得を含む）
  - 変数プールの deep copy
  - retry の sleep で dispatcher が止まる
  - ノードごとに同期で DB に書く

効かないところ:

- graphon の文書によると、実行時間の大半は LLM の呼び出しです。エンジンを替えても、1 回の実行の速さはあまり変わりません。効くのは、スループット、耐久性、待ちのコスト、公平性です。
- graphon の文書は、ネイティブ化について次のように評価しています。
  - 「ネイティブのコアと Python のノードに分ける」案は却下している。
  - ネイティブ化するなら、Go より Rust/PyO3 をプロセス内に組み込む方がよいとしている。
  - 理由は、言語の境界を越えられないアダプタがあることです。HTTP は例外の型を、コード実行は例外のインスタンスを、LLM は Python の generator を、そのまま呼び出し側に渡しています。
  - kairo でこれを避けるには、**ノード単位で境界を引く**必要があります。ノードの実行は丸ごと Python 側で行い、境界を越えるのは入力、出力、チャンク、エラーの分類だけにします。アダプタの単位では分けません。

## 4. 統合の形（案）

### 4.1 推奨: kairo を Dify の裏のエンジンにし、ノードは Python ワーカーで動かす

```
Dify API ──(DSL→IR 変換, Submit/Signal/Cancel)──▶ kairod
   ▲                                               │ タスク（pull・クレジット制）
   │ SSE / 永続化（アダプタ）                         ▼
   └──── 実行イベント（欠落なし）◀──────── Python ワーカー（DifyNodeFactory のノードをそのまま実行）
```

- ノードの実装（LLM、tool、knowledge、code、agent など）は Dify の Python コードをそのまま使います。Python ワーカーは、kairo のワーカープロトコルで、ノード 1 つを 1 タスクとして実行します。
- 純粋なノードは、kairo の保護ステップとして Go で実装します。対象は if-else、variable-aggregator、assigner、list-operator、start / end / answer の組み立てです。
- Dify 側に残すもの:
  - API
  - 会話とメッセージの保存
  - 会話変数
  - トリガー
  - HITL のフォームと配送
  - 実行履歴の DB（アダプタが kairo のイベントから書く）
- 段階的に移せます。アプリ単位や機能フラグで、graphon と並走させられます。

### 4.2 採らない案

- **ノードをすべて Go に移植する。** ir-runtime がこの道をたどり、次の点で止まりました。
  - テンプレートを Jinja2 互換にできなかった
  - document-extractor が未実装（スタブ）のまま
  - knowledge retrieval の互換性が保てなかった
  - 認証情報の解決がない

  Python 実装を捨てる前提でしか成り立ちません。
- **graphon の API（GraphEngine と Layer）を Go で模倣して差し替える。** Layer と RuntimeState は Python のオブジェクトを渡す前提なので、プロセス内でしか成り立ちません。

### 4.3 先に決める必要がある、kairo 側の要件の変更

1. **グラフの構成要素。** 要件は「構成要素は 5 つのみ」です。任意の DAG と、3 値のエッジ状態による OR 結合を表すには、`graph`（エッジとハンドルを持つ構成要素）を足すか、既存の `par` / `cond` を一般化する必要があります。いずれにせよ ADR が要ります。
2. **分岐。** ハンドルを出すノードを enum の出力として扱えば、「text で分岐しない」はそのまま守れます。if-else は保護ステップにします。
3. **作用の型への写し方。** 次のどちらにするかを決める必要があります。
   - Dify の retry の挙動（どのノードも失敗したら再実行し、要確認で止まらない）をそのまま再現する「互換の作用モード」を設ける。
   - ノードの種類ごとに作用の型を割り当てる（llm は非保護、http と tool は実、など）。
4. **欠落しない実行記録。** obs ストリームは欠落を許すので、Dify の実行履歴には使えません。ログ（WAL）から読み出す、欠落しないイベントの出口が要ります。
5. **エラー処理の追加。** fail-branch、default-value、partial-succeeded、実行の上限（steps、時間、深さ）。
6. **iteration / loop の意味論。** 並列数、エラーモード、flatten、前判定ループ、loop 変数。

### 4.4 互換性の検証方法

- **基準: graphon 0.7.0。** Dify 1.17.1 が使っている版です。
- **適合性のコーパス**
  - Dify のワークフロー fixture（`api/tests/fixtures/workflow/`。ir-runtime に 23 本の写しがある）
  - graphon のテスト（約 24k 行）
- **差分ハーネス。** ノードをモックにした状態で、graphon 0.7.0 と kairo に同じ DSL と入力を与え、次の 3 つを比べます。
  1. ノードが実行された順序と、スキップされたノードの集合
  2. 出力
  3. SSE のイベント列（正規化した後のもの）

  ir-runtime は「成功するか」と「フィールドがあるか」までしか確かめておらず、ここが弱点でした。
- **性能の比較。** 同じハーネスで、Celery を含む実際の構成と比べます。graphon の文書も「まず Celery の層を計測する」と勧めています。

## 5. ir-runtime から使えるもの

- 実際の DSL の fixture 23 本と、不正な DSL 5 本
- DSL のパーサ。kind ごとのパラメータキーの一覧として使えます。`parentId` を捨てている点は直す必要があります。
- graphon のイベントのスキーマ。Dify の `_handle_event` が読むフィールドのチェックリストとして使えます。
- 次の処理の実装（テストつき）:
  - if-else の演算子
  - assigner の 12 操作
  - list-operator
  - エッジの状態機械とスキップの伝播
  - ResponseCoordinator
- plugin daemon と sandbox のプロトコルのメモ

真似しないもの:

- 永続化がないこと
- コンパイル結果を実行時に使っていないこと
- 副作用のあるノードまで投機的に実行すること
- フレームスタックが 1 つしかないこと
