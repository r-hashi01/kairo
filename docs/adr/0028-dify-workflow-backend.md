# 0028. Dify のワークフロー実行を置き換えられるエンジンにする（ノードは Python ワーカーで動かす）

- 状態: 承認（2026-10-02）
- 日付: 2026-10-02
- 関連: FR1, FR4 / ADR 0009, 0013, 0015, 0029 / 調査メモ `docs/compat/dify.md`

## 背景

Dify のワークフロー（workflow アプリと chatflow）は、graphon の `GraphEngine` で動いている。Dify 1.17.1 は graphon 0.7.0 に固定している。実行は Celery のワーカー（gevent）の中で行い、1 つの実行に複数のスレッド（greenlet）を使う。

graphon には、次の弱点がある（調査メモの 3 節）。

- 実行中に状態を保存できないので、クラッシュから再開できない。
- 待っている実行にもスレッドを使う。
- テナント間で、外部の割り当て（RPM / TPM）を配分する仕組みがない。

いずれも kairo が解いている問題である。

一方で、ノードの実装（LLM、tool、ナレッジ検索、code、agent、文書抽出、ファイル）は Python で、Dify の依存（plugin daemon、モデルの認証情報、DB、ストレージ）に深くつながっている。以前の Go の試作（ir-runtime）は、ノードを Go に移植しようとして互換性を保てなかった。graphon 自身の文書も、アダプタの単位で言語の境界を引く案を却下している。

## 決定

### 統合の形

- kairo は、Dify の裏でワークフローを実行するエンジンになる。Dify の API、会話とメッセージ、会話変数、トリガー、HITL のフォームと配送、実行履歴の DB は、Dify 側に残す。
- Dify は、ワークフローの DSL を kairo の IR に変換し、`Submit` / `Signal` / `Cancel` で実行を操作する。結果とイベントは、kairo から受け取る。
- 言語の境界は**ノード単位**で引く。
  - Dify の Python のノード（`DifyNodeFactory` が作るもの）は、Python のワーカーが丸ごと実行する。ワーカーは、kairo のワーカープロトコル（ADR 0009）でタスクを受け取る。
  - 境界を越えるのは、入力（解決済みの変数）、出力、ストリームのチャンク、エラーの種類だけにする。例外の型や generator は、Python の中で閉じる。
- 純粋なノードは、kairo の中で Go で実行する。対象は if-else、variable-aggregator、assigner、list-operator、start / end / answer の組み立てで、保護ステップ（ADR 0015）として扱う。
- 実行は、アプリ単位などで graphon と切り替えられるようにし、段階的に移す。

### 互換性の基準と検証

- 基準は **Dify 1.17.1 と graphon 0.7.0** とする。基準を上げるときは、この ADR に追記する。
- **差分ハーネス**を、互換性の判定に使う。
  - ノードをモックにした状態で、graphon 0.7.0 と kairo に同じ DSL と入力を与える。
  - 次の 4 つを比べる。
    1. 実行されたノードと、その順序の制約（同時に動けるものは集合として比べる）
    2. スキップされたノードの集合
    3. 出力
    4. SSE のイベント列（ID と時刻を正規化した後のもの）
  - 入力のコーパスは、Dify のワークフローの fixture（`api/tests/fixtures/workflow/`）と、graphon のテストから作る。
- 同じハーネスで、Celery を含む実際の構成と性能を比べる。

### 置き場所

- kairo のリポジトリに置くもの: DSL から IR への変換と Go の純粋なノード（別モジュール `compat/dify`。本体の依存は増やさない、ADR 0014）、Python のワーカー SDK、差分ハーネス。
- Dify のリポジトリに置くもの: Dify 側のアダプタ（実行の投入、イベントから SSE への変換、実行履歴の保存）。

### kairo 本体に要る変更（それぞれ別の ADR で決める）

- 任意のグラフとして実行する（0029）
- エラー処理（fail-branch、default-value、partial-succeeded）と、実行の上限（ステップ数、時間、呼び出しの深さ）
- Dify の条件（if-else と loop の break 条件。文字列の比較を含む）の扱い（ADR 0013 との関係を決める）
- iteration / loop の意味論（並列数、エラーモード、flatten、前判定ループ、loop 変数）
- 欠落しない実行イベントの出口（Dify の実行履歴と SSE に使う。obs は欠落を許すので使えない）
- Dify のノードの作用の型への割り当て（Dify の retry の挙動との関係）
- Answer / End の応答ストリームの順序制御（Dify 側のアダプタに置くか、kairo に置くか）

## 結果

- ノードの Python 実装を書き直さずに、エンジンだけを置き換えられる。Dify の新しいノードも、ワーカーに登録するだけで動く。
- 得られるもの: クラッシュからの再開、待っている実行のコストの削減、外部の割り当ての公平な配分、実行の取り消し（ADR 0026）、冪等な投入（ADR 0023、0027）。
- 悪くなること:
  - ノードごとに、kairo とワーカーの間で往復が 1 回増える（UNIX ソケット。LLM の呼び出しと比べれば小さいが、計測して確かめる）。
  - 解決済みの入力をワーカーに渡すため、大きな変数のコピーが増えうる（ブロブの参照で渡せるかは別に詰める）。
  - Dify との互換性を保つ作業（graphon の版上げへの追従）が続く。
- テスト: 差分ハーネスを CI で流す（graphon 0.7.0 を入れた Python 環境が要る）。合格の基準は、コーパスの全件でノードの集合、スキップの集合、出力、SSE が一致すること。

## 検討した代替案

- ノードもすべて Go に移植する: ir-runtime で、Jinja2、文書抽出、ナレッジ検索、認証情報の扱いの互換性を保てなかった。Python 実装を捨てる前提でしか成り立たない。
- graphon の API（GraphEngine と Layer）を模倣して、プロセス内で差し替える: Layer と RuntimeState は Python のオブジェクトを受け渡す前提で、Go のプロセスからは提供できない。
- graphon を高速化する（Rust/PyO3 を含む）: 1 回の実行の速さは上がるが、クラッシュからの再開、待っている実行のコスト、外部の割り当ての配分は解決しない。

## 実装で詰めた詳細（2026-10-05 追記、ADR 0017）

- **変換器（`compat/dify`）。**
  - ADR のとおり別のモジュールにした。
  - 入力は DSL の YAML ではなく、`workflow` の JSON にした。Dify は DB にグラフを JSON で持っており、YAML のための依存を足さずに済む。差分ハーネスでは、Python 側が YAML を JSON にして渡す。
- **純粋なノードのための組み込みの保護アクション。** 決定の「純粋なノードは Go の保護ステップにする」の範囲で、2 つ足した。
  - `kairo.template`: answer のテンプレートを、graphon の書式（値の文字列化、見つからない変数の扱い）のまま展開する。
  - `kairo.coalesce`: variable-aggregator。最初に存在する変数を選ぶ。null でも存在すれば選び、どれもなければキーを作らない。グループにも対応する。
- **list-operator。** v1 ではワーカーで動かす（`dify.list-operator`）。Go への移植は、差分ハーネスのデータが揃ってから行う。
- **差分ハーネスの実際。**
  - graphon 0.7.0 の `graphon.dsl.loads` で Dify の DSL を読み込み、外部に作用するノードの `_run` と、モデルやツールの実体を作る処理をモックに差し替えて実行する（`harness/graphon_trace.py`）。
  - 正解データはリポジトリに置き（`compat/dify/testdata/graphon`）、CI では Python を使わない。
  - 比べるのは、実行の状態、成功・例外で終わったノードとその出力、実行全体の出力。SSE のイベント列の比較は、アダプタ（ADR 0036）を作るときに足す。
  - 2026-10-05 の時点で、31 本がすべて一致している。内訳は、Dify のテスト用データ 24 本、入力やモックを変えたケース 3 本、自前のデータ 2 本（エラー処理、iteration の要素の失敗）とそのケース 2 本。
- **テスト用データの写し。** Dify のワークフローのテスト用データから作った正解データを、kairo のリポジトリに置いている。
- **Python のワーカー SDK（`sdk/python`、2026-10-05）。**
  - ワーカープロトコルの実装は依存なし（`kairo_worker.Worker`）。graphon のノードを 1 つずつ実行する実行器は、graphon が入っているときだけ使う（`kairo_worker.graphon.GraphonNodeRunner`）。
  - 実行器は、タスクの入力を新しい変数プールに入れ、ノードファクトリでノードを作って実行する。
    - チャンクはライブの出力にする。
    - 結果のうち、`inputs`、`process_data`、`metadata` は `meta` に、使用量は `tokens` に入れる。
    - 外部に作用するノードのタイムアウトや切断は、「結果が不明」として返す。
  - ノードファクトリは差し替えられる。Dify では DifyNodeFactory（モデル、プラグイン、ファイル）を渡す。ここでは graphon の DSL 用のファクトリ（`slim_factory`）を使う。
  - エンドツーエンドのテスト（`TestGraphonWorkerEndToEnd`）: Dify のワークフローを変換して kairo のエンジンで実行し、ノードは Python のワーカーが graphon の実装（Jinja2 のテンプレート、ローカルの HTTP サーバへのリクエスト）で実行する。`GRAPHON_PYTHON` があるときだけ動く。
- **リトライは、graphon と同じく llm / code / http-request / tool にだけ付ける。** graphon は、ほかのノードの `retry_config` を無視するため。
