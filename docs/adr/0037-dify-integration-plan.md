# 0037. Dify との統合は、WorkflowEntry の差し替えと、Dify 専用の kairo デーモンで行う

- 状態: 承認
- 日付: 2026-10-05
- 関連: ADR 0023, 0026, 0029, 0033, 0034, 0036 / ADR 0028 の「Dify 側のアダプタ」

## 背景

ADR 0028 と 0036 で、Dify のワークフローの実行を kairo に移す方針を決めた。

- ノードは Python のワーカーで実行する。
- Dify 側のアダプタが、kairo のトレースから graphon のイベントを組み立て、Dify の既存の下流をそのまま使う。

変換器、差分ハーネス、ワーカー SDK はできた。残っているのは、Dify の中のどこで差し替えるか、kairo とどうつなぐかである。

Dify 1.17.1 の実行の流れは、次のとおり（`docs/compat/dify.md` と、今回の調査による）。

- **streaming の実行:**
  - API のプロセスが Celery のタスクを投入し、Redis の topic を購読する。
  - Celery のワーカーの中で、実行ごとに 1 つのジェネレータが動く。ジェネレータは、キュー、タスクパイプライン、ResponseStreamFilter を持ち、子スレッドの runner が `WorkflowEntry.run()` のイベントを `_handle_event` で queue のイベントにする。
  - パイプラインが SSE の辞書を作り、topic に publish する。
- **graph のイベント以外に、下流が読むもの:**
  - レイヤーとパイプラインが、`GraphRuntimeState` を読む。読むのは `variable_pool`（システム変数、会話変数）、`outputs`、`total_tokens`、`llm_usage`、`node_run_steps`、`exceptions_count`、`start_at`。
  - pause の保存は、`graph_runtime_state.dumps()` を保存する。
  - ObservabilityLayer は、ノードのオブジェクトを受け取るフックを使う。
- **停止:** Redis の stop フラグとコマンドのリストで伝える。
- **human-input の再開:** フォームが送信されると、再開のタスクが pause の snapshot から再実行する。
- **ノードの生成:** DifyNodeFactory は、実行の文脈（`_dify`: tenant、app、user、invoke_from など。JSON にできる）と、プロセスの中の依存（Flask の文脈、ログインユーザー、設定、各種シングルトン）を使う。

## 決定

### 1. kairo 側: Dify 専用のデーモン `kairo-dify`

`compat/dify/cmd/kairo-dify` に置く。中身は次のとおり。

- kairo のエンジン
- ワーカープロトコルのサーバ（UNIX ソケットか TCP）
- Dify 向けの HTTP API

HTTP API:

| エンドポイント | 中身 |
|---|---|
| `PUT /v1/plans/{name}` | Dify のワークフロー（JSON）を変換して登録する。名前は Dify が決める（ワークフローの ID とその版のハッシュ） |
| `POST /v1/runs` | 実行を投入する。中身は、計画、入力、`sys`、会話変数、実行 ID（冪等キー、ADR 0023）、テナント、上限、深さ、入口。耐久化してから返る |
| `POST /v1/runs/{id}/signal` | human-input の回答（ADR 0036 の `SignalStep`） |
| `POST /v1/runs/{id}/cancel` | 取り消し（ADR 0026） |
| `GET /v1/runs/{id}/events?after=N` | その実行のトレースとチャンクを、N 番より後から順に流す（NDJSON。実行が終わると閉じる） |

- **実行ごとのイベントの流れ:** デーモンが実行イベントの出口（ADR 0034）を自分で購読し、実行ごとに番号を振ってメモリに持つ。持つのは実行が終わってから一定時間（既定 10 分）まで。
  - 購読者は、`after` で続きから読める（Celery のタスクの再試行や、human-input の後の再接続）。
- **認証:** 共有の秘密（`KAIRO_DIFY_API_KEY`）を Bearer で受け取る。デーモンは内部のネットワークだけで待ち受ける前提とし、TLS は前に置くプロキシに任せる。
  - ワーカーのソケットも、同じ鍵での Hello を要求する（ワーカープロトコルの Hello に `token` を足す）。

### 2. Dify 側: `WorkflowEntry` で差し替える

- 設定 `WORKFLOW_ENGINE: Literal["graphon", "kairo"] = "graphon"` を `WorkflowConfig` に足す。`kairo` のとき、`WorkflowEntry` は GraphEngine を作らず、`KairoGraphEngine` を作る。
- **`KairoGraphEngine`:** GraphEngine と同じ形（`run()`、`layer()`、`graph_runtime_state`）を持つ。
  - `run()` の手順:
    1. ワークフローを kairo に登録する（未登録なら）。
    2. 実行を投入する。実行 ID は Dify の `workflow_run_id`。
    3. その実行のイベントの流れを読む。
    4. 読んだものを graphon のイベント（ADR 0036）に組み立てて yield する。
  - レイヤーには、組み立てたイベントで `on_graph_start` / `on_event` / `on_graph_end` を呼ぶ。
  - `graph_runtime_state` は本物の `GraphRuntimeState` を使う。アダプタがトレースから、ノードの出力・変数の書き換え・使用量・ステップ数・出力を反映していく。
    - こうすることで、レイヤー、パイプライン、`dumps()` は変更なしで動く。
  - ObservabilityLayer の、ノードのオブジェクトを受け取るフックは呼ばない。ノードの span は、ワーカー側で張る（後述）。
- **runner、`_handle_event`、パイプライン、レイヤー、ResponseStreamFilter、SSE への変換は変えない。**
- **停止:** `KairoGraphEngine` は、イベントを待つ間も Dify のコマンドチャネル（stop フラグと Redis のコマンド）を見る。停止を見つけたら、kairo に `cancel` を送る。kairo が実行を終えると、`GraphRunAbortedEvent` を yield する。
- **human-input:**
  - kairo の実行が human-input の signal を待ったら、`GraphRunPausedEvent`（HitlRequired）を yield して終える。Dify の pause の保存はそのまま動く。保存されるのは、kairo の実行 ID と、読んだイベントの番号を含む状態。
  - フォームの送信で動く再開のタスクは、kairo に signal を送り、保存した番号の続きからイベントを読む。
- **ワーカー:** Dify のプロセスとして `flask kairo-worker` を足す。
  - 中身: `kairo_worker.Worker` と `GraphonNodeRunner` に、DifyNodeFactory を作る関数を渡す。
  - 実行の文脈（`_dify`）は、変換器がすべての `dify.*` ステップの入力に `$input.__dify` を足して渡す。ワーカーはそれを使って、Flask の文脈、ログインユーザー、ファイルのアクセス範囲を張り直してからノードを作る。
  - ObservabilityLayer の代わりに、ワーカーがノードごとに span を張る。

### 3. 範囲と段階

- **第 1 段階:**
  - 対象: workflow と chatflow の streaming の実行（Celery の経路）、human-input、停止。
  - 対象外: ノードの単体実行（`single_step_run`）、デバッガーの単体実行、トリガーと非同期の経路。これらは graphon のまま動かす。
- **第 1 段階の限界:**
  - Celery のタスク（イベントの読み手）が途中で落ちても、kairo の実行は続く。しかし、Dify の DB の実行記録はそこで止まる。
  - 今の Dify では実行ごと失われるので、悪くはならない。
  - 記録を欠かさないようにするのは第 2 段階とする。デーモンの購読を、Dify 側の記録の完了を待って ack する形にする（別の ADR で決める）。
- **作業場所:**
  - Dify の変更: Dify のリポジトリの 1.17.1 から切ったブランチ（`feat/kairo-engine`）で行う。
  - kairo 側（デーモン、HTTP API、ワーカープロトコルの認証）: このリポジトリで行う。

### 4. Celery（graphon）の経路との比較

統合ができたら、同じ Dify で `WORKFLOW_ENGINE` だけを切り替えて、次を比べる。結果はこの ADR に追記する。

- **ワークフロー:**
  - LLM を模擬サーバ（OpenAI 互換、遅延を固定し、ストリームで返す）に向けたもの
  - 分岐・反復・human-input を含むもの
- **測る値:**
  - 1 実行の遅延: 投入から最初の SSE まで、投入から終わりまで
  - 同時に N 実行（N = 10, 100, 1000）を流したときの処理量と遅延の分布
  - Celery のワーカーのプロセスとスレッドの数、メモリ、CPU
  - kairo 側のメモリと CPU
- **同じ結果になること:** 両方の経路で、SSE のイベント列と、DB に残る実行記録（ノードの実行、出力、状態）が同じになることを確かめる。

## 結果

- Dify の下流（UI、SSE、実行記録、会話、pause の保存）を変えずに、実行を kairo に移せる。設定ひとつで graphon に戻せる。
- Dify の Celery のタスクは、ノードを実行しなくなる。kairo のイベントを読んで下流に流すだけの軽い仕事になる。ノードは、kairo の割り当て（RPM / TPM、テナント間の公平、ADR 0008）のもとで、ワーカーが実行する。
- 悪くなること:
  - 実行ごとの Celery のタスク（とそのスレッド）は、第 1 段階では残る。待っている実行（human-input）は、pause で手放すので残らない。
  - デーモンが、実行ごとのイベントをメモリに持つ。終わった実行は、一定時間で捨てる。
  - Dify と kairo の間に、HTTP のホップが 1 つ増える。
- テスト:
  - kairo 側: デーモンの HTTP API のテスト。認証、冪等な投入、イベントの続きからの読み出し、取り消し、signal。
  - Dify 側:
    - `KairoGraphEngine` の単体テスト: 組み立てたイベントの列を、差分ハーネスの graphon の列と比べる（ADR 0036 の第 1 段階の比較）。
    - Dify の既存の workflow のテスト（table runner など）を、`WORKFLOW_ENGINE=kairo` でも流す。

## 検討した代替案

- **Celery のタスクをやめ、イベントの読み手を常駐のプロセス 1 つにまとめる:** 実行ごとの状態（パイプライン、ResponseStreamFilter）を常駐のプロセスが持つことになる。Dify の生成器とパイプラインの作り（実行ごとにオブジェクトを作る）を大きく変える必要がある。第 2 段階以降の検討とする。
- **変換器を Dify の中（Python）に移植する:** 差分ハーネスで確かめた Go の変換器と、二重に持つことになる。
- **kairo の汎用のデーモン（kairod）に Dify 向けの API を足す:** 本体のモジュールは `compat/dify` を import できない（依存の向き、ADR 0014・0018）。Dify 専用のデーモンを `compat/dify` に置く方が素直。

## 実装で詰めた詳細（2026-10-05 追記、ADR 0017）

### kairo 側

- **ワーカーの認証。** ワーカープロトコルの Hello に `token` を足した（省略可能なフィールドなので、古いワーカーも読める）。`protocol.Server.Token` が設定されていれば、一致しない Hello の接続を、応答せずに切る（定数時間の比較）。Go と Python のワーカーに `Token` / `token` を足した。→ `TestWorkerToken`、`test_hello_carries_the_token`
- **デーモン（`compat/dify/daemon`、`cmd/kairo-dify`）。**
  - 計画の名前: Dify が付けた名前に、変換器の版（`dify.ConverterVersion`）を足した名前で登録する。変換器が変わると、同じワークフローでも変換し直す。
  - イベントの番号: `<デーモンの起動ごとの乱数>.<番号>`。別の起動の番号を渡されたら、今あるイベントの先頭から返す。
  - 保持の期間を過ぎた実行や、再起動の前に終わった実行のイベントを読むと、`RunInfo` から作った `run_end` を 1 つ返す。
  - 第 1 段階の ack: イベントの出口のエントリは、実行ごとのバッファに入れた時点で ack する。デーモンが再起動すると、まだ読まれていないイベントは失われる（第 1 段階の限界のとおり）。
  - API の鍵（`KAIRO_DIFY_API_KEY`）がなければ起動しない（`-insecure-no-auth` は試験用）。ワーカーの鍵（`KAIRO_WORKER_TOKEN`）の既定は API の鍵。UNIX ソケットは 0600 にする。
- **secret の扱い。**
  - 環境変数の値は計画に入れない。実行の入力 `__env` で渡す（`dify.EnvInput`）。そうしないと、secret 型の値が、デーモンが保存する計画のファイルに平文で残る。→ `TestEnvironmentValuesAreNotInThePlan`
  - 実行の入力（secret を含む）は WAL に入る。`-keys` で、データディレクトリのすべてを暗号化できる（ADR 0021 の鍵。JSON のファイルで渡す）。
- **変換器。**
  - `dify.*` のステップには、セレクタの入力に加えて `__dify`（Dify の実行の文脈と `workflow_id`）と `__sys`（すべてのシステム変数）を渡す。
  - `dify.*` の仕様の試行回数を 1 にした。graphon は、ノードの再試行が有効なときしか再試行しない。以前はエンジンの既定で再試行していた（実際の Dify との比較で見つかった）。
  - human-input の期限切れの分岐は、kairo の待機の期限切れに加えて、Dify の期限切れ（signal のハンドルが `__timeout`）でも選ぶ。
- **コアのトレース。** map と loop の開始のトレースに、入力（map の対象のリスト、loop の変数の初期値）を入れた。ADR 0034 の決定どおり。状態とログの形式は変わらない。→ `TestTraceOfMapLoopAndVars`、`TestTraceOfLoopStartInput`
- **ワーカー SDK（`GraphonNodeRunner`）。**
  - ノードが実行中に出すイベント（チャンク、推論、検索結果、エージェントのログ、ポーリングの進み）は、ライブの出力として `{"event": <クラス名>, "data": {...}}` の JSON で送る。
  - 使用量（`llm_usage`）をメタデータに入れる。
  - ノードの実行 ID は `uuid5(run_id, step_id)`。アダプタも同じ式で求めるので、ワーカーの中で書かれた記録（エージェントのログなど）とイベントの ID がそろう。
  - ファイルの値（`dify_model_identity` を持つ辞書）は graphon の `File` に戻す。

### Dify 側（ブランチ `feat/kairo-engine`）

- **場所。** `api/core/workflow/kairo/` に、`client.py`（kairo-dify の HTTP クライアント）、`synthesizer.py`（イベントの合成）、`engine.py`（`KairoGraphEngine`）、`worker.py`（ノードのワーカー）を置いた。`flask kairo-worker` を足した。`WorkflowEntry` は `engine` 引数で切り替える。runner は `WORKFLOW_ENGINE` を見る。再開する実行は、一時停止したときのエンジンで動かす。
- **合成（graphon の engine と同じ状態の変化）。**
  - ノードの出力を変数プールに入れる。応答の出力をまとめ、使用量を足し、ステップ数と失敗の数を数える。
  - 辺の通過とスキップは、graphon の EdgeProcessor と SkipPropagator をそのまま移植し、グラフの上で計算する。kairo のスキップのトレースには頼らない。iteration と loop の中は、graphon が回ごとに別の frame を作るのに合わせて、回ごとに状態を持つ。
  - ステップ数はトップレベルのノードだけ数える。iteration と loop の中の使用量はコンテナに足し、コンテナが終わったときに実行に足す。
  - iteration と loop の入力、出力、メタデータ（回ごとの所要時間、loop の変数の表、終わった理由）は graphon と同じ形にする。loop の中の応答の出力は、回の終わりに実行の出力へまとめる。iteration の中の変数の書き換えは、実行の変数プールに入れない（graphon は回ごとにプールの写しで動く）。
  - kairo が自分で実行する純粋なノード（start、end、answer、if-else、variable-aggregator、list-operator）の `inputs` と `process_data` は、アダプタが同じ変数プールの上でそのノードの実装を呼んで得る。出力と分岐の向きは kairo のものを使う。トップレベルのノードだけが対象で、変数を書き換える assigner は対象外。assigner は、変数の書き換えのトレースから graphon と同じ `process_data` を作る。
  - 実行が失敗したときのメッセージは、失敗したノードのエラーにする。取り消しは `Aborted: <理由>` にする（どちらも graphon と同じ）。
- **停止。** 実行を読んでいる間、コマンドチャネル（Redis の stop フラグとコマンド）を 0.1 秒ごとに見る。中止の命令が来たら、kairo に取り消しを送る。
- **human-input。**
  - kairo の待機が始まったら、アダプタが Dify の human-input ノードの実装を実行する。これでフォームが作られ、一時停止が要求される。
  - 実行中のほかのノードが終わるまで読み続けてから、一時停止のイベントを出して読むのをやめる（graphon も、実行中のノードが終わってから止まる）。
  - kairo の実行 ID と、読んだイベントの番号は、変数プールの `__kairo.resume` に入れる。プールは、Dify の一時停止の保存に含まれる。
  - 再開すると、アダプタがノードを実行し直す。送信された（または期限切れの）結果を、kairo に signal として送る。そのノードの終わりのイベントは、kairo の待機が終わったところで出す。
  - 第 1 段階では、iteration と loop の中の human-input は扱わない。

### 実際の Dify での確認（2026-10-05）

- 動かした環境: 手元の Docker の Dify 1.17.1。api と Celery のワーカーだけを、ブランチのソースを載せた同じイメージに差し替えた。kairo-dify は Linux のコンテナで動かした。
- Dify のテスト用のワークフローのうち、LLM、tool、human-input、question-classifier を使わない 19 件と、追加の 3 件（停止、human-input、iteration）で確かめた。対象は、サービス API の SSE と、DB に残るノードの実行の記録。graphon と kairo で、すべて同じになった。
  - 比べるときは、ID、時刻、所要時間を除いた。
  - 次の違いは比較から外した。並列の枝のノードの順序（ADR 0036 のとおり集合として比べる）と、サンドボックスが付ける一時ファイル名。
- まだ確かめていないもの:
  - LLM のストリーミング。この環境の LLM は実際の OpenAI の設定しかなく、費用がかかる。
  - tool、agent、knowledge-retrieval。
  - ファイルを入力にする実行。

### Celery（graphon）との比較の結果（2026-10-05、4 節）

- **条件:**
  - 上の確認と同じ環境（Docker Desktop、macOS、arm64）で測った。
  - Celery のワーカーは Dify の既定の設定（gevent、`-c 4`）のまま。
  - 負荷をかけたクライアントは、api のコンテナの中で動かした。
  - 測定中のマシンの load average は 28〜52 と高く、絶対値はぶれる。同じ時間帯の相対の比較として読む。
- **ワークフロー（LLM と外部への HTTP は使っていない）:**
  - native: kairo が自分で実行する 5 つのノード（start、if-else 2 つ、variable-aggregator、end）
  - iter: list-operator のあとに、20 回の iteration（中は if-else）
  - code: サンドボックスの code ノード 1 つ
- **1 実行の所要時間（中央値 / 95 パーセンタイル、ms）と処理量:**

  | ワークフロー | 同時実行数 | graphon | kairo |
  |---|---|---|---|
  | native | 1 | 454 / 552 | 51 / 68 |
  | native | 4 | 442 / 572 | 174 / 221 |
  | native | 64 | 1,158 / 26,898（8.1 回/秒） | 445 / 9,835（18.0 回/秒） |
  | iter | 1 | 3,009 / 3,384 | 140 / 177 |
  | iter | 4 | 2,853 / 3,585 | 483 / 765 |
  | iter | 64 | 7,169 / 170,516（1.4 回/秒） | 1,039 / 24,590（8.7 回/秒） |
  | code | 1 | 274 / 394 | 58 / 78 |
  | code | 4 | 251 / 544 | 130 / 333 |
  | code | 64 | 668 / 21,682（9.1 回/秒） | 362 / 8,637（20.0 回/秒） |

  すべての実行が成功した。

- **読み取れること:**
  - graphon では、1 実行の大半が graphon の GraphEngine 自身の時間だった。ノードがほぼ何もしない native でも、1 実行に約 450ms かかる。iteration は 1 回あたり約 150ms かかる。kairo では、それぞれ約 50ms と約 7ms になった。
  - 同時実行数を上げると、どちらも処理量が頭打ちになる。原因は Celery の同時実行数（4）。第 1 段階でも、1 実行が Celery のタスクを 1 つ占めるため（イベントを読んで Dify の下流に流す）。
  - kairo の場合、同時実行数 64 のとき、Celery のプロセスの CPU が平均 70〜86%（最大 98%）で張りついていた。kairo-dify は平均 2〜5%、ノードのワーカーは 0〜3% だった。
    - 残っているボトルネックは、Dify の Python 側（イベントの合成、キュー、タスクパイプライン、SSE と DB への書き込み）。
    - 第 2 段階（Celery のタスクをやめ、イベントの読み手を常駐させる）を検討する根拠になる。
  - graphon の測定では、CPU とメモリの記録が取れなかった（記録のスクリプトの不具合）。資源の比較は、取り直してから追記する。

### Celery なしでの再計測（2026-10-06）

ADR 0038・0039 の実装後に取り直した。前回の測り方と違う点は次の 3 つ。

- 負荷をかけるクライアントは、api とは別のコンテナで動かした。
- CPU は、各コンテナの cgroup の累積値（`cpu.stat` の `usage_usec`）を測定の前後で引いて、1 実行あたりに割った。
- graphon は Celery の同時実行数を 2 通りで測った。`-c 4`（この環境の設定）と `-c 64` で、設定値のせいで差がついていないかを見るため。

load average は 13〜22 だった。

- **1 実行の所要時間（中央値 ms）と処理量（回/秒）:**

  | ワークフロー | 同時実行数 | graphon `-c 4` | graphon `-c 64` | kairo（Celery なし） |
  |---|---|---|---|---|
  | native | 1 | 456 | 469 | 49 |
  | native | 64 | 1,211（8.0 回/秒） | 583（14.5 回/秒） | 405（19.4 回/秒） |
  | iter | 1 | 2,837 | 2,859 | 184 |
  | iter | 64 | 7,530（1.35 回/秒） | 3,871（2.65 回/秒） | 1,493（6.4 回/秒） |
  | code | 1 | 183 | 175 | 54 |
  | code | 64 | 652（11.6 回/秒） | 505（16.5 回/秒） | 325（23.1 回/秒） |

- **1 実行あたりの CPU（ms、同時実行数 64）:**

  | ワークフロー | 構成 | api | 実行側（Celery／ノードのワーカー＋kairo-dify） | DB | 合計（サンドボックスを除く） |
  |---|---|---|---|---|---|
  | native | graphon `-c 64` | 9 | 47 | 18 | 75 |
  | native | kairo | 8 | 41 + 3 | 19 | 72 |
  | iter | graphon `-c 64` | 21 | 214 | 76 | 320 |
  | iter | kairo | 18 | 153 + 3 | 71 | 249 |
  | code | graphon `-c 64` | 10 | 43 | 15 | 70 |
  | code | kairo | 7 | 34 + 3 | 13 | 58 |

- **メモリ（測定中の最大）:** Celery のワーカー（約 460MiB）と kairo のノードのワーカー（約 450MiB）はほぼ同じ。kairo では kairo-dify が 30〜100MiB 増える。DB は graphon より 20〜40MiB 多い（下流のスレッドが DB の接続枠いっぱいまで接続を開くため）。
- **読み取れること:**
  - 1 実行の所要時間は、kairo が 3.5〜15 倍短い。Celery の設定によらない。
  - 処理量の差は、前回（2.2〜6 倍）の一部が Celery の `-c 4` によるものだった。`-c 64` と比べると 1.3〜2.4 倍になる。
  - 1 実行あたりの CPU は、kairo が 4〜22% 少ない。DB の CPU はほぼ同じで、DB への書き込みの量は変わっていない。
  - kairo の処理量は、同時実行数 1 から 64 までほとんど伸びない。ノードのワーカーの CPU は、iter で 0.98 コア、native で 0.79 コアだった。1 つの Python プロセス（GIL）が上限になっている。DB は 0.4 コア程度で、まだ余裕がある。
    - ADR 0039 の「DB を上限にする」には、まだ届いていない。ノードのワーカーを複数のプロセスに増やせば（区画は ADR 0038 のとおり、ワーカー間で分けられる）、次の上限が見えるはず。

### プロセスを増やしたときの処理量（2026-10-06）

1 プロセスの計測で、ノードのワーカー（Python の 1 プロセス）が上限だったため、プロセス数を増やして測った。

- graphon は Celery のワーカーを同じ数だけ増やした（各 `-c 64`）。
- DB の接続枠に収めるため、各プロセスの接続枠を 30（8 プロセスでは 15）にした。

処理量（回/秒、同時実行数 128。1 プロセスの列は同時実行数 64）:

| ワークフロー | graphon 1 | graphon 4 | kairo 1 | kairo 4 | kairo 8 |
|---|---|---|---|---|---|
| native | 14.5 | 17.0 | 19.4 | 36.2 | 44.6 |
| iter | 2.65 | 3.4 | 6.4 | 13.0 | 18.9 |
| code | 16.5 | 28.9 | 23.1 | 38.3 | — |

- kairo は、プロセスを増やすと処理量が伸びる（1 → 8 で native 2.3 倍、iter 3.0 倍）。
- graphon は、Celery を 4 倍にしても伸びが小さい（native 1.2 倍、iter 1.3 倍）。Celery の各プロセスの CPU は 0.17〜0.26 コアで、CPU を使い切っていない。graphon の 1 実行は、処理より待ちに時間を使っている（iter は負荷によらず 1 実行が約 2.8 秒）。
- kairo の 8 プロセスで、DB の CPU は約 1 コアだった（4 プロセスでもほぼ同じ）。このマシンは他のプロセスで load average が 15〜25 あり、どこが次の上限かは、この環境では切り分けられなかった。
- code の同時実行数 128 では、両方で 1 件ずつ、サンドボックスが `operation not permitted` で失敗した。エンジンによらない。
