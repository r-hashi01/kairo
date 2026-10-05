# kairo — LLM ワークフローランタイム（v0）

ランタイム自身が性能のボトルネックにならないことを目標にした、組み込み可能な Go 製ワークフロー実行エンジンです。本体は外部依存なしで、標準ライブラリだけで動きます。保存先の既定はファイル形式です。

指定があれば、SQL データベースにも保存できます。対応しているのは SQLite、PostgreSQL、MySQL、TiDB、Oracle です（ADR 0020）。それぞれ製品ごとの別モジュール（`store/...`）になっているので、組み込み側には、選んだ製品のドライバだけが依存に加わります。

保存データは、鍵を渡せば暗号化し、改ざんを検知できます（`engine.Config.Keys`、ADR 0021）。

- 実行は「値」として扱います。待機中の実行が消費するのは、状態のバイト数だけです。
- 作用に型を付けます（非保護 / 保護 / 実 / 待機）。耐久化・リトライ・冪等性の扱いは、この型から決まります。
- 書き込みは3系統に分けます。耐久性ログ、可観測性ストリーム、ライブストリームです。

## 実測値（Apple M4 Max / 14 コア、`go test` 上）

| 項目 | 目標 | 実測 | 測定 |
|---|---|---|---|
| 1遷移あたりのコスト | ≤ 20 µs | 約 1 µs | `BenchmarkTransition` |
| 1実行あたりのランタイム CPU（5ノード、外部待ちゼロ） | ≤ 1 ms | 約 50 µs（ベンチの計測用 goroutine を含む） | `BenchmarkFiveNodeRun` |
| スループット（同上） | — | 約 9万実行/秒 | 同上 |
| ノード間の受け渡し遅延 | p99 ≤ 1 ms | p50 60 µs / p99 0.5 ms | `TestHandoffLatency` |
| 待機中の1実行あたりメモリ | ≤ 20 KB | メモリ保持時 1.7 KB / 退避時 0.9 KB | `TestWaitingRunMemory`（10万件） |
| 耐久性 ack 遅延（group commit、fsync あり） | p99 ≤ 5 ms | p99 約 10 ms（macOS） | `TestFileCommitLatency` |

耐久性 ack は未達です。macOS の `File.Sync` は `F_FULLFSYNC`（ドライブキャッシュまでフラッシュ）を使うため遅くなります。Linux の NVMe で fdatasync を使えば 1〜2 ms 程度が見込めますが、まだ測っていません。100万件保持（32コア / 128GB）の規模試験も未実施です。

## 構成

```
ir/          実行計画（IR）。定義 JSON、ノード仕様（作用の型）、コンパイラ
core/        純粋な状態遷移コア (状態, イベント) → (新状態, 命令)。スナップショットの符号化
engine/      シャード（1コア1イベントループ）、耐久化の順序制御、退避・復元、リカバリ
sched/       アドミッション制御、テナント間ラウンドロビン、宛先ごとの RPM/TPM
wal/         シャードごとの追記ログ、group commit、差し替え可能な Sink
blob/        内容ハッシュで参照するブロブ、原子的な置き換え（スナップショット用）
timerwheel/  階層タイミングホイール（挿入・取消 O(1)、空なら OS タイマーを張らない）
obs/         可観測性ストリーム（非同期・バッチ・溢れたら捨てる）
live/        ライブストリーム（トークン中継。記録もイベント化もしない）
protocol/    ワーカープロトコル（UNIX ソケット / TCP、pull 型、クレジット制）と Go 版 SDK
executor/httpexec/  組み込み HTTP 実行器（HTTP/2 多重化、SSE 中継）
api/         デーモンの HTTP API
cmd/kairod/  デーモン
```

### 1つの実行の流れ

1. `Submit` を受けると、アドミッション制御を通して耐久性ティアを決めます（実の作用を含む計画は最低 `file`）。実行 ID のハッシュで担当シャードに投入します。
2. シャードのループが `core.Apply` を呼び、イベントを適用します。出てきた命令を振り分けます。
   - 非保護の命令は、すぐディスパッチャへ渡します。クラッシュしても再実行すれば済むためです。
   - 実の命令は、まず「意図」をログに記録します。ログがそこまで耐久化されてから解放します（出力コミット）。
   - タイマーはタイミングホイールに登録します。
3. ディスパッチャが、宛先のクォータとテナント間の公平性に従って、取りに来たワーカーへ命令を渡します。
4. 結果は `Engine.Complete` で受け取ります。大きい出力はワーカー側の goroutine でブロブへ移してから、シャードに戻します。
5. 待つだけになった実行は、スナップショットを取ってメモリから外します。イベントが届いたら復元します。

## 要件との対応

| 要件 | 実装 |
|---|---|
| FR1 IR | `ir/plan.go`、`ir/graph.go`。実行の基本形は任意のグラフ（DAG）で、構成要素は `graph`、コンテナの `map`・`loop`、`wait` と、葉の `step` です。`seq` / `par` / `cond` はコンパイル時にグラフへ書き換える糖衣です（ADR 0029。要件の「5つの構成要素」を変更）。条件分岐は型付きフィールド（bool / int / number / enum）だけに許可し、`text` はコンパイル時に拒否します。計画は一度だけコンパイルし、不変の `Plan` を全実行で共有します |
| FR2 状態遷移コア | `core/apply.go`。時刻はイベントに含めて渡します。map の走査順に依存しません。同じイベント列から同じバイト列の状態が得られることをテストしています。実行内の並列は「複数の命令が同時に出ている状態」として表します。シャードは実行 ID の FNV ハッシュで決め、再起動しても変わりません |
| FR3 命令と作用の型 | `ir.Effect` のゼロ値は `EffectReal` です。宣言のないアクションは実として扱います。命令は `run_id` / `step_id` / `attempt` / `idempotency_key` を持ちます。冪等キーは map の添字と loop の回数を含む `step_id` から作るので、試行をまたいでも変わりません |
| FR4 実行器プロトコル | `protocol/`。長さ付きフレームと JSON を使う pull 型で、クレジット制のフロー制御をします。ワーカーが切断したら、未送信の命令はキューに戻し、送信済みの命令は「結果不明」として扱います |
| FR5 スケジューラ | `sched/admission.go`（上限、待機キュー、溢れたら 429、テナント間ラウンドロビン）と `sched/dispatcher.go`（宛先ごとの RPM/TPM トークンバケット、テナント同時実行数、実際のトークン数での精算） |
| FR6 耐久性ログ | `wal/`。シャード×ティアごとの追記ログで、処理中に溜まった書き込みを1回にまとめる group commit です。`Sink` インターフェースが要求するのは「追記して、障害範囲の外から ack を返す」ことだけです。ファイル実装は起動のたびに新しいセグメントを作り、既存のバイトは書き換えません |
| 実の命令の順序ルール | 1. 意図を記録し、そこまで耐久化されてから解放します（`engine/shard.go` の `dispatch` と `hold`）。2. 結果はログに記録します。完了の通知もログが耐久化されてから返します。3. 結果不明のときは、`idempotent_retry` の宣言があれば同じキーで再試行し、なければ `blocked`（要確認）で止めます。`POST /v1/runs/{id}/resolve` で人が決着させます |
| FR7 可観測性 | `obs/`。有界チャネルに入れ、あふれたら捨てて数を数えます。送り先は `obs.Sink` で差し替えられます |
| FR8 ライブストリーム | `live/`。購読者ごとの有界バッファで、`Next` がまとめて返します。ログには残しません |
| FR9 タイマーと長時間待機 | `timerwheel/` と `engine/shard.go` の `maybeEvict` / `load`。ホイールが空なら OS タイマーを張らないので、待機中のシャードは一切起きません |
| FR10 組み込み形態 | ライブラリとしては `engine.New` → `RegisterPlan` / `RegisterExecutor` → `Start`。デーモンは `cmd/kairod`、他言語からは `protocol` を使います |

## 不変条件のテスト

| 不変条件 | テスト |
|---|---|
| 遷移の経路に同期 I/O がない | `core/purity_test.go`：import を許可リストで確認し、`time.Now` や `go` 文を静的に禁止しています |
| 実行ごとに goroutine を常駐させない | `TestNoGoroutinePerRun`：2万件の待機実行で goroutine 数が増えないことを確認します |
| ポーリングしない | `TestIdleEngineDoesNotWake`：待機実行と遠いタイマーを持った状態で、シャードの起床回数が 0 のままであることを確認します |
| 書き込みは追記のみ | `TestFileSinkAppendOnly`：途中で切れた末尾も含め、既存セグメントが変わらないことを確認します |
| 実の命令は耐久化されていない状態を根拠に発行されない | `TestRealCommandWaitsForDurableIntent`：ack を遅らせたログで、命令を受け取った時点に意図の記録が耐久化済みであることを確認します |
| 外部 I/O の間ロックを持たない | 構造で担保しています。シャードとディスパッチャは所有権モデルでロックを持たず、I/O はすべて別の goroutine が行って結果をメッセージで返します |
| 1実行のコスト = 状態のバイト数 + 命令数 | `TestWaitingRunMemory`、`BenchmarkTransition` |

race 検出器付きの `go test -race ./...` も通っています。

## 未決事項への v0 での判断

1. IR の直列化形式は JSON にしました。永続化するときは、定義とコンパイル時のノード仕様を組にした `ir.Frozen` を保存します。レジストリが後から変わっても、再起動後に同じハッシュの計画を復元できます。
2. 複製は v0 に含めていません。`Config.Sinks` で `TierReplicated` の Sink を差し込めば使えるインターフェースだけ用意しています。
3. ブロブにする境界は 16 KiB（`Config.BlobThreshold`）にしました。型付きフィールドは常に状態の中に残すので、ブロブ化されたあとも条件分岐に使えます。

## 使い方

```sh
go build -o kairod ./cmd/kairod
./kairod -data ./data -tier file

# http.* のアクションは組み込み HTTP 実行器が担当する
curl -XPOST localhost:8420/v1/nodes -d '{"action":"http.classify","effect":"unprotected",
  "outputs":{"label":{"type":"enum","values":["refund","other"]}},"destination":"openai"}'

curl -XPOST localhost:8420/v1/plans -d '{"name":"support","root":{"kind":"seq","nodes":[
  {"kind":"step","id":"classify","action":"http.classify","params":{"url":"https://..."},"input":{"text":"$input.text"}},
  {"kind":"cond","if":{"field":"classify.label","op":"eq","value":"refund"},
   "then":{"kind":"seq","nodes":[
     {"kind":"wait","id":"approve","signal":"approve","timeout":"48h"},
     {"kind":"step","id":"reply","action":"kairo.pass","input":{"by":"approve.payload.by"}}]}}]}}'

curl -XPOST localhost:8420/v1/runs -d '{"plan":"support","tenant":"acme","input":{"text":"refund please"}}'
curl -XPOST localhost:8420/v1/runs/<id>/signals/approve -d '{"by":"alice"}'
curl localhost:8420/v1/runs/<id>/wait
curl -N localhost:8420/v1/runs/<id>/stream     # SSE でライブ出力を受け取る
```

宛先ごとのクォータは `-limits limits.json`（`{"openai":{"RPM":500,"TPM":200000,"TenantConcurrency":50}}`）で指定します。外部ワーカーは `<data>/worker.sock` に接続します。Go からは `protocol.Worker` を使います。

## 開発

作業の手順・不変条件・変更の種類ごとの注意は [AGENTS.md](AGENTS.md) にまとめています（Claude Code は CLAUDE.md 経由で読み込みます）。

```sh
make check    # = scripts/check.sh：gofmt・vet・build・テスト・不変条件
make race     # 上記に race 検出を加える
make bench    # = scripts/bench.sh：非機能要件の予算判定（超えたら失敗）
make hooks    # pre-commit で check --quick を走らせる
```

`.claude/` には Claude Code 用の hooks（編集前のガード、編集後の gofmt / vet、終了前の検証）、スキル（`/verify`、`/bench`、`/core-change`）、レビュー用サブエージェント（`invariant-reviewer`）があります。CI は `.github/workflows/ci.yml` です。

## v0 の制約（まだやっていないこと）

- リカバリでは、残っているログ（圧縮後のもの）をメモリに読み込みます。ログ圧縮は ADR 0016 を参照してください。
- 実行中のタスクの取り消し（ADR 0026）は、実行器への依頼です。`ctx` に従わない実行器や、`Cancel` を知らない古いワーカーは、最後まで動きます（結果は無視されます）。
- シャード数はデータディレクトリごとに固定です（`SHARDS` ファイルで検査します）。
- gRPC ストリームは未実装です。UNIX ソケットと TCP のフレームプロトコルだけです。
- ループは do-while で、`max_iter` に達するとその時点の出力で終了します。前の反復の値は、上書きされるまで参照できます。
