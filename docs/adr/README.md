# ADR（Architecture Decision Records）

設計判断を1判断1ファイルで記録します。運用のルールは [0000](0000-record-decisions.md) にあり、書式は [template.md](template.md) です。判断を変えるときは既存のファイルを書き換えず、新しい ADR で置き換えます。

| # | 決定 | 状態 |
|---|---|---|
| [0000](0000-record-decisions.md) | 設計判断を ADR として記録する | 承認（0017 で補足） |
| [0001](0001-pure-transition-core.md) | 状態遷移は I/O のない純粋関数とし、時刻はイベントで渡す | 承認 |
| [0002](0002-shard-per-core-event-loop.md) | 1コア1シャードのイベントループが実行を所有し、ロックを持たない | 承認 |
| [0003](0003-typed-effects-undeclared-is-real.md) | 作用に型を付け、宣言のないものは「実」として扱う | 承認 |
| [0004](0004-output-commit-for-real-commands.md) | 状態は先に適用し、外に出る作用だけを耐久化まで保留する（出力コミット） | 承認 |
| [0005](0005-per-shard-append-only-log.md) | 耐久性ログはシャード×ティアごとの追記ログとし、group commit する | 承認（一部を 0016 で置き換え） |
| [0006](0006-ir-serialization-json.md) | IR の直列化は JSON とし、計画はノード仕様と組にして凍結する | 承認 |
| [0007](0007-no-replication-in-v0.md) | 複製は v0 に含めず、Sink の差し替えで後から足せるようにする | 承認 |
| [0008](0008-blob-threshold-and-typed-fields.md) | 16 KiB を超える出力はブロブにし、型付きフィールドは状態に残す | 承認 |
| [0009](0009-worker-protocol-framed-json.md) | ワーカープロトコルは長さ付きフレーム＋JSON で、pull とクレジット制にする | 承認 |
| [0010](0010-hierarchical-timing-wheel.md) | タイマーは階層タイミングホイールで持ち、OS タイマーは1本だけ | 承認 |
| [0011](0011-snapshot-eviction.md) | 待つだけの実行はスナップショットにしてメモリから外す（耐久化済み LSN まで） | 承認 |
| [0012](0012-scheduler-single-owner.md) | スケジューラはトークンバケットとテナント間ラウンドロビンで、単一 goroutine が所有する | 承認 |
| [0013](0013-branch-only-on-typed-fields.md) | 条件分岐は bool / int / number / enum の型付きフィールドに限る | 承認 |
| [0014](0014-stdlib-only.md) | ランタイムは標準ライブラリだけで作る | 承認 |
| [0015](0015-shape-values-with-protected-steps.md) | 値の整形は保護ステップ（pass / append）で行い、IR の構成要素を増やさない | 承認 |
| [0016](0016-log-compaction.md) | ログはチェックポイントとセグメント単位の退役で圧縮する | 承認 |
| [0017](0017-approve-adr-before-implementation.md) | ADR は実装に入る前に承認を得る | 承認 |
| [0018](0018-optional-sqlite-backend.md) | 保存先の既定はファイル形式とし、SQLite は別モジュールの任意の実装として試す | 承認 |
| [0019](0019-no-atomic-storage-interface.md) | 保存先インターフェースは変えない（SQLite の第 2 段階は行わない） | 承認 |
| [0020](0020-pluggable-sql-backends.md) | 指定があれば SQL データベース（PostgreSQL、MySQL、TiDB、Oracle、SQLite）に保存できるようにする | 承認 |
| [0021](0021-encrypt-and-authenticate-stored-data.md) | 保存するログとスナップショットを、任意で暗号化し改ざんを検知する | 承認 |
| [0022](0022-blob-dir-hashed-names.md) | blob.Dir は、オブジェクト名をハッシュしたファイル名で保存する | 承認 |
| [0023](0023-durable-idempotent-submit.md) | Submit は開始が耐久化してから返し、RunID を冪等キーとして扱う | 承認 |
| [0024](0024-per-run-blob-collection.md) | ブロブは実行ごとにまとめて保存し、実行の終了時に消す | 承認（SQL 側は 0025 で置き換え） |
| [0025](0025-binary-collation-and-prefix-group-delete.md) | 名前は大文字小文字を区別してバイト順で比較し、MySQL / TiDB には VARBINARY で手当てする | 承認 |
| [0026](0026-cancel-running-tasks.md) | 中断した手順は、実行中のタスクにも取り消しを届ける | 承認 |
| [0027](0027-durable-finished-markers.md) | 終わった実行の記録を期限つきで永続化し、再起動をまたいで冪等にする | 承認 |
| [0028](0028-dify-workflow-backend.md) | Dify のワークフロー実行を置き換えられるエンジンにする（ノードは Python ワーカーで動かす） | 承認 |
| [0029](0029-graph-execution-model.md) | 任意のグラフ（DAG）を実行の基本形にする（木の構成要素はグラフに変換する） | 承認 |
| [0030](0030-error-strategies-and-run-limits.md) | ノードごとのエラー処理（retry / fail-branch / default-value）と、実行の上限を持つ | 承認 |
| [0031](0031-dify-conditions-and-node-handles.md) | Dify の条件は組み込みの保護ステップで評価し、分岐のハンドルはノードの設置ごとに宣言できるようにする | 承認 |
| [0032](0032-iteration-and-loop-semantics.md) | map と loop を、Dify の iteration と loop の意味論で動かせるようにする | 承認 |
| [0033](0033-variables-and-assignment.md) | 書き換えられる変数（loop 変数と会話変数）を持ち、代入は組み込みの保護ステップで行う | 承認 |
| [0034](0034-execution-event-feed.md) | 欠落しない実行イベントの出口を、ログから決定的に導いて作る | 承認 |
| [0035](0035-dify-node-effects-and-unknown-outcomes.md) | Dify のノードに作用の型を割り当て、結果が不明な実の命令は、明示した設置に限って「失敗」として扱えるようにする | 承認 |
| [0036](0036-dify-adapter-synthesizes-graphon-events.md) | Dify 側のアダプタは、kairo のトレースから graphon のイベントを合成し、Dify の既存の下流をそのまま使う | 承認 |
| [0037](0037-dify-integration-plan.md) | Dify との統合は、WorkflowEntry の差し替えと、Dify 専用の kairo デーモンで行う | 承認 |
| [0038](0038-dify-without-celery.md) | Dify のワークフローの実行から Celery をなくし、仕事の配布は kairo が担う。枠の単位は実行ではなくノードの実行にする | 承認 |
| [0039](0039-concurrency-from-constraints.md) | 同時実行数は固定値で決めず、外部の制約、手元の資源、下流の容量から決める | 承認 |
| [0040](0040-downstream-that-cannot-finish.md) | Dify の記録を書けない実行は、やり直したうえで失敗として記録し、ack する | 承認 |
| [0041](0041-per-run-cost.md) | 実行 1 回あたりのコストは、その実行に要る仕事だけにする | 承認 |
| [0042](0042-n8n-integration-plan.md) | n8n との統合は、n8n の engine v2 の「データプレーン」を kairo で置き換える形で行う | 承認 |
| [0043](0043-output-ports-and-batches.md) | 出力の口が複数あるステップと、切り出しながら回すループを IR に足す | 承認 |
| [0044](0044-run-end-notice-to-workers.md) | 実行の終わりを、希望したワーカーに知らせる | 承認 |
| [0045](0045-step-result-that-waits.md) | ステップの結果として「期限まで待ってから、この出力で終わる」を返せるようにする | 承認 |
| [0046](0046-run-affinity-for-workers.md) | 実行ごとの状態を持つワーカーには、同じ実行の仕事をなるべく同じ接続に渡す | 承認 |
| [0047](0047-n8n-node-effects-by-type.md) | 外に作用しない n8n のノードは、「実」でなく「非保護」のステップにする | 承認 |
| [0048](0048-distribution-as-bundled-binary.md) | kairo は、kairod のバイナリを同梱した言語ごとのパッケージとして配る | 0051 により置き換え |
| [0049](0049-sdk-graph-and-code-apis.md) | SDK は、グラフの API と、コードで書くワークフローの API の 2 段で用意する | 承認 |
| [0050](0050-keep-outputs-of-finished-runs.md) | kairod では、指定した実行は、終わった後も出力を冪等の期間のあいだ残す | 承認 |
| [0051](0051-no-resident-runtime.md) | kairo は常駐するサーバーを前提にしない: コアを利用者のプロセスに埋め込み、DB だけを使う | 承認 |
| [0052](0052-http-actions.md) | アクションを HTTP(S) で呼べるようにする（結果はその場か、後からコールバックで受ける） | 承認 |
| [0053](0053-scheduler-entry-points.md) | サーバーレスのランタイムを、スケジューラーの定期実行か、次の時刻を知らせる一度きりの予約で起こす | 承認 |
| [0054](0054-remove-finished-runs-embedded.md) | 埋め込みのランタイムは、終わった実行を、それを含む実行ごとまとめて、冪等の期間の後に消す | 承認 |
| [0055](0055-jev-typed-decisions.md) | 型付きの判断（TypeSafe Jev）を、別モジュールのアクションとして、kairod と埋め込みの両方から使えるようにする | 承認 |
| [0056](0056-time-budgets-are-measured-not-tested.md) | 時間の予算は、テストでも CI でも判定しない: 決まったマシンで測り、変更の前後で比べる | 承認 |
| [0057](0057-distribution-and-releases.md) | 配布の形: SDK は kairo-sdk として npm と PyPI に、kairo.wasm を同梱し、タグからの 1 本のリリースで出す | 承認 |

0001〜0015 は v0 の実装時に下した判断を後からまとめて起票したもので、2026-10-01 に承認されました。
