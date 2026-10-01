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

0001〜0015 は v0 の実装時に下した判断を後からまとめて起票したもので、2026-10-01 に承認されました。
