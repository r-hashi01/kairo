# 0076. 運用指標は実行記録から分離し、有界 cardinality の非耐久な計数・分布として公開する

- 状態: 承認（2026-11-11）
- 日付: 2026-11-11

## 背景

kairo の耐久ログと snapshot は、実行を復元し、実の命令を安全に解放するための記録である。一方、開始確認の再試行、workflow claim の経路、WAL の ack、計画停止の Drain、worker 切断、結果不明による blocked は、運用者が過負荷や停止失敗を診断するための情報であり、同じイベント列から同じ実行状態を作るためには要らない。これらを耐久記録へ加えると、観測のために保存量とリプレイの入力を増やす。

既存の観測面は目的が異なる。実行イベント feed は購読者の ack まで保持する耐久的な出口であり、`obs` と SDK の `Observe` は run、step、action、error などを含む詳細なライブ観測である。詳細観測の値をそのまま metrics の label にすると、RunID、worker 名、action、error 文字列などによって series 数が実行数や入力に比例して増える。

運用上必要な境界も一つの process に集まっていない。耐久開始の window と WAL ack、workflow claim、blocked、worker 切断は kairod が知る。開始未確認を同じ RunID で確認し直す回数と、Drain 開始時の手元の task・workflow drive、Drain の完了は SDK が知る。どちらか一方だけを観測しても、開始から停止までの詰まりを区別できない。

観測のために shard loop や SDK の実行経路を待たせたり、run ごとの goroutine、polling、対応表を増やしたりしてはならない。無効時は、時刻の読み取り、割り当て、background goroutine を含む実行ごとの費用を増やさない必要がある。

## 決定

### 決定項 1（運用指標を非耐久な別の観測面にする）

開始、claim、WAL、Drain、worker 切断、blocked の運用指標は、core の状態、耐久ログ、snapshot、実行イベント feed へ記録しない。process 内で起きた境界から作る非耐久なライブ観測とし、process の再起動をまたぐ連続性は外部の監視先が持つ。

運用指標の欠落や exporter の失敗は、実行の成功、耐久 ack、worker credit、Drain の完了条件を変えない。実行の監査や個別 run の復元には、耐久ログ、実行イベント feed、詳細観測を使い、運用指標から復元しない。

### 決定項 2（同じ測定契約を、境界を所有する process が生成する）

kairod と各 SDK は、名前、種類、単位、属性をそろえた任意の運用測定の受け口を持つ。測定の種類は counter の増分、gauge の現在値、histogram の観測値の三つとする。受け口がない状態を既定とする。

kairod は耐久開始、WAL、claim、worker 切断、blocked を生成する。SDK は開始確認の再試行と Drain を生成する。同じ事象を両方で数えず、ネットワーク境界の両側でしか分からない値は別の名前にする。Go、Python、TypeScript の SDK は同じ意味の境界に同じ測定名と属性値を使う。

運用測定は既存の run・step の詳細観測と別の型にする。運用測定には RunID、payload、action、workflow、tenant、worker 名、URL、error 文字列を入れない。詳細観測を metrics に変換する利用者も、これらを label にしてはならない。

### 決定項 3（測定名、単位、属性の集合を公開契約にする）

次の名前と意味を公開契約とする。counter の単位 `{event}` は単調増加する事象数、gauge の単位 `{start}` は現在の開始数、histogram の値は個々の観測である。

| 測定名 | 種類・単位 | 有限な属性 | 意味 |
|---|---|---|---|
| `kairo_run_start_unconfirmed_total` | counter・`{event}` | `cause=durability_timeout\|transport_ambiguity` | 開始済みか確認できない応答を受けた回数 |
| `kairo_run_start_confirmation_retries_total` | counter・`{event}` | `cause=unconfirmed\|overloaded\|transport` | 同じ RunID で開始または確認を再送した回数 |
| `kairo_run_start_confirmation_seconds` | histogram・`s` | `outcome=confirmed\|cancelled\|failed` | 最初の開始要求から確認終了までの時間 |
| `kairo_workflow_claim_total` | counter・`{event}` | `path=direct\|scan`、`outcome=claimed\|empty\|cancelled\|failed` | run-id 直接 claim と候補走査の結果 |
| `kairo_workflow_claim_seconds` | histogram・`s` | `path=direct\|scan`、`outcome=claimed\|empty\|cancelled\|failed` | 各 claim 経路が結果を返すまでの時間 |
| `kairo_durable_start_inflight` | gauge・`{start}` | `shard`、`tier=memory\|file\|replicated` | log の ack を待つ開始の現在数 |
| `kairo_durable_start_queued` | gauge・`{start}` | `shard`、`tier=memory\|file\|replicated` | start admission で待つ現在数 |
| `kairo_durable_start_rejected_total` | counter・`{event}` | `shard`、`tier=memory\|file\|replicated` | run を作る前に過負荷で拒否した開始数 |
| `kairo_durable_start_queue_seconds` | histogram・`s` | `shard`、`tier=memory\|file\|replicated` | start admission の待ち時間 |
| `kairo_durable_start_inbox_seconds` | histogram・`s` | `shard`、`tier=memory\|file\|replicated` | admission 後に shard が扱うまでの時間 |
| `kairo_durable_start_wal_ack_seconds` | histogram・`s` | `shard`、`tier=memory\|file\|replicated` | shard が開始を扱ってから log の ack までの時間 |
| `kairo_drain_total` | counter・`{event}` | `outcome=completed\|timeout\|cancelled\|unsupported\|failed` | Drain 呼び出しの終了結果 |
| `kairo_drain_seconds` | histogram・`s` | `outcome=completed\|timeout\|cancelled\|unsupported\|failed` | Drain 開始から終了までの時間 |
| `kairo_drain_starting_tasks` | histogram・`{task}` | なし | Drain 開始時に worker が保持する task 数 |
| `kairo_drain_starting_drives` | histogram・`{drive}` | なし | Drain 開始時に process が駆動する workflow 数 |
| `kairo_worker_disconnect_total` | counter・`{event}` | `outstanding=none\|present` | worker 接続が切れた回数 |
| `kairo_worker_disconnect_tasks` | histogram・`{task}` | なし | 切断時に結果を確認できなかった task 数 |
| `kairo_run_blocked_total` | counter・`{event}` | `reason=real_outcome_unknown` | 実の命令の結果不明により run が blocked へ入った回数 |

`shard` はその data directory に固定された有限集合、`tier` とその他の属性は上表の列挙値だけを取る。新しい blocked 理由や outcome を足すときは、自由文字列を流用せず、意味と有限性を公開契約へ追加する。

### 決定項 4（counter と histogram の lifecycle を process 単位にする）

counter と histogram は、受け口を有効にした process の開始から終了までを一つの lifecycle とし、再起動で初期化する。gauge は観測した時点の現在値であり、process 終了後の値を保持しない。外部監視先は process を識別する resource 情報を持ち、再起動後の counter を前の値へ加算したものと誤認しない。

counter は一つの事象につき増分 1 を生成する。histogram は時間または個数の raw observation を一つ生成し、bucket の境界と集約期間は exporter が決める。時間は単調時計で測り、秒へ変換して公開する。wall-clock の値や時刻そのものは測定値にしない。

測定の受け口を途中で差し替えず、process の構築時に有効か無効かを決める。したがって、受け口が有効になる前の事象を後から補完せず、無効になった期間をゼロ件として解釈しない。

### 決定項 5（観測は実行経路を待たせず、無効時の費用を持たせない）

測定の生成は shard loop、worker protocol、SDK の step 実行と Drain を待たせない。外部 I/O と exporter の処理は実行境界の外で行う。受け口が遅い場合は有界な観測バッファから捨て、実行側へ背圧を返さない。捨てた数は既存の process stats から読めるようにする。

無効時は、既存の境界で既に得ている値を調べる一回の分岐だけを許し、運用指標のための時刻読み取り、割り当て、lock、goroutine、channel 送信を行わない。有効時も run ごとの goroutine、polling、RunID ごとの集約状態を持たない。測定を追加する変更は、同じマシンの変更前後で hot path の性能を計測する。

### この ADR で決定しないこと

- Prometheus、OpenTelemetry その他の監視製品へ変換する adapter と、認証を含む metrics endpoint は、各連携の実装仕様へ委ねる。この ADR の名前、種類、単位、属性は製品固有の名前へ変換する前の契約である。
- histogram の bucket、集約期間、SLO、alert の閾値、dashboard は運用設定へ委ねる。時間の閾値を kairo の正しさのテストにはしない。
- 個別の run、worker、tenant、action を追跡する方法は、既存の詳細観測と実行イベント feed に委ねる。
- 運用指標を再起動後も kairo 自身が保存すること、課金や監査の記録に使うことは、この ADR の範囲外とする。

## 結果

- 良い影響: 開始、claim、WAL、Drain、切断、blocked の詰まりを、耐久記録や個別 run の高 cardinality 値を増やさず区別できる。
- 良い影響: daemon と SDK のどちらが事象を所有するかが明確になり、同じ事象の二重計数を避けられる。
- 良い影響: 外部監視製品へ依存せず、Go、Python、TypeScript で同じ意味の測定を変換できる。
- 悪い影響: 非耐久で背圧を掛けないため、process 停止や観測先の遅延時には測定が欠落し得る。
- 悪い影響: 公開した測定名、単位、属性値は互換性を要し、後から自由に変更できない。
- 中立な影響: counter は再起動で初期化するため、期間をまたぐ値と rate の組み立ては外部監視先の責務になる。
- 新しく守る制約: 運用測定に非有界な値を含めない。観測失敗を実行結果へ反映しない。無効時に実行ごとの追加費用を持たず、有効時も実行経路を待たせない。

## 検討した代替案

- 実行イベント feed に運用指標を加える: ack まで保持する耐久経路へ運用上の時刻と再試行を混ぜ、購読者の遅延でログ退役まで止める。
- 既存の詳細観測だけを metrics に変換する: RunID、worker 名、action、error 文字列を label にする実装を誘発し、start admission の現在値や SDK の Drain 境界も一貫して表せない。
- kairod だけで全てを測る: SDK 内の確認再試行、Drain 開始時の workflow drive、利用者の取消を kairod から正確に区別できない。
- SDK だけで全てを測る: shard・tier ごとの start window、WAL ack、worker 切断後の結果不明、blocked 遷移を SDK から正確に知れない。
- 運用指標を WAL に記録する: 診断のための値をリプレイ入力と保存量へ加え、process-local な遅延を再起動後も正しさの記録として残してしまう。
- 既定で常時集約する: 観測を使わない process にも時刻読み取り、atomic 更新、buffer、goroutine の費用を課す。

## 関連する ADR

- ADR 0034 の「obs との関係」: 運用指標も非耐久だが、個別実行の詳細観測とは型と cardinality を分ける。
- ADR 0041 の「1. 原則」: 無効な運用指標のために、実行ごとの常駐費用を増やさない根拠。
- ADR 0056 の「何をどこで確かめるか」: duration は計測値として公開し、時間の閾値を正しさのテストにしない。
- ADR 0074 の決定項 4（遅延の寄与を境界ごとに観測する）: start admission、inbox、WAL、claim を別の測定にする根拠。
- ADR 0075 の決定項 3（能力応答と Drain の完了確認を分ける）: Drain の対応可否、完了、期限切れを異なる outcome として数える根拠。

## 参照

- [Issue 17: Expose operational metrics for start confirmation, claims, WAL latency and drain](https://github.com/r-hashi01/kairo/issues/17)（2026-11-11確認）: 必要な運用境界、cardinality、lifecycle、性能の受け入れ条件。
- [`sched.StartAdmission` の統計](../../sched/start_admission.go): shard・tier ごとの window、開始確認中、待ち、拒否、各待ち時間を現在保持する一次資料。
- [`Engine.Stats`](../../engine/engine.go): kairod が process-local な統計を公開する既存境界の一次資料。
- [kairod 接続時の開始確認と workflow claim](../../sdk/go/kairod.go): SDK が開始未確認を再送し、直接または待機 claim を要求する境界の一次資料。
- [`Kairo.Drain`](../../sdk/go/kairo.go): Drain 開始時の task と workflow drive、および完了・取消を SDK が知る一次資料。
- [worker 切断時の未確認 task 処理](../../protocol/server.go): kairod が切断と outstanding task 数を知り、結果不明として完了する境界の一次資料。
- [結果不明を blocked にする状態遷移](../../core/apply.go): blocked の現行理由が、冪等再試行を宣言していない実の命令の結果不明である一次資料。
