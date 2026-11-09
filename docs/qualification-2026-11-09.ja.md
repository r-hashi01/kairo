# kairod suspend の追加検証（2026-11-09）

GitHub Issue #12 の最初の実測です。対象は ADR 0069 までを含む未コミット差分、比較元は `00376a0`（v0.4.1）です。再現手順は `scripts/qualification/README.md` にあります。

## 複数 process の耐久予備試験

```sh
KAIRO_QUALIFY_PROCESSES=4 \
KAIRO_QUALIFY_RUNS=100 \
KAIRO_QUALIFY_SETTLE_SECONDS=15 \
scripts/qualify-kairod.sh
```

- 環境: Apple M4 Max / 14 CPUs、Go 1.27.2、Node 26.3.0、Python 3.14.5、macOS arm64
- 1 台の file tier kairod（fsync あり）
- Go・Python・TypeScript を各 4 process
- 各 process 100 workflows、合計 1,200 workflows
- 各 workflow: non-idempotent real action → 25ms timer → signal wait → 完了
- 結果: 1,200/1,200 完了、real action marker 1,200/1,200、重複 0、所要 109 秒
- kairod RSS: 開始 25,232 KiB、最大 1,207,024 KiB、負荷終了時 1,003,888 KiB、15 秒静置後 58,832 KiB
- 静置の終盤は 59,280 KiB が数秒続き、最後が 58,832 KiB だった。少なくともこの件数と時間では、仕事が止まった後も RSS が増え続ける挙動は見えなかった。

これは時間単位・日単位の soak の代わりではない。長時間版は同じ script の process / runs を増やし、raw log と RSS 時系列を指定した出力先へ残す。

## 総数と同時実行数を分けた追加ラウンド

最初の harness は `RUNS` 件を各 process で一斉に始めていたため、総数を 300 へ増やすと、長時間運転ではなく 3,600 workflow の起動 burst になった。この条件では claim request が候補全体を繰り返し走査し、次の結果になった。

- process ごとの in-flight 300（全体 3,600）: Go の claim が client timeout、Python が 300 秒の driver timeout。TypeScript 1,200件は完了。real action marker は 2,400件、RSS 最大 5,638,288 KiB、60秒静置後 13,424 KiB。
- process ごとの in-flight 25（全体最大 300）: 全3,600 real action は1回ずつ実行されたが、子runの開始耐久化 ack が HTTP の30秒上限を越え、1件で `start not confirmed`。固定300秒のdriver上限で別のworkerも停止し、実行中だった non-idempotent action 1件は結果不明の `blocked` になった。再起動後、Go の親1,200件中1,199件は完了、1件がその確認待ちとして残った。RSS 最大 215,888 KiB、静置後 96,800 KiB。
- process ごとの in-flight 10（全体最大 120）: macOS の fsync と当時の高い machine load の下で、同じ30秒の開始 ack 上限を越えた。また、先に自分のworkflowを終えたdriverが、他processから渡されたactionを実行中のままworkerを閉じる終了競合を見つけた。

これらを受け、qualification harness を次の形に変えた。

- `KAIRO_QUALIFY_RUNS`（総数）と `KAIRO_QUALIFY_INFLIGHT`（processごとの同時数）を分離する。
- process 全体を drain する `tick` を、各process内で直列化する。
- driver timeoutを `KAIRO_QUALIFY_TIMEOUT_SECONDS` でsoak時間より長く設定できるようにする。
- 全driverが自分のworkloadを終えたbarrierへ着くまでworkerを閉じない。

修正後、次の定常ラウンドが通った。

```sh
KAIRO_QUALIFY_PROCESSES=4 \
KAIRO_QUALIFY_RUNS=300 \
KAIRO_QUALIFY_INFLIGHT=2 \
KAIRO_QUALIFY_TIMEOUT_SECONDS=900 \
KAIRO_QUALIFY_SETTLE_SECONDS=60 \
scripts/qualify-kairod.sh
```

- Go・Python・TypeScriptを各4 process、合計3,600 workflows。
- 3,600/3,600完了、real action marker 3,600/3,600、重複0。
- 所要96秒。
- kairod RSS: 開始15,520 KiB、最大170,496 KiB、負荷終了時168,544 KiB、60秒静置後55,200 KiB。

したがって、このマシンと負荷では全体24 in-flightの定常運転は通り、全体120以上のburst寄りの条件では30秒の開始耐久化上限を越え得る。これは厳密な最大同時数ではない。macOSの `F_FULLFSYNC`、測定時のload、候補全体を走査するclaim経路が混ざっているため、境界の決定にはin-flightを段階的に変えた別の負荷試験が要る。高並列ラウンドでも、完了済みreal actionの重複は観測されず、強制停止時の結果不明を成功扱いしなかった。

## run-id claim直接化とSDK内Tick直列化の後

既存の意味を変えない範囲で、次を実装した。

- `POST /v1/workflow-drives/claim` に `run_id` がある場合、全候補snapshotの前に対象runのshardへ直接claimする。取れない場合だけ従来の走査へ戻り、`next_at` とlong-pollの意味を保つ。
- Go・Python・TypeScriptの `Tick` をSDK内部でprocess単位に直列化した。qualification driver固有のlockは外し、複数の呼び出し元からSDKの直列化を試す。
- 3 SDKに同じ性質の回帰試験を追加した。Goは同時HTTP claim数、Python・TypeScriptはfake backendの同時tick数が1を越えないことを確認する。

各条件で、4 process × 3 SDK、300 workflows/process（合計3,600）を流した。`in-flight` は全12 processの合計である。

| in-flight | 結果 | 所要 | RSS最大 | 備考 |
|---:|---|---:|---:|---|
| 48 | 3,600/3,600、重複0 | 95秒 | 169,312 KiB | 成功 |
| 72 | 3,600/3,600、重複0 | 96秒 | 169,008 KiB | 成功 |
| 96 | 3,600/3,600、重複0 | 95秒 | 173,008 KiB | 成功 |
| 108 | 3,600/3,600、重複0 | 102秒 | 169,824 KiB | 成功 |
| 120（1回目） | 3,385 markers、Python 1 process失敗 | 108秒 | 163,552 KiB | 子run開始のHTTP 504 |
| 120（2回目） | 3,600/3,600、重複0 | 100秒 | 171,584 KiB | 成功 |

したがって、直接claimとTick直列化の後は108までの各ラウンドが通り、120も必ず失敗する固定上限ではない。ただし同じ120で1回は30秒の開始ack上限を越えたため、この付近から失敗確率が無視できなくなる。単発の成功値を製品の同時実行上限にはしない。開始未確認を親workflowの失敗へ変えない判断はADR 0072、計画停止でworkerの実行中taskを完了させる判断はADR 0073として、いずれも提案にした。

## ADR 0072・0073承認後の実装検証

ADR 0072・0073の承認後、次を実装した。

- 3 SDKは、冪等なrun開始のHTTP 504と応答不明の通信エラーだけを、同じRunID・plan・input・parentで上限付きbackoff再確認する。確定したHTTPエラーとworkerへ渡したreal actionは再試行しない。
- kairod接続の `Drain` は、workerが負の既存Credit frameで受信停止を要求し、kairodが未送信taskをdispatcherへ戻してCredit 0でackする。以後の各ResultはCredit 1で受理をackする。新しいframe typeは増やしていない。
- Drain開始後の新しいworkflow claimは拒否する。開始済みのlong-poll claimが戻った場合も、駆動せずに返却する。
- 3 SDKはworker task数とworkflow駆動数をDrain開始時に記録し、完了と期限切れ・中断をloggerへ出す。

決定的な回帰試験では、最初の開始を504にしても3 SDKが同じRunIDを再送すること、実行中taskのhandlerが返る前にもResult受理ackの前にもDrainが返らないことを確認した。Goのprotocol統合試験では、credit内の次taskがdraining workerへ渡らず別workerへ渡り、最初のResultをkairodが受理してからDrainが返った。Go SDKのkairod統合試験でも、real action実行中の `Kairo.Drain` が結果受理まで待った。

`scripts/check.sh` と `scripts/check.sh --race` は、Go・Python・TypeScriptの全SDK試験、不変条件試験、race detectorを含めて通った。120 in-flightの再qualificationと長時間soakは、実装の回帰試験とは別の運用試験として残す。

## ADR 0074承認後のstart admission負荷曲線

ADR 0074の適応型start admission、開始前429、3 SDKの同一RunID再試行を実装後、Go・Python・TypeScript各4 processで負荷曲線を流した。各roundは100 workflows/process、合計1,200 workflowsである。file tier、fsyncあり。`in-flight` は全12 processの合計で、120を3回反復した（最後の1回は区間計測も有効）。

| in-flight | 完了 / marker | 所要 | RSS最大 | window最大 | lane内待ち最大 | 429拒否累計 | start queue / inbox / WAL wait 最大 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 48 | 1,200 / 1,200 | 40秒 | 110,672 KiB | 7 | 4 | 84 | 233.89 / 2.00 / 74.02 ms |
| 120（1回目） | 1,200 / 1,200 | 37秒 | 108,736 KiB | 6 | 0 | 1 | 40.43 / 1.14 / 74.48 ms |
| 120（2回目） | 1,200 / 1,200 | 36秒 | 109,344 KiB | 6 | 1 | 0 | 48.15 / 4.35 / 72.11 ms |
| 120（区間計測あり） | 1,200 / 1,200 | 42秒 | 108,224 KiB | 6 | 0 | 0 | 76.47 / 5.51 / 83.36 ms |

全roundでreal actionの重複は0、HTTP 504 / `start not confirmed` は0、failed logとobs dropも0だった。48のroundでは起動時の集中に対して429が出たが、すべてrun作成前の確定した過負荷であり、SDKが `Retry-After` と同じRunIDで回復した。したがって、この負荷曲線では30秒の開始未確認を流量制御に使わず、window内へadmitするか文書化した429を返す、という受け入れ条件を満たした。拒否数が同時数に単調でないのは、windowを固定値にせず、shardごとの到着順とgroup commitの実測から学習するためである。

区間計測を有効にした120 roundでは、start admissionとは別に次のraw値を得た。

- engineの `step.dispatch` からworkerの `step.started` まで: 2,400件、p50 16ms、p99 114ms、最大175ms。
- worker handler: 2,400件（real actionとsleep step）、p50 0.21ms、p99 205.04ms、最大461.94ms。
- process全体のTick（claim requestに続くworkflow replayを含む）: 1,210件、p50 1,715.42ms、p99 3,848.40ms、最大6,916.11ms。これは純粋なclaim時間ではなく上限である。

`scripts/qualify-kairod.sh` は以後、1秒ごとの `/v1/stats`、kairodのobs、driverのworker観測とTick所要をraw logへ残す。start queue、shard inbox、WAL/group commit、dispatch-to-worker、handlerを混ぜずに再計算できる。claim requestだけの製品metricsとhistogramはIssue #17の範囲であり、ここではwire形式を増やしていない。

ホットパス変更の前後に同じマシンで `scripts/bench.sh` を1回ずつ実行した。ただし変更後はload averageがさらに高く、厳密な性能比較には使わない。

| 指標 | 変更前（load 5.52） | 変更後（load 9.16） | 差 |
|---|---:|---:|---:|
| transition | 1,002 ns | 1,052 ns | +5.0% |
| CPU / 5-node run | 69.11 µs | 71.22 µs | +3.1% |
| throughput | 100,734 runs/s | 98,071 runs/s | -2.6% |
| handoff p99 | 1,337.54 µs | 1,536.58 µs | +14.9% |
| waiting memory | 2,075 bytes | 2,075 bytes | 変化なし |
| durable ack p99（macOS） | 16,392.5 µs | 10,047.9 µs | 参考値 |

`scripts/check.sh` と `scripts/check.sh --race` はこの実装と決定的な429・公平性・window調整テストを含めて通った。低負荷での反復比較、長時間soak、障害注入の拡張は引き続きIssue #13で行う。

## 試験が見つけた競合

最初の 60-workflow 試験では、複数 process が同じ node spec / plan を同時登録すると HTTP 500 になった。保存処理が全 request で同じ `path.tmp` を使い、一方の `Rename` が他方の一時ファイルを先に移動していた。

一時ファイルを同じディレクトリの一意な名前に変えた。`api.TestWriteJSONConcurrentAtomicReplace` は、同じ path への 100 並列の置換を 20 回実行して、全 writer の成功、最後の JSON の完全性、一時ファイルが残らないことを確認した。修正後は 60-workflow と 1,200-workflow の両方が通った。

## SIGKILL と再起動

```sh
scripts/fault-kairod.sh
```

real action の結果を耐久化して timer で suspend した後、kairod を `SIGKILL` した。daemon が止まっている間に timer の期限を越え、同じ data directory で kairod と別の SDK instance を起動した。

- parent workflow と timer child を回復した。
- signal wait まで進み、signal 後に期待した結果で完了した。
- real action の marker は 1 個だけだった。再実行されていない。

worker disconnect の結果不明は `protocol.TestWorkerDisconnect`、意図の耐久化前に real command を出さないことは `engine.TestRealCommandWaitsForDurableIntent` が、より決定的な hook で検証している。process-level 試験は file WAL、binary の停止、socket の再接続をまたぐ部分を補う。

## 性能比較 script の動作確認

`scripts/bench-compare.sh` で baseline と candidate を交互に測り、生出力と中央値・最小・最大を残せるようにした。1 回ずつの smoke measurement は両方とも load average 約 9 であり、低負荷の性能判断には使わない。

| 指標 | v0.4.1 baseline | candidate | 備考 |
|---|---:|---:|---|
| transition | 1,088 ns | 1,044 ns | 1 回だけ |
| CPU / 5-node run | 72.72 µs | 72.26 µs | 1 回だけ |
| throughput | 100,519 runs/s | 103,078 runs/s | 1 回だけ |
| handoff p99 | 897 µs | 1,323.88 µs | 高負荷・分散が大きいので未判定 |
| waiting memory | 2,011 bytes | 2,075 bytes | +64 bytes。ADR 0069 直前の既測値は 2,075 bytes で、0069 自体では増えていない |

低負荷時に 5 回以上を交互測定し、raw output の machine / load header と中央値・範囲を採るまで、性能 qualification は未完了とする。

## 設計承認が必要な残り

- ADR 0070（提案）: kairod が async URL action の callback と期限切れを所有する。
- ADR 0071（提案）: kairod の signal を耐久化まで同期し、先取り wait と receipt を原子的に作る。

どちらも通信または保存の意味を増やすため、承認前には実装していない。
