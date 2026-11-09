# kairod の追加検証

GitHub Issue #12 の、通常のテストより長い検証を再現するための補助です。時間値を CI の合否に使わず、正しさとして固定できた競合だけを通常テストへ移します（ADR 0056）。

2026-11-09 の最初の実測と、そこから見つかった競合は [`docs/qualification-2026-11-09.ja.md`](../../docs/qualification-2026-11-09.ja.md) にあります。

## 複数 process の qualification

```sh
scripts/qualify-kairod.sh
KAIRO_QUALIFY_PROCESSES=4 KAIRO_QUALIFY_RUNS=1000 \
  KAIRO_QUALIFY_INFLIGHT=25 KAIRO_QUALIFY_TIMEOUT_SECONDS=1800 \
  KAIRO_QUALIFY_SETTLE_SECONDS=30 \
  KAIRO_QUALIFY_OUT=/tmp/kairo-soak \
  scripts/qualify-kairod.sh
```

1 台の file tier（fsync あり）の kairod へ、Go・Python・TypeScript の driver を同数つなぎます。各 workflow は non-idempotent な real action、timer、signal wait を通ります。real action は run ごとのファイルを `O_EXCL` 相当で作るので、二度実行されると driver が失敗します。

`KAIRO_QUALIFY_RUNS` は process ごとの総数、`KAIRO_QUALIFY_INFLIGHT` は process ごとの同時 workflow 数です。`tick` は workflow ごとの処理ではなく process 全体の ready workflow を drain する操作なので、SDK自身が各 process 内で直列化します。harnessは複数の呼び出し元からその性質も試します。長時間化には、同時数を際限なく増やさず `RUNS` を増やします。

`KAIRO_QUALIFY_TIMEOUT_SECONDS` は各 driver process 全体の上限です。上限で worker が止まると、実行中だった non-idempotent action は意図どおり結果不明の `blocked` になり得るため、soak の予定時間より十分長くします。

各 driver は自分の workload が終わってもすぐには worker を閉じず、全driverが完了barrierへ着くまで待ちます。同じactionを複数processが配るため、これは別processの実行中actionを終了時に切断しないためです。

出力には、言語・process ごとのログ、kairod の RSS の時系列、workflow 数、real action marker 数、負荷終了直後と静置後の RSS を残します。`kairod-stats.jsonl` は1秒ごとの `/v1/stats` を持ち、`Starts` から shard・tier ごとのwindow、開始確認中、待ち、拒否累計、start queue・inbox・WAL/group-commitの移動平均を追えます。`kairod-obs.jsonl` はrunとstep dispatchの時刻を持ちます。各driver logの `phase=worker` はworkerでのstep受取時刻とhandler実行時間を持つため、dispatchとの対応を調べられます。`phase=claim_drive` はprocess全体のTick（claim requestに続くworkflow replayを含む）の所要時間で、純粋なclaim時間の上限として扱います。これらの時間値はraw measurementであり、合否条件ではありません。RSS は保持中の終了 marker、Go の heap と OS の都合を含む観測値であり、1 回の増減だけでは leak と判定しません。同じ件数の roundを増やし、仕事が止まった後も増え続けるかを確認します。

この harness が最初に見つけた、node / plan の同時登録が固定名の一時ファイルを奪い合う競合は、`api.TestWriteJSONConcurrentAtomicReplace` に短い回帰テストとして移しました。

## process-level の障害注入

```sh
KAIRO_FAULT_OUT=/tmp/kairo-fault scripts/fault-kairod.sh
```

現在の scenario は、real action の結果が耐久化し、子の timer で suspend した後に kairod を `SIGKILL` します。timer の期限を daemon 停止中に越え、同じデータディレクトリで kairod と別の SDK instance を起動します。workflow が signal wait と完了まで進み、real action marker が 1 個だけであることを確認します。

worker disconnect で non-idempotent real action が blocked になり、unprotected だけが別 worker へ渡る性質は `protocol.TestWorkerDisconnect` が決定的に検証しています。意図の耐久化 ack を遅らせる性質は `engine.TestRealCommandWaitsForDurableIntent` が検証します。process-level scenario は、これらを置き換えず、実 binary・file WAL・再起動をまたぐ接続部分を補います。

## 同じ条件での性能比較

比較元を別ディレクトリに用意し、現在の checkout と交互に測ります。

```sh
KAIRO_BENCH_REPETITIONS=5 \
  scripts/bench-compare.sh /tmp/kairo-baseline /tmp/kairo-bench-results
```

各回の `scripts/bench.sh` の生出力（machine、Go version、load を含む）と、transition、5-node CPU、throughput、handoff p99、待機 memory の中央値・最小・最大を保存します。比較中は電源モード、Go version、データの置き場を変えず、他の重い仕事を止めます。load が高い結果は保存しても合否の根拠にはしません。

## まだ別判断であるもの

- async URL action over kairod: ADR 0070（提案）
- wait より先に送る signal と receipt over kairod: ADR 0071（提案）

どちらも保存・通信の意味を増やすため、承認前に実装しません。
