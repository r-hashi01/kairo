# process-level workflow claim scan の計測（2026-11-18）

GitHub Issue #46 に対する、現行実装のスケーラビリティ境界の計測です。ここでは設計を変えず、`POST /v1/workflow-drives/claim` が RunID を指定しないときの全候補走査を測りました。

## 測り方

- Apple M4 Max、14 CPU、Go 1.27.4。計測開始時の load average は 5.91 / 6.25 / 6.63 でした。ほかの意図的な負荷はかけていません。
- engine は 8 shard、保存 I/O と運用測定は無効です。
- 100、1,000、5,000、10,000 件の active code workflow を作り、どれにも一致しない workflow version で claim します。このため、1 request は全 shard の候補を集めて sort し、全候補へ `GetInput` します。
- claimant 1 は1件ずつ、claimant 8 は8件を同時に開始します。各条件を2秒ずつ3回測り、表は3回の中央値です。
- shard-loop への影響は、各 claim wave と同時に通常の `Engine.Get` を1回流したときの遅延で見ます。
- `alloc/claim` は、同時8件のときも wave 全体の `B/op` を8で割った値です。

再計測は、ほかの重い処理を止めた同じマシンで次を実行します。

```sh
scripts/bench-claims.sh
```

## 結果

| active workflows | 同時 claimant | claim p50 | claim p99 | CPU / claim | alloc / claim | shard probe p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 1 | 0.132 ms | 0.392 ms | 0.229 ms | 0.17 MiB | 0.004 ms |
| 100 | 8 | 0.397 ms | 0.913 ms | 0.393 ms | 0.17 MiB | 0.067 ms |
| 1,000 | 1 | 1.248 ms | 1.748 ms | 2.247 ms | 1.94 MiB | 0.036 ms |
| 1,000 | 8 | 4.583 ms | 8.018 ms | 4.011 ms | 1.94 MiB | 0.255 ms |
| 5,000 | 1 | 7.134 ms | 8.007 ms | 12.707 ms | 11.08 MiB | 0.567 ms |
| 5,000 | 8 | 22.509 ms | 25.394 ms | 20.042 ms | 11.07 MiB | 4.585 ms |
| 10,000 | 1 | 14.762 ms | 15.858 ms | 28.029 ms | 22.05 MiB | 0.227 ms |
| 10,000 | 8 | 44.397 ms | 48.016 ms | 41.134 ms | 22.04 MiB | 8.074 ms |

CPU が実時間を超える行があるのは、process 全体の複数 CPU の合計だからです。shard probe はサンプル数と GC の位置で揺れるため、単発の行ではなく3回の中央値と規模の傾向を使います。

## 現在わかる境界

走査時間、CPU、割り当てはいずれも active workflow 数にほぼ比例します。claimant を増やすと1件あたりのthroughputは並列化で上がりますが、各requestの応答時間と、同じshardを使う通常の問い合わせの遅延は悪化します。

このマシンでは、active 1,000件・同時claimant 8ではshard probeのp99が0.255msですが、5,000件・8では4.585msです。したがって、後述する1msの干渉基準で見た現行方式の境界は、1,000件と5,000件の間にあります。これは製品の件数上限ではなく、現在の全件走査を見直すための観測上の境界です。

active 10,000件・同時claimant 8では、1 waveで約185MiBを確保し、process CPUを約329ms使い、claimのp99は約48ms、shard probeのp99は約8msでした。多数processが同じ通知で一斉に起きる運用では、`processes × active workflows` の費用がkairod側の仕事と競合します。

## 設計変更を提案する根拠

次のどちらかを、想定するworkflow数・claim request頻度・process数で再現したら、index、cursor、または別のscheduling structureを検討するADRを提案します。時間値なのでCIの合否には使いません（ADR 0056）。

1. 同じマシンの低負荷計測を3回行った中央値で、同時走査中のshard probe p99が1msを超える。1msは新しい製品要件ではなく、既存のnode hand-off予算を、補助的なscheduler走査が通常処理へ干渉してよい上限として使うレビュー開始条件です。
2. `CPU / claim × 1秒あたりのprocess-level claim request数` が100ms/秒を超える。これは1 CPU coreの10%です。claim scanはworkflow本体の実行・耐久化・結果記録ではないため、1 coreの10%を継続して使う見込みになった時点で設計レビューを始めます。これも上限ではなく、実際の配備条件を入れて判断するための条件です。

今回の10,000件・同時8件は、1秒に1 waveでもCPU条件を超え、干渉条件も超えます。1,000件・同時8件は、1秒に1 waveならCPUは約32ms/秒で、干渉条件も超えません。

ADR 0041・0068・0069により、改善するとしても全runが通常時に永続indexのwriteを払う形や、background pollingをそのまま採ることはできません。公平性、workflow version routing、lost wakeup防止、再起動後のready再構築を保つ案を、別ADRで比較して承認を得てから実装します。
