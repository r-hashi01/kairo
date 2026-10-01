---
name: bench
description: Measure kairo against its non-functional budgets (transition cost, CPU per run, hand-off latency, memory per waiting run, durable-ack latency) with scripts/bench.sh, and compare before/after a change. Use after changing hot paths (core/, engine/shard.go, sched/, wal/, mpsc/, timerwheel/) or when the user asks about performance.
---

# bench

1. 変更の効果を見るときは、変更前の値も取ります（`git stash` で退避してから `scripts/bench.sh` を実行し、`git stash pop` で戻す）。未コミットの作業を失わないよう、stash の前後で `git status` を確認してください。
2. `scripts/bench.sh` を実行します。`OVER` の行があれば終了コードは 1 です。
3. 値は計測ごとにぶれます。予算に近い値や、変化が 10% 未満の値は、2〜3 回測って中央値で判断します。hand-off 遅延は特にぶれやすい指標です。
4. 報告には、指標ごとの前後の値と予算、ぶれの幅を表にして示します。マシン（`sysctl -n machdep.cpu.brand_string` または `/proc/cpuinfo`）も書きます。予算はサーバー（32 コア）での目標値だということも添えてください。
5. macOS では、耐久性 ack の遅延は F_FULLFSYNC のため比較対象外です。数値だけ情報として示します。
