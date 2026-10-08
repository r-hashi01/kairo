---
name: bench
description: Measure kairo against its non-functional budgets (transition cost, CPU per run, hand-off latency, memory per waiting run, durable-ack latency) with scripts/bench.sh, and compare before/after a change. Use after changing hot paths (core/, engine/shard.go, sched/, wal/, mpsc/, timerwheel/) or when the user asks about performance.
---

# bench

1. 変更の効果を見るときは、変更前の値も取ります（`git stash` で退避してから `scripts/bench.sh` を実行し、`git stash pop` で戻す）。未コミットの作業を失わないよう、stash の前後で `git status` を確認してください。
2. `scripts/bench.sh` を実行します。判定はしません（ADR 0056）。予算を超えた行には `over` と出ますが、終了コードは 0 です。測れなかった指標があるときだけ `FAIL` と出て、終了コードが 1 になります。
3. 値は計測ごとにぶれます。予算に近い値や、変化が 10% 未満の値は、2〜3 回測って中央値で判断します。hand-off 遅延は特にぶれやすい指標です。
4. 報告には、指標ごとの前後の値と予算、ぶれの幅を表にして示します。マシン（出力の先頭の `== machine` の行）も書きます。前後の比較は、同じマシンで負荷の低いときの値どうしでだけ行います。予算はサーバー（32 コア）での目標値だということも添えてください。
5. macOS では、耐久性 ack の遅延は F_FULLFSYNC のため比較対象外です。数値だけ情報として示します。
