---
name: core-change
description: Checklist for changing the pure state-transition core (core/) or its contract with the engine - new events, commands, state fields, IR constructs, or anything that affects replay, the snapshot codec, or the log format. Use before and while editing core/, ir/, or engine/records.go.
---

# core-change

作業を始める前に、`AGENTS.md` と `core/AGENTS.md` を読みます。

## 変更前に決めること
- その変更は、どのイベントに対して状態のどこを変えるか。命令は増えるか。
- IR の構成要素を増やすなら、既存の組み合わせで表せない理由は何か（要件は「5つの構成要素のみ」）。
- 状態の符号化形式は変わるか（→ `codecVersion`）。ログの記録形式は変わるか（→ 過去のログを読めること）。

- 関係する ADR（`docs/adr/README.md` の索引から）を読みます。その判断を変えることになるなら、先に `/adr` で起票します。

## 実装中に守ること
- `core/` には I/O、時計、goroutine、ロック、乱数を入れません（hook がブロックします）。
- map を走査するときは、キーをソートします。
- `ErrIgnored` を返すなら状態を変えません。
- 状態を変えたイベントは、`engine/shard.go` の `event` を通ってログに入ります。コアの外で状態をいじってはいけません。

## テスト
- `core/core_test.go` の `sim` を使って、新しい挙動のテストを足します。最後に `x.checkReplay()` を呼びます（リプレイ結果のバイト一致と、符号化の往復を確認する）。
- 並列やマップの挙動なら、`rng` を使ってイベント順を入れ替えても出力が変わらないことを確認します。
- エンジンまで影響するなら、`engine/engine_test.go` に統合テストを足します。リカバリに関わるなら、再起動を挟むテストも足します。

## 仕上げ
- `/verify` を実行します。
- ホットパスなら `/bench` を実行します。
- `invariant-reviewer` サブエージェントに差分をレビューさせます。
