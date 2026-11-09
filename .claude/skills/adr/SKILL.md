---
name: adr
description: Write an Architecture Decision Record for kairo in docs/adr (next number, template, index, supersession). Use when a change adds/changes/relaxes an invariant, adds an IR construct / effect type / event or command kind, changes the log, snapshot or worker-protocol format, adds a dependency, deviates from the requirements, or when the user asks to record a decision.
---

# adr

1. `docs/adr/authoring-guideline.md` を全文読み、次に `docs/adr/README.md` と関係しそうな既存の ADR を読みます。すでに同じ判断があるなら新しく書かず、それを示します。
2. 番号は、既存の最大番号 + 1（4 桁）にします。ファイル名は `docs/adr/NNNN-slug.md`（slug は英小文字とハイフン）です。
3. `docs/adr/template.md` と執筆ガイドラインの構成で書きます。
   - 背景は事実と制約を一次資料で確認し、ADR単体で問題が分かるようにします。
   - 決定は長期に守る性質に絞り、実装詳細、移行計画、進捗、テスト手順を入れません。複数の判断は `決定項 N（章題）` にします。
   - 適用範囲と「この ADR で決定しないこと」を書き、実在する委譲先を示します。
   - 結果には良い・悪い・中立の影響と、新しく守る制約を書きます。具体的なテスト計画は検証文書かIssueへ置きます。
   - 代替案は、実際に検討したものだけを書きます。
   - Issue、PR、外部URLは本文に混ぜず「参照」へ置きます。他ADRの判断は `ADR NNNN の決定項 N（章題）` で参照します。
   - 状態は「提案」にします。「承認」にするのは利用者です。日付は今日にします。
4. 既存の判断を覆すときは、新しい ADR の「関連する ADR」に古い番号と置き換える決定項を書き、古い ADR の状態行だけを「NNNN により置き換え」に変えます。本文は書き換えません。
5. `docs/adr/README.md` の表に 1 行足し（古い ADR を置き換えたなら、その行の状態も更新し）、`scripts/adr-lint.sh` を実行します。
6. 利用者には、起票した ADR のパスと、決定の要点を 1〜2 文で伝え、承認を求めます。
7. **承認されるまで実装に入りません（ADR 0017）。** 待っているあいだにできるのは、ADR が要らない作業だけです。判断に計測が要るなら、その計測の計画を ADR に書いて承認を求めます。承認されたら、状態を利用者の言葉どおり「承認（日付）」にし、索引も更新してから実装を始めます。
