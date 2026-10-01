---
name: adr
description: Write an Architecture Decision Record for kairo in docs/adr (next number, template, index, supersession). Use when a change adds/changes/relaxes an invariant, adds an IR construct / effect type / event or command kind, changes the log, snapshot or worker-protocol format, adds a dependency, deviates from the requirements, or when the user asks to record a decision.
---

# adr

1. `docs/adr/README.md` と、関係しそうな既存の ADR を読みます。すでに同じ判断があるなら新しく書かず、それを示します。
2. 番号は、既存の最大番号 + 1（4 桁）にします。ファイル名は `docs/adr/NNNN-slug.md`（slug は英小文字とハイフン）です。
3. `docs/adr/template.md` の構成（背景 / 決定 / 結果 / 検討した代替案）で書きます。
   - 決定には、実装のどこに現れるか（パッケージ・型・関数）を書きます。
   - 結果には、それを守らせるテストや hook を書きます。まだないなら「なし」と書き、足すかどうかを利用者に聞きます。
   - 代替案は、実際に検討したものだけを書きます。
   - 状態は「提案」にします。「承認」にするのは利用者です。日付は今日にします。
4. 既存の判断を覆すときは、新しい ADR の関連欄に古い番号を書き、古い ADR の状態行だけを「NNNN により置き換え」に変えます。本文は書き換えません。
5. `docs/adr/README.md` の表に 1 行足し（古い ADR を置き換えたなら、その行の状態も更新し）、`scripts/adr-lint.sh` を実行します。
6. 利用者には、起票した ADR のパスと、決定の要点を 1〜2 文で伝え、承認を求めます。
