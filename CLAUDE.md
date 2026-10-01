@AGENTS.md

## Claude Code 向けの補足

- 編集のたびに hook が gofmt と go vet を実行します。失敗内容は次のターンに返ってくるので、その場で直してください。
- ターンを終える前に、Stop hook が `scripts/check.sh --quick` を実行します。失敗すると終了できないので、直すか、直せない理由を利用者に伝えてください。
- `core/` か `engine/` の差分が大きいときは、終える前に `invariant-reviewer` サブエージェントで差分をレビューさせてください。
- 手順が決まっている作業にはスキルを使います：`/verify`、`/bench`、`/core-change`。
