# core/ — 純粋な状態遷移

`Apply(plan, state, event, out) → (commands, error)` がこのパッケージのすべてです。

- 非テストコードで import してよいのは `bytes` `encoding/json` `encoding/binary` `errors` `slices` `strconv` `strings` `time`（`time.Duration` の型としてのみ）と `kairo/ir` だけです。`TestCoreIsPure` がこれを検査します。
- 時刻は `ev.At`（`machine.at`）だけから得ます。ID は `NextAct` / `NextScope` / `NextTimer` の連番で振ります。
- map を走査して命令を出すときは、必ずキーをソートします（`sortedActs` を使う）。出力 JSON も決定的でなければなりません（`json.Marshal` は map のキーをソートする）。
- `ErrIgnored` を返したイベントは状態を変えてはいけません（ログに書かれないため）。
- 状態の形を変えたら `codec.go` を更新します。形式が変われば `codecVersion` を上げ、`checkReplay` で往復を確認します。
- 新しい挙動には、`sim` を使ったテストに `x.checkReplay()` を付けて足してください。
