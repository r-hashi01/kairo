# 0032. map と loop を、Dify の iteration と loop の意味論で動かせるようにする

- 状態: 承認（2026-10-02）
- 日付: 2026-10-02
- 関連: FR1 / ADR 0015, 0028, 0029, 0030, 0031, 0033

## 背景

Dify（graphon 0.7.0）の iteration と loop は、kairo の map と loop に近いが、次の点が違う（調査メモの 2 節）。

**iteration**
- 並列実行の上限は `parallel_nums`（既定 10）で、`is_parallel` が false なら 1 つずつ。kairo の map には、上限（`max_concurrency`）がある。
- 要素の中で失敗したときの扱い（`error_handle_mode`）が 3 種類ある。kairo の map では、要素の失敗は実行全体の失敗になる。
  - `terminated`: 全体を失敗にする。
  - `continue-on-error`: その要素の結果を null にして続ける。
  - `remove-abnormal-output`: その要素を結果から除く。
- 結果は、本体の中の指定したノード（`output_selector`）の出力を index 順に並べたもの。すべての要素がリストなら、平らにつなぐ（`flatten_output`、既定 true）。
- 空の配列なら、すぐに成功する。

**loop**
- 回数の上限は `loop_count`。
- break 条件を、**最初の回の前**と、各回の後に評価する。kairo の loop は do-while（後判定）だけ。
- 本体の中の `loop-end` ノードに着いたら、その回で抜ける。
- loop 変数（ADR 0033）を持つ。出力は、loop 変数と回数（`loop_round`）。
- 本体の中で 1 つでも失敗すると、loop 全体が失敗する。

## 決定

### map（iteration）

- `on_element_error` を足す。値は `fail`（既定。今と同じ）、`null`、`omit`。
- **要素の失敗の封じ込め:**
  - `null` か `omit` の map の中で、要素の中の失敗が実行を失敗にしかけたら、その失敗はその要素で止める。対象は、エラー処理のないノードの確定した失敗（ADR 0030）。
  - その要素の中で実行中のタスクを取り消し、タイマーを外し、要素のスコープを捨てる。
  - 要素の結果は、`null` なら null、`omit` なら除く（index の順は保つ）。
  - 封じ込めはいちばん内側の map で行う。外側には、その map が成功したものとして伝わる。
  - 結果が不明な実の命令の要確認（不変条件 5）は、封じ込めない。今までどおり実行を止める。
- `output` を足す。本体の中のノードへの参照で、要素の結果をそのノードの出力にする。省略すると、今と同じく本体の出力になる。
- `flatten` を足す。true なら、要素の結果がすべて配列のときに平らにつなぐ。
- 並列の上限は、今の `max_concurrency` を使う。Dify の `is_parallel: false` は 1 にする。

### loop

- `check` を足す。値は `after`（既定。今の do-while）と `before`（Dify。最初の回の前にも評価する）。
  - `before` で最初の判定が成り立たなければ、本体を 1 度も実行せずに終わる。出力は loop 変数の初期値になる。
- 条件の書き方は 2 つ認める。
  - 今の `while`（型付きフィールド、ADR 0013）
  - `until`: 本体の中、または loop の前に置いた分岐ノード（`kairo.switch`、ADR 0031）のハンドルで抜ける
- `break_on` を足す。本体のグラフのメンバーの ID を並べ、そのどれかがその回で実行されたら（スキップされなければ）、その回で抜ける。Dify の `loop-end` は、これに写す。
- loop の出力は、loop 変数（ADR 0033）があればそのオブジェクトに `loop_round` を足したもの。なければ、今と同じく本体の出力。

### 共通

- 新しいイベントや命令の種類は足さない。要素の封じ込めは、既存の取り消し（`CmdAbort`）とタイマーの解除で表す。
- Dify の `iteration_*` / `loop_*` の SSE（回の開始と終わり）は、実行イベントの出口（ADR 0034）から作る。

## 結果

- Dify の iteration と loop を、意味を変えずに IR に写せる。
- 要素ごとの失敗の封じ込めが入り、`fail` で実行全体を止めることが、いちばん内側の map で止まりうるようになる。コアの `fail` を、スコープを指定して呼べるように分ける。
- 状態の形式は、map の活性化に除いた要素の印が増える程度。`codecVersion` を上げる。
- テスト:
  - 3 つのエラーモード。要素の中で実行中のタスクが取り消されることも確かめる。
  - flatten、output、空の配列。
  - loop の前判定、`break_on`、loop 変数の出力。
  - 要素の失敗の順序を入れ替えても、同じ状態のバイト列になる。
  - `checkReplay`。
  - ADR 0028 の差分ハーネスで、iteration と loop を含む fixture が一致する。

## 検討した代替案

- Dify の iteration と loop を、新しいコンテナとして足す: map と loop と意味の大半が重なる。設定を足す方が、構成要素を増やさずに済む。
- 要素の失敗の封じ込めを、本体のノードごとのエラー処理（ADR 0030）だけで表す: Dify は、本体のどのノードが失敗しても要素ごとに扱う。すべてのノードに default-value を付けるような変換は、出力の形が変わってしまう。

## 実装で詰めた詳細（2026-10-05 追記、ADR 0017）

- **定義の名前。**
  - map: `on_element_error`（`fail` / `null` / `omit`）、`element_output`、`flatten`
    - `element_output` にしたのは、`output` がグラフの出力ですでに使われているため。
    - `element_output` は、本体の中のノードでなければならない（コンパイル時に検査する）。
  - loop: `check`（`before` / `after`）、`break`（Dify の条件。1 つの case）と `break_input`、`break_on`、`vars`、`loop_output`（`vars` にすると、Dify と同じく変数と `loop_round` を出力にする）
    - `while` と `break` は、どちらか一方だけ。どちらもなければ、`max_iter` 回か `break_on` まで回す。
    - loop 変数を宣言したら、`loop_output: vars` が必要。loop の値（`<loop>.<変数>` で読むもの）に、変数を持つため。
- **loop の条件 `break` は、`kairo.switch` ではなく loop 自身に持たせた。** ADR では「本体の中か前に置いた分岐ノード」としていた。しかし Dify の break 条件は、loop 変数と本体のノードを、loop のスコープで評価するもので、最初の回の前の判定は本体の外で行う必要がある。評価器は `kairo.switch` と共通（ADR 0031）。
- **graphon に合わせた判定。**
  - 最初の回の前の判定では、`ValueError` を「抜けない」として扱う。`TypeError` は loop を失敗にする。各回の後の判定のエラーは、loop を失敗にする。
  - `break_on` のノードがその回に実行されたら、判定の結果によらず抜ける。手書きのグラフでは、スキップしたノードの値を消すので（ADR 0029）、「値がある」は「この回に実行された」と同じになる。次の回の前にも消す。
- **0 回で終わった loop の出力。** 変数のオブジェクト（`loop_round` なし）にした。graphon の出力は `{}` だが、`<loop>.<変数>` は初期値を読める。kairo では loop の値と出力が同じものなので、変数を残す。差は、実行イベントの出口（ADR 0034）で出力を写すときに埋める。
- **要素の失敗の封じ込め（`failAt`）の対象。**
  - 対象: リトライを使い切ったステップの失敗、保護ステップの評価のエラー、分岐の値の誤り、map の対象がリストでないこと、loop の判定のエラー。
  - 対象外（実行全体を止める）: 実行の上限（ステップ数・時間）と取り消し。
  - 封じ込めたら、その要素の活性化をすべて取り除き、実行中のタスクには `CmdAbort` を、タイマーには解除を出し、要素のスコープ（と、中の map のスコープ）を捨てる。
- **状態の形式。** 変えていない。omit した要素は、結果を空（nil）のまま残し、map が終わったときに除く。
- **テスト:**
  - `TestMapElementErrorModes`: 結果の届く順を 10 通りに変えて、状態のバイト列が同じになることも確かめる
  - `TestMapElementAbortsItsTasks`、`TestMapElementOutputAndFlatten`、`TestDifyLoop`、`TestLoopBreakOn`、`TestVarsCompileErrors`
- **レビューで直したこと（2026-10-05）:**
  - 要素を中断するときは、先に要素の活性化をすべて集めてから取り除く。親から消していたため、深い階層の活性化が取り残され、後でその結果が届くとコアが panic していた。
  - 要素の中に要確認のステップがあれば、封じ込めずに実行を失敗にする（不変条件 5。要確認を黙って捨てない）。
  - 分岐の値の誤りも、封じ込めの対象にした（書いたとおりに実装されていなかった）。
  - 実の命令が、意図の耐久化を待つ間に要素の中断で取り消された場合は、耐久化の後に出さない。エンジンの `release` で、活性化と試行がまだ有効かを確かめる（`core.Dispatched`）。
  - `break_on` は、loop の本体の手書きのグラフの、直接のメンバーに限る。loop を始めるときにも値を消す。cond の中のノードは、スキップされても値が消えないので、前の活性化の値で早く抜けていた。
  - テスト: `TestAbortElementRemovesDeepActivations`、`TestElementFailureWithPendingReviewFailsRun`、`TestBadBranchValueIsContained`、`TestBreakOnAcrossActivations`（core）、`TestHeldRealCommandOfAbortedElement`（engine）
