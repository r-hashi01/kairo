# 0031. Dify の条件は組み込みの保護ステップで評価し、分岐のハンドルはノードの設置ごとに宣言できるようにする

- 状態: 承認（2026-10-02）
- 日付: 2026-10-02
- 関連: FR1 / 不変条件 1, 2 / ADR 0013, 0015, 0028, 0029

## 背景

Dify の if-else と、loop の break 条件は、任意の変数を文字列や数値として比べる（graphon `utils/condition`）。

- 演算子: contains、not contains、start with、end with、is、is not、empty、not empty、in、not in、all of、=、≠、>、<、≥、≤、null、not null、exists、not exists
- 論理演算子: and / or
- ファイルの属性（`sub_variable_condition`）にも条件を書ける。
- LLM の出力の文字列で分岐するのが、ふつうの使い方である。

ADR 0013 は、自由文での分岐を防ぐために、条件を型付きのフィールドに限っている。理由は、再現性と検査のしやすさである。

もう 1 つの問題として、if-else のハンドル（case の ID）や question-classifier のハンドル（分類の ID）は、**ノードの設置ごと**に違う。ADR 0029 では、ハンドルをノード仕様の enum の候補から取るので、アクションごとにしか決められない。

## 決定

### 条件は組み込みの保護ステップで評価する

- 組み込みの保護アクション `kairo.switch` を足す。
  - 入力（評価したい値）と、Dify の条件の並び（case ごとの条件の列と論理演算子）を受け取る。
  - 出力は `{"handle": <最初に成り立った case の ID か "false">}` で、ハンドルとして使う（ADR 0029 の分岐ノード）。
  - 保護アクションなので、コアの中で評価し、出力はログに残らない（リプレイで再計算する、ADR 0015）。
- 演算子は、graphon 0.7.0 の `utils/condition` の振る舞いに合わせて、`ir` に実装する。型の変換（文字列と数値の比較など）、null と存在の扱い、配列とファイルの属性を含む。
  - 評価は純粋な関数で、時計も乱数も使わない（不変条件 1）。同じ入力からは同じ結果になる。
  - 演算子の集合は固定で、利用者がコードを登録することはできない。ADR 0015 が退けた「任意の関数を保護ステップにする」とは違う。
- ADR 0013 の制約（kairo 自身の `cond` と loop の `while` は、型付きのフィールドだけで分岐する）は、そのまま残す。自由文での分岐は、`kairo.switch` を明示的に置いた場合だけ起きる。
  - 自由文の分岐が明示されるので、検査の対象（どこで文字列を比べているか）は IR から読める。
- loop の break 条件は、`kairo.switch` を本体の最後（と、前判定のときは最初。ADR 0032）に置き、その出力の `handle` で判定する。

### ハンドルは、ノードの設置ごとに宣言できる

- step の定義に `handles`（文字列の配列）を足す。
  - ノード仕様に `Branch` があるときは、その設置のハンドルの集合になる。enum の候補の代わりに使う。
  - ADR 0029 のコンパイル時の検査（辺のハンドルが集合に含まれるか）と、実行時の検査（出力の値が集合に含まれるか）は、この集合で行う。
- question-classifier は、Python ワーカーの LLM ステップ（`Branch: "class_id"`）にし、設置ごとに `handles` で分類の ID を宣言する。
- `kairo.switch` の `handles` は、DSL の変換器が case の ID と `"false"` から作る。

## 結果

- Dify の if-else、question-classifier、loop の break 条件を、意味を変えずに表せる。
- 自由文での分岐は、`kairo.switch` を置いたところに限られ、IR から見える。
- 悪くなること:
  - `ir` に、条件の評価器（演算子と型の変換）が増える。graphon の版が上がって振る舞いが変わったら、追従する必要がある。
  - 保護ステップの入力に大きな値（LLM の長い出力）が来ると、コアの中で比較するコストがかかる。ブロブ化した値は、中身を取りに行けない（I/O になる）ので、比べる値はブロブにしない（ADR 0008 のしきい値以下）か、比べる前にワーカーで評価する。どちらにするかは、実装で詰めて追記する。
- テスト:
  - 演算子ごとの表のテスト。graphon 0.7.0 の `utils/condition` のテストを移植する。
  - 設置ごとのハンドルの、コンパイル時と実行時の検査。
  - `checkReplay`。
  - ADR 0028 の差分ハーネスで、if-else と question-classifier を含む fixture が一致する。

## 検討した代替案

- ADR 0013 を全体として緩め、`cond` で文字列を比べられるようにする: kairo 自身の定義でも自由文で分岐できてしまい、0013 の意図がなくなる。
- 条件の評価をワーカー（Python）に任せる: 純粋な評価に往復がかかる。また、コアの中で決定的に評価できるものを外に出すと、リプレイで結果を再現できず、ログに残す必要が出る。
- ハンドルをノード仕様で決め、設置ごとに別のアクションを登録する: 設置の数だけアクションが増え、ノード仕様が実行時の構成と結び付いてしまう。

## 実装で詰めた詳細（2026-10-05 追記、ADR 0017）

- **評価器。** graphon 0.7.0 の `utils/condition/processor.py` を、Python の意味論のまま Go に移した（`core/condition.go`）。
  - 真偽の判定（`not value`）
  - int と float の区別（JSON の数値に小数点か指数があれば float）
  - `True == 1` を含む等値
  - `int()` / `float()` の文字列の変換。`int("3.5")` はエラーになる。
  - 真偽値の変数に対する期待値の変換（`"false"` を JSON として読む）
  - `str()` での文字列化（`contains` の期待値が文字列でないとき）
  - 短絡評価（後ろの条件のエラーは起きない）
- **変数がない場合。** スキップされたノードやないフィールドを参照すると、graphon と同じく `Variable ... not found`（`ValueError`）で失敗する。null という値があるのとは区別する。
- **テンプレート。** 期待値の中の `{{#名前#}}` は、そのステップの入力の名前で解決する。DSL の変換器は、Dify のセレクタ（`node.var`）をそのまま入力の名前にする。
- **ファイルの属性。**
  - 対象: `dify_model_identity` が `__dify__file__` のオブジェクトの配列。
  - `name` は `filename` に、`url` は `remote_url`（なければ `url`）に対応させる。
  - ローカルのファイルの署名付き URL は、コアでは作れない（差分ハーネスで差が出れば、アダプタ側で値を足す）。
- **ブロブ化された値は比べない。** 比べる値がブロブの参照なら、`ValueError` で失敗する。
  - Dify の変数の上限（`MAX_VARIABLE_SIZE`、既定 200KB）より小さい値を状態に残すため、Dify 用の構成では `BlobThreshold` をそれ以上にする。
- **出力。** Dify の if-else と同じく `result` と `selected_case_id` を持ち、分岐には `handle` を使う。
- **ハンドル。**
  - `kairo.switch` のハンドルは、case の ID と `"false"`。`handles` を書いた場合は、それと一致することを検査する。
  - 分岐しないアクションに `handles` を書くと拒否する。
- **テスト:**
  - `TestSwitchConditions`: graphon のテストの移植と、演算子ごとの表
  - `TestSwitchMissingVariable`、`TestSwitchFileConditions`、`TestSwitchCasesAndErrors`、`TestPerNodeHandles`
- **出力を Dify の if-else と同じ形にした（2026-10-05）。** 出力は `{"result", "selected_case_id"}` で、分岐のフィールドも `selected_case_id` にした（`handle` のキーをやめた）。
- **期待値の中のテンプレートの解決も graphon に合わせた。**
  - 見つからない変数は、波括弧を外した名前のまま残る。
  - 値の文字列化は `Segment.text` と同じにした（配列は Python のリストの表記、オブジェクトは `json.dumps`）。
