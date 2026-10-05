# compat/dify — Dify のワークフローを kairo で動かすための変換器と差分ハーネス

ADR 0028〜0036 の実装の一部です。kairo 本体（`kairo` モジュール）の依存を増やさないように、別のモジュールにしています。

## 変換器（`convert.go`）

Dify の DSL の `workflow`（グラフ、会話変数、環境変数）を JSON で受け取り、kairo の定義（`ir.Definition`）と、使う `dify.*` アクションのノード仕様を返します。

- グラフは、グラフのまま変換します（ADR 0029）。iteration と loop の子ノードは、その本体のグラフになります。
- 純粋なノードは、kairo の保護ステップになります。

  | Dify のノード | kairo のアクション |
  |---|---|
  | start、end | `kairo.pass` |
  | answer | `kairo.template` |
  | if-else | `kairo.switch` |
  | variable-aggregator | `kairo.coalesce` |
  | assigner | `kairo.assign` |
  | iteration | `map` |
  | loop | `loop` |

- 外部に作用するノード（llm、code、http-request、tool、template-transform、question-classifier など）は、アクション `dify.<種類>` のステップになり、ワーカーが実行します。
  - パラメータはノードの設定です。
  - 入力は、設定が参照する変数で、セレクタ（`node.var`）を名前にします。
  - 作用の型は ADR 0035 の表のとおりです。
- 実行の入力は `{<開始ノードの変数>: 値, ..., "sys": {"query": ...}}` です。
- 会話変数は、実行の変数（ADR 0033）になります。

## 差分ハーネス（`harness/`、`harness_test.go`）

Dify のワークフローのテスト用データを graphon 0.7.0 で実行し、その結果を正解データ（`testdata/graphon/*.json`）にします。`TestGraphonParity` が、同じワークフローを変換して kairo のコアで実行し、次の 3 つを比べます。

- 実行の状態
- 成功（または例外）で終わったノードとその出力
- 実行全体の出力

外部に作用するノードは、両側で同じモックにします（`default_outputs` と `mockOutputs`）。ケースのファイル（`harness/cases/<データ>.<名前>.json`）で、入力、ノードの出力、エラーを変えられます。`harness/fixtures/` は kairo が自前で足したデータです。

正解データの作り直し（graphon 0.7.0 の入った Python 環境が要ります。CI では、リポジトリにある正解データだけを使います）:

```sh
GRAPHON_PYTHON=/path/to/venv/bin/python \
DIFY_FIXTURES=/path/to/dify/api/tests/fixtures/workflow \
  compat/dify/harness/gen_golden.sh
```
