# kairo-worker — kairo のワーカー SDK（Python）

kairo のワーカープロトコル（ADR 0009。取り消しは ADR 0026）の Python 実装と、Dify のワークフローのノード（graphon）を kairo のタスクとして実行する実行器です（ADR 0028）。

## ワーカー

```python
from kairo_worker import Result, Worker

def handler(task, ctx):
    ctx.emit("streamed text")          # ライブの出力
    if ctx.cancelled.is_set():         # 手順が打ち切られた（ADR 0026）
        return Result(error="cancelled", retryable=True)
    return Result(output={"echo": task.input}, meta={"usage": 1})

Worker("py-1", ["my.action"], handler, concurrency=8).run("/path/to/worker.sock")
```

- ハンドラは、取り消されたときも必ず `Result` を返します。返すことで、ランタイム側の同時実行の枠とクレジットが空きます。
- 失敗は、確定した失敗（`retryable`）と、結果が不明なもの（`unknown`）を分けて返します。
- 依存はありません（標準ライブラリだけ）。

## graphon のノードの実行（`kairo_worker.graphon`）

`compat/dify` が変換したワークフローの `dify.<種類>` のステップを、graphon 0.7.0 のノードで実行します。

```python
from kairo_worker import Worker
from kairo_worker.graphon import GraphonNodeRunner, slim_factory

runner = GraphonNodeRunner(slim_factory())   # Dify では DifyNodeFactory を作る関数を渡す
Worker("graphon", ["dify.template-transform", "dify.http-request"], runner).run(sock)
```

コマンドとしても起動できます: `python -m kairo_worker.serve --socket <path> --actions dify.template-transform,dify.http-request`

- **入力:** タスクの入力（セレクタ → 値）を新しい変数プールに入れ、ノードを作って実行します。
- **出力:**
  - ストリームのチャンクは、ライブの出力として送ります。
  - 出力はタスクの出力にします。
  - `inputs`、`process_data`、`metadata` は `meta` に入れます（実行イベントの出口、ADR 0034）。
- **失敗:**
  - graphon の `error_type` をエラーの種類にします。
  - 外部に作用するノード（http-request、tool、agent）がタイムアウトや切断で失敗したときは、「結果が不明」として返します（ADR 0035）。

## テスト

```sh
python3 -m unittest discover -s tests            # プロトコルとワーカー（依存なし）
GRAPHON_PYTHON=/path/to/python go test ./compat/dify -run EndToEnd   # graphon のノードを含むエンドツーエンド
```
