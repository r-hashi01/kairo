# 型付きの判断で分岐する（kairo-jev）

LLM に判断させて、その答えで分岐するための手引きです。決定の背景は ADR 0055 にあります。

kairo は、型の付いたフィールド（bool・int・number・enum）でしか分岐を許しません。TypeSafe Jev は、文章ではなく型の決まった答えを返すモデルなので、文章を解析するステップを書かずに、答えでそのまま分岐できます。

| アクション | 出力 | 分岐に使う型 |
|---|---|---|
| `jev.noul` | `{"yes": 0.92, "confidence": 0.8}` | `yes`: number |
| `jev.choice.<名前>` | `{"choice": "billing", "probabilities": {...}, "confidence": 0.7}` | `choice`: enum |
| `jev.score` | `{"score": 7.5, "confidence": 0.6}` | `score`: number |

問いは、ステップの `params` か入力で渡します。両方にあれば、入力が優先です。

```json
{"state": "判断の対象", "instructions": "問い", "criteria": {"billing": "支払い", "technical": "障害"}}
```

`criteria` は、選択肢の名前の配列、`{"name", "desc"}` の配列、`{名前: 説明}` のオブジェクトのどれでも書けます。答えが選択肢にないときは、確定した失敗になります。

## 起動

```sh
cd executor/jev
export TYPESAFE_API_KEY=...          # JEV_MODEL、JEV_URL で、モデルと宛先を変えられる

# kairod のワーカーとして
go run ./cmd/kairo-jev -worker /var/lib/kairo/worker.sock -actions jev.noul,jev.choice.route

# 埋め込みのランタイム（TypeScript・Python）向けの HTTP(S) のアクションとして
KAIRO_ACTION_SECRET=... go run ./cmd/kairo-jev -http :8443 -tls-cert cert.pem -tls-key key.pem
```

両方を同時に指定することもできます。

## kairod の定義で分岐する

判断ごとに名前を付けて、ノード仕様に選択肢を enum として宣言し、`branch` をその名前にします。

```json
{"action": "jev.choice.route", "effect": "unprotected", "destination": "typesafe/jev", "branch": "choice",
 "outputs": {"choice": {"type": "enum", "values": ["billing", "technical"]}}}
```

```json
{"name": "route", "root": {"kind": "graph", "id": "g",
  "nodes": [
    {"kind": "step", "id": "cls", "action": "jev.choice.route", "input": {"state": "$input.text"},
     "params": {"instructions": "Which team should handle this?", "criteria": {"billing": "Payments", "technical": "Bugs, outages"}}},
    {"kind": "step", "id": "billing", "action": "notify.billing"},
    {"kind": "step", "id": "technical", "action": "notify.technical"}],
  "edges": [
    {"from": "cls", "handle": "billing", "to": "billing"},
    {"from": "cls", "handle": "technical", "to": "technical"}]}}
```

## 埋め込みのワークフローから使う

アクションに `url` を書くと、`kairo-jev -http` を ADR 0052 の取り決め（署名付きの POST）で呼びます。

```ts
const k = new Kairo({ backend, secret: process.env.KAIRO_ACTION_SECRET, callbackUrl: 'https://app.example.com/kairo/callback' });
k.defineAction('jev.noul', { effect: 'unprotected', url: 'https://jev.internal.example.com/kairo/action', handler: async () => null });
k.workflow('triage', async (ctx, text: string) => {
	const d = await ctx.call('jev.noul', { state: text, instructions: 'Is this urgent?' });
	return d.yes > 0.5 ? ctx.call('page', text) : 'later';
});
```

## Go のプログラムに埋め込む

エンジンを Go のプログラムに埋め込んでいるなら、`jev.New(client)` を実行器として登録できます。

```go
x := jev.New(jevapi.NewClient(jevapi.WithAPIKey(os.Getenv("TYPESAFE_API_KEY"))))
e.RegisterExecutor([]string{"jev.noul", "jev.choice.route"}, 16, x)
```

## 失敗の分け方

| 起きたこと | 扱い |
|---|---|
| 429・529（go-jev が再試行した後） | 再試行できる失敗（宛先の同時実行を下げる） |
| そのほかの 4xx | 確定した失敗 |
| 5xx、つながらない、時間切れ | 結果不明 |
| 答えが選択肢にない | 確定した失敗（`invalid_answer`） |

判断は外に作用しないので、非保護として宣言します。どの失敗も、やり直して構いません。
