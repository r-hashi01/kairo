# 0055. 型付きの判断（TypeSafe Jev）を、別モジュールのアクションとして、kairod と埋め込みの両方から使えるようにする

- 状態: 承認
- 日付: 2026-10-08
- 関連: ADR 0009（ワーカーのプロトコル）、0018（依存のある実装は別モジュール）、0029（グラフと分岐）、0039（宛先の制限）、0051（常駐しない kairo）、0052（アクションの HTTP(S)）

## 背景

kairo は、条件分岐を型の付いたフィールド（bool・int・number・enum）でしか許さない（`ir.FieldKind.Branchable`）。LLM の答え（文章）で分岐したいときは、今は利用者が、文章を解析して型付きの値に直すステップを自分で書く必要がある。

TypeSafe Jev は、判断だけを返すモデルである。答えは、文章ではなく、次の型で返る。

- `noul`: はい／いいえの確率（数）
- `choice`: 選択肢の名前（列挙）と、選択肢ごとの確率
- `score`: 点数（数）

これは、kairo の分岐の規則にそのまま合う。答えをノード仕様の `Outputs` に型付きで宣言し、`choice` を `Branch` にすれば、LLM の判断で、解析のステップなしに分岐できる。

`github.com/mattn/go-jev` は、その Go の SDK である（MIT、依存は標準ライブラリだけ、`go 1.27.0` 以上）。

- このために、リポジトリの Go を 1.27.1 に上げた（全モジュール）。
- 上げる前と後で `scripts/bench.sh` を同じ負荷の下で交互に測り、予算の項目が同等か改善することを確かめた。遷移のコストは約 1,180ns から約 1,000ns になった。

制約は次の 2 つである。

- 本体（`kairo` モジュール。kairod を含む）は、標準ライブラリだけを使う。go-jev を直接 import できない。
- 基本の形は、埋め込みのランタイム（TypeScript・Python、ADR 0051）である。そこでは、Go のコードはプロセスの中で動かない。

## 決定

別モジュール `executor/jev`（`go.mod` を持つ）に、go-jev を使う判断のアクションを置く。同じ処理を、2 つの入口から使えるようにする。

### アクション

| アクション | 出力 | 型（`Outputs`） |
|---|---|---|
| `jev.noul` | `{"yes": <確率>, "confidence": <数>}` | `yes`: number |
| `jev.choice` | `{"choice": <名前>, "probabilities": {...}, "confidence": <数>}` | `choice`: enum（選択肢の名前） |
| `jev.score` | `{"score": <数>, "confidence": <数>}` | `score`: number |

- **問い（`instructions`）と選択肢（`criteria`）:** ステップの `params`（kairod の定義）か、入力（埋め込みの `ctx.call`）で渡す。両方にあれば入力を優先する。判断の対象（`state`）は入力の `state` で渡す。
- **選択肢ごとの型:** `jev.choice` の enum の値は、選択肢によって変わる。そのため、利用者は判断ごとにアクションの名前を付けて、ノード仕様を宣言する（例: `jev.choice.route`、`Outputs: {"choice": {"type": "enum", "values": ["billing", "technical", "sales"]}}`、`Branch: "choice"`）。
  - `jev.` で始まる名前の手順は、すべてこのアクションが受ける。`jev.choice.*` は `jev.choice` として動く。
  - 返ってきた名前が、宣言した値にないときは、確定した失敗にする（型の約束を破らない）。
- **作用:** 外に作用しないので、非保護（`unprotected`）として宣言する。宛先（`Destination`）は `typesafe/jev` を既定にし、RPM と同時実行の制限（ADR 0039）をかけられるようにする。
- **失敗の分け方:** go-jev が 429・529 を再試行した後の失敗は、再試行できる失敗にする。そのほかの 4xx は、確定した失敗にする。つながらない、時間切れは、結果不明にする。非保護なので、どれもやり直してよい。
- **鍵:** 環境変数 `TYPESAFE_API_KEY` から読む（go-jev の作法に合わせる）。モデルと URL も、環境変数か引数で変えられるようにする。

### 入口 1: kairod のワーカー

- `executor/jev/cmd/kairo-jev -worker <kairod のソケット>` は、ワーカーのプロトコル（ADR 0009、`protocol.Worker`）で kairod につなぎ、`jev.*` の手順を引き取る。
- kairod 本体は go-jev に依存しない。kairod の組み込みの実行器（`http.*`）にはしない。
- Go のプログラムにエンジンを埋め込む利用者は、`jev.Executor` を `RegisterExecutor` に直接渡せる。

### 入口 2: HTTP(S) のアクションのサーバー（埋め込みのランタイム向け）

- `kairo-jev -http <アドレス>` は、ADR 0052 の取り決め（`POST /action`、`Kairo-Signature` の署名、200 で結果）で判断を返す。
- TypeScript と Python の埋め込みのワークフローからは、アクションに `url` を書くだけで使える。
- 判断は数秒で終わるので、200 でその場に返す。202 とコールバックは使わない。
- **Go の側の部品:** 署名の作成と検証、`/action` の受け口を、本体モジュールの新しいパッケージ `httpaction` に置く（標準ライブラリだけ）。go-jev 以外の Go 製のアクションのサーバーも、これで書ける。
  - TypeScript と Python の実装（`sdk/*/http`）と、同じ署名と同じ本文の形にする。互いに呼べることをテストで確かめる。

### 置き場所と検査

- `executor/jev` は、`store/*` と同じく別モジュールにする。本体からは import しない（ADR 0018）。
- `scripts/check.sh` は、別モジュールを今までどおり vet・test する。TypeSafe の API を呼ぶテストは、`TYPESAFE_API_KEY` があるときだけ動かす。それ以外は、手元の偽のサーバー（`WithURL`）で確かめる。
- `govulncheck`（CI）も、今までどおり別モジュールにかける。

## 結果

- LLM に判断させて分岐するワークフローを、文章の解析なしに、型の約束（`Outputs`、`Branch`）の上で書ける。
- kairod と埋め込み（TypeScript・Python）の両方から、同じ判断のアクションを使える。
- **外部の依存:** `github.com/mattn/go-jev`。別モジュールに閉じ、本体は標準ライブラリだけのまま保つ。
- **新しく守ること:**
  - `jev.choice` の答えが、宣言した enum の値にないときは、確定した失敗にする。
  - Go の `httpaction` と、TypeScript・Python の HTTP のアクションの署名と本文の形を、同じに保つ。
- **テスト（足す）:**
  - `executor/jev`: 偽の Jev のサーバーに対して、3 つの型の答えが出力の型どおりになること。宣言にない選択肢を失敗にすること。429 と 4xx と、つながらないときの分け方。`params` と入力の優先の順。
  - `httpaction`: 署名の作成と検証（古い時刻、改ざん、違う秘密）。TypeScript の `sign` で作った署名を Go が受け、Go の署名を TypeScript が受けること。
  - 結合: kairod に `kairo-jev` をワーカーとしてつなぎ、`jev.choice.*` の `Branch` で分岐する定義が、答えどおりの枝に進むこと。
  - 結合: TypeScript の埋め込みのワークフローから、`url` で `kairo-jev -http` を呼び、型付きの答えを受けること。

## 検討した代替案

- **kairod の組み込みの実行器にする（`http.*` と同じ扱い）:** kairod を含む本体が go-jev に依存する。本体は標準ライブラリだけ、という規約に反する。
- **組み込みの `http.*` 実行器で、Jev の API を直接呼ぶ:** 型の解釈（確率と選択肢の取り出し）と、宣言との照合を、定義の側で書くことになる。今の分岐の規則（型付きのフィールドだけ）では、それを書く場所がない。
- **TypeScript と Python の SDK に、Jev の呼び出しをそれぞれ書く:** 言語ごとに同じものを作って保つことになる。Go の SDK を 1 か所で使い、HTTP のアクションとして配るほうが、1 つで済む。そのための取り決めは、ADR 0052 ですでにある。
- **`choice` の enum を実行時に決める:** ノード仕様は、コンパイルのときに決まり、実行中に変わらない（AGENTS.md）。選択肢は、判断ごとのアクションの名前で宣言する。

## 実装で詰めた詳細（2026-10-08 追記、ADR 0017）

- **置き場所:**
  - 判断の実行器は `executor/jev`（別モジュール `kairo/executor/jev`）に、コマンドは `executor/jev/cmd/kairo-jev` に置いた。依存は `github.com/mattn/go-jev v0.0.3` だけである。
  - 署名と受け口は `httpaction`（本体モジュール、標準ライブラリだけ）に置いた。
- **選択肢の照合:** 「宣言した enum の値」との照合は、ステップの問いに渡された選択肢（`criteria`）との照合で行う。
  - 実行器に渡るタスク（`task.Task`）は、ノード仕様を持たないためである。HTTP のアクションの呼び出しも同じである。
  - 選択肢とノード仕様の enum を同じにするのは、定義を書く側の責任とした（手引きの例はそうしている）。
- **選択肢の書き方:** `criteria` は、名前の配列、`{"name", "desc"}` の配列、`{名前: 説明}` のオブジェクトのどれでも受ける。API へは go-jev の `Options` で送るので、オブジェクトの順番も保つ。
- **失敗の分け方:** 5xx は、ADR では決めていなかった。go-jev が再試行しないので、結果不明にした（非保護なのでやり直される）。手引き（`docs/jev.md`）にも表で載せた。
- **HTTP の受け口の振る舞い:**
  - 結果不明は、200 の本文では表せない（SDK の側では成功と区別できない）。そのため `httpaction.Handler` は 502 で返し、呼んだ側は結果不明として扱う。
  - 202 とコールバックは使わない（ADR のとおり）。
- **`httpaction.Handler` の引数:** 実行器と同じ形（`Execute(ctx, task, emit)`）の関数を受けるようにした。ほかの Go の実行器も、そのまま HTTP(S) で配れる。
- **`kairo-jev` の設定:** `-worker`（unix ソケットのパス、または `tcp:host:port`）、`-token`（`KAIRO_WORKER_TOKEN`）、`-actions`、`-concurrency`、`-http`、`-secret`（`KAIRO_ACTION_SECRET`）、`-tls-cert` / `-tls-key`、`-model`（`JEV_MODEL`）、`-url`（`JEV_URL`）。
  - kairod との接続が切れたら、1 秒おいてつなぎ直す。
- **CI:** 脆弱性検査（`govulncheck`）の対象に、`executor/` の下のモジュールを足した。`scripts/check.sh` は、別モジュールを自動で見つけて vet・test するので、変えていない。
- **テスト:**
  - `httpaction`: 署名の検証（ない、違う秘密、改ざん、古い、未来の時刻、読めない時刻）。受け口の 200・502・401・404。TypeScript の SDK の `sign` / `verify` との相互の受け入れ（node で実際に動かす）。
  - `executor/jev`: 偽の Jev のサーバーに対する、3 つの型の出力、`params` と入力の優先、選択肢の 3 つの書き方と順番、選択肢にない答え、429・422・500・つながらないときの分け方。
  - 結合: エンジンとワーカーのプロトコルの上で、`jev.choice.route` の `Branch` が答えどおりの枝に進み、もう一方が飛ばされること。TypeScript の埋め込みのワークフローが、`url` で `httpaction.Handler` を呼び、型付きの答えで分岐すること。
  - 実際の TypeSafe の API を呼ぶテストは、まだない（鍵がないため）。
