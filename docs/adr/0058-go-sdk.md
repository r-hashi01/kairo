# 0058. Go の SDK を用意する: コアを WASM なしで埋め込み、TypeScript・Python と同じ表と同じ取り決めで動かす

- 状態: 承認
- 日付: 2026-10-08
- 関連: ADR 0049（コードで書く API）、0051（常駐しない kairo）、0052（HTTP(S) のアクション）、0053（スケジューラーの差し込み口）、0054（終わった実行の削除）、0057（配布）

## 背景

kairo は Go で書かれていますが、利用者向けの SDK は TypeScript と Python にしかありません。Go の利用者が使えるのは、次の 2 つだけです。

- エンジン（`engine.New`）を、kairod と同じ形（ファイルの WAL、シャード）でライブラリとして使う。
- ワーカーのクライアント（`protocol.Worker`）で、kairod から仕事を受ける。

コードで書くワークフローの API（ADR 0049）、DB だけで動く埋め込みのランタイム（ADR 0051）、HTTP(S) のアクションの受け口（ADR 0052）、スケジューラーの差し込み口（ADR 0053）は、Go にはありません。

KairoCode（kairo の上に作るコーディングエージェント、別のリポジトリ）は、Go で作ることにしました（2026-10-08、利用者が決定）。Go の SDK は、その土台になります。

Go には、ほかの言語にない利点があります。

- **速い:** コアを WASM を通さずに、そのまま呼べます。1 イベントあたり約 5µs で、Node の WASM（約 20µs）の 4 分の 1、Python の WASM（約 111µs）の 20 分の 1 です（ADR 0051 の計測）。
- **配りやすい:** 1 つのバイナリで配れます。WASM のランタイムも要りません。

## 決定

本体のモジュールに、Go の SDK のパッケージ `github.com/r-hashi01/kairo/sdk/go`（パッケージ名 `kairo`）を足す。TypeScript・Python の SDK と、同じ意味、同じ表、同じ取り決めで動かす。

### 1. コードで書くワークフロー

```go
k, err := kairo.Open(ctx, kairo.Options{DB: db, Dialect: kairo.SQLite})

kairo.Action(k, "classify", kairo.Unprotected, func(ctx *kairo.TaskContext, req Req) (Verdict, error) { ... })
kairo.Action(k, "refund-payment", kairo.Real, func(ctx *kairo.TaskContext, req Req) (Receipt, error) { ... })

kairo.Workflow(k, "refund", func(ctx *kairo.Context, req Req) (Receipt, error) {
	v, err := kairo.Call[Verdict](ctx, "classify", req)
	if err != nil {
		return Receipt{}, err
	}
	if v.Risky {
		if _, err := kairo.WaitFor[Approval](ctx, "approve"); err != nil {
			return Receipt{}, err
		}
	}
	if err := ctx.Sleep(time.Minute); err != nil {
		return Receipt{}, err
	}
	return kairo.Call[Receipt](ctx, "refund-payment", req)
})

err = k.Start(ctx)
out, err := kairo.Run[Receipt](ctx, k, "refund", req, kairo.WithID("refund-o-42"))
```

- **型:** 入力と出力は、ジェネリクスで型を付ける。中身は JSON で記録する（ほかの言語と同じ）。
- **呼び出しの ID:** ADR 0049 と同じ規則にする。`<ワークフローの ID>/<sha256(種類 + "\0" + 正規化した JSON の入力) の先頭 24 文字>.<n>` で、正規化は鍵を並べた JSON にする。Go のワークフローと TypeScript のワークフローが、同じ入力から同じ ID を求める。
- **組み込み:** `ctx.Now()`、`ctx.Random()`、`ctx.Sleep(d)`、`kairo.WaitFor[T]`、`kairo.Child[T]`（子のワークフロー）。
- **並列:** Go の呼び出しはブロックする。並列は `kairo.Parallel(ctx, fns...)` で書く。
  - goroutine を自由に使うと、呼び出しの順番（呼び出しの ID の `n`）が決まらない。そこで `Parallel` は、各関数の呼び出しの ID を、関数の並び順で決めてから goroutine を起こす。
  - ワークフローの中で、`Parallel` の外で goroutine を使うことは禁止する（文書に書き、可能な範囲で検出する）。
- **取り消しと中断:** `ctx` は `context.Context` でもあり、取り消されると呼び出しが `kairo.ErrCancelled` を返す。サーバーレスの形（suspend）では、待ちに当たると `kairo.ErrSuspended` を返す。
- **閉じるとき:** プロセスを閉じても、待っていた呼び出しは取り消さない（ADR 0050 で TypeScript の SDK を直したのと同じ）。

### 2. 埋め込みのランタイム

- **コア:** `wasmcore` の `Core`（TypeScript・Python が WASM 越しに呼ぶのと同じもの）を、直接呼ぶ。出来事と命令の JSON の形も同じにし、意味のずれを防ぐ。
- **保存先:** 標準ライブラリの `database/sql` を使う。ドライバーは利用者が渡す（SQLite なら `modernc.org/sqlite`、PostgreSQL なら `pgx` の `stdlib`）。本体のモジュールは、依存を足さない。
  - 方言は `kairo.SQLite` と `kairo.Postgres` の 2 つ。プレースホルダー、行のロック（`BEGIN IMMEDIATE` と `FOR UPDATE`）、`ON CONFLICT` の違いを吸収する。
  - **表は TypeScript・Python と同じ**（`run`・`event`・`timer`・`lease`、`parent` の列、索引）。Go のプロセスと TypeScript のプロセスが、同じ DB の同じ実行を扱える。
- **ほかのプロセスの終わりで起きる:** PostgreSQL の `LISTEN/NOTIFY` は、ドライバーごとに API が違い、`database/sql` にはない。差し込み口（`Options.Notify`）を用意し、`pgx` を使う実装を別モジュール `github.com/r-hashi01/kairo/sdk/go/pgnotify` に置く（ADR 0018 の考え方）。差し込まなければ、ほかのプロセスでの終わりは、`Tick` と作業のついでの掃除で拾う（TypeScript の SQLite と同じ）。
- **借用、期限切れの回復、終わった実行の削除:** ADR 0051・0054 を、TypeScript と同じ規則で実装する（借用の期間、`KAIRO_KEEP_FINISHED`、木の単位の削除）。

### 3. 他の言語にもまだないもので、Go の SDK で最初から持つもの

KairoCode に要るので、Go の SDK では最初から持たせる。TypeScript・Python には、後で同じものを足す（別の ADR）。

- **ライブの出力（ストリーミング）:** アクションの `ctx.Emit(chunk)` を、同じプロセスの購読者（`k.Subscribe(runID)`）に届ける。記録はしない（kairod の live と同じ考え方、欠けてもよい）。
- **要確認の決着:** `k.Resolve(callID, output)` と `k.ResolveError(callID, msg)`。要確認（blocked）で止まったステップを、人の答えで決着させる（kairod の `Resolve` と同じ意味）。
- **子の実行の一覧:** `k.Children(runID)`。親の ID から子の実行を引く（UI と再生に使う）。`parent` の列と索引は ADR 0054 ですでにある。

### 4. kairod をバックエンドにする

- `kairo.Open` の代わりに `kairo.Connect(url, worker)` で、kairod の HTTP API とワーカーのプロトコル（`protocol.Worker`）を使う。
- ワークフローのコードは同じ。呼び出しは、`keep_output` を付けて投入する（ADR 0050）。

### 5. HTTP(S) のアクションとスケジューラー

- `k.Handler()` は `http.Handler` を返し、`/action`・`/callback`・`/tick` を受ける。取り決めは ADR 0052・0053 のとおり。署名には `httpaction` を使う。
- `kairo.TickHandler(open)` は、関数を呼ぶ形のスケジューラー（AWS Lambda など）の入口。

## 結果

- Go の利用者が、TypeScript・Python と同じ書き心地で、コードで書くワークフローを使える。KairoCode の土台になる。
- 3 つの SDK が、同じ DB の同じ実行を扱える。たとえば、Go のサービスが始めたワークフローを、Python のワーカーのプロセスが再開できる。
- **新しく守ること:**
  - 3 つの SDK で、呼び出しの ID の規則、表の形、HTTP(S) のアクションの取り決めを同じに保つ。片方を変えたら、もう片方も変える。
  - Go のワークフローの中で、`Parallel` の外で goroutine を使わない。
- **形式:** 新しい形式は作らない。TypeScript・Python と同じ表を使う。
- **テスト（足す）:**
  - TypeScript・Python の SDK のテストと同じ内容を、Go に移す（再開して二度動かない、眠りとシグナル、取り消し、サーバーレス、HTTP(S) のアクション、`/tick` と `wake`、終わった実行の削除）。
  - **言語をまたぐテスト:** 同じ SQLite のファイルで、Go のプロセスが始めたワークフローを TypeScript のプロセスが続け、その逆も行う。呼び出しの ID がそろい、実の作用が二度起きないこと。
  - 呼び出しの ID の規則の一致（同じ入力から、3 つの SDK が同じ ID を求める）。
  - ライブの出力、決着、子の一覧。
  - 性能: 埋め込みの Go の SDK で、SQLite と PostgreSQL の、端から端までの実行の数を測る（ADR 0051 の表に足す）。

## 検討した代替案

- **Go の利用者には、エンジン（`engine.New`）をそのまま使ってもらう:** エンジンは kairod の形（ファイルの WAL、シャード、常駐）で、DB だけで動く形ではない。コードで書く API もない。
- **Go からも WASM のコアを呼ぶ:** 意味のずれは防げるが、Go の中で WASM のランタイムが要り、遅くなる。`wasmcore.Core` は Go のコードそのものなので、直接呼べば同じ意味のまま速い。
- **保存先を別モジュール（ドライバーごと）にする:** `database/sql` で足りるので、本体に置ける。ドライバーの選択は利用者に任せる。`LISTEN/NOTIFY` だけは標準の API がないので、別モジュールにする。
- **パッケージを `github.com/r-hashi01/kairo/kairo` などにする:** `sdk/ts`、`sdk/python` と並べて `sdk/go` にした。import の path の末尾は `go` になるが、パッケージ名は `kairo` にする。

## 実装で詰めた詳細（2026-10-09 追記、ADR 0017）

決定の 1・2・3・5 を実装した。4（kairod をバックエンドにする `kairo.Connect`）は、まだ実装していない（KairoCode には要らないので後にした）。

- **パッケージの構成:**
  - `sdk/go`（パッケージ `kairo`）: 本体のモジュールの中。依存はない。
  - `sdk/go/kairotest`: 振る舞いのテストを、保存先を引数に取る公開の一式にしたもの。`sdk/go` はメモリの保存先で、`sdk/go/storetest` は実際の SQLite と PostgreSQL で、同じ一式を流す。
  - `sdk/go/storetest`（別モジュール）: ドライバー（`modernc.org/sqlite`、`pgx`）を持つテスト専用のモジュール。言語をまたぐテストも置く。
  - `sdk/go/pgnotify`（別モジュール）: `pgx` で `LISTEN` する。`pgnotify.Wrap(store, dsn)` で SQL の保存先を包み、`Notifier` を足す。
- **保存先:** `Store` を interface にし、`MemStore`（メモリ）と `SQLStore`（`database/sql`）を用意した。
  - SQLite では、`SQLStore` が接続を 1 本に絞り（`SetMaxOpenConns(1)`）、`BEGIN IMMEDIATE` で書き込みのロックを先に取る。`database/sql` には `BEGIN IMMEDIATE` の指定がないので、接続を取り出して自分で `BEGIN` を送る。
  - 利用者の `*sql.DB` は閉じない（`Close` は何もしない）。
- **呼び出しの ID の正規化:** TypeScript の `canonical` と、バイトの単位で一致させた。
  - `encoding/json` は `<`・`>`・`&`・U+2028・U+2029 をエスケープし、`-0` を `-0` と書く。そのため、文字列の書き出しと `-0` は自前にした。
  - 鍵は、JavaScript の既定の並べ方（UTF-16 の単位）で並べる。
  - TypeScript の実装で作った 19 件の正解（`testdata/callids.jsonl`）と一致することを、テストで確かめる。
  - **既存のずれ（この ADR では直していない）:** Python の SDK は `json.dumps(sort_keys=True)` を使っている。そのため、鍵の並べ方（コードポイントの順）と、整数の値の浮動小数（`1.0` を `1.0` と書く）で、TypeScript・Go と ID がずれる場合がある。Python を直すのは別の変更にする。
- **`Parallel` の中の呼び出しの ID:** 決定では「呼び出しの ID を並び順で決めてから goroutine を起こす」としたが、関数を動かす前に、中でどの呼び出しをするかは分からない。そこで、`Parallel` の枝ごとに番号（`p0`、`p1`、入れ子は `p0.p1`）を付け、枝の中の呼び出しの ID を `<ワークフロー>/<ハッシュ>.<枝>.<n>` にした。
  - Go の中では決定的で、同じ入力の呼び出しが別の枝にあっても衝突しない。
  - その代わり、`Parallel` の中の呼び出しは、TypeScript の `Promise.all` の中の呼び出しと ID が一致しない。言語をまたいで同じワークフローを続けるのは、`Parallel` の外の呼び出しに限られる（文書に書く）。
- **要確認の呼び出し:** 呼び出しが要確認（blocked）で止まったら、wait の形では決着するまで待ち、suspend の形では `ErrSuspended` を返す。決着（`Resolve`）すると、ワークフローが続く（suspend の形では、決着で呼び出しが終わったことを受けて再駆動する）。
  - TypeScript と Python の SDK は、要確認の呼び出しをエラーとして返し、ワークフローを失敗させる。決着の API を TypeScript・Python に足すときに、こちらにそろえる。
- **ライブの出力:** `k.Subscribe(id)` は、`id` と、その下の実行（ID が `id + "/"` で始まるもの: 呼び出しと子のワークフロー）の出力を届ける。記録はせず、受け手が遅ければ捨てる。
- **作業の終わりを待つ:** `sync.WaitGroup` は、`Wait` の最中に 0 から `Add` すると誤用になる。作業が新しい作業を生むので、数と条件変数（`sync.Cond`）で待つ。
- **テスト:**
  - `sdk/go`: 呼び出しの ID の一致（19 件）、期間の設定、HTTP(S) のアクション（200、別のプロセスへの 202 とコールバック、署名、手元でない `http://`）、`/tick` と `wake`。
  - `kairotest`（メモリ・SQLite・PostgreSQL で流す、7 件）: 別のプロセスでの再開で二度動かない（`Parallel` を含む）、眠り・シグナル・子のワークフロー・子の一覧、取り消し、要確認の決着、ライブの出力、サーバーレスでの呼び出しをまたぐ再開と `wake`、終わった木の削除。
  - `storetest`: 言語をまたぐテスト。同じ SQLite のファイルで、Go が始めたワークフローを TypeScript が続ける場合と、その逆。どちらも、続けた側は終わった呼び出しを動かさない。
  - `storetest`: `pgnotify` で、別のプロセスで終わった実行が、ここの待ちをすぐ起こす。`pgnotify` で包まないと落ちることも確かめた。
  - race 検出器つきでも通る。

### Python の正規化をそろえた（2026-10-09 追記）

Python の SDK の `_canonical` は `json.dumps(sort_keys=True)` だったので、TypeScript・Go と同じ値でも違う文字列になることがあった。違いは次のとおり。
- 小数部のない浮動小数点数（`1.0`）
- 指数の書き方（`1e-07`）
- 2^53 を超える整数
- `-0.0`、`NaN`
- キーの並び（コードポイント順と UTF-16 の単位順の違い）
- 対になっていないサロゲート

そのため、言語をまたいで再開すると呼び出しの ID がずれ、終わった呼び出しをもう一度実行していた。`JSON.stringify` と 1 文字ずつ同じになるように書き直した。

- 言語共通のテストデータは `sdk/testdata/callids.jsonl` に移し、Go と Python のテストの両方がそれを使う。
- 言語間の再開のテスト（`sdk/go/storetest/xlang_test.go`）に、Python と Go の組を足した。
  - 呼び出しの入力は、上の違いが出る値（`1.0`、全角とサロゲート対のキー）にした。
  - 古い正規化では落ちることを確かめた。
- **互換性：** 古い Python の SDK で始め、上のような値を呼び出しの入力に持ったまま止まっているワークフローは、新しい版で再開すると、その呼び出しの ID が変わり、もう一度実行される。ID が変わるのは、入力にこの一覧の値を含む呼び出しだけである。

### kairod をバックエンドにする（決定 4、2026-10-09 追記）

決定 4 を実装した。

- **形**：`kairo.Connect(ctx, url, worker, kairo.Options{...})`。
  - `Options` は `Open` と同じ型を使う。効くのは `Logger`、`Observe`、`Concurrency`（同時に走らせる手順の数、既定 16）、`WorkerToken`（新設。kairod のワーカーのトークン、ADR 0037）、`Now` の 5 つである。
  - `Store` やサスペンドモードを渡すとエラーにする。
- **実行の置き場所を、小さなインタフェース（`runs`）にまとめた。** 中身は、作る・読む・入力付きで読む・待つ・シグナル・取り消しである。
  - 埋め込みのランタイムは、もとからこの形のメソッドを持っていた。
  - kairod の側（`remote`）は、HTTP API（`/v1/runs` など）でこれらを行う。呼び出しは `keep_output` を付けて投入する（ADR 0050）。
- **手順は、Go の `protocol.Worker` で kairod のワーカーの受け口につないで受ける。** 受けた手順は、埋め込みと同じ `serve` で動かす。つながりが切れたら、つなぎ直す。
  - 観測の `step.started` / `step.finished` を出す。
  - 実行の始まりと決着は kairod のことなので、`run.*` は出さない。
- **ワークフローは、動かすプロセスがそのまま動かす。** 駆動の借用（ADR 0059）は kairod にない。
  - `Run` は埋め込みの待ちモードと同じく、裏で動かして結果を待つ。
  - 版は、kairod に入力を尋ねて読む（ADR 0060 の `GetInput`）。入力を返さない古い kairod では、今の版で動かす。
- **埋め込みにしかないものは、`ErrNeedsEmbedded` を返す。** 対象はサスペンドモード、`List`、`Children`、`Resolve` / `ResolveFailed`、`Tick`、待ちより先に来たシグナル（ADR 0059）である。
  - 結果を後でコールバックで返す HTTP(S) のアクション（ADR 0052）も、kairod の手順を引き渡せないので、結果不明として返す。
- **kairod が結果をもう持っていない実行（ADR 0027 の印だけが残ったもの）は、`ErrResultLost` にする。**
- **テスト**：`sdk/go/kairod_test.go` で、kairod と同じ部品（エンジン、HTTP API、ワーカーの受け口）をテストのプロセスの中に組み立てて確かめる。
  - 確かめること：呼び出し、sleep、シグナル。
  - 途中で止めたプロセスの代わりに別のプロセスが再開し、実の呼び出しが 1 回で済むこと。
  - kairod から読み戻した版で動くこと。
  - 埋め込み専用の API が `ErrNeedsEmbedded` を返すこと。
