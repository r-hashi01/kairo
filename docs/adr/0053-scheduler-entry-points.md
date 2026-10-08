# 0053. サーバーレスのランタイムを、スケジューラーの定期実行か、次の時刻を知らせる一度きりの予約で起こす

- 状態: 承認
- 日付: 2026-10-08
- 関連: 不変条件 6（ポーリングしない）、ADR 0041（実行 1 回あたりのコスト）、0051（常駐しない kairo）、0052（アクションの HTTP(S)）

## 背景

常駐しない形（ADR 0051）の suspend モードでは、次のことが、誰かが `tick()` を呼ぶまで進まない。

- 時刻の来たタイマー（`ctx.sleep`、手順の時間切れ、再試行の待ち時間）
- 期限の切れた借用（止まったプロセスのステップ、コールバックの来ない HTTP のアクション）

ADR 0051 は「スケジューラーから `tick()` を呼ぶ」とだけ決め、差し込み方は決めていない。今は利用者が自分で、関数の入口を作り、`Kairo` を開き、`start()` して `tick()` を呼び、認証もしなければならない。

起こし方には 2 つの形がある。

- **定期実行（cron）:** Vercel Cron、EventBridge Scheduler の rate 式、Cloud Scheduler など。設定が簡単で、どこにでもある。ただし、仕事がなくても起きる（ポーリング）。間隔は最短 1 分なので、`sleep(5s)` も最大 1 分遅れる。
- **一度きりの予約:** EventBridge Scheduler の `at()`、Cloud Tasks の `scheduleTime`、QStash の `Upstash-Not-Before` など。「この時刻に 1 回呼んで」と頼める。仕事があるときだけ、必要な時刻に起きる。kairo の「ポーリングしない」（不変条件 6）に合うのはこちらである。

どちらも、サービスごとに API（SDK）が違う。kairo の本体に、サービスごとの依存は持ち込みたくない（依存は利用者の了承が要る）。

## 決定

SDK に、スケジューラーから呼ばれる入口を 1 つと、「次に起こす時刻」を知らせる差し込み口を 1 つ足す。サービスごとの API を呼ぶ部分は、利用者が書く（例を文書に置く）。

### 1. HTTP の入口 `/tick`（定期実行・一度きりの予約の両方から呼ぶ）

- ADR 0052 の受け口（TypeScript: `k.fetchHandler()`、Python: `k.asgi_app()`）に、3 つ目の道として `<base>/tick` を足す。
  - `GET` と `POST` を受ける（Vercel Cron は `GET` で呼ぶ）。
  - `tick()` を行い、終わってから `200` と `{"next": <unix ms> | null}` を返す。`next` は下の 2. の時刻である。
- **認証:** `Authorization: Bearer <tickSecret>` を確かめ、合わなければ `401` を返す。
  - Vercel Cron が `Authorization: Bearer $CRON_SECRET` を付けて呼ぶ形に合わせた。ほかのサービスも、ヘッダーを 1 つ付けるだけで呼べる。
  - `tickSecret`（Python は `tick_secret`）を設定しなければ、`/tick` は `404` にする（誰でも起こせる状態にしない）。
  - `/action` と `/callback` の署名（ADR 0052）とは別にする。スケジューラーは本文に署名できないためである。
  - 比べるときは、時間が一定の比較を使う。
- `/tick` は冪等である。二重に呼ばれても、同じ時刻のタイマーや借用は、コアとストアの行のロックで 1 回だけ処理される（今の `tick()` と同じ）。

### 2. 次に起こす時刻 `nextWake()` と差し込み口 `wake`

- **`nextWake()`:** ストアから、まだ処理していないタイマーの時刻の最小値と、借用の期限の最小値を読み、早い方を返す。どちらもなければ `null` を返す。
  - ストアに 1 本の問い合わせ（`SELECT MIN(at)` と `SELECT MIN(until)`）を足す。どちらにも索引がある（`timer_at`、`lease_until`）。
  - これはすべての実行についての時刻である。実行ごとではない。
- **`wake` の差し込み口:** `Kairo` の設定に `wake?: (at: number) => Promise<void>` を足す（Python は `wake: Callable[[int], Awaitable[None]]`）。
  - suspend モードで、`run()`・`tick()`・`signal()`・コールバックの処理が終わって戻る前に、`nextWake()` を読み、時刻があれば `wake(at)` を 1 回呼ぶ。
  - 利用者は、`wake` の中で一度きりの予約を作る（例: EventBridge Scheduler の `at()` で `/tick` を呼ぶ）。
  - 予約の重複は利用者の側で許してよい。早すぎた `/tick` は何もせず、次の時刻を返すだけである。遅れた分は、その時に処理される。
- **費用（ADR 0041）:** 増えるのは、suspend モードで処理が戻るときの問い合わせ 1 本と `wake` の呼び出し 1 回だけである。wait モード（常駐するプロセス）では呼ばない。待っているあいだは何も動かない。

### 3. 関数の入口の補助

- TypeScript: `tickHandler(make: () => Promise<Kairo>)` を出す。返すのは、関数を呼ばれるたびに `make()` で `Kairo` を作り（`start()` 済み）、`tick()` して `close()` する関数である。AWS Lambda の EventBridge の呼び出しや、Node のスクリプトから使う。
  - Python も同じ `tick_handler(make)` を出す。
- HTTP で受けられる環境（Vercel、Next.js、Deno、Bun、ASGI のサーバー）では、1. の `/tick` を使う。

### 適用の範囲

- 埋め込みのランタイム（ADR 0051）の suspend モードに入れる。
- 常駐するプロセス（wait モード、kairod）は、自分のタイマーで起きるので要らない。
- Cloudflare Workers は、今の埋め込みのランタイムが `node:wasi` と `node:sqlite` を使うため、動かない。この ADR の対象外とする。

## 結果

- サーバーレスで動かす利用者は、関数 1 つ（`fetchHandler()` / `asgi_app()`）を置き、スケジューラーから `/tick` を呼ばせるだけで動かせる。
- `wake` を使えば、ポーリングせず、必要な時刻にだけ起きられる。`sleep(5s)` も、予約サービスの精度で起きる。
- **形式:** `/tick` の取り決め（認証、応答の `next`）を新しく決めて保つ。ログ・状態・ストアの表の形式は変わらない。
- **新しく守ること:**
  - `tickSecret` のない `/tick` は開かない。
  - `wake` は suspend モードでだけ、処理が戻る前に呼ぶ。待っているあいだは呼ばない。
- **テスト（足す、TypeScript と Python の両方）:**
  - `/tick`: 正しいトークンで時刻の来たタイマーが進み、`next` が次の時刻を返すこと。トークンなし・違うトークンは `401`、`tickSecret` を設定していなければ `404` になること。
  - `nextWake()`: タイマーと借用の早い方を返すこと。何もなければ `null` を返すこと。
  - `wake`: `sleep` で止まったワークフローで、その時刻を受け取ること。受け取った時刻に `/tick` を呼ぶと、ワークフローが先に進むこと（cron なしで最後まで動く）。
  - `tickHandler`: 新しいプロセスとして `tick` し、閉じること。
- 例（文書）: Vercel Cron（`vercel.json` の `crons`）、AWS Lambda と EventBridge Scheduler（rate 式と `at()`）、Cloud Run と Cloud Scheduler / Cloud Tasks、QStash を、SDK の README に置く。

## 検討した代替案

- **定期実行だけにする:** 簡単だが、仕事がなくても起き続ける（ポーリング）。`sleep` の精度も 1 分になる。定期実行は、`wake` の取りこぼしを拾う備えとして残す。
- **サービスごとの差し込み（EventBridge、Cloud Tasks、QStash）を SDK に入れる:** それぞれの SDK への依存が増え、本体を依存なしに保てない。必要なら、別のパッケージとして足せる（ADR 0018 の考え方）。
- **`wake` を、タイマーを仕掛けるたびに呼ぶ:** 1 回の処理でタイマーが何度も仕掛けられ、予約の数が増える。戻る前に、全体で一番早い時刻を 1 回だけ知らせれば足りる。
- **`/tick` にも本文の署名を求める:** スケジューラーは、決まったヘッダーは付けられるが、本文に HMAC を付けられない。
