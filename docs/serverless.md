# サーバーレスで動かす

常駐するプロセスなしで kairo のワークフローを動かすための手引きです。決定の背景は ADR 0051（常駐しない kairo）、0052（アクションの HTTP(S)）、0053（スケジューラーの差し込み口）にあります。

## 仕組み

suspend モードの `Kairo` は、今進められるところまで進めて返ります。`run()` は、タイマーやシグナルを待つ呼び出しに当たると `Suspended` を投げます。続きは、次のどれかが起きたときに進みます。

- **`/tick`（または `tick()`）:** 時刻の来たタイマー（`sleep`、時間切れ、再試行の待ち）と、期限の切れた借用を処理します。
- **`signal()`:** シグナルを待っている呼び出しに届けます。
- **`/callback`:** 別の場所で動いた HTTP のアクションの結果を受け取ります。

`/tick` を呼ぶ方法は 2 つあります。

| 方法 | 使うもの | 起きる時刻 | 向いている場面 |
|---|---|---|---|
| 定期実行 | Vercel Cron、EventBridge Scheduler の rate 式、Cloud Scheduler | 仕事がなくても毎回起きる。`sleep(5s)` も次の回まで遅れる | まず動かしたいとき。取りこぼしを拾う備え |
| 一度きりの予約 | `wake` から EventBridge Scheduler の `at()`、Cloud Tasks、QStash | 必要な時刻にだけ起きる | 本番。ポーリングしたくないとき |

両方を併用するのがおすすめです。予約で必要な時刻に起こし、粗い定期実行（数分〜1 時間ごと）で、予約の作り損ねを拾います。`/tick` は何度呼んでも安全です。

## 共通の準備

- **保存先は PostgreSQL にします。** サーバーレスの関数のファイルシステムは呼び出しをまたいで残らないので、SQLite は使えません。
- **呼び出しのたびに `Kairo` を作り、`start()` します。** 作るたびに WASM の読み込みがかかります（Node で約 20ms、Python で約 240ms。ADR 0051 の計測）。
- **秘密を 2 つ用意します。** `tickSecret` は `/tick` の Bearer トークン、`secret` は HTTP のアクションとコールバックの署名に使います。HTTP のアクションを使わないなら、`secret` は要りません。

```ts
// kairo.ts
import { EmbeddedBackend, Kairo, PostgresStore } from '@kairo/worker';
import pg from 'pg';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

export async function make(wake?: (at: number) => Promise<void>): Promise<Kairo> {
	const k = new Kairo({
		backend: await EmbeddedBackend.open({ store: new PostgresStore(pool) }),
		mode: 'suspend',
		tickSecret: process.env.CRON_SECRET,
		wake,
	});
	k.defineAction('fetch', { effect: 'unprotected', handler: async (url: string) => (await fetch(url)).text() });
	k.defineAction('summarize', { effect: 'unprotected', handler: async (text: string) => text.slice(0, 200) });
	k.workflow('digest', async (ctx, url: string) => {
		const text = await ctx.call('fetch', url);
		await ctx.sleep(60_000);
		return ctx.call('summarize', text);
	});
	await k.start();
	return k;
}
```

## Vercel（Next.js）と Vercel Cron

1 つのルートで `/action`、`/callback`、`/tick` を受けます。Vercel Cron は、`CRON_SECRET` を設定すると `Authorization: Bearer <CRON_SECRET>` を付けて `GET` で呼びます。これを `tickSecret` に渡します。

```ts
// app/api/kairo/[...path]/route.ts
import { make } from '@/kairo';

async function handle(req: Request): Promise<Response> {
	const k = await make();
	try {
		return await k.fetchHandler()(req);
	} finally {
		await k.close();
	}
}
export { handle as GET, handle as POST };
```

```json
// vercel.json
{ "crons": [{ "path": "/api/kairo/tick", "schedule": "*/5 * * * *" }] }
```

定期実行の間隔の下限は、プランによって違います。短い `sleep` を正確に起こしたいなら、下の一度きりの予約を併用します。

## AWS Lambda と EventBridge Scheduler

`tickHandler()` を Lambda の入口にします。HTTP を通さないので、トークンは要りません。

```ts
// tick.ts（Lambda の入口）
import { tickHandler } from '@kairo/worker';
import { make } from './kairo';

export const handler = tickHandler(() => make(wake));
```

`wake` で、その時刻に 1 回だけこの Lambda を呼ぶ予約を作ります（`@aws-sdk/client-scheduler`。依存は利用者のアプリの側に置きます）。

```ts
import { CreateScheduleCommand, SchedulerClient } from '@aws-sdk/client-scheduler';

const scheduler = new SchedulerClient({});

async function wake(at: number): Promise<void> {
	const when = new Date(Math.max(at, Date.now() + 1000)).toISOString().slice(0, 19); // at() は秒まで、UTC
	await scheduler.send(new CreateScheduleCommand({
		Name: `kairo-${at}`, // 同じ時刻の予約は 1 つにまとまる（重複の作成はエラーになるので握りつぶしてよい）
		ScheduleExpression: `at(${when})`,
		ScheduleExpressionTimezone: 'UTC',
		FlexibleTimeWindow: { Mode: 'OFF' },
		ActionAfterCompletion: 'DELETE',
		Target: { Arn: process.env.TICK_LAMBDA_ARN!, RoleArn: process.env.SCHEDULER_ROLE_ARN! },
	})).catch((e) => { if (e.name !== 'ConflictException') throw e; });
}
```

取りこぼしの備えとして、同じ Lambda を rate 式（`rate(15 minutes)` など）でも呼ばせます。

## Python（ASGI）

`k.asgi_app()` を、FastAPI や Starlette にマウントします。

```python
import os

from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool
from kairo_worker import EmbeddedBackend, Kairo, PostgresStore

pool = AsyncConnectionPool(os.environ["DATABASE_URL"], open=False)


async def make(wake=None) -> Kairo:
    await pool.open()  # 2 回目からは何もしない
    k = Kairo(backend=await EmbeddedBackend.open(PostgresStore(pool)), mode="suspend",
              tick_secret=os.environ["CRON_SECRET"], wake=wake)

    @k.action("summarize", effect="unprotected")
    def summarize(text, ctx):
        return text[:200]

    @k.workflow("digest")
    async def digest(ctx, text):
        await ctx.sleep(60)
        return await ctx.call("summarize", text)

    await k.start()
    return k


async def kairo_app(scope, receive, send):
    """/kairo/action、/kairo/callback、/kairo/tick を受ける ASGI のアプリ。"""
    k = await make()
    try:
        await k.asgi_app()(scope, receive, send)
    finally:
        await k.close()


app = FastAPI()
app.mount("/kairo", kairo_app)
```

Lambda などから直接呼ぶときは、`tick_handler(make)` を使います（`asyncio.run(handler())`）。

## 一度きりの予約のほかの例

`wake(at)` の中で、時刻 `at`（unix ms）に `/tick` を `Authorization: Bearer <tickSecret>` 付きで呼ぶ予約を作れば、どのサービスでも構いません。

- **Google Cloud Tasks:** HTTP のタスクを、`scheduleTime` を `at` にして作ります。ヘッダーに `Authorization` を入れます。
- **Upstash QStash:** `/tick` の URL に公開し、`Upstash-Not-Before`（unix 秒）を `at` にします。`Authorization` は `Upstash-Forward-Authorization` で転送させます。

`wake` は、suspend モードで `run()`・`tick()`・`signal()`・コールバックが戻る前に、そのとき DB にある一番早い時刻で 1 回呼ばれます。同じ時刻で何度も呼ばれることがあるので、予約サービスの側で重複をまとめるか、重複を許してください。早すぎた `/tick` は何もせずに次の時刻を返すだけなので、害はありません。

## 対応していない環境

- **Cloudflare Workers:** 今の埋め込みのランタイムは `node:wasi` と `node:sqlite` を使うので、動きません。
