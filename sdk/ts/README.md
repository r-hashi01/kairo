# kairo-sdk for TypeScript

**Durable workflows that run inside your Node process.** No workflow server, no broker: the kairo core runs here as WebAssembly, and SQLite or PostgreSQL holds the state.

Write a workflow as an ordinary `async` function. Every call it makes is recorded, so after a crash or a redeploy, running it again with the same id picks up where it stopped. Finished calls return their recorded results, and nothing that acts on the world runs twice.

[GitHub](https://github.com/r-hashi01/kairo) · [Serverless guide](https://github.com/r-hashi01/kairo/blob/main/docs/serverless.md) · [Python SDK](https://pypi.org/project/kairo-sdk/) · MIT

```sh
npm install kairo-sdk        # Node 22+; no runtime dependencies
```

## Quick start

```ts
import { EmbeddedBackend, Kairo, SQLiteStore } from 'kairo-sdk';

const k = new Kairo({ backend: await EmbeddedBackend.open({ store: new SQLiteStore('kairo.db') }) });

k.defineAction('classify', {
  effect: 'unprotected',                 // safe to run again after a crash
  handler: async (req: { amount: number }) => ({ risky: req.amount > 100 }),
});
k.defineAction('refund-payment', {
  effect: 'real',                        // recorded before it runs; never runs twice
  handler: async (req: { order: string }) => ({ refunded: req.order }),
});

k.workflow('refund', async (ctx, req: { order: string; amount: number }) => {
  const verdict = await ctx.call('classify', req);
  if (verdict.risky) await ctx.waitFor('approve');    // wait days for a human, at no cost
  await ctx.sleep(60_000);                            // a durable timer (ms)
  return ctx.call('refund-payment', req);
});

await k.start();
console.log(await k.run('refund', { order: 'o-42', amount: 50 }, { id: 'refund-o-42' }));
await k.close();
```

## The model

### Actions and their effects

An action is a function your workflows call. You declare what it does to the world, and kairo decides from that how it may be retried.

| `effect` | Meaning | If the process dies while it runs |
|---|---|---|
| `'unprotected'` | Safe to repeat: an LLM call, a read, a computation | It runs again |
| `'real'` (the default) | Acts on the world: a payment, an email, a write | It stops for review (`blocked`). It is never re-run blindly |

A real step is dispatched only after its intent is committed to the database. Options: `timeout` (`'30s'`, `'5m'`), `destination` (a rate-limit key), `maxAttempts` and `backoff` (below), and `url` for [actions over HTTP(S)](#actions-over-https).

A handler that throws fails its step for good. To say otherwise, throw one of these:

| Thrown | Means | The step |
|---|---|---|
| `RetryableError` | It did not take effect: try again | Is retried up to `maxAttempts` (default 3; 1 for a real action), `backoff` apart (default `'200ms'`, doubled each time) |
| `UnknownOutcomeError` | It may have taken effect | Is never taken for success: a real step stops for review, an unprotected one is retried |

### Workflows

```ts
k.workflow('name', async (ctx, input) => { ... });
```

| In a workflow | Does |
|---|---|
| `ctx.call(action, input)` | Runs an action once, however often the workflow runs again |
| `ctx.parallel([() => ctx.call(...), ...])` | Runs calls at once |
| `ctx.sleep(ms)` | Waits in the database, not in memory: the process may stop meanwhile |
| `ctx.waitFor(signal, { timeout? })` | Waits for `k.signal(id, signal, payload)` and returns the payload; past `timeout` (ms) it throws `TimedOutError` |
| `ctx.now()`, `ctx.random()` | The time and a random number, the same each time the workflow runs again |
| `ctx.workflow(name, input)` | Runs a child workflow |

A workflow must be deterministic between its calls: read the clock and randomness through `ctx.now()` and `ctx.random()`, and do I/O in actions.

A call that fails or is cancelled throws `CallError` (`runId`, `action`, `status`: `'failed'` or `'cancelled'`, `error`: the failure as recorded); a cancelled one has a `CancelledError` as its `cause`. A workflow whose function throws fails: `run` and `result` throw `WorkflowError` (`id`, `error`; `cause` is what the function threw, when it failed in this process).

### Running

| | |
|---|---|
| `k.run(name, input, { id, meta?, signal? })` | Starts workflow `id`, or resumes it, and returns its result. The id is an idempotency key. An aborted `signal` throws `StoppedError`; the workflow goes on |
| `k.submit(name, input, { id, meta? })` | Starts workflow `id` and returns its id once the start is durable, without waiting |
| `k.result(id, { signal? })` | The result of workflow `id`, from any process, once it has finished |
| `k.signal(id, name, payload)` | Delivers a signal to the first wait for it; a wait the workflow has not reached yet receives it when it does |
| `k.cancel(id)` | Cancels the workflow and the calls it waits for |
| `k.list({ workflow?, status?, since?, until?, after?, limit? })` | Workflows (not child workflows or calls), oldest first, with `meta`, `createdAt` and `updatedAt` |

A process that drives a workflow holds a lease on it. If the process stops, another process that has the workflow takes it up once the lease expires; while the lease lives, a `run` elsewhere waits instead of running the workflow twice.

Limits, per process: `concurrency` on `Kairo` (steps at once), and `limit` (steps at once) and `rate` (starts a minute) on an action, counted by its `destination` when it has one.

### Logging and observing

What goes wrong in the runtime's background work (lease renewals, sweeps, timers, workflows driven in the background, callbacks) goes to `logger` on `Kairo`: an object with `warn(msg, attrs?)` and `error(msg, attrs?)`, the console by default.

`observe` on `Kairo` is given an `Observation` as each thing happens, to count, time and trace runs (metrics, OpenTelemetry) without kairo depending on either. It is called inside the runtime, so it must not block; one that throws is logged and the runtime goes on.

```ts
const k = new Kairo({ backend, observe: (o) => metrics.record(o) });
```

| `kind` (`ObservationKind`) | When | With |
|---|---|---|
| `run.started` | This process made a new run | `runId`, `plan`, `parent`, `workflow` |
| `run.settled` | A run completed, failed, was cancelled, or stopped for review (`blocked`) | `status`, `error` |
| `step.started` | A step's handler is about to run | `action`, `stepId`, `attempt` |
| `step.finished` | It returned; also when an outcome comes on the callback (no `duration`) | `status` (`StepStatus`: `ok`, `retryable`, `failed`, `unknown`, `waiting`, `pending`), `error`, `duration` (ms) |

With kairod (`HttpBackend`), only `step.started` and `step.finished` of the steps this process's worker runs are observed: runs are kairod's.

## Storage

```ts
new SQLiteStore('kairo.db')                               // node:sqlite; one file
new PostgresStore(new pg.Pool({ connectionString }))      // your node-postgres pool
```

PostgreSQL also wakes a wait in one process when a run settles in another (`LISTEN`/`NOTIFY`). Finished runs are removed, together with everything they started, once they have been kept long enough: `keepFinished` on `EmbeddedBackend.open`, or `KAIRO_KEEP_FINISHED` (`24h` by default, `7d`, `forever`). A workflow that is still waiting keeps its history for as long as it waits.

## Serverless

With `mode: 'suspend'`, an invocation advances the workflow as far as it can and returns: `run` throws `Suspended` at a timer or a signal. A scheduler brings it back.

```ts
const k = new Kairo({
  backend: await EmbeddedBackend.open({ store: new PostgresStore(pool) }),
  mode: 'suspend',
  tickSecret: process.env.CRON_SECRET,   // "Authorization: Bearer ..." on /tick
  wake: async (at) => { /* schedule a one-off call of /tick at `at` (unix ms) */ },
});
defineWorkflows(k);
await k.start();
// One route serves /action, /callback and /tick:
export const handler = k.fetchHandler();       // (Request) => Promise<Response>
```

- **`/tick`:** Cron (e.g. Vercel Cron) or one-off schedulers call it. It takes up due timers and steps whose process died, and answers `{ "next": <unix ms> }`.
- **`wake(at)`:** Called before a call returns, with the earliest time something is due. Schedule exactly one wake-up then, instead of polling.
- **`tickHandler(make)`:** The entry point for schedulers that call a function directly (e.g. AWS Lambda from EventBridge Scheduler).

Recipes for Vercel, Lambda, Cloud Tasks and QStash are in the [serverless guide](https://github.com/r-hashi01/kairo/blob/main/docs/serverless.md).

## Actions over HTTP(S)

An action can live elsewhere: on a GPU box, in another service, behind a URL.

```ts
const k = new Kairo({ backend, secret: process.env.KAIRO_ACTION_SECRET, callbackUrl: 'https://app.example.com/kairo/callback' });
k.defineAction('render', { effect: 'unprotected', url: 'https://gpu.example.com/kairo/action', async: true, handler: async () => null });
```

- Calls are signed (`Kairo-Signature`, HMAC-SHA256 with a timestamp) and carry an `Idempotency-Key`.
- A `200` answer is the result. A `202` means the result comes later, as a signed callback.
- `4xx` fails the step. `5xx`, a timeout or a broken connection make the outcome unknown.
- HTTPS is required, except to `localhost` or with `allowInsecure`. Trust your own CA with `ca`.

The serving side is the same `k.fetchHandler()`, with the action's real `handler`. For `node:http`, wrap it with `nodeHandler()`. A handler's `RetryableError` is sent as a retryable failure; its `UnknownOutcomeError` is answered with `502` (and an async action sends no callback), which the caller takes as unknown.

## With kairod

If you can run a resident process, [kairod](https://github.com/r-hashi01/kairo#kairod-when-you-can-keep-a-process-running) gives the same model microsecond hand-offs, RPM/TPM limits across processes, and workers in any language:

```ts
const k = new Kairo({ url: 'http://127.0.0.1:8420', worker: '/var/lib/kairo/worker.sock' });
```

The workflow code is the same.

## License

MIT
