# kairo-sdk for Python

**Durable workflows that run inside your Python process.** No workflow server, no broker: the kairo core runs here as WebAssembly, and SQLite or PostgreSQL holds the state.

Write a workflow as an ordinary `async` function. Every call it makes is recorded, so after a crash or a redeploy, running it again with the same id picks up where it stopped. Finished calls return their recorded results, and nothing that acts on the world runs twice.

[GitHub](https://github.com/r-hashi01/kairo) · [Serverless guide](https://github.com/r-hashi01/kairo/blob/main/docs/serverless.md) · [TypeScript SDK](https://www.npmjs.com/package/kairo-sdk) · MIT

```sh
pip install "kairo-sdk[embedded]"     # Python 3.12+; the extra adds wasmtime
```

## Quick start

```python
import asyncio
from kairo_sdk import EmbeddedBackend, Kairo, SQLiteStore

async def main():
    k = Kairo(backend=await EmbeddedBackend.open(SQLiteStore("kairo.db")))

    @k.action("classify", effect="unprotected")     # safe to run again after a crash
    def classify(req, ctx):
        return {"risky": req["amount"] > 100}

    @k.action("refund-payment")                     # effect="real" (the default): never runs twice
    def refund_payment(req, ctx):
        return {"refunded": req["order"]}

    @k.workflow("refund")
    async def refund(ctx, req):
        verdict = await ctx.call("classify", req)
        if verdict["risky"]:
            await ctx.wait_for("approve")            # wait days for a human, at no cost
        await ctx.sleep(60)                          # a durable timer (seconds)
        return await ctx.call("refund-payment", req)

    await k.start()
    print(await k.run("refund", {"order": "o-42", "amount": 50}, id="refund-o-42"))
    await k.close()

asyncio.run(main())
```

Action handlers may be plain functions (they run on a thread) or `async` functions. Each takes the input and a context.

## The model

### Actions and their effects

| `effect` | Meaning | If the process dies while it runs |
|---|---|---|
| `"unprotected"` | Safe to repeat: an LLM call, a read, a computation | It runs again |
| `"real"` (the default) | Acts on the world: a payment, an email, a write | It stops for review (`blocked`). It is never re-run blindly |

A real step is dispatched only after its intent is committed to the database. Options: `timeout` (`"30s"`, `"5m"`), `destination` (a rate-limit key), `max_attempts` and `backoff` (below), and `url` for [actions over HTTP(S)](#actions-over-https).

A handler that raises fails its step for good. To say otherwise, raise one of these:

| Raised | Means | The step |
|---|---|---|
| `RetryableError` | It did not take effect: try again | Is retried up to `max_attempts` (default 3; 1 for a real action), `backoff` apart (default `"200ms"`, doubled each time) |
| `UnknownOutcomeError` | It may have taken effect | Is never taken for success: a real step stops for review, an unprotected one is retried |

### Workflows

| In a workflow | Does |
|---|---|
| `await ctx.call(action, input)` | Runs an action once, however often the workflow runs again |
| `await asyncio.gather(ctx.call(...), ...)` | Runs calls at once |
| `await ctx.sleep(seconds)` | Waits in the database, not in memory: the process may stop meanwhile |
| `await ctx.wait_for(signal, timeout=None)` | Waits for `k.signal(id, signal, payload)` and returns the payload; past `timeout` (seconds) it raises `TimedOutError` |
| `await ctx.now()`, `await ctx.random()` | The time and a random number, the same each time the workflow runs again |
| `await ctx.workflow(name, input)` | Runs a child workflow |

A workflow must be deterministic between its calls: read the clock and randomness through `ctx.now()` and `ctx.random()`, and do I/O in actions.

A call that fails or is cancelled raises `CallError` (`run_id`, `action`, `status`: `"failed"` or `"cancelled"`, `message`); a cancelled one is a `Cancelled` too. A workflow whose function raises fails: `run` and `result` raise `WorkflowError` (`id`, `message`; its `__cause__` is what the function raised, when it failed in this process). Both are `RuntimeError`s.

### Running

| | |
|---|---|
| `await k.run(name, input, id=..., meta=None)` | Starts workflow `id`, or resumes it, and returns its result. The id is an idempotency key. A caller that stops waiting (its task cancelled, or timed out) gets `StoppedError`; the workflow goes on |
| `await k.submit(name, input, id=..., meta=None)` | Starts workflow `id` and returns its id once the start is durable, without waiting |
| `await k.result(id)` | The result of workflow `id`, from any process, once it has finished |
| `await k.signal(id, name, payload)` | Delivers a signal to the first wait for it; a wait the workflow has not reached yet receives it when it does |
| `await k.cancel(id)` | Cancels the workflow and the calls it waits for |
| `await k.list(workflow=, status=, since=, until=, after=, limit=)` | Workflows (not child workflows or calls), oldest first, with `meta`, `created_at` and `updated_at` |

A process that drives a workflow holds a lease on it. If the process stops, another process that has the workflow takes it up once the lease expires; while the lease lives, a `run` elsewhere waits instead of running the workflow twice.

Limits, per process: `concurrency` on `Kairo` (steps at once), and `limit` (steps at once) and `rate` (starts a minute) on an action, counted by its `destination` when it has one.

## Storage

```python
SQLiteStore("kairo.db")                       # the standard library's sqlite3; one file
PostgresStore(pool)                           # your psycopg_pool.AsyncConnectionPool
```

PostgreSQL also wakes a wait in one process when a run settles in another (`LISTEN`/`NOTIFY`). Finished runs are removed, together with everything they started, once they have been kept long enough: `keep_finished` on `EmbeddedBackend.open`, or `KAIRO_KEEP_FINISHED` (`24h` by default, `7d`, `forever`). A workflow that is still waiting keeps its history for as long as it waits.

## Serverless

With `mode="suspend"`, an invocation advances the workflow as far as it can and returns: `run` raises `Suspended` at a timer or a signal. A scheduler brings it back.

```python
k = Kairo(
    backend=await EmbeddedBackend.open(PostgresStore(pool)),
    mode="suspend",
    tick_secret=os.environ["CRON_SECRET"],    # "Authorization: Bearer ..." on /tick
    wake=schedule_one_off_tick,               # async (at: unix ms) -> None
)
define_workflows(k)
await k.start()
app = k.asgi_app()                            # an ASGI app: /action, /callback, /tick
```

- **`/tick`:** Cron or one-off schedulers call it. It takes up due timers and steps whose process died, and answers `{"next": <unix ms>}`.
- **`wake(at)`:** Called before a call returns, with the earliest time something is due. Schedule exactly one wake-up then, instead of polling.
- **`tick_handler(make)`:** The entry point for schedulers that call a function directly (e.g. AWS Lambda from EventBridge Scheduler).

## Actions over HTTP(S)

An action can live elsewhere: on a GPU box, in another service, behind a URL.

```python
k = Kairo(backend=backend, secret=os.environ["KAIRO_ACTION_SECRET"], callback_url="https://app.example.com/kairo/callback")
k.action("render", effect="unprotected", url="https://gpu.example.com/kairo/action", async_=True)(lambda i, ctx: None)
```

- Calls are signed (`Kairo-Signature`, HMAC-SHA256 with a timestamp) and carry an `Idempotency-Key`.
- A `200` answer is the result. A `202` means the result comes later, as a signed callback.
- `4xx` fails the step. `5xx`, a timeout or a broken connection make the outcome unknown.
- HTTPS is required, except to `localhost` or with `allow_insecure=True`. Trust your own CA with `ca`.

The serving side is the same `k.asgi_app()`, with the action's real handler. A handler's `RetryableError` is sent as a retryable failure; its `UnknownOutcomeError` is answered with `502` (and an async action sends no callback), which the caller takes as unknown.

## With kairod

If you can run a resident process, [kairod](https://github.com/r-hashi01/kairo#kairod-when-you-can-keep-a-process-running) gives the same model microsecond hand-offs, RPM/TPM limits across processes, and workers in any language. The workflow code is the same:

```python
k = Kairo("http://127.0.0.1:8420", worker="/var/lib/kairo/worker.sock")
```

### Workers

To serve kairod's graph plans (not workflows written as code), a worker pulls tasks over kairod's worker protocol. It is the standard library only.

```python
from kairo_sdk import Result, Worker

def handler(task, ctx):
    ctx.emit("streamed text")                 # live output
    if ctx.cancelled.is_set():                # the step was cancelled or timed out
        return Result(error="cancelled", retryable=True)
    return Result(output={"echo": task.input})

Worker("py-1", ["my.action"], handler, concurrency=8).run("/var/lib/kairo/worker.sock")
```

A handler always returns a `Result`, even when cancelled: that frees its slot. It tells a definite failure (`retryable`) from an unknown outcome (`unknown`).

### Dify nodes (graphon)

`kairo_sdk.graphon` runs Dify's workflow nodes ([graphon](https://pypi.org/project/graphon/) 0.7.0) as kairo tasks, for plans converted by `compat/dify`. Install with `pip install "kairo-sdk[graphon]"`, then:

```sh
python -m kairo_sdk.serve --socket /var/lib/kairo/worker.sock --actions dify.template-transform,dify.http-request
```

## License

MIT
