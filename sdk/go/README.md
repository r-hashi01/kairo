# kairo for Go

**Durable workflows that run inside your Go process.** No workflow server, no broker: the kairo core runs here natively (no WebAssembly), and SQLite or PostgreSQL holds the state.

Write a workflow as an ordinary Go function. Every call it makes is recorded, so after a crash or a redeploy, running it again with the same id picks up where it stopped. Finished calls return their recorded results, and nothing that acts on the world runs twice.

The tables, call ids and contracts are the TypeScript and Python SDKs' ([`kairo-sdk`](https://www.npmjs.com/package/kairo-sdk)): processes in any of the three languages can share one database and resume each other's workflows.

[GitHub](https://github.com/r-hashi01/kairo) · [pkg.go.dev](https://pkg.go.dev/github.com/r-hashi01/kairo/sdk/go) · [Serverless guide](https://github.com/r-hashi01/kairo/blob/main/docs/serverless.md) · MIT

```sh
go get github.com/r-hashi01/kairo@latest     # Go 1.27+; the standard library only
go get modernc.org/sqlite                     # or any database/sql driver for SQLite or PostgreSQL
```

## Quick start

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	kairo "github.com/r-hashi01/kairo/sdk/go"
	_ "modernc.org/sqlite"
)

type Req struct {
	Order  string `json:"order"`
	Amount int    `json:"amount"`
}

func main() {
	ctx := context.Background()
	db, _ := sql.Open("sqlite", "kairo.db")
	store, _ := kairo.NewSQLStore(db, kairo.SQLite)
	k, err := kairo.Open(ctx, kairo.Options{Store: store})
	if err != nil {
		log.Fatal(err)
	}
	defer k.Close()

	// Safe to run again after a crash.
	kairo.Action(k, "classify", kairo.Unprotected, func(_ *kairo.TaskContext, r Req) (bool, error) {
		return r.Amount > 100, nil
	})
	// Recorded before it runs; never runs twice.
	kairo.Action(k, "refund-payment", kairo.Real, func(_ *kairo.TaskContext, r Req) (string, error) {
		return "refunded " + r.Order, nil // call your payment API here
	})

	kairo.Workflow(k, "refund", func(ctx *kairo.Context, r Req) (string, error) {
		risky, err := kairo.Call[bool](ctx, "classify", r)
		if err != nil {
			return "", err
		}
		if risky {
			if _, err := kairo.WaitFor[string](ctx, "approve"); err != nil { // days, at no cost
				return "", err
			}
		}
		return kairo.Call[string](ctx, "refund-payment", r)
	})

	if err := k.Start(ctx); err != nil {
		log.Fatal(err)
	}
	out, err := kairo.Run[string](ctx, k, "refund", Req{Order: "o-42", Amount: 50}, kairo.WithID("refund-o-42"))
	fmt.Println(out, err)

	// Meanwhile, from your API, a Slack button, another process:
	//   k.Signal(ctx, "refund-o-42", "approve", "alice")
}
```

Run it, kill it at any point, and run it again: the workflow resumes, and `refund-payment` runs exactly once.

## The model

### Actions and their effects

An action is a function your workflows call. You declare what it does to the world, and kairo decides from that how it may be retried.

| Effect | Meaning | If the process dies while it runs |
|---|---|---|
| `kairo.Unprotected` | Safe to repeat: an LLM call, a read, a computation | It runs again |
| `kairo.Real` | Acts on the world: a payment, an email, a write | It stops for review (`blocked`). It is never re-run blindly |

A real step is dispatched only after its intent is committed to the database. Options: `Timeout(d)`, `Destination(name)` (a rate-limit key), `MaxAttempts(n)` and `Backoff(d)` (below), `IdempotentRetry()` (a real action that may run again with the same idempotency key, `TaskContext.IdempotencyKey`), `Limit(n)` and `Rate(perMinute)`, and `URL(url, async)` for [actions over HTTP(S)](#actions-over-https).

A handler that returns an error fails its step for good. To say otherwise, wrap the error:

| Returned | Means | The step |
|---|---|---|
| `kairo.Retryable(err)` | It did not take effect: try again | Is retried up to `MaxAttempts` (default 3; 1 for a real action), `Backoff` apart (default 200ms, doubled each time) |
| `kairo.Unknown(err)` | It may have taken effect | Is never taken for success: a real step stops for review, an unprotected one is retried |

`TaskContext` is a `context.Context`, done when the step is aborted (its timeout, its workflow cancelled, the process closing). `t.Emit(chunk)` sends live output to `k.Subscribe(id)`; it is not recorded.

### Workflows

```go
kairo.Workflow(k, "name", func(ctx *kairo.Context, in In) (Out, error) { ... })
```

| In a workflow | Does |
|---|---|
| `kairo.Call[O](ctx, action, in)` | Runs an action once, however often the workflow runs again |
| `kairo.Parallel(ctx, fns...)` | Runs calls at once; each function gets its branch's `Context` |
| `ctx.Sleep(d)` | Waits in the database, not in memory: the process may stop meanwhile |
| `kairo.WaitFor[P](ctx, signal, kairo.WaitTimeout(d))` | Waits for `k.Signal(ctx, id, signal, payload)` and returns the payload; past the timeout it returns `kairo.ErrTimedOut` |
| `ctx.Now()`, `ctx.Random()` | The time and a random number, the same each time the workflow runs again |
| `kairo.Child[O](ctx, name, in)` | Runs a child workflow |

A workflow must be deterministic between its calls: read the clock and randomness through `ctx.Now()` and `ctx.Random()`, do I/O in actions, and run calls at once only through `Parallel`. A panic fails the workflow; it does not crash the process.

To change a workflow's function while runs of it are under way, give it a version. A run keeps the version it started with to its end, and a process drives only the runs whose version it has. New runs start with the current version; keep the old one, `Draining`, until no run of it is left (`RunInfo.Version`, `k.List`).

```go
kairo.Workflow(k, "refund", refundV1, kairo.WorkflowVersion("1"), kairo.Draining()) // finishes the runs that started with it
kairo.Workflow(k, "refund", refundV2, kairo.WorkflowVersion("2"))                   // new runs; one current version per name
```

A change to an action's settings (timeout, retries, backoff) applies to its calls already under way. One exception: while a real call's attempt is out, the call does not go on under settings that are no longer real (it could run twice). The tick reports it as `run.stuck` (`kairo.ErrEffectWeakened`); restore the action, or settle the call with `k.Resolve`, `k.ResolveFailed` or `k.Cancel`.

A call that fails or is cancelled returns a `*kairo.CallError` (`RunID`, `Action`, `Status`: `failed` or `cancelled`, `Message`); `errors.Is(err, kairo.ErrCancelled)` holds for a cancelled one. A workflow that fails returns a `*kairo.WorkflowError` (`ID`, `Message`; `Err` is the error it failed with, when it failed in this process).

### Running

| | |
|---|---|
| `kairo.Run[O](ctx, k, name, in, kairo.WithID(id), kairo.WithMeta(m))` | Starts workflow `id`, or resumes it, and returns its result. The id is an idempotency key. In a resident process the workflow is driven in the background: a `ctx` that ends returns `kairo.ErrStopped`, and the workflow goes on |
| `k.Submit(ctx, name, in, ...)` | Starts workflow `id` and returns its id once the start is durable, without waiting |
| `kairo.Await[O](ctx, k, id)` | The result of workflow `id`, from any process, once it has finished |
| `k.Signal(ctx, id, name, payload)` | Delivers a signal to the first wait for it; a wait the workflow has not reached yet receives it when it does |
| `k.Cancel(ctx, id)` | Cancels the workflow, the workflows it made and the calls they wait for |
| `k.Resolve(ctx, callID, output)`, `k.ResolveFailed(ctx, callID, msg)` | Settles a call stopped for review (a real step whose outcome is unknown), or one stuck as above |
| `k.List(ctx, kairo.Filter{Workflow, Status, Since, Until, After, Limit})` | Workflows (not child workflows or calls), oldest first, with `Meta`, `Created`, `Updated` |
| `k.Get(ctx, id)`, `k.Children(ctx, id)` | A run, and the runs it made |

A process that drives a workflow holds a lease on it. If the process stops, another process that has the workflow takes it up once the lease expires (`Options.Lease`, 30s by default); while the lease lives, a `Run` elsewhere waits instead of running the workflow twice. `Close` hands its leases over at once.

Limits, per process: `Options.Concurrency` (steps at once), and `Limit` and `Rate` on an action, counted by its `Destination` when it has one.

### Logging and observing

What goes wrong in the runtime's background work (lease renewals, sweeps, timers, callbacks) goes to `Options.Logger` (`*slog.Logger`, `slog.Default()` by default).

`Options.Observe` is given a `kairo.Observation` as each thing happens, to count, time and trace runs (metrics, OpenTelemetry) without kairo depending on either. It is called inside the runtime, so it must not block; one that panics is logged and the runtime goes on.

| `Kind` | When | With |
|---|---|---|
| `kairo.ObsRunStarted` | This process made a new run | `RunID`, `Plan`, `Parent`, `Workflow` |
| `kairo.ObsRunSettled` | A run completed, failed, was cancelled, or stopped for review | `Status`, `Error` |
| `kairo.ObsStepStarted` | A step's handler is about to run | `Action`, `StepID`, `Attempt` |
| `kairo.ObsStepFinished` | It returned; also when an outcome comes on the callback (no `Duration`) | `Status` (`StepOK`, `StepRetryable`, `StepFailed`, `StepUnknown`, `StepWaiting`, `StepPending`), `Error`, `Duration` |
| `kairo.ObsRunStuck` | A tick could not take a run on (it is put off a lease period; the other runs go on) | `Error` |

### Testing

`kairotest.New(t)` gives your tests a kairo in memory, in suspend mode, on a clock the test moves (from 2026-01-01T00:00:00Z, or `kairotest.At`). Nothing waits in real time: a workflow that sleeps or waits returns `kairo.ErrSuspended`, and `env.Advance(d)` fires what comes due, in order, each at its own time.

```go
env := kairotest.New(t)
kairo.Action(env.K, "charge", kairo.Real, stubCharge)
kairo.Workflow(env.K, "refund", refund) // sleeps a day, then waits an hour for "approve"
env.Start()
_, err := kairo.Run[string](ctx, env.K, "refund", order, kairo.WithID("r-1")) // kairo.ErrSuspended
env.Advance(24 * time.Hour)
env.Signal("r-1", "approve", "alice")
out, err := kairo.Await[string](ctx, env.K, "r-1")
env.Calls("r-1") // call kairo.sleep, wait approve, call charge: Kind, Name, Input, Output, Status
```

Other options go in `kairotest.With(func(o *kairo.Options) { ... })`. The environment closes when the test ends.

## Storage

```go
kairo.NewSQLStore(db, kairo.SQLite)               // any SQLite driver (modernc.org/sqlite, ...)
kairo.NewSQLStore(db, kairo.Postgres)             // any PostgreSQL driver (github.com/jackc/pgx/v5/stdlib, ...)
kairo.NewMemStore()                               // runs that need not outlive the process; tests
```

The `*sql.DB` is yours: kairo does not close it. For PostgreSQL, `pgnotify.Wrap(store, dsn)` (`github.com/r-hashi01/kairo/sdk/go/pgnotify`, a module of its own) also wakes a wait in one process when a run settles in another (`LISTEN`/`NOTIFY`). Finished runs are removed, together with everything they started, once they have been kept long enough: `Options.KeepFinished`, or `KAIRO_KEEP_FINISHED` (`24h` by default, `7d`, `forever`). A workflow that is still waiting keeps its history for as long as it waits.

## Serverless

With `Mode: kairo.Suspend`, an invocation advances the workflow as far as it can and returns: `Run` returns `kairo.ErrSuspended` at a timer or a signal. A scheduler brings it back.

```go
k, _ := kairo.Open(ctx, kairo.Options{
	Store: store,
	Mode:  kairo.Suspend,
	HTTP:  kairo.HTTPOptions{TickSecret: os.Getenv("CRON_SECRET")}, // "Authorization: Bearer ..." on /tick
	Wake:  func(at time.Time) error { return scheduleTick(at) },      // a one-off call of /tick then
})
defineWorkflows(k)
k.Start(ctx)
http.Handle("/kairo/", k.Handler()) // /kairo/action, /kairo/callback, /kairo/tick
```

- **`/tick`:** Cron or one-off schedulers call it. It takes up due timers and steps whose process died, and answers `{ "next": <unix ms> }`. `k.Tick(ctx)` does the same in code.
- **`Wake(at)`:** Called before a call returns, with the earliest time something is due. Schedule exactly one wake-up then, instead of polling.
- **`kairo.TickHandler(open)`:** The entry point for schedulers that call a function directly (e.g. AWS Lambda from EventBridge Scheduler).

## Actions over HTTP(S)

An action can live elsewhere: on a GPU box, in another service, behind a URL.

```go
k, _ := kairo.Open(ctx, kairo.Options{Store: store, HTTP: kairo.HTTPOptions{
	Secret:      os.Getenv("KAIRO_ACTION_SECRET"),
	CallbackURL: "https://app.example.com/kairo/callback",
}})
kairo.Action(k, "render", kairo.Unprotected, renderHere, kairo.URL("https://gpu.example.com/kairo/action", true))
```

- Calls are signed (`Kairo-Signature`, HMAC-SHA256 with a timestamp) and carry an `Idempotency-Key`.
- A `200` answer is the result. A `202` means the result comes later, as a signed callback.
- `4xx` fails the step. `5xx`, a timeout or a broken connection make the outcome unknown.
- HTTPS is required, except to `localhost` or with `AllowInsecure`. Trust your own CA with `CA`.

The serving side is the same `k.Handler()`, with the action's real handler. A handler's `kairo.Retryable` is sent as a retryable failure; its `kairo.Unknown` is answered with `502`, which the caller takes as unknown.

## With kairod

If you can run a resident process, [kairod](https://github.com/r-hashi01/kairo#kairod-when-you-can-keep-a-process-running) gives the same model microsecond hand-offs, RPM/TPM limits across processes, and workers in any language:

```go
k, err := kairo.Connect(ctx, "http://127.0.0.1:8420", "/var/lib/kairo/worker.sock", kairo.Options{})
```

The workflow code is the same: the runs live in kairod (over its HTTP API), and this process runs its actions' steps (over kairod's worker socket) and drives its workflows. There are no drive leases: a workflow is driven where it is run. Suspend mode, `List`, `Children`, `Resolve`, `Tick` and signals sent before their wait need the embedded runtime (`kairo.ErrNeedsEmbedded`).

## License

MIT
