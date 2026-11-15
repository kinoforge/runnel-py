# Runnel for Python

The Python client and worker for [Runnel](https://github.com/kinoforge/runnel), the job queue. It talks to `runnel serve` over gRPC, so workers get jobs pushed to them as soon as they have room.

Needs Python 3.11 or later and Runnel 0.0.4 or later. Fully typed.

```sh
pip install kinoforge-runnel
```

The package is imported as `runnel`.

## Enqueue a job

From asyncio code:

```python
from runnel import AsyncClient

async with AsyncClient("http://runnel.internal:8700", "shop", token=os.environ["RUNNEL_KEY"]) as runnel:
    ins = await runnel.enqueue("emails", "welcome", {"to": "ana@example.com"})
    job = await runnel.enqueue_and_wait("emails", "welcome", {"to": "ana@example.com"}, wait_timeout=30)
    print(job.state, job.result)
```

From plain code, such as a Django or Flask view, `Client` has the same calls without `await`:

```python
from runnel import Client

runnel = Client("http://runnel.internal:8700", "shop", token=os.environ["RUNNEL_KEY"])
runnel.enqueue("emails", "welcome", {"to": "ana@example.com"}, max_attempts=5, delay=60)
```

The key needs the `producer` role to enqueue and `reader` to read jobs. Payloads and results are JSON; pass `bytes` to send a payload as it is. Durations are seconds and times are `datetime`.

## Run a worker

```python
from runnel import AsyncClient, Job, JobContext, Worker

worker = Worker(AsyncClient("http://runnel.internal:8700", "shop", token=os.environ["RUNNEL_KEY"]), ["emails"], concurrency=10)

@worker.handle("welcome")
async def welcome(job: Job, ctx: JobContext) -> dict[str, bool]:
    await mailer.send(job.payload["to"])
    return {"sent": True}

worker.serve()
```

The key needs the `worker` role. What the handler returns becomes the job's result. If it raises, the job is retried after the backoff, and is `dead` once its attempts run out.

`serve()` runs until SIGINT or SIGTERM. Inside your own event loop, `await worker.run()` and call `worker.stop()` to end it.

The worker renews the leases of its running jobs every third of `lease`. On shutdown it stops taking jobs, waits up to `shutdown_timeout` for running ones, then cancels them; a cancelled job is given back untouched, without using an attempt.

A handler can be a plain function too. It runs in a thread, so blocking code does not hold up the other jobs.

## Outcomes

Return or raise one of these to choose what happens next:

| Call | What happens |
|---|---|
| `fail(err)` | the job is `dead` now, with no more retries |
| `retry_in(seconds, err)` | the job runs again after exactly `seconds` |
| `busy(seconds)` | the job runs again after `seconds`, without using an attempt |
| `pending(handle, seconds)` | the job waits for outside work; its poller runs after `seconds` |

```python
from runnel import busy, fail

@worker.handle("charge")
async def charge(job: Job, ctx: JobContext) -> object:
    res = await payments.charge(job.payload)
    if res.status == 429:
        raise busy(res.retry_after)
    if res.status == 400:
        raise fail(res.error)
    return res.receipt
```

## Outside work that takes a while

Give the kind a handler that starts the work and a `poll` that checks on it. `job.handle` holds what the handler passed to `pending`:

```python
async def start(job: Job, ctx: JobContext) -> object:
    task = await renderer.start(job.payload)
    return pending(task.id, 10)

async def check(job: Job, ctx: JobContext) -> object:
    task = await renderer.status(job.handle)
    if task.running:
        return pending("", 10)
    if task.lost:
        raise fail("renderer lost the task")
    return {"url": task.url}

worker.handle("video.render", start, poll=check)
```

## Cancellation

An async handler is cancelled with `asyncio.CancelledError`. A plain handler cannot be stopped from outside, so it should check `ctx.cancelled`, a `threading.Event`. Either way, `ctx.reason` says why:

| Reason | When |
|---|---|
| `JobTimeoutError` | the attempt ran longer than the job's `timeout`; the job is retried |
| `DeadlineError` | the job's `deadline` passed; the job is `expired` |
| `LeaseLostError` | the job was cancelled, or its lease ran out; the result is dropped |
| `ShutdownError` | the worker is stopping; the job is given back |

Deadlines and leases are measured on the server's clock. The worker learns it from every answer, so a worker whose own clock is off still stops on time.

## Errors

Calls raise `RunnelError` with the gRPC `code`, the Runnel `reason` and, when the server is full, `retry_after` in seconds:

```python
from runnel import RunnelError

try:
    await runnel.enqueue("emails", "welcome")
except RunnelError as err:
    if err.reason == "queue_full":
        await asyncio.sleep(err.retry_after or 1)
```

`reason` is one of `invalid`, `not_found`, `stale`, `queue_full`, `group_full`, `no_space`, `dependency`, `conflict`, `final` and `not_final`.

## Callbacks

When a namespace has a callback secret, Runnel signs every callback. Check it before trusting the body:

```python
from runnel import parse_callback

@app.post("/hooks/runnel")
def hook():
    body = parse_callback(request.headers.get("Runnel-Signature", ""), request.get_data(), [os.environ["RUNNEL_CALLBACK_SECRET"]])
    print(body["event"], body["job"]["id"], body["job"]["state"])
    return "", 204
```

Pass the raw body, not parsed JSON. Give two secrets while you rotate them. A signature older than 5 minutes is refused; set `max_age` to change that. A delivery can arrive twice: dedupe by `body["delivery"]`. Invalid signatures raise `SignatureError`.

## Options

`AsyncClient` and `Client`:

| Option | Default | Meaning |
|---|---|---|
| `url` | | the server, `http://` or `https://` |
| `namespace` | | the namespace every call works in |
| `token` | | a namespace key `rnk_…`, or the admin token |
| `credentials` | system roots | `grpc.ChannelCredentials` for `https://`, such as your own CA or a client certificate |
| `options` | | gRPC channel options |
| `interceptors` | | gRPC client interceptors |
| `max_wait` | `60` | the longest single wait `wait` asks the server for, in seconds |

`Worker`:

| Option | Default | Meaning |
|---|---|---|
| `queues` | | the queues to take jobs from |
| `concurrency` | `10` | jobs run at once |
| `lease` | `30` | seconds a job is held between heartbeats |
| `shutdown_timeout` | `30` | seconds `run` waits for running jobs when stopping |
| `mode` | `stream` | `stream` has jobs pushed over one gRPC stream; `poll` claims them with long-polling calls |
| `claim_wait` | `20` | in `poll` mode, seconds one claim waits for jobs |
| `id` | host, pid and a random suffix | the worker's name in leases |
| `finish_tries` | `6` | tries to report an outcome when the server is unreachable |
| `retry_min`, `retry_max` | `0.2`, `5` | the backoff in seconds between those tries and between reconnects |
| `logger` | `logging.getLogger("runnel")` | where errors and warnings go |

## Lower level

`client.rpc` holds the typed stubs for every call of the [gRPC API](https://github.com/kinoforge/runnel/blob/main/docs/grpc-api.md), including settings and admin. The messages are in `runnel.v1`, such as `runnel.v1.config_pb2`.

## Tests

`make test` runs the unit tests. `make test-contract` runs the [shared SDK scenarios](https://github.com/kinoforge/runnel/tree/main/sdk-contract) against a real server at `RUNNEL_URL`; CI starts one in Docker. `make lint` runs ruff and mypy, and `make generate` rebuilds `runnel.v1` from the Runnel protos.

## License

Apache 2.0.
