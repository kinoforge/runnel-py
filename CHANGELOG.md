# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

### Fixed

- After a heartbeat, a job's lease is renewed by the lease the server granted at claim time, at most the worker's `lease`. A server whose `max_lease` is shorter than the worker's `lease` no longer leaves the worker thinking it holds a job the server has taken back.
- A handler that returns normally after its timeout, deadline or the worker's shutdown ended its context is reported as that timeout, deadline or release, not as done.
- Stopping an idle polling worker no longer waits for its long-poll claim (up to 20s) or a reconnect backoff to end.
- A worker refuses a `concurrency` or `lease` that is not positive.
- A plain function that returns a coroutine is awaited like an async handler, with the same cancellation and error handling.
- A URL with a user, password, path or query is refused with `ValueError` instead of having those parts silently dropped. Pass credentials with `token=`.
- A heartbeat that times out is followed by the next one at once, as in the Go worker. Before, the next beat was skipped, so one slow `Extend` could lose every running job's lease.
- When a job comes back to the same worker while its earlier run is still ending, the earlier run no longer removes the new one, which kept its heartbeats.

## [0.1.0b1] - 2026-11-15

### Added

- `AsyncClient` (asyncio) and `Client` (blocking): enqueue one job, a batch or a flow, enqueue and wait, get, wait, list, cancel, retry and stats, over gRPC.
- `Worker` on asyncio: handlers per kind, async or plain functions, with pollers for outside work, concurrency, jobs pushed over the `Work` stream or claimed by long polling, heartbeats, a lease watchdog, timeouts and deadlines on the server's clock, and graceful shutdown that gives unfinished jobs back. `serve()` runs it until SIGINT or SIGTERM.
- Outcome helpers `fail`, `retry_in`, `busy` and `pending`.
- `RunnelError` with the gRPC code, the Runnel reason and the retry delay.
- `verify_callback` and `parse_callback` for signed callbacks, checked against the shared test vectors.
- The generated, typed stubs and messages for the whole Runnel API, in `client.rpc` and `runnel.v1`.
- The shared SDK contract scenarios C01 to C19, run in CI against the Runnel image.

[Unreleased]: https://github.com/kinoforge/runnel-py/compare/v0.1.0b1...HEAD
[0.1.0b1]: https://github.com/kinoforge/runnel-py/releases/tag/v0.1.0b1
