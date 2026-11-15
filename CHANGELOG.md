# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

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
