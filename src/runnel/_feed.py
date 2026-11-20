from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from runnel.client import AsyncClient
from runnel.errors import to_runnel_error
from runnel.job import duration
from runnel.v1 import types_pb2, worker_pb2


@dataclass(frozen=True)
class FeedConfig:
    client: AsyncClient
    worker: str
    queues: list[str]
    lease: float
    claim_wait: float
    retry_min: float
    retry_max: float
    log: logging.Logger
    free: Callable[[], int]
    deliver: Callable[[types_pb2.Job], None]


class Feed(Protocol):
    async def run(self) -> None: ...
    def grant(self, n: int) -> None: ...
    def stop(self) -> None: ...
    def abort(self) -> None: ...


async def pause(seconds: float, wake: asyncio.Event) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(wake.wait(), seconds)


def cancelled_by_us(aborted: asyncio.Event) -> bool:
    task = asyncio.current_task()
    return aborted.is_set() and (task is None or task.cancelling() == 0)


class Backoff:
    def __init__(self, c: FeedConfig) -> None:
        self._c = c
        self._pause = c.retry_min

    def reset(self) -> None:
        self._pause = self._c.retry_min

    async def wait(self, wake: asyncio.Event) -> None:
        await pause(self._pause, wake)
        self._pause = min(self._pause * 2, self._c.retry_max)


async def _drain(q: asyncio.Queue[worker_pb2.WorkRequest | None]) -> AsyncIterator[worker_pb2.WorkRequest]:
    while (m := await q.get()) is not None:
        yield m


class StreamFeed:
    def __init__(self, c: FeedConfig) -> None:
        self._c = c
        self._q: asyncio.Queue[worker_pb2.WorkRequest | None] | None = None
        self._call: Any = None
        self._stopped = False
        self._stop_event = asyncio.Event()
        self._aborted = asyncio.Event()

    async def run(self) -> None:
        c = self._c
        backoff = Backoff(c)
        while not self._stopped:
            q: asyncio.Queue[worker_pb2.WorkRequest | None] = asyncio.Queue()
            start = worker_pb2.WorkStart(namespace=c.client.namespace, worker=c.worker, queues=c.queues, lease=duration(c.lease))
            q.put_nowait(worker_pb2.WorkRequest(start=start))
            self._q = q
            self.grant(c.free())
            try:
                self._call = c.client.rpc.workers.Work(_drain(q))
                async for r in self._call:
                    backoff.reset()
                    c.client.clock.observe(r.now if r.HasField("now") else None)
                    if r.WhichOneof("msg") == "job":
                        c.deliver(r.job)
            except asyncio.CancelledError:
                if not cancelled_by_us(self._aborted):
                    raise
            except Exception as err:
                if not self._stopped:
                    c.log.error("runnel: work stream failed: %s", to_runnel_error(err))
            finally:
                self._q = None
                self._call = None
                q.put_nowait(None)
            if not self._stopped:
                await backoff.wait(self._stop_event)

    def grant(self, n: int) -> None:
        if n > 0 and self._q is not None:
            self._q.put_nowait(worker_pb2.WorkRequest(credit=n))

    def stop(self) -> None:
        self._stopped = True
        self._stop_event.set()
        if self._q is not None:
            self._q.put_nowait(None)

    def abort(self) -> None:
        self.stop()
        self._aborted.set()
        if self._call is not None:
            self._call.cancel()


class PollFeed:
    def __init__(self, c: FeedConfig) -> None:
        self._c = c
        self._stopped = False
        self._wake = asyncio.Event()
        self._aborted = asyncio.Event()
        self._call: Any = None

    async def run(self) -> None:
        c = self._c
        backoff = Backoff(c)
        while not self._stopped:
            self._wake.clear()
            n = c.free()
            if n <= 0:
                await self._wake.wait()
                continue
            req = worker_pb2.ClaimRequest(
                namespace=c.client.namespace,
                worker=c.worker,
                queues=c.queues,
                limit=n,
                lease=duration(c.lease),
                wait=duration(c.claim_wait),
            )
            try:
                self._call = c.client.rpc.workers.Claim(req)
                res = await self._call
            except asyncio.CancelledError:
                if not cancelled_by_us(self._aborted):
                    raise
                break
            except Exception as err:
                if self._stopped:
                    break
                c.log.error("runnel: claim failed: %s", to_runnel_error(err))
                await backoff.wait(self._aborted)
                continue
            finally:
                self._call = None
            backoff.reset()
            c.client.clock.observe(res.now if res.HasField("now") else None)
            for j in res.jobs:
                c.deliver(j)

    def grant(self, n: int) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stopped = True
        self._aborted.set()
        self._wake.set()
        if self._call is not None:
            self._call.cancel()

    def abort(self) -> None:
        self.stop()
        self._aborted.set()
        if self._call is not None:
            self._call.cancel()
