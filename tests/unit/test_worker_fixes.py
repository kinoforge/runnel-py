import asyncio
import time
from collections.abc import Awaitable, Callable, Coroutine
from types import SimpleNamespace
from typing import Any

import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from runnel import Job, LeaseLostError, Worker
from runnel._channel import target
from runnel._clock import ServerClock
from runnel.v1 import types_pb2, worker_pb2


class Workers:
    def __init__(self, jobs: list[types_pb2.Job], extend: Callable[[int], Awaitable[list[str]]]) -> None:
        self.jobs = jobs
        self.extend = extend
        self.beats = 0
        self.claimed = False
        self.finished: list[worker_pb2.Outcome] = []

    async def _claim(self) -> worker_pb2.ClaimResponse:
        if not self.claimed:
            self.claimed = True
            return worker_pb2.ClaimResponse(jobs=self.jobs)
        await asyncio.sleep(3600)
        return worker_pb2.ClaimResponse()

    def Claim(self, req: Any) -> asyncio.Future[worker_pb2.ClaimResponse]:
        return asyncio.ensure_future(self._claim())

    async def Extend(self, req: Any, timeout: float) -> worker_pb2.ExtendResponse:
        self.beats += 1
        return worker_pb2.ExtendResponse(lost=await self.extend(self.beats))

    async def Finish(self, req: Any) -> worker_pb2.FinishResponse:
        self.finished.append(req.outcome)
        return worker_pb2.FinishResponse()


async def no_loss(n: int) -> list[str]:
    return []


def worker(jobs: list[types_pb2.Job], extend: Callable[[int], Awaitable[list[str]]] = no_loss, **kw: Any) -> tuple[Worker, Workers]:
    workers = Workers(jobs, extend)
    client = SimpleNamespace(rpc=SimpleNamespace(workers=workers), namespace="ns", clock=ServerClock())
    kw.setdefault("shutdown_timeout", 0.2)
    return Worker(client, ["q"], mode="poll", **kw), workers  # type: ignore[arg-type]


async def until(what: str, seconds: float, ok: Callable[[], bool]) -> None:
    end = time.monotonic() + seconds
    while not ok():
        if time.monotonic() > end:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.01)


async def test_a_heartbeat_renews_by_the_granted_lease() -> None:
    start = time.time()
    ts = Timestamp()
    ts.FromNanoseconds(int((start + 1.0) * 1e9))

    async def first_only(n: int) -> list[str]:
        if n > 1:
            raise ConnectionError("unavailable")
        return []

    w, _ = worker([types_pb2.Job(id="j1", kind="k", queue="q", lease_until=ts)], first_only, lease=3.0)
    lost_at: list[float] = []

    async def handler(job: Job, ctx: Any) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            if isinstance(ctx.reason, LeaseLostError):
                lost_at.append(time.time())
            raise

    w.handle("k", handler)
    ran = asyncio.create_task(w.run())
    await until("the lease to end", 6, lambda: bool(lost_at))
    w.stop()
    await ran
    assert lost_at[0] - start < 3.6


async def test_a_sync_handler_that_returns_after_its_timeout_is_retried() -> None:
    job = types_pb2.Job(id="j1", kind="k", queue="q", timeout=Duration(nanos=100_000_000))
    w, workers = worker([job])

    def handler(job: Job, ctx: Any) -> None:
        ctx.cancelled.wait(5)

    w.handle("k", handler)
    ran = asyncio.create_task(w.run())
    await until("the job to finish", 3, lambda: bool(workers.finished))
    w.stop()
    await ran
    assert workers.finished[0].kind == worker_pb2.OUTCOME_KIND_RETRY


async def test_a_callable_returning_a_coroutine_is_awaited_and_its_error_kept() -> None:
    w, workers = worker([types_pb2.Job(id="j1", kind="k", queue="q"), types_pb2.Job(id="j2", kind="ok", queue="q")])

    async def boom(job: Job) -> None:
        raise ValueError("boom")

    async def fine(job: Job) -> str:
        return "fine"

    def call(fn: Callable[[Job], Coroutine[Any, Any, Any]]) -> Callable[[Job, Any], Coroutine[Any, Any, Any]]:
        return lambda job, ctx: fn(job)

    w.handle("k", call(boom))
    w.handle("ok", call(fine))
    ran = asyncio.create_task(w.run())
    await until("both jobs to finish", 3, lambda: len(workers.finished) == 2)
    w.stop()
    await ran
    kinds = sorted((o.kind, o.error) for o in workers.finished)
    assert kinds == [(worker_pb2.OUTCOME_KIND_DONE, ""), (worker_pb2.OUTCOME_KIND_RETRY, "boom")]


async def test_an_idle_poll_worker_stops_at_once() -> None:
    w, _ = worker([], shutdown_timeout=5)
    w.handle("k", lambda job, ctx: None)
    ran = asyncio.create_task(w.run())
    await asyncio.sleep(0.05)
    stopped = time.monotonic()
    w.stop()
    await ran
    assert time.monotonic() - stopped < 1


@pytest.mark.parametrize("kw", [{"concurrency": 0}, {"lease": 0}, {"lease": -1.0}])
async def test_a_concurrency_or_lease_that_is_not_positive_is_refused(kw: dict[str, Any]) -> None:
    w, _ = worker([], **kw)
    w.handle("k", lambda job, ctx: None)
    with pytest.raises(ValueError, match="positive"):
        await w.run()


@pytest.mark.parametrize("url", ["https://user:pw@runnel.internal", "https://runnel.internal/base", "https://runnel.internal?ca=x.pem"])
def test_a_url_with_parts_the_client_cannot_use_is_refused(url: str) -> None:
    with pytest.raises(ValueError, match="only scheme"):
        target(url)


def test_a_plain_url_is_taken() -> None:
    assert target("https://runnel.internal:8700/").address == "runnel.internal:8700"
