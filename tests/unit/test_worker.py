import asyncio
import time
from types import SimpleNamespace
from typing import Any

from runnel import Job, LeaseLostError, Worker
from runnel._clock import ServerClock
from runnel.v1 import types_pb2, worker_pb2
from runnel.worker import _Run


class Workers:
    def __init__(self) -> None:
        self.calls: list[float] = []
        self.again = asyncio.Event()

    async def Extend(self, req: Any, timeout: float) -> worker_pb2.ExtendResponse:
        self.calls.append(asyncio.get_running_loop().time())
        if len(self.calls) == 2:
            self.again.set()
        if len(self.calls) == 1:
            await asyncio.sleep(timeout)
            raise TimeoutError("deadline exceeded")
        return worker_pb2.ExtendResponse()


def worker(lease: float) -> tuple[Worker, Workers]:
    workers = Workers()
    client = SimpleNamespace(rpc=SimpleNamespace(workers=workers), namespace="ns", clock=ServerClock())
    return Worker(client, ["q"], lease=lease), workers  # type: ignore[arg-type]


def run(id: str) -> _Run:
    pb = types_pb2.Job(id=id, kind="k")
    return _Run(pb, Job.from_pb(pb), 100.0, time.time() + 100)


async def test_a_timed_out_heartbeat_is_followed_at_once() -> None:
    w, workers = worker(lease=1.2)
    w._running["j1"] = run("j1")
    beats = asyncio.create_task(w._beats())
    await asyncio.wait_for(workers.again.wait(), 3)
    w._done.set()
    await beats
    assert len(workers.calls) >= 2
    assert workers.calls[1] - workers.calls[0] < 0.6


async def test_an_ending_run_keeps_the_run_that_replaced_it() -> None:
    w, _ = worker(lease=30)
    started = asyncio.Event()

    async def handler(job: Job, ctx: Any) -> None:
        started.set()
        await asyncio.sleep(10)

    w.handle("k", handler)
    first, second = run("j1"), run("j1")
    w._running["j1"] = first
    task = asyncio.create_task(w._execute(first))
    await started.wait()
    first.lost = True
    w._running["j1"] = second
    first.cancel(LeaseLostError())
    await task
    assert w._running.get("j1") is second
