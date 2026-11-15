import asyncio
import time
from typing import Any

import grpc
import pytest

from runnel import Job, JobContext, NewJob, RunnelError, busy, fail, pending, retry_in
from runnel.v1 import config_pb2

from .conftest import FAST, MakeEnv


async def test_c01_a_worker_completes_a_job_with_a_result(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    e.work(c, {"welcome": lambda j, ctx: {"sent": True, "to": j.payload["to"]}})
    job = await c.enqueue_and_wait("q", "welcome", {"to": "ana"}, wait_timeout=15)
    assert job.state == "completed"
    assert job.result == {"sent": True, "to": "ana"}
    assert (await c.get(job.id)).result == {"sent": True, "to": "ana"}


async def test_c02_a_job_that_raises_once_completes_on_attempt_2(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    calls = 0

    async def flaky(j: Job, ctx: JobContext) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return "ok"

    e.work(c, {"flaky": flaky})
    job = await c.enqueue_and_wait("q", "flaky", backoff=FAST, wait_timeout=15)
    assert job.state == "completed"
    assert job.attempt == 2
    assert [x.error for x in job.errors] == ["boom"]


async def test_c03_a_job_that_always_raises_is_dead_after_its_attempts(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()

    def broken(j: Job, ctx: JobContext) -> None:
        raise RuntimeError("still broken")

    e.work(c, {"broken": broken})
    job = await c.enqueue_and_wait("q", "broken", max_attempts=2, backoff=FAST, wait_timeout=15)
    assert job.state == "dead"
    assert job.attempt == 2
    assert len(job.errors) == 2


async def test_c04_fail_ends_a_job_at_once(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()

    async def strict(j: Job, ctx: JobContext) -> None:
        raise fail(ValueError("bad input"))

    e.work(c, {"strict": strict})
    job = await c.enqueue_and_wait("q", "strict", wait_timeout=15)
    assert job.state == "dead"
    assert job.attempt == 1
    assert job.errors[0].error == "bad input"


async def test_c05_retry_in_waits_the_delay_before_the_next_attempt(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    starts: list[float] = []

    async def later(j: Job, ctx: JobContext) -> None:
        starts.append(time.time())
        if len(starts) == 1:
            raise retry_in(2, "not yet")

    e.work(c, {"later": later})
    job = await c.enqueue_and_wait("q", "later", wait_timeout=15)
    assert job.state == "completed"
    assert len(starts) == 2
    assert starts[1] - starts[0] >= 2


async def test_c06_busy_tries_again_without_using_an_attempt(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    calls = 0

    async def crowded(j: Job, ctx: JobContext) -> Any:
        nonlocal calls
        calls += 1
        return busy(0.3) if calls == 1 else "ran"

    e.work(c, {"crowded": crowded})
    job = await c.enqueue_and_wait("q", "crowded", wait_timeout=15)
    assert job.state == "completed"
    assert calls == 2
    assert job.attempt == 1
    assert job.errors == ()


async def test_c07_pending_parks_the_job_and_the_poller_finishes_it(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    handles: list[str] = []

    async def start(j: Job, ctx: JobContext) -> Any:
        return pending("task-81", 0.3)

    async def poll(j: Job, ctx: JobContext) -> Any:
        handles.append(j.handle)
        return pending("", 0.3) if len(handles) < 3 else {"url": "https://cdn.example/v.mp4"}

    e.work(c, {"render": (start, poll)})
    job = await c.enqueue_and_wait("q", "render", wait_timeout=15)
    assert job.state == "completed"
    assert job.result == {"url": "https://cdn.example/v.mp4"}
    assert handles == ["task-81", "task-81", "task-81"]
    assert job.attempt == 1


async def test_c08_a_cancelled_job_never_reaches_a_worker(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    ins = await c.enqueue("q", "noop")
    assert (await c.cancel(ins.id)).state == "cancelled"
    calls = 0

    async def noop(j: Job, ctx: JobContext) -> None:
        nonlocal calls
        calls += 1

    w = e.work(c, {"noop": noop})
    await asyncio.sleep(1.5)
    await w.stop()
    assert calls == 0
    assert (await c.get(ins.id)).state == "cancelled"


async def test_c13_a_full_queue_is_refused_with_the_servers_retry_delay(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    await c.rpc.config.PutQueue(config_pb2.PutQueueRequest(namespace=e.ns, queue=config_pb2.Queue(name="full", max_queued=1)))
    await c.enqueue("full", "k")
    with pytest.raises(RunnelError) as info:
        await c.enqueue("full", "k")
    assert info.value.code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert info.value.reason == "queue_full"
    assert info.value.retry_after is not None
    assert info.value.retry_after > 0


async def test_c14_a_unique_key_makes_one_job(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    res = [await c.enqueue("q", "k", unique_key="invoice-7") for _ in range(5)]
    assert len({r.id for r in res}) == 1
    assert sum(r.duplicate for r in res) == 4


async def test_c15_a_flow_runs_in_order_and_passes_results_on(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    order: list[str] = []
    seen: list[Any] = []

    async def step(j: Job, ctx: JobContext) -> str:
        name: str = j.payload["name"]
        order.append(name)
        if name == "d":
            seen.extend(sorted(x.result for x in await ctx.dependencies()))
        return name.upper()

    e.work(c, {"step": step}, concurrency=4)

    def job(name: str) -> NewJob:
        return {"queue": "q", "kind": "step", "payload": {"name": name}}

    ids = await c.enqueue_flow(
        {
            "a": {"job": job("a")},
            "b": {"job": job("b"), "after": ["a"]},
            "c": {"job": job("c"), "after": ["a"]},
            "d": {"job": job("d"), "after": ["b", "c"]},
        }
    )
    d = await c.wait(ids["d"].id, 15)
    assert d.state == "completed"
    assert order[0] == "a"
    assert order[3] == "d"
    assert seen == ["B", "C"]


async def test_c18_a_key_without_the_producer_role_cannot_enqueue(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client(["reader"])
    with pytest.raises(RunnelError) as info:
        await c.enqueue("q", "k")
    assert info.value.code == grpc.StatusCode.PERMISSION_DENIED
