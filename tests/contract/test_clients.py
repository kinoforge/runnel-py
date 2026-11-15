import asyncio

import grpc
import pytest

from runnel import Client, RunnelError

from .conftest import ALL_ROLES, URL, MakeEnv


async def test_the_sync_client_enqueues_waits_lists_and_maps_errors(make_env: MakeEnv) -> None:
    e = await make_env()
    e.work(await e.client(), {"welcome": lambda j, ctx: {"to": j.payload["to"]}})
    token = await e.token(ALL_ROLES)
    reader = await e.token(["reader"])

    def run() -> None:
        with Client(URL, e.ns, token=token) as c:
            job = c.enqueue_and_wait("q", "welcome", {"to": "ana"}, wait_timeout=15)
            assert job.state == "completed"
            assert job.result == {"to": "ana"}
            later = c.enqueue("q", "welcome", {"to": "bo"}, delay=60)
            assert c.cancel(later.id).state == "cancelled"
            assert c.retry(later.id).state == "waiting"
            page = c.list_jobs(queue="q", state="completed")
            assert [j.id for j in page.jobs] == [job.id]
            assert c.stats()["q"]["completed"] == 1
        with Client(URL, e.ns, token=reader) as r, pytest.raises(RunnelError) as info:
            r.enqueue("q", "k")
        assert info.value.code == grpc.StatusCode.PERMISSION_DENIED

    await asyncio.to_thread(run)


async def test_the_async_client_lists_cancels_retries_and_counts(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    ins = await c.enqueue_many([{"queue": "q", "kind": "k", "delay": 60}, {"queue": "q", "kind": "k", "delay": 60}])
    assert (await c.cancel(ins[0].id)).state == "cancelled"
    page = await c.list_jobs(queue="q", state="cancelled")
    assert [j.id for j in page.jobs] == [ins[0].id]
    assert (await c.retry(ins[0].id)).state == "waiting"
    counts = (await c.stats())["q"]
    assert counts.get("cancelled", 0) == 0
    with pytest.raises(RunnelError) as info:
        await c.get("01a1e01b-7fb6-7ace-ab56-982c9c129e34")
    assert info.value.code == grpc.StatusCode.NOT_FOUND
    assert info.value.reason == "not_found"
