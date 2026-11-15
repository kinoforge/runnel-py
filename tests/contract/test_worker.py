import asyncio
import os
import signal
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from runnel import AsyncClient, CallbackBody, DeadlineError, Job, JobContext, LeaseLostError, ShutdownError, parse_callback

from .conftest import CALLBACK_HOST, FAST, MODES, URL, Dropping, MakeEnv, block_until_cancelled, until


async def deadline_check(make_env: MakeEnv, **client: Any) -> None:
    e = await make_env()
    c = await e.client(**client)
    seen: dict[str, Any] = {}

    async def slow(j: Job, ctx: JobContext) -> None:
        try:
            await block_until_cancelled()
        finally:
            seen["reason"] = ctx.reason
            seen["at"] = time.time()

    e.work(c, {"slow": slow})
    deadline = datetime.now(UTC) + timedelta(seconds=2)
    ins = await c.enqueue("q", "slow", deadline=deadline)
    job = await c.wait(ins.id, 15)
    assert job.state == "expired"
    assert isinstance(seen["reason"], DeadlineError)
    assert abs(seen["at"] - deadline.timestamp()) < 0.5


async def test_c09_the_handler_is_cancelled_at_the_deadline_and_the_job_expires(make_env: MakeEnv) -> None:
    await deadline_check(make_env)


async def test_c19_deadlines_use_the_servers_clock_when_the_workers_clock_is_off(make_env: MakeEnv) -> None:
    await deadline_check(make_env, now=lambda: time.time() + 30)


async def test_c17_a_signed_callback_arrives_once_and_verifies(make_env: MakeEnv) -> None:
    secret = "whsec_contract"
    e = await make_env(callback_secret=secret)
    c = await e.client()
    got: list[CallbackBody] = []

    class Hook(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["Content-Length"]))
            got.append(parse_callback(self.headers["Runnel-Signature"], body, [secret]))
            self.send_response(204)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", 0), Hook)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        e.work(c, {"notify": lambda j, ctx: {"ok": True}})
        ins = await c.enqueue("q", "notify", callback_url=f"http://{CALLBACK_HOST}:{srv.server_address[1]}/hook")

        async def count() -> int:
            return len(got)

        await until(count, lambda n: n > 0)
        await asyncio.sleep(1.5)
        assert len(got) == 1
        assert got[0]["job"]["id"] == ins.id
        assert got[0]["job"]["state"] == "completed"
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.mark.parametrize("mode", MODES)
async def test_c10_heartbeats_keep_a_long_job(make_env: MakeEnv, mode: str) -> None:
    e = await make_env()
    c = await e.client()
    runs = 0

    async def long(j: Job, ctx: JobContext) -> str:
        nonlocal runs
        runs += 1
        await asyncio.sleep(4)
        return "done"

    e.work(c, {"long": long}, mode=mode, lease=1.5)
    e.work(await e.client(), {"long": long}, mode=mode, lease=1.5)
    job = await c.enqueue_and_wait("q", "long", wait_timeout=15)
    assert job.state == "completed"
    assert job.attempt == 1
    assert job.errors == ()
    assert runs == 1


@pytest.mark.parametrize("mode", MODES)
async def test_c11_a_lease_that_runs_out_is_reported_lost_and_the_job_runs_again(make_env: MakeEnv, mode: str) -> None:
    e = await make_env()
    c = AsyncClient(URL, e.ns, token=await e.token(["producer", "reader", "worker"]), interceptors=[Dropping("Extend")])
    e.clients.append(c)
    seen: dict[str, Any] = {}
    calls = 0

    async def held(j: Job, ctx: JobContext) -> str:
        nonlocal calls
        calls += 1
        if calls > 1:
            return "second"
        try:
            await block_until_cancelled()
        finally:
            seen["reason"] = ctx.reason
        return "first"

    e.work(c, {"held": held}, mode=mode, lease=1.5)
    job = await c.enqueue_and_wait("q", "held", backoff=FAST, wait_timeout=15)
    assert isinstance(seen["reason"], LeaseLostError)
    assert job.state == "completed"
    assert job.attempt == 2
    assert len(job.errors) == 1


@pytest.mark.parametrize("mode", MODES)
async def test_c12_shutdown_gives_back_held_jobs_untouched(make_env: MakeEnv, mode: str) -> None:
    e = await make_env()
    c = await e.client()
    seen: dict[str, Any] = {}

    async def block(j: Job, ctx: JobContext) -> None:
        try:
            await block_until_cancelled()
        finally:
            seen["reason"] = ctx.reason

    w = e.work(c, {"block": block}, mode=mode, concurrency=1, shutdown_timeout=0.3)
    ids = [r.id for r in await c.enqueue_many([{"queue": "q", "kind": "block"} for _ in range(3)])]

    async def running() -> int:
        return w.worker.running

    await until(running, lambda n: n == 1)
    await w.stop()
    assert isinstance(seen["reason"], ShutdownError)
    for id in ids:
        j = await c.get(id)
        assert j.state == "waiting"
        assert j.attempt == 0


@pytest.mark.parametrize("mode", MODES)
async def test_c16_concurrency_caps_jobs_running_at_once(make_env: MakeEnv, mode: str) -> None:
    e = await make_env()
    c = await e.client()
    now = most = 0

    async def task(j: Job, ctx: JobContext) -> None:
        nonlocal now, most
        now += 1
        most = max(most, now)
        await asyncio.sleep(0.3)
        now -= 1

    e.work(c, {"task": task}, mode=mode, concurrency=2)
    ids = [r.id for r in await c.enqueue_many([{"queue": "q", "kind": "task"} for _ in range(6)])]
    for id in ids:
        assert (await c.wait(id, 15)).state == "completed"
    assert most == 2


SERVE = """
import os, sys
from runnel import AsyncClient, Worker
w = Worker(AsyncClient(os.environ["URL"], os.environ["NS"], token=os.environ["TOKEN"]), ["q"], shutdown_timeout=5)
w.handle("ping", lambda j, ctx: "pong")
print("ready", flush=True)
w.serve()
print("stopped", flush=True)
"""


async def test_serve_works_until_sigterm_then_stops(make_env: MakeEnv) -> None:
    e = await make_env()
    c = await e.client()
    env = {**os.environ, "URL": URL, "NS": e.ns, "TOKEN": await e.token(["worker"])}
    proc = await asyncio.create_subprocess_exec(sys.executable, "-c", SERVE, env=env, stdout=asyncio.subprocess.PIPE)
    assert proc.stdout is not None
    assert await proc.stdout.readline() == b"ready\n"
    job = await c.enqueue_and_wait("q", "ping", wait_timeout=15)
    assert job.result == "pong"
    proc.send_signal(signal.SIGTERM)
    out, _ = await asyncio.wait_for(proc.communicate(), 10)
    assert proc.returncode == 0
    assert out == b"stopped\n"
