from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

import grpc
import pytest

from runnel import AsyncClient, Backoff, Worker
from runnel.v1 import admin_pb2
from runnel.worker import Handler

URL = os.environ.get("RUNNEL_URL", "http://127.0.0.1:8700")
CALLBACK_HOST = os.environ.get("RUNNEL_CALLBACK_HOST", "127.0.0.1")
ADMIN_TOKEN = os.environ.get("RUNNEL_ADMIN_TOKEN", "contract-admin-token-0123456789")
ALL_ROLES = ("producer", "reader", "worker", "admin")
FAST: Backoff = {"base": 0.1, "factor": 1.0, "jitter": 0.0}
MODES = ("stream", "poll")

T = TypeVar("T")

log = logging.getLogger("runnel")
if not os.environ.get("RUNNEL_DEBUG"):
    log.addHandler(logging.NullHandler())
    log.propagate = False


@dataclass
class Running:
    worker: Worker
    task: asyncio.Task[None]

    async def stop(self) -> None:
        self.worker.stop()
        await self.task


@dataclass
class Env:
    ns: str
    admin: AsyncClient
    clients: list[AsyncClient] = field(default_factory=list)
    workers: list[Running] = field(default_factory=list)

    async def token(self, roles: Sequence[str]) -> str:
        res = await self.admin.rpc.admin.CreateKey(admin_pb2.CreateKeyRequest(namespace=self.ns, name="contract", roles=list(roles)))
        return res.token

    async def client(self, roles: Sequence[str] = ALL_ROLES, **extra: Any) -> AsyncClient:
        c = AsyncClient(URL, self.ns, token=await self.token(roles), **extra)
        self.clients.append(c)
        return c

    def work(self, c: AsyncClient, handlers: dict[str, Handler | tuple[Handler, Handler]], **options: Any) -> Running:
        defaults: dict[str, Any] = {"lease": 3.0, "shutdown_timeout": 5.0, "claim_wait": 1.0, "retry_min": 0.05}
        w = Worker(c, ["q"], **{**defaults, **options})
        for kind, h in handlers.items():
            if isinstance(h, tuple):
                w.handle(kind, h[0], poll=h[1])
            else:
                w.handle(kind, h)
        r = Running(w, asyncio.create_task(w.run()))
        self.workers.append(r)
        return r

    async def close(self) -> None:
        for r in self.workers:
            await r.stop()
        for c in [*self.clients, self.admin]:
            await c.close()


MakeEnv = Callable[..., Awaitable[Env]]


@pytest.fixture
async def make_env() -> AsyncIterator[MakeEnv]:
    made: list[Env] = []

    async def make(callback_secret: str = "") -> Env:
        ns = "c" + secrets.token_hex(5)
        admin = AsyncClient(URL, ns, token=ADMIN_TOKEN)
        req = (
            admin_pb2.PutNamespaceRequest(name=ns, callback_secret=callback_secret)
            if callback_secret
            else admin_pb2.PutNamespaceRequest(name=ns)
        )
        await admin.rpc.admin.PutNamespace(req)
        e = Env(ns, admin)
        made.append(e)
        return e

    yield make
    for e in made:
        await e.close()


async def until(fn: Callable[[], Awaitable[T]], ok: Callable[[T], bool], timeout: float = 20.0) -> T:
    end = time.monotonic() + timeout
    while True:
        v = await fn()
        if ok(v) or time.monotonic() > end:
            return v
        await asyncio.sleep(0.1)


async def block_until_cancelled() -> None:
    await asyncio.Event().wait()


class Dropping(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, method: str) -> None:
        self.method = method

    async def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        m = client_call_details.method
        name = m.decode() if isinstance(m, bytes) else m
        if name.endswith("/" + self.method):
            raise ConnectionError(f"{self.method} dropped")
        return await continuation(client_call_details, request)
