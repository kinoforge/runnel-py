from __future__ import annotations

import time
from collections.abc import Awaitable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self, TypeVar, Unpack

import grpc

from runnel import _requests as rq
from runnel._channel import ChannelOptions, async_channel
from runnel._clock import Now, ServerClock
from runnel.errors import to_runnel_error
from runnel.job import FlowStep, Job, JobOptions, JobState, NewJob
from runnel.v1 import admin_pb2_grpc, config_pb2_grpc, job_pb2, job_pb2_grpc, worker_pb2_grpc

T = TypeVar("T")

DEFAULT_MAX_WAIT = 60.0


@dataclass(frozen=True)
class AsyncRpc:
    jobs: job_pb2_grpc.JobServiceAsyncStub
    workers: worker_pb2_grpc.WorkerServiceAsyncStub
    config: config_pb2_grpc.ConfigServiceAsyncStub
    admin: admin_pb2_grpc.AdminServiceAsyncStub


async def call(aw: Awaitable[T]) -> T:
    try:
        return await aw
    except grpc.RpcError as err:
        raise to_runnel_error(err) from err


class AsyncClient:
    def __init__(
        self,
        url: str,
        namespace: str,
        *,
        token: str | None = None,
        credentials: grpc.ChannelCredentials | None = None,
        options: ChannelOptions = (),
        interceptors: Sequence[grpc.aio.ClientInterceptor] = (),
        now: Now = time.time,
        max_wait: float = DEFAULT_MAX_WAIT,
    ) -> None:
        self.url = url
        self.namespace = namespace
        self.clock = ServerClock(now)
        self._token = token
        self._credentials = credentials
        self._options = options
        self._interceptors = interceptors
        self._max_wait = max_wait
        self._channel: grpc.aio.Channel | None = None
        self._rpc: AsyncRpc | None = None

    @property
    def rpc(self) -> AsyncRpc:
        if self._rpc is None:
            ch = async_channel(self.url, self._token, self._credentials, self._options, self._interceptors)
            self._channel = ch
            self._rpc = AsyncRpc(
                jobs=job_pb2_grpc.JobServiceStub(ch),
                workers=worker_pb2_grpc.WorkerServiceStub(ch),
                config=config_pb2_grpc.ConfigServiceStub(ch),
                admin=admin_pb2_grpc.AdminServiceStub(ch),
            )
        return self._rpc

    async def enqueue(self, queue: str, kind: str, payload: Any = None, **options: Unpack[JobOptions]) -> rq.Inserted:
        res = await call(self.rpc.jobs.Enqueue(rq.enqueue(self.namespace, [NewJob(queue=queue, kind=kind, payload=payload, **options)])))
        return rq.first(res)

    async def enqueue_many(self, jobs: Iterable[NewJob]) -> list[rq.Inserted]:
        res = await call(self.rpc.jobs.Enqueue(rq.enqueue(self.namespace, jobs)))
        return [rq.inserted(r) for r in res.results]

    async def enqueue_flow(self, steps: Mapping[str, FlowStep]) -> dict[str, rq.Inserted]:
        return rq.flow_result(await call(self.rpc.jobs.EnqueueFlow(rq.flow(self.namespace, steps))))

    async def enqueue_and_wait(
        self, queue: str, kind: str, payload: Any = None, *, wait_timeout: float | None = None, **options: Unpack[JobOptions]
    ) -> Job:
        ins = await self.enqueue(queue, kind, payload, **options)
        return await self.wait(ins.id, wait_timeout)

    async def get(self, id: str) -> Job:
        res = await call(self.rpc.jobs.GetJob(job_pb2.GetJobRequest(namespace=self.namespace, id=id)))
        return rq.job(res.job, res.HasField("job"))

    async def wait(self, id: str, timeout: float | None = None) -> Job:
        until = None if timeout is None else time.monotonic() + timeout
        while True:
            left = self._max_wait if until is None else min(until - time.monotonic(), self._max_wait)
            res = await call(self.rpc.jobs.WaitJob(rq.wait(self.namespace, id, left)))
            j = rq.job(res.job, res.HasField("job"))
            if res.finished or j.final or (until is not None and time.monotonic() >= until):
                return j

    async def list_jobs(
        self,
        *,
        queue: str | None = None,
        kind: str | None = None,
        group: str | None = None,
        state: JobState | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> rq.JobPage:
        return rq.page(
            await call(
                self.rpc.jobs.ListJobs(
                    rq.list_jobs(self.namespace, queue=queue, kind=kind, group=group, state=state, cursor=cursor, limit=limit)
                )
            )
        )

    async def cancel(self, id: str) -> Job:
        res = await call(self.rpc.jobs.CancelJob(job_pb2.CancelJobRequest(namespace=self.namespace, id=id)))
        return rq.job(res.job, res.HasField("job"))

    async def retry(self, id: str) -> Job:
        res = await call(self.rpc.jobs.RetryJob(job_pb2.RetryJobRequest(namespace=self.namespace, id=id)))
        return rq.job(res.job, res.HasField("job"))

    async def stats(self) -> rq.Stats:
        return rq.stats(await call(self.rpc.jobs.GetStats(job_pb2.GetStatsRequest(namespace=self.namespace))))

    async def close(self) -> None:
        ch, self._channel, self._rpc = self._channel, None, None
        if ch is not None:
            await ch.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, kind: type[BaseException] | None, err: BaseException | None, tb: TracebackType | None) -> None:
        await self.close()
