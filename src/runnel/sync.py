from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from types import TracebackType
from typing import Any, Self, TypeVar, Unpack

import grpc

from runnel import _requests as rq
from runnel._channel import ChannelOptions, sync_channel
from runnel._clock import Now, ServerClock
from runnel.client import DEFAULT_MAX_WAIT
from runnel.errors import to_runnel_error
from runnel.job import FlowStep, Job, JobOptions, JobState, NewJob
from runnel.v1 import admin_pb2_grpc, config_pb2_grpc, job_pb2, job_pb2_grpc, worker_pb2_grpc

T = TypeVar("T")


@dataclass(frozen=True)
class Rpc:
    jobs: job_pb2_grpc.JobServiceStub
    workers: worker_pb2_grpc.WorkerServiceStub
    config: config_pb2_grpc.ConfigServiceStub
    admin: admin_pb2_grpc.AdminServiceStub


def call(fn: Callable[[], T]) -> T:
    try:
        return fn()
    except grpc.RpcError as err:
        raise to_runnel_error(err) from err


class Client:
    def __init__(
        self,
        url: str,
        namespace: str,
        *,
        token: str | None = None,
        credentials: grpc.ChannelCredentials | None = None,
        options: ChannelOptions = (),
        interceptors: Sequence[Any] = (),
        now: Now = time.time,
        max_wait: float = DEFAULT_MAX_WAIT,
    ) -> None:
        self.url = url
        self.namespace = namespace
        self.clock = ServerClock(now)
        self._max_wait = max_wait
        self._channel = sync_channel(url, token, credentials, options, interceptors)
        ch = self._channel
        self.rpc = Rpc(
            jobs=job_pb2_grpc.JobServiceStub(ch),
            workers=worker_pb2_grpc.WorkerServiceStub(ch),
            config=config_pb2_grpc.ConfigServiceStub(ch),
            admin=admin_pb2_grpc.AdminServiceStub(ch),
        )

    def enqueue(self, queue: str, kind: str, payload: Any = None, **options: Unpack[JobOptions]) -> rq.Inserted:
        req = rq.enqueue(self.namespace, [NewJob(queue=queue, kind=kind, payload=payload, **options)])
        return rq.first(call(lambda: self.rpc.jobs.Enqueue(req)))

    def enqueue_many(self, jobs: Iterable[NewJob]) -> list[rq.Inserted]:
        req = rq.enqueue(self.namespace, jobs)
        return [rq.inserted(r) for r in call(lambda: self.rpc.jobs.Enqueue(req)).results]

    def enqueue_flow(self, steps: Mapping[str, FlowStep]) -> dict[str, rq.Inserted]:
        req = rq.flow(self.namespace, steps)
        return rq.flow_result(call(lambda: self.rpc.jobs.EnqueueFlow(req)))

    def enqueue_and_wait(
        self, queue: str, kind: str, payload: Any = None, *, wait_timeout: float | None = None, **options: Unpack[JobOptions]
    ) -> Job:
        return self.wait(self.enqueue(queue, kind, payload, **options).id, wait_timeout)

    def get(self, id: str) -> Job:
        res = call(lambda: self.rpc.jobs.GetJob(job_pb2.GetJobRequest(namespace=self.namespace, id=id)))
        return rq.job(res.job, res.HasField("job"))

    def wait(self, id: str, timeout: float | None = None) -> Job:
        until = None if timeout is None else time.monotonic() + timeout
        while True:
            left = self._max_wait if until is None else min(until - time.monotonic(), self._max_wait)
            req = rq.wait(self.namespace, id, left)
            res = call(partial(self.rpc.jobs.WaitJob, req))
            j = rq.job(res.job, res.HasField("job"))
            if res.finished or j.final or (until is not None and time.monotonic() >= until):
                return j

    def list_jobs(
        self,
        *,
        queue: str | None = None,
        kind: str | None = None,
        group: str | None = None,
        state: JobState | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> rq.JobPage:
        req = rq.list_jobs(self.namespace, queue=queue, kind=kind, group=group, state=state, cursor=cursor, limit=limit)
        return rq.page(call(lambda: self.rpc.jobs.ListJobs(req)))

    def cancel(self, id: str) -> Job:
        res = call(lambda: self.rpc.jobs.CancelJob(job_pb2.CancelJobRequest(namespace=self.namespace, id=id)))
        return rq.job(res.job, res.HasField("job"))

    def retry(self, id: str) -> Job:
        res = call(lambda: self.rpc.jobs.RetryJob(job_pb2.RetryJobRequest(namespace=self.namespace, id=id)))
        return rq.job(res.job, res.HasField("job"))

    def stats(self) -> rq.Stats:
        return rq.stats(call(lambda: self.rpc.jobs.GetStats(job_pb2.GetStatsRequest(namespace=self.namespace))))

    def close(self) -> None:
        self._channel.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, kind: type[BaseException] | None, err: BaseException | None, tb: TracebackType | None) -> None:
        self.close()
