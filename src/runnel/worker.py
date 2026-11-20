from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import re
import secrets
import signal
import socket
import threading
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, overload

from runnel._feed import Feed, FeedConfig, PollFeed, StreamFeed, pause
from runnel.client import AsyncClient
from runnel.errors import RETRYABLE, DeadlineError, JobTimeoutError, LeaseLostError, ShutdownError, to_runnel_error
from runnel.job import Job, duration
from runnel.outcome import Control, Outcome, done, fail, release, retry
from runnel.v1 import types_pb2, worker_pb2

Handler = Callable[[Job, "JobContext"], Any]


@dataclass(frozen=True)
class Handlers:
    run: Handler
    poll: Handler | None = None


@dataclass
class _Run:
    pb: types_pb2.Job
    job: Job
    lease: float
    lease_until: float
    cancelled: threading.Event = field(default_factory=threading.Event)
    reason: BaseException | None = None
    task: asyncio.Future[Any] | None = None
    done: asyncio.Task[Any] | None = None
    cancellable: bool = False
    lost: bool = False

    def cancel(self, reason: BaseException) -> None:
        if self.reason is not None:
            return
        self.reason = reason
        self.cancelled.set()
        if self.cancellable and self.task is not None:
            self.task.cancel()


class JobContext:
    def __init__(self, worker: Worker, run: _Run) -> None:
        self.worker = worker
        self._run = run

    @property
    def cancelled(self) -> threading.Event:
        return self._run.cancelled

    @property
    def reason(self) -> BaseException | None:
        return self._run.reason

    async def dependencies(self) -> list[Job]:
        return await self.worker.dependencies(self._run.job)


def default_id() -> str:
    host = re.sub(r"[^A-Za-z0-9._-]", "-", socket.gethostname())[:64] or "worker"
    return f"{host}-{os.getpid()}-{secrets.token_hex(3)}"


async def _within(aw: Awaitable[Any], timeout: float) -> bool:
    _, waiting = await asyncio.wait({asyncio.ensure_future(aw)}, timeout=timeout)
    return not waiting


class Worker:
    def __init__(
        self,
        client: AsyncClient,
        queues: Sequence[str],
        *,
        id: str | None = None,
        concurrency: int = 10,
        lease: float = 30.0,
        shutdown_timeout: float = 30.0,
        mode: Literal["stream", "poll"] = "stream",
        claim_wait: float = 20.0,
        finish_tries: int = 6,
        retry_min: float = 0.2,
        retry_max: float = 5.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self.id = id or default_id()
        self.client = client
        self.queues = list(queues)
        self.concurrency = concurrency
        self.lease = lease
        self.shutdown_timeout = shutdown_timeout
        self.mode = mode
        self.claim_wait = claim_wait
        self.finish_tries = finish_tries
        self.retry_min = retry_min
        self.retry_max = retry_max
        self.log = logger or logging.getLogger("runnel")
        self._handlers: dict[str, Handlers] = {}
        self._running: dict[str, _Run] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._stop = asyncio.Event()
        self._done = asyncio.Event()
        self._feed: Feed | None = None
        self._stopping = False

    @overload
    def handle(self, kind: str, run: None = None, *, poll: Handler | None = None) -> Callable[[Handler], Handler]: ...
    @overload
    def handle(self, kind: str, run: Handler, *, poll: Handler | None = None) -> Handler: ...
    def handle(self, kind: str, run: Handler | None = None, *, poll: Handler | None = None) -> Handler | Callable[[Handler], Handler]:
        def add(fn: Handler) -> Handler:
            self._handlers[kind] = Handlers(fn, poll)
            return fn

        return add if run is None else add(run)

    @property
    def running(self) -> int:
        return len(self._running)

    def stop(self) -> None:
        self._stop.set()

    def serve(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            for s in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(s, self.stop)
            try:
                await self.run()
            finally:
                await self.client.close()

        asyncio.run(main())

    async def run(self) -> None:
        if self._feed is not None:
            raise RuntimeError("runnel: worker is already running")
        if not self.queues or not self._handlers:
            raise ValueError("runnel: a worker needs queues and handlers")
        if self.concurrency <= 0 or self.lease <= 0:
            raise ValueError("runnel: concurrency and lease must be positive")
        config = FeedConfig(
            client=self.client,
            worker=self.id,
            queues=self.queues,
            lease=self.lease,
            claim_wait=self.claim_wait,
            retry_min=self.retry_min,
            retry_max=self.retry_max,
            log=self.log,
            free=lambda: 0 if self._stopping else self.concurrency - len(self._running),
            deliver=self._deliver,
        )
        feed: Feed = PollFeed(config) if self.mode == "poll" else StreamFeed(config)
        self._feed = feed
        beats = asyncio.create_task(self._beats())
        fed = asyncio.create_task(feed.run())
        try:
            await self._stop.wait()
            self._stopping = True
            feed.stop()
            if not await _within(self._drained(), self.shutdown_timeout):
                for r in list(self._running.values()):
                    r.cancel(ShutdownError())
                await self._drained()
            if not await _within(fed, self.shutdown_timeout):
                feed.abort()
            await fed
            await self._drained()
            await asyncio.gather(*list(self._tasks))
        finally:
            feed.abort()
            fed.cancel()
            self._done.set()
            beats.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beats

    async def _drained(self) -> None:
        tasks = [r.done for r in self._running.values() if r.done is not None]
        await asyncio.gather(*(asyncio.shield(t) for t in tasks), return_exceptions=True)

    def _spawn(self, aw: Awaitable[Any]) -> asyncio.Task[Any]:
        t = asyncio.ensure_future(aw)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
        return t

    def _deliver(self, pb: types_pb2.Job) -> None:
        if self._stopping:
            self._spawn(self._finish(pb, release()))
            return
        now = self.client.clock.now()
        until = pb.lease_until.seconds + pb.lease_until.nanos / 1e9 if pb.HasField("lease_until") else now + self.lease
        granted = until - now
        r = _Run(pb, Job.from_pb(pb), min(self.lease, granted) if granted > 0 else self.lease, until)
        old = self._running.get(pb.id)
        if old is not None:
            self._lose(pb.id, old)
        self._running[pb.id] = r
        r.done = self._spawn(self._execute(r))

    def _timers(self, r: _Run) -> list[asyncio.TimerHandle]:
        loop = asyncio.get_running_loop()
        out: list[asyncio.TimerHandle] = []
        t = r.job.timeout
        if t is not None and t > 0:
            out.append(loop.call_later(t, r.cancel, JobTimeoutError(t)))
        if r.job.deadline is not None:
            left = r.job.deadline.timestamp() - self.client.clock.now()
            out.append(loop.call_later(max(0.0, left), r.cancel, DeadlineError()))
        return out

    async def _execute(self, r: _Run) -> None:
        try:
            timers = self._timers(r)
            try:
                outcome = await self._attempt(r)
            finally:
                for t in timers:
                    t.cancel()
            if r.lost:
                self.log.warning("runnel: lease lost, result dropped: %s", r.pb.id)
                return
            await self._finish(r.pb, outcome)
        finally:
            if self._running.get(r.pb.id) is r:
                del self._running[r.pb.id]
            if self._feed is not None:
                self._feed.grant(1)

    async def _attempt(self, r: _Run) -> Outcome:
        h = self._handlers.get(r.job.kind)
        if h is None:
            return fail(f'runnel: no handler for kind "{r.job.kind}"').outcome
        fn = h.poll if r.job.polling else h.run
        if fn is None:
            return fail(f'kind "{r.job.kind}" has a handle but no poller').outcome
        ctx = JobContext(self, r)
        is_coro = inspect.iscoroutinefunction(fn)
        value, outcome = await self._run_task(r, fn(r.job, ctx) if is_coro else asyncio.to_thread(fn, r.job, ctx), is_coro)
        if outcome is not None:
            return outcome
        if inspect.isawaitable(value):
            value, outcome = await self._run_task(r, value, True)
            if outcome is not None:
                return outcome
        aborted = self._aborted_outcome(r.reason)
        if aborted is not None:
            return aborted
        return value.outcome if isinstance(value, Control) else done(value)

    async def _run_task(self, r: _Run, aw: Awaitable[Any], cancellable: bool) -> tuple[Any, Outcome | None]:
        r.cancellable = cancellable
        r.task = asyncio.ensure_future(aw)
        if r.reason is not None and r.cancellable:
            r.task.cancel()
        try:
            await asyncio.wait({r.task})
        except asyncio.CancelledError:
            r.task.cancel()
            raise
        if r.task.cancelled():
            return None, self._classify(asyncio.CancelledError(), r.reason)
        err = r.task.exception()
        if err is not None:
            return None, self._classify(err, r.reason)
        return r.task.result(), None

    def _aborted_outcome(self, reason: BaseException | None) -> Outcome | None:
        if isinstance(reason, ShutdownError):
            return release()
        if isinstance(reason, JobTimeoutError):
            return retry(f"timed out after {reason.timeout:g}s")
        if isinstance(reason, DeadlineError):
            return fail("deadline passed").outcome
        return None

    def _classify(self, err: BaseException, reason: BaseException | None) -> Outcome:
        aborted = self._aborted_outcome(reason)
        if aborted is not None:
            return aborted
        return err.outcome if isinstance(err, Control) else retry(err)

    async def _finish(self, pb: types_pb2.Job, outcome: Outcome) -> None:
        wait = self.retry_min
        req = worker_pb2.FinishRequest(namespace=self.client.namespace, worker=self.id, id=pb.id, version=pb.version, outcome=outcome)
        for i in range(1, self.finish_tries + 1):
            try:
                await self.client.rpc.workers.Finish(req)
            except Exception as e:
                err = to_runnel_error(e)
                if err.code not in RETRYABLE:
                    self.log.warning("runnel: finish refused, result dropped: %s: %s", pb.id, err)
                    return
                self.log.error("runnel: finishing job failed: %s try %d: %s", pb.id, i, err)
                await asyncio.sleep(wait)
                wait = min(wait * 2, self.retry_max)
            else:
                return

    async def dependencies(self, job: Job) -> list[Job]:
        if not job.depends_on:
            return []
        try:
            res = await self.client.rpc.workers.Dependencies(
                worker_pb2.DependenciesRequest(namespace=self.client.namespace, ids=job.depends_on)
            )
        except Exception as err:
            raise to_runnel_error(err) from err
        return [Job.from_pb(j) for j in res.jobs]

    async def _beats(self) -> None:
        loop = asyncio.get_running_loop()
        every = self.lease / 3
        due = loop.time() + every
        stopped = asyncio.ensure_future(self._done.wait())
        try:
            while not self._done.is_set():
                await pause(max(0.0, due - loop.time()), self._done)
                if self._done.is_set():
                    return
                beat = asyncio.create_task(self._heartbeat())
                await asyncio.wait({beat, stopped}, return_when=asyncio.FIRST_COMPLETED)
                if not beat.done():
                    beat.cancel()
                    return
                due = max(due + every, loop.time())
        finally:
            stopped.cancel()

    async def _heartbeat(self) -> None:
        sent = dict(self._running)
        if not sent:
            return
        try:
            res = await self.client.rpc.workers.Extend(
                worker_pb2.ExtendRequest(namespace=self.client.namespace, worker=self.id, ids=list(sent), lease=duration(self.lease)),
                timeout=self.lease / 3,
            )
            self.client.clock.observe(res.now if res.HasField("now") else None)
            now = self.client.clock.now()
            lost = set(res.lost)
            for id, r in sent.items():
                if id in lost:
                    self._lose(id, r)
                else:
                    r.lease_until = now + r.lease
        except Exception as err:
            self.log.error("runnel: heartbeat failed: %s", to_runnel_error(err))
        now = self.client.clock.now()
        for id, r in sent.items():
            if now >= r.lease_until:
                self._lose(id, r)

    def _lose(self, id: str, r: _Run) -> None:
        if self._running.get(id) is r and not r.lost:
            r.lost = True
            r.cancel(LeaseLostError())
