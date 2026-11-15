from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cached_property
from typing import Any, Literal, NotRequired, TypedDict, cast

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from runnel.v1 import types_pb2

JobState = Literal["blocked", "waiting", "running", "polling", "completed", "dead", "cancelled", "expired"]

FINAL_STATES: frozenset[JobState] = frozenset({"completed", "dead", "cancelled", "expired"})


def job_state(s: types_pb2.State.ValueType) -> JobState:
    return cast("JobState", types_pb2.State.Name(s).removeprefix("STATE_").lower())


def pb_state(s: JobState) -> types_pb2.State.ValueType:
    return types_pb2.State.Value("STATE_" + s.upper())


def encode_json(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value)
    return json.dumps(value, separators=(",", ":")).encode()


def decode_json(raw: bytes) -> Any:
    return None if not raw else json.loads(raw)


def duration(seconds: float) -> Duration:
    d = Duration()
    d.FromNanoseconds(round(seconds * 1e9))
    return d


def seconds(d: Duration) -> float:
    return d.seconds + d.nanos / 1e9


def timestamp(at: datetime) -> Timestamp:
    t = Timestamp()
    t.FromDatetime(at)
    return t


def _time(msg: Any, name: str) -> datetime | None:
    return cast("datetime", getattr(msg, name).ToDatetime(tzinfo=UTC)) if msg.HasField(name) else None


@dataclass(frozen=True)
class AttemptError:
    attempt: int
    queue: str
    kind: str
    at: datetime | None
    error: str


@dataclass(frozen=True)
class Job:
    id: str
    namespace: str
    queue: str
    kind: str
    state: JobState
    priority: int = 0
    group: str = ""
    pool: str = ""
    attempt: int = 0
    max_attempts: int = 0
    version: int = 0
    worker: str = ""
    handle: str = ""
    unique_key: str = ""
    depends_on: tuple[str, ...] = ()
    callback_url: str = ""
    meta: Mapping[str, str] = field(default_factory=dict)
    raw_payload: bytes = b""
    raw_result: bytes = b""
    errors: tuple[AttemptError, ...] = ()
    run_at: datetime | None = None
    start_by: datetime | None = None
    deadline: datetime | None = None
    timeout: float | None = None
    lease_until: datetime | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @classmethod
    def from_pb(cls, j: types_pb2.Job) -> Job:
        return cls(
            id=j.id,
            namespace=j.namespace,
            queue=j.queue,
            kind=j.kind,
            state=job_state(j.state),
            priority=j.priority,
            group=j.group,
            pool=j.pool,
            attempt=j.attempt,
            max_attempts=j.max_attempts,
            version=j.version,
            worker=j.worker,
            handle=j.handle,
            unique_key=j.unique_key,
            depends_on=tuple(j.depends_on),
            callback_url=j.callback_url,
            meta=dict(j.meta),
            raw_payload=j.payload,
            raw_result=j.result,
            errors=tuple(AttemptError(e.attempt, e.queue, e.kind, _time(e, "at"), e.error) for e in j.errors),
            run_at=_time(j, "run_at"),
            start_by=_time(j, "start_by"),
            deadline=_time(j, "deadline"),
            timeout=seconds(j.timeout) if j.HasField("timeout") else None,
            lease_until=_time(j, "lease_until"),
            created_at=_time(j, "created_at"),
            started_at=_time(j, "started_at"),
            finished_at=_time(j, "finished_at"),
        )

    @cached_property
    def payload(self) -> Any:
        return decode_json(self.raw_payload)

    @cached_property
    def result(self) -> Any:
        return decode_json(self.raw_result)

    @property
    def final(self) -> bool:
        return self.state in FINAL_STATES

    @property
    def polling(self) -> bool:
        return self.handle != ""


class Backoff(TypedDict, total=False):
    base: float
    max: float
    factor: float
    jitter: float


class JobOptions(TypedDict, total=False):
    priority: int
    group: str
    pool: str
    run_at: datetime
    delay: float
    max_attempts: int
    unique_key: str
    unique_for: float
    start_timeout: float
    deadline: datetime
    ttl: float
    timeout: float
    backoff: Backoff
    depends_on: Sequence[str]
    callback_url: str
    meta: Mapping[str, str]


class NewJob(JobOptions):
    queue: str
    kind: str
    payload: NotRequired[Any]


class FlowStep(TypedDict):
    job: NewJob
    after: NotRequired[Sequence[str]]


_DURATIONS = ("delay", "unique_for", "start_timeout", "ttl", "timeout")
_TIMES = ("run_at", "deadline")


def new_job(j: NewJob) -> types_pb2.NewJob:
    out = types_pb2.NewJob(
        queue=j["queue"],
        kind=j["kind"],
        payload=encode_json(j.get("payload")),
        priority=j.get("priority", 0),
        group=j.get("group", ""),
        pool=j.get("pool", ""),
        max_attempts=j.get("max_attempts", 0),
        unique_key=j.get("unique_key", ""),
        depends_on=list(j.get("depends_on", ())),
        callback_url=j.get("callback_url", ""),
        meta=dict(j.get("meta", {})),
    )
    for name in _DURATIONS:
        value = j.get(name)
        if value is not None:
            getattr(out, name).CopyFrom(duration(cast("float", value)))
    for name in _TIMES:
        at = j.get(name)
        if at is not None:
            getattr(out, name).CopyFrom(timestamp(cast("datetime", at)))
    b = j.get("backoff")
    if b is not None:
        out.backoff.factor = b.get("factor", 0.0)
        out.backoff.jitter = b.get("jitter", 0.0)
        if "base" in b:
            out.backoff.base.CopyFrom(duration(b["base"]))
        if "max" in b:
            out.backoff.max.CopyFrom(duration(b["max"]))
    return out
