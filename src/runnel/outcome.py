import json
from typing import Any

from runnel.job import duration, encode_json
from runnel.v1 import worker_pb2

Outcome = worker_pb2.Outcome
Kind = worker_pb2.OutcomeKind


class Control(Exception):
    def __init__(self, outcome: Outcome, message: str) -> None:
        super().__init__(message)
        self.outcome = outcome


def message(err: Any) -> str:
    if err is None:
        return ""
    if isinstance(err, BaseException):
        return str(err) or type(err).__name__
    return err if isinstance(err, str) else json.dumps(err)


def fail(err: Any) -> Control:
    m = message(err)
    return Control(Outcome(kind=Kind.OUTCOME_KIND_FAIL, error=m), m)


def retry_in(delay: float, err: Any = None) -> Control:
    m = message(err)
    return Control(Outcome(kind=Kind.OUTCOME_KIND_RETRY, error=m, delay=duration(delay)), m)


def busy(delay: float) -> Control:
    return Control(Outcome(kind=Kind.OUTCOME_KIND_BUSY, delay=duration(delay)), "busy")


def pending(handle: str, delay: float) -> Control:
    return Control(Outcome(kind=Kind.OUTCOME_KIND_PENDING, handle=handle, delay=duration(delay)), "pending")


def done(result: Any) -> Outcome:
    return Outcome(kind=Kind.OUTCOME_KIND_DONE, result=encode_json(result))


def retry(err: Any) -> Outcome:
    return Outcome(kind=Kind.OUTCOME_KIND_RETRY, error=message(err) or "error")


def release() -> Outcome:
    return Outcome(kind=Kind.OUTCOME_KIND_RELEASE)
