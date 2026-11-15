import hashlib
import hmac
import json
import re
import time
from collections.abc import Sequence
from typing import Any, TypedDict, cast

from runnel.errors import SignatureError
from runnel.job import JobState

DEFAULT_MAX_AGE = 300.0

_INT = re.compile(r"^[+-]?\d+$")


class CallbackJob(TypedDict, total=False):
    id: str
    namespace: str
    queue: str
    kind: str
    state: JobState
    attempt: int
    result: Any
    error: str


class CallbackBody(TypedDict):
    event: str
    delivery: int
    job: CallbackJob


def _bytes(body: str | bytes) -> bytes:
    return body.encode() if isinstance(body, str) else body


def sign(secret: str, t: int, body: str | bytes) -> str:
    mac = hmac.new(secret.encode(), f"{t}.".encode() + _bytes(body), hashlib.sha256).hexdigest()
    return f"t={t},v1={mac}"


def _parse(header: str) -> tuple[int, list[str]]:
    t = 0
    sigs: list[str] = []
    for part in header.split(","):
        key, _, value = part.strip().partition("=")
        if key == "t":
            if not _INT.match(value):
                raise SignatureError("runnel: bad callback signature: bad time")
            t = int(value)
        elif key == "v1":
            sigs.append(value)
    if t == 0 or not sigs:
        raise SignatureError("runnel: bad callback signature: missing t or v1")
    return t, sigs


def verify_callback(
    header: str, body: str | bytes, secrets: Sequence[str], *, max_age: float | None = None, now: float | None = None
) -> None:
    limit = max_age if max_age and max_age > 0 else DEFAULT_MAX_AGE
    t, sigs = _parse(header)
    age = (time.time() if now is None else now) - t
    if abs(age) > limit:
        raise SignatureError(f"runnel: bad callback signature: signed {round(age)}s from now")
    for secret in secrets:
        if not secret:
            continue
        want = sign(secret, t, body).encode()
        if any(hmac.compare_digest(f"t={t},v1={sig}".encode(), want) for sig in sigs):
            return
    raise SignatureError("runnel: bad callback signature: no secret matches")


def parse_callback(
    header: str, body: str | bytes, secrets: Sequence[str], *, max_age: float | None = None, now: float | None = None
) -> CallbackBody:
    verify_callback(header, body, secrets, max_age=max_age, now=now)
    return cast("CallbackBody", json.loads(body))
