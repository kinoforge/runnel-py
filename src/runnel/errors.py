from collections.abc import Iterable
from typing import Literal, cast

import grpc

ErrorReason = Literal[
    "invalid", "not_found", "stale", "queue_full", "group_full", "no_space", "dependency", "conflict", "final", "not_final"
]

RETRYABLE = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNKNOWN, grpc.StatusCode.INTERNAL, grpc.StatusCode.DEADLINE_EXCEEDED})


class RunnelError(Exception):
    def __init__(self, message: str, code: grpc.StatusCode, reason: ErrorReason | None = None, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.reason = reason
        self.retry_after = retry_after

    @property
    def busy(self) -> bool:
        return self.code == grpc.StatusCode.RESOURCE_EXHAUSTED

    def __str__(self) -> str:
        return f"{self.code.name}: {self.message}"


def _metadata(pairs: Iterable[tuple[str, str | bytes]] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in pairs or ():
        out[key.lower()] = value.decode() if isinstance(value, bytes) else value
    return out


def to_runnel_error(err: BaseException) -> RunnelError:
    if isinstance(err, RunnelError):
        return err
    if isinstance(err, grpc.aio.AioRpcError) or (isinstance(err, grpc.RpcError) and isinstance(err, grpc.Call)):
        code, message, md = err.code(), err.details() or "", _metadata(err.trailing_metadata())
    else:
        return _wrap(RunnelError(str(err) or type(err).__name__, grpc.StatusCode.UNKNOWN), err)
    retry_after: float | None = None
    try:
        retry_after = float(md["retry-after"])
    except (KeyError, ValueError):
        retry_after = None
    reason = cast("ErrorReason | None", md.get("runnel-error") or None)
    return _wrap(RunnelError(message, code, reason, retry_after), err)


def _wrap(out: RunnelError, cause: BaseException) -> RunnelError:
    out.__cause__ = cause
    return out


class ShutdownError(Exception):
    def __init__(self) -> None:
        super().__init__("runnel: worker shutting down")


class JobTimeoutError(Exception):
    def __init__(self, timeout: float) -> None:
        super().__init__(f"runnel: attempt timed out after {timeout:g}s")
        self.timeout = timeout


class DeadlineError(Exception):
    def __init__(self) -> None:
        super().__init__("runnel: deadline passed")


class LeaseLostError(Exception):
    def __init__(self) -> None:
        super().__init__("runnel: lease lost")


class SignatureError(ValueError):
    pass
