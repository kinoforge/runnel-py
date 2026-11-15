import grpc
import pytest

from runnel import RunnelError
from runnel._channel import target
from runnel.errors import to_runnel_error


def aio_error(code: grpc.StatusCode, trailing: tuple[tuple[str, str], ...] = ()) -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata(*trailing), "queue full")


def test_names_the_runnel_error_and_the_retry_delay() -> None:
    src = aio_error(grpc.StatusCode.RESOURCE_EXHAUSTED, (("runnel-error", "queue_full"), ("retry-after", "3")))
    err = to_runnel_error(src)
    assert err.code == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert err.reason == "queue_full"
    assert err.retry_after == 3.0
    assert err.busy
    assert err.__cause__ is src
    assert err.message == "queue full"


def test_leaves_reason_and_delay_empty_when_the_server_sent_none() -> None:
    err = to_runnel_error(aio_error(grpc.StatusCode.PERMISSION_DENIED))
    assert err.reason is None
    assert err.retry_after is None
    assert not err.busy


def test_other_errors_are_unknown() -> None:
    err = to_runnel_error(ConnectionResetError("reset"))
    assert err.code == grpc.StatusCode.UNKNOWN
    assert err.message == "reset"
    assert to_runnel_error(err) is err
    assert isinstance(err, RunnelError)


@pytest.mark.parametrize(
    ("url", "address", "secure"),
    [
        ("http://runnel.internal:8700", "runnel.internal:8700", False),
        ("https://runnel.example.com", "runnel.example.com:443", True),
        ("http://[::1]:8700/", "[::1]:8700", False),
    ],
)
def test_target(url: str, address: str, secure: bool) -> None:
    t = target(url)
    assert t.address == address
    assert t.secure is secure


def test_target_needs_a_scheme() -> None:
    with pytest.raises(ValueError, match="http"):
        target("runnel.internal:8700")
