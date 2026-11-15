from collections.abc import Sequence
from typing import Any, NamedTuple
from urllib.parse import urlsplit

import grpc

ChannelOptions = Sequence[tuple[str, Any]]


class Target(NamedTuple):
    address: str
    secure: bool


def target(url: str) -> Target:
    u = urlsplit(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise ValueError(f"runnel: url must be http:// or https://, got {url!r}")
    host = f"[{u.hostname}]" if ":" in u.hostname else u.hostname
    port = u.port or (443 if u.scheme == "https" else 80)
    return Target(f"{host}:{port}", u.scheme == "https")


def _auth(metadata: Any, token: str) -> list[tuple[str, str | bytes]]:
    out: list[tuple[str, str | bytes]] = [(k, v) for k, v in metadata or () if k.lower() != "authorization"]
    out.append(("authorization", f"Bearer {token}"))
    return out


class _Details(NamedTuple):
    method: str
    timeout: float | None
    metadata: Any
    credentials: grpc.CallCredentials | None
    wait_for_ready: bool | None
    compression: grpc.Compression | None


class _SyncAuth(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, token: str) -> None:
        self._token = token

    def _details(self, d: Any) -> Any:
        return _CallDetails(
            d.method, d.timeout, _auth(d.metadata, self._token), d.credentials, d.wait_for_ready, getattr(d, "compression", None)
        )

    def intercept_unary_unary(self, continuation: Any, details: Any, request: Any) -> Any:
        return continuation(self._details(details), request)

    def intercept_unary_stream(self, continuation: Any, details: Any, request: Any) -> Any:
        return continuation(self._details(details), request)

    def intercept_stream_unary(self, continuation: Any, details: Any, requests: Any) -> Any:
        return continuation(self._details(details), requests)

    def intercept_stream_stream(self, continuation: Any, details: Any, requests: Any) -> Any:
        return continuation(self._details(details), requests)


class _CallDetails(_Details, grpc.ClientCallDetails):
    pass


class _AsyncAuth:
    def __init__(self, token: str) -> None:
        self._token = token

    def _details(self, d: grpc.aio.ClientCallDetails) -> grpc.aio.ClientCallDetails:
        md = grpc.aio.Metadata(*_auth(d.metadata, self._token))
        return grpc.aio.ClientCallDetails(d.method, d.timeout, md, d.credentials, d.wait_for_ready)


class _UnaryUnaryAuth(_AsyncAuth, grpc.aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return await continuation(self._details(client_call_details), request)


class _UnaryStreamAuth(_AsyncAuth, grpc.aio.UnaryStreamClientInterceptor):
    async def intercept_unary_stream(self, continuation: Any, client_call_details: Any, request: Any) -> Any:
        return await continuation(self._details(client_call_details), request)


class _StreamUnaryAuth(_AsyncAuth, grpc.aio.StreamUnaryClientInterceptor):
    async def intercept_stream_unary(self, continuation: Any, client_call_details: Any, request_iterator: Any) -> Any:
        return await continuation(self._details(client_call_details), request_iterator)


class _StreamStreamAuth(_AsyncAuth, grpc.aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(self, continuation: Any, client_call_details: Any, request_iterator: Any) -> Any:
        return await continuation(self._details(client_call_details), request_iterator)


def _async_auth(token: str) -> list[grpc.aio.ClientInterceptor]:
    return [_UnaryUnaryAuth(token), _UnaryStreamAuth(token), _StreamUnaryAuth(token), _StreamStreamAuth(token)]


def sync_channel(
    url: str, token: str | None, credentials: grpc.ChannelCredentials | None, options: ChannelOptions, interceptors: Sequence[Any]
) -> grpc.Channel:
    t = target(url)
    ch = (
        grpc.secure_channel(t.address, credentials or grpc.ssl_channel_credentials(), options)
        if t.secure
        else grpc.insecure_channel(t.address, options)
    )
    chain = [*interceptors, *([_SyncAuth(token)] if token else [])]
    return grpc.intercept_channel(ch, *chain) if chain else ch


def async_channel(
    url: str,
    token: str | None,
    credentials: grpc.ChannelCredentials | None,
    options: ChannelOptions,
    interceptors: Sequence[grpc.aio.ClientInterceptor],
) -> grpc.aio.Channel:
    t = target(url)
    chain = [*(_async_auth(token) if token else []), *interceptors]
    if t.secure:
        return grpc.aio.secure_channel(t.address, credentials or grpc.ssl_channel_credentials(), options, interceptors=chain)
    return grpc.aio.insecure_channel(t.address, options, interceptors=chain)
