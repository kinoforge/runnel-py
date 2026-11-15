import time
from collections.abc import Callable

from google.protobuf.timestamp_pb2 import Timestamp

Now = Callable[[], float]


class ServerClock:
    def __init__(self, local: Now = time.time) -> None:
        self.local = local
        self._offset = 0.0

    def observe(self, server: Timestamp | None) -> None:
        if server is not None and (server.seconds or server.nanos):
            self._offset = server.seconds + server.nanos / 1e9 - self.local()

    def now(self) -> float:
        return self.local() + self._offset

    def to_local(self, server: float) -> float:
        return server - self._offset
