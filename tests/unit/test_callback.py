import json
from pathlib import Path
from typing import Any

import pytest

from runnel import SignatureError, parse_callback, sign, verify_callback

VECTORS: list[dict[str, Any]] = json.loads((Path(__file__).parent.parent / "fixtures" / "callback-signatures.json").read_text())["vectors"]


def test_has_the_shared_vectors() -> None:
    assert len(VECTORS) >= 10


@pytest.mark.parametrize("v", VECTORS, ids=[v["name"] for v in VECTORS])
def test_vector(v: dict[str, Any]) -> None:
    def check() -> None:
        verify_callback(v["header"], v["body"], v["secrets"], max_age=v["max_age_seconds"], now=v["now"])

    if v["valid"]:
        check()
    else:
        with pytest.raises(SignatureError):
            check()


def test_bytes_verify_the_same_as_text() -> None:
    body = '{"event":"job.completed","delivery":7,"job":{"id":"j1","state":"completed"}}'
    header = sign("s", 1_800_000_000, body)
    got = parse_callback(header, body.encode(), ["s"], now=1_800_000_000)
    assert got["delivery"] == 7
    assert got["job"]["state"] == "completed"
