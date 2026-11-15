from datetime import UTC, datetime

from runnel import Job, busy, fail, pending, retry_in
from runnel._clock import ServerClock
from runnel.job import JobState, duration, new_job, pb_state, seconds, timestamp
from runnel.v1 import types_pb2, worker_pb2


def test_sends_payloads_as_json_bytes_and_raw_bytes_untouched() -> None:
    assert new_job({"queue": "q", "kind": "k", "payload": {"a": 1}}).payload == b'{"a":1}'
    raw = b'{"n":12345678901234567}'
    assert new_job({"queue": "q", "kind": "k", "payload": raw}).payload == raw
    assert new_job({"queue": "q", "kind": "k"}).payload == b""


def test_turns_seconds_into_durations() -> None:
    j = new_job({"queue": "q", "kind": "k", "delay": 1.5, "backoff": {"base": 0.1, "factor": 3}})
    assert seconds(j.delay) == 1.5
    assert seconds(j.backoff.base) == 0.1
    assert j.backoff.factor == 3
    assert not j.HasField("timeout")
    assert not j.backoff.HasField("max")


def test_reads_a_job() -> None:
    at = datetime(2026, 11, 15, 10, tzinfo=UTC)
    pb = types_pb2.Job(
        id="j1", state=types_pb2.STATE_POLLING, handle="task-1", payload=b'{"x":true}', deadline=timestamp(at), timeout=duration(2)
    )
    j = Job.from_pb(pb)
    assert j.state == "polling"
    assert j.polling
    assert not j.final
    assert j.payload == {"x": True}
    assert j.result is None
    assert j.deadline == at
    assert j.timeout == 2.0
    assert j.started_at is None


def test_maps_every_state_both_ways() -> None:
    states: list[JobState] = ["blocked", "waiting", "running", "polling", "completed", "dead", "cancelled", "expired"]
    for s in states:
        assert Job.from_pb(types_pb2.Job(state=pb_state(s))).state == s


def test_outcomes() -> None:
    k = worker_pb2.OutcomeKind
    assert fail(ValueError("bad")).outcome == worker_pb2.Outcome(kind=k.OUTCOME_KIND_FAIL, error="bad")
    r = retry_in(2, "not yet").outcome
    assert (r.kind, r.error, seconds(r.delay)) == (k.OUTCOME_KIND_RETRY, "not yet", 2.0)
    assert seconds(busy(0.3).outcome.delay) == 0.3
    p = pending("task-1", 10).outcome
    assert (p.kind, p.handle) == (k.OUTCOME_KIND_PENDING, "task-1")


def test_server_clock() -> None:
    c = ServerClock(lambda: 1000.0)
    c.observe(timestamp(datetime.fromtimestamp(1030.5, UTC)))
    assert c.now() == 1030.5
    assert c.to_local(1040.5) == 1010.0
    c.observe(None)
    assert c.now() == 1030.5
