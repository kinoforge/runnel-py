from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from runnel.job import FlowStep, Job, JobState, NewJob, duration, new_job, pb_state
from runnel.v1 import job_pb2, types_pb2


@dataclass(frozen=True)
class Inserted:
    id: str
    duplicate: bool = False
    error: str = ""


@dataclass(frozen=True)
class JobPage:
    jobs: list[Job]
    next_cursor: str


Stats = dict[str, dict[str, int]]


def inserted(r: types_pb2.Inserted) -> Inserted:
    return Inserted(r.id, r.duplicate, r.error)


def enqueue(ns: str, jobs: Iterable[NewJob]) -> job_pb2.EnqueueRequest:
    return job_pb2.EnqueueRequest(namespace=ns, jobs=[new_job(j) for j in jobs])


def first(res: job_pb2.EnqueueResponse) -> Inserted:
    if not res.results:
        raise RuntimeError("runnel: enqueue answered no result")
    return inserted(res.results[0])


def flow(ns: str, steps: Mapping[str, FlowStep]) -> job_pb2.EnqueueFlowRequest:
    return job_pb2.EnqueueFlowRequest(
        namespace=ns,
        steps=[job_pb2.FlowStep(name=name, job=new_job(s["job"]), after=list(s.get("after", ()))) for name, s in steps.items()],
    )


def flow_result(res: job_pb2.EnqueueFlowResponse) -> dict[str, Inserted]:
    return {name: inserted(r) for name, r in res.steps.items()}


def job(j: types_pb2.Job, present: bool) -> Job:
    if not present:
        raise RuntimeError("runnel: answer without a job")
    return Job.from_pb(j)


def wait(ns: str, id: str, seconds: float) -> job_pb2.WaitJobRequest:
    return job_pb2.WaitJobRequest(namespace=ns, id=id, wait=duration(max(0.0, seconds)))


def list_jobs(
    ns: str, *, queue: str | None, kind: str | None, group: str | None, state: JobState | None, cursor: str | None, limit: int | None
) -> job_pb2.ListJobsRequest:
    return job_pb2.ListJobsRequest(
        namespace=ns,
        queue=queue or "",
        kind=kind or "",
        group=group or "",
        state=pb_state(state) if state else types_pb2.STATE_UNSPECIFIED,
        cursor=cursor or "",
        limit=limit or 0,
    )


def page(res: job_pb2.ListJobsResponse) -> JobPage:
    return JobPage([Job.from_pb(j) for j in res.jobs], res.next_cursor)


def stats(res: job_pb2.GetStatsResponse) -> Stats:
    return {q.queue: dict(q.counts) for q in res.queues}
