"""/api/v1/workers — Worker registry + HTTP-based queue access.

Worker daemon (別プロセス、リモートホスト含む) はこれらの endpoint を
HTTP で叩いて control plane と通信する:

- POST   /api/v1/workers                              — register
- PUT    /api/v1/workers/{id}/heartbeat               — heartbeat (5s 毎)
- DELETE /api/v1/workers/{id}                         — graceful deregister
- GET    /api/v1/workers                              — admin: 一覧

- GET    /api/v1/workers/{id}/workloads               — 現在 enabled な workload list (worker drain 用)
- POST   /api/v1/workers/{id}/claim                   — workload 指定で claim batch
- POST   /api/v1/workers/{id}/complete                — task pk 群を complete
- POST   /api/v1/workers/{id}/fail                    — task pk を fail
- POST   /api/v1/workers/{id}/runs                    — runs テーブルに 1 件 record
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from pipeline.models.workload import Workload
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workers import WorkerNotFound, WorkerRepository
from pipeline.repositories.workloads import WorkloadNotFound, WorkloadRepository

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


# ---------------- request / response models ----------------


class WorkerRegisterRequest(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    pid: int | None = None
    tags: list[str] = Field(default_factory=list)
    resources: dict[str, Any] = Field(default_factory=dict)
    worker_id: str | None = None


class WorkerInfo(BaseModel):
    id: str
    host: str
    pid: int | None = None
    tags: list[str] = Field(default_factory=list)
    resources: dict[str, Any] = Field(default_factory=dict)
    state: str
    started_at: str | None = None
    last_seen_at: str | None = None
    current_workload: str | None = None
    current_phase: str | None = None
    rows_processed: int = 0
    errors_total: int = 0


class WorkersListResponse(BaseModel):
    workers: list[WorkerInfo]
    total: int


class GpuMetric(BaseModel):
    gpu_idx: int
    temp_c: float | None = None
    util_pct: float | None = None
    mem_used_mb: int | None = None


class HeartbeatRequest(BaseModel):
    current_workload: str | None = None
    current_phase: str | None = None
    rows_processed_delta: int = 0
    errors_total_delta: int = 0
    gpu_metrics: list[GpuMetric] | None = None


class ClaimRequest(BaseModel):
    workload_slug: str
    limit: int = Field(default=10, ge=1, le=10000)


class ClaimedTaskOut(BaseModel):
    pk: str
    attempt: int
    extra: dict[str, Any]
    enqueued_at: str


class ClaimResponse(BaseModel):
    workload_slug: str
    tasks: list[ClaimedTaskOut]


class CompleteRequest(BaseModel):
    workload_slug: str
    pks: list[str] = Field(min_length=1)


class FailRequest(BaseModel):
    workload_slug: str
    pk: str
    error: str | None = None


class RunRecordRequest(BaseModel):
    workload_slug: str
    pk: str
    attempt: int
    started_at: str
    success: bool
    exit_code: int | None = None
    duration_ms: int
    stdout: str | None = None
    stderr: str | None = None
    output_json: dict[str, Any] | None = None
    error: str | None = None


class WorkloadsForWorkerResponse(BaseModel):
    workloads: list[Workload]


# ---------------- helpers ----------------


def _wrepo(request: Request) -> WorkerRepository:
    return WorkerRepository(request.app.state.db)


def _qrepo(request: Request) -> QueueRepository:
    return QueueRepository(request.app.state.db)


def _rrepo(request: Request) -> RunsRepository:
    return RunsRepository(request.app.state.db)


def _wlrepo(request: Request) -> WorkloadRepository:
    return WorkloadRepository(request.app.state.db)


def _get_worker_or_404(request: Request, worker_id: str) -> dict[str, Any]:
    try:
        return _wrepo(request).get(worker_id)
    except WorkerNotFound as e:
        raise HTTPException(404, detail=str(e)) from e


def _get_workload_or_404(request: Request, slug: str) -> Workload:
    try:
        return _wlrepo(request).get(slug)
    except WorkloadNotFound as e:
        raise HTTPException(404, detail=str(e)) from e


# ---------------- registry ----------------


@router.post("", response_model=WorkerInfo, status_code=status.HTTP_201_CREATED)
def register_worker(body: WorkerRegisterRequest, request: Request) -> WorkerInfo:
    rec = _wrepo(request).register(
        host=body.host, pid=body.pid,
        tags=body.tags, resources=body.resources,
        worker_id=body.worker_id,
    )
    return WorkerInfo(**rec)


@router.put("/{worker_id}/heartbeat", response_model=WorkerInfo)
def heartbeat(worker_id: str, body: HeartbeatRequest, request: Request) -> WorkerInfo:
    try:
        rec = _wrepo(request).heartbeat(
            worker_id,
            current_workload=body.current_workload,
            current_phase=body.current_phase,
            rows_processed_delta=body.rows_processed_delta,
            errors_total_delta=body.errors_total_delta,
        )
    except WorkerNotFound as e:
        raise HTTPException(404, detail=str(e)) from e
    # GPU metrics があれば worker_metrics に INSERT (= 時系列 store)
    if body.gpu_metrics:
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).isoformat()
        db = request.app.state.db
        with db.transaction() as conn:
            for g in body.gpu_metrics:
                conn.execute(
                    "INSERT OR REPLACE INTO worker_metrics "
                    "(worker_id, gpu_idx, ts, temp_c, util_pct, mem_used_mb) "
                    "VALUES (:wid, :gi, :ts, :tc, :up, :mu)",
                    {"wid": worker_id, "gi": g.gpu_idx, "ts": ts,
                     "tc": g.temp_c, "up": g.util_pct, "mu": g.mem_used_mb},
                )
    return WorkerInfo(**rec)


@router.get("/metrics", response_model=dict[str, Any])
def list_workers_metrics(request: Request, minutes: int = 30) -> dict[str, Any]:
    """過去 N 分の全 worker の GPU metrics を返す。 UI Dashboard graph 用."""
    from datetime import datetime, timedelta, timezone
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    db = request.app.state.db
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT worker_id, gpu_idx, ts, temp_c, util_pct, mem_used_mb "
            "FROM worker_metrics WHERE ts >= :since ORDER BY worker_id, gpu_idx, ts",
            {"since": since},
        )
        rows = [dict(r) for r in cur.fetchall()]
    # worker_id ごとに集約
    by_worker: dict[str, dict[int, list[dict[str, Any]]]] = {}
    for r in rows:
        by_worker.setdefault(r["worker_id"], {}).setdefault(r["gpu_idx"], []).append({
            "ts": r["ts"], "temp_c": r["temp_c"],
            "util_pct": r["util_pct"], "mem_used_mb": r["mem_used_mb"],
        })
    return {"workers": by_worker, "since_minutes": minutes}


@router.delete("/{worker_id}", status_code=status.HTTP_204_NO_CONTENT)
def deregister(worker_id: str, request: Request) -> None:
    _wrepo(request).deregister(worker_id)


@router.get("", response_model=WorkersListResponse)
def list_workers(request: Request) -> WorkersListResponse:
    items = _wrepo(request).list_all()
    return WorkersListResponse(
        workers=[WorkerInfo(**r) for r in items], total=len(items)
    )


# ---------------- drain / queue access ----------------


@router.get("/{worker_id}/workloads", response_model=WorkloadsForWorkerResponse)
def workloads_for_worker(worker_id: str, request: Request) -> WorkloadsForWorkerResponse:
    """この worker が claim できる enabled workload を返す。

    `host_affinity` が空: 全 host が候補。
    `host_affinity` が指定: worker.host がリストに含まれる場合のみ。
    """
    worker = _get_worker_or_404(request, worker_id)
    worker_host = worker.get("host") if isinstance(worker, dict) else getattr(worker, "host", None)
    all_wls = _wlrepo(request).list_all()
    out = []
    for w in all_wls:
        if not w.enabled:
            continue
        affinity = list(w.host_affinity or [])
        if affinity and worker_host not in affinity:
            continue
        out.append(w)
    return WorkloadsForWorkerResponse(workloads=out)


@router.post("/{worker_id}/claim", response_model=ClaimResponse)
def claim(worker_id: str, body: ClaimRequest, request: Request) -> ClaimResponse:
    _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    tasks = _qrepo(request).claim(
        w.queue_table,
        worker_id=worker_id,
        limit=min(body.limit, w.batch_size),
        lease_secs=w.lease_secs,
    )
    return ClaimResponse(
        workload_slug=w.slug,
        tasks=[
            ClaimedTaskOut(pk=t.pk, attempt=t.attempt, extra=t.extra, enqueued_at=t.enqueued_at)
            for t in tasks
        ],
    )


@router.post("/{worker_id}/complete", status_code=status.HTTP_204_NO_CONTENT)
def complete(worker_id: str, body: CompleteRequest, request: Request) -> None:
    _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    for pk in body.pks:
        _qrepo(request).complete(w.queue_table, pk)


@router.post("/{worker_id}/fail", status_code=status.HTTP_204_NO_CONTENT)
def fail(worker_id: str, body: FailRequest, request: Request) -> None:
    _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    _qrepo(request).fail(w.queue_table, body.pk, max_attempts=w.max_attempts, error=body.error)


@router.post("/{worker_id}/runs", status_code=status.HTTP_201_CREATED)
def record_run(worker_id: str, body: RunRecordRequest, request: Request) -> dict[str, str]:
    _get_worker_or_404(request, worker_id)
    rid = _rrepo(request).record(
        workload_slug=body.workload_slug,
        pk=body.pk,
        worker_id=worker_id,
        attempt=body.attempt,
        started_at=body.started_at,
        success=body.success,
        exit_code=body.exit_code,
        duration_ms=body.duration_ms,
        stdout=body.stdout,
        stderr=body.stderr,
        output_json=body.output_json,
        error=body.error,
    )
    return {"id": rid}
