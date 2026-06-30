"""/api/v1/workloads — Workload CRUD + enqueue / runs / queue stats endpoint."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from pipeline.models.workload import Workload, WorkloadCreate, WorkloadUpdate
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workloads import (
    WorkloadAlreadyExists,
    WorkloadNotFound,
    WorkloadRepository,
)

router = APIRouter(prefix="/api/v1/workloads", tags=["workloads"])


class WorkloadListResponse(BaseModel):
    workloads: list[Workload]
    total: int


class EnableRequest(BaseModel):
    enabled: bool


class EnqueueRequest(BaseModel):
    pk: str = Field(min_length=1, description="task の primary key")
    extra: dict[str, Any] = Field(default_factory=dict)


class EnqueueBatchRequest(BaseModel):
    items: list[EnqueueRequest] = Field(min_length=1, max_length=10000)


class EnqueueResponse(BaseModel):
    inserted: int
    duplicates: int


class RunRecord(BaseModel):
    id: str
    workload_slug: str
    pk: str
    worker_id: str
    attempt: int
    started_at: str
    finished_at: str | None = None
    success: bool | None = None
    exit_code: int | None = None
    duration_ms: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    output_json: dict[str, Any] | None = None
    error: str | None = None


class RunsListResponse(BaseModel):
    runs: list[RunRecord]
    total: int


class QueueStats(BaseModel):
    by_state: dict[str, int]
    total: int


class VramObservationRequest(BaseModel):
    """worker が報告する自分のプロセス VRAM 占有値 (= nvidia-smi の compute-apps used_memory)。"""
    worker_id: str | None = None
    used_mb: int = Field(ge=0, le=200_000, description="自プロセスの GPU VRAM 占有 (MB)")


class VramObservationResponse(BaseModel):
    accepted: bool
    observed_vram_mb_peak: int | None = None
    observed_vram_sample_count: int = 0


def _repo(request: Request) -> WorkloadRepository:
    return WorkloadRepository(request.app.state.db)


def _queue_repo(request: Request) -> QueueRepository:
    db = request.app.state.db
    secondary = getattr(request.app.state, "secondary_db", None)
    repo = QueueRepository(db, secondary)
    if secondary is not None:
        # workload.queue_backend='mariadb' の queue を secondary に振り替え
        repo.wire_from_workloads(WorkloadRepository(db).list_all())
    return repo


def _runs_repo(request: Request) -> RunsRepository:
    return RunsRepository(request.app.state.db)


def _get_or_404(request: Request, slug: str) -> Workload:
    try:
        return _repo(request).get(slug)
    except WorkloadNotFound as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@router.get("", response_model=WorkloadListResponse)
def list_workloads(request: Request) -> WorkloadListResponse:
    items = _repo(request).list_all()
    return WorkloadListResponse(workloads=items, total=len(items))


@router.post("", response_model=Workload, status_code=status.HTTP_201_CREATED)
def create_workload(payload: WorkloadCreate, request: Request) -> Workload:
    try:
        return _repo(request).create(payload)
    except WorkloadAlreadyExists as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"workload '{e.args[0]}' は既に存在します",
        ) from e


@router.get("/{slug}", response_model=Workload)
def get_workload(slug: str, request: Request) -> Workload:
    try:
        return _repo(request).get(slug)
    except WorkloadNotFound as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@router.put("/{slug}", response_model=Workload)
def update_workload(slug: str, payload: WorkloadUpdate, request: Request) -> Workload:
    try:
        return _repo(request).update(slug, payload)
    except WorkloadNotFound as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@router.patch("/{slug}/enabled", response_model=Workload)
def patch_enabled(slug: str, body: EnableRequest, request: Request) -> Workload:
    try:
        return _repo(request).set_enabled(slug, body.enabled)
    except WorkloadNotFound as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@router.patch("/{slug}/supervisor_enabled", response_model=Workload)
def patch_supervisor_enabled(slug: str, body: EnableRequest, request: Request) -> Workload:
    """supervisor (= 自動オーケストレーター) によるこの workload への介入を
    許可するか個別 toggle。 False のとき: supervisor はルール条件は評価するが、
    patch_workload / filter 変更系の action は no-op (log のみ)。 オペレータが
    手で priority/batch_size 等を握りたい場面用 (= max throughput テスト等)。
    """
    try:
        return _repo(request).set_supervisor_enabled(slug, body.enabled)
    except WorkloadNotFound as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


@router.delete("/{slug}", status_code=status.HTTP_204_NO_CONTENT)
def delete_workload(slug: str, request: Request) -> None:
    try:
        _repo(request).delete(slug)
    except WorkloadNotFound as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e)) from e


# -------- task enqueue / runs / queue stats --------


@router.post(
    "/{slug}/tasks",
    response_model=EnqueueResponse,
    status_code=status.HTTP_201_CREATED,
)
def enqueue_task(slug: str, body: EnqueueRequest, request: Request) -> EnqueueResponse:
    w = _get_or_404(request, slug)
    inserted = _queue_repo(request).enqueue(w.queue_table, body.pk, body.extra)
    return EnqueueResponse(inserted=1 if inserted else 0, duplicates=0 if inserted else 1)


@router.post(
    "/{slug}/tasks/batch",
    response_model=EnqueueResponse,
    status_code=status.HTTP_201_CREATED,
)
def enqueue_batch(
    slug: str, body: EnqueueBatchRequest, request: Request, strict: bool = False
) -> EnqueueResponse:
    w = _get_or_404(request, slug)
    items = [(it.pk, it.extra) for it in body.items]
    repo = _queue_repo(request)
    if strict:
        # strict: INSERT OR IGNORE を使わず plain INSERT。呼出側が source CAS で一意性を
        # 保証する前提 (= video-dispatcher 等)。衝突は IGNORE せず collided として返す。
        r = repo.enqueue_many_strict(w.queue_table, items)
        return EnqueueResponse(inserted=r["inserted"], duplicates=r["collided"])
    n = repo.enqueue_many(w.queue_table, items)
    return EnqueueResponse(inserted=n, duplicates=len(items) - n)


@router.get("/{slug}/queue", response_model=QueueStats)
def get_queue_stats(slug: str, request: Request) -> QueueStats:
    w = _get_or_404(request, slug)
    by_state = _queue_repo(request).count_by_state(w.queue_table)
    return QueueStats(by_state=by_state, total=sum(by_state.values()))


class QueuePeekResponse(BaseModel):
    items: list[dict[str, Any]]


@router.get("/{slug}/queue/peek", response_model=QueuePeekResponse)
def peek_queue(
    slug: str,
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    state: str | None = Query(None, description="絞り込む state (pending/claimed/failed)"),
) -> QueuePeekResponse:
    """queue の中身を覗く (admin / dispatcher 用)。state 指定で絞り込み可。offset でページネーション可。"""
    w = _get_or_404(request, slug)
    items = _queue_repo(request).peek(w.queue_table, limit=limit, offset=offset)
    if state:
        items = [it for it in items if it.get("state") == state]
    return QueuePeekResponse(items=items)


@router.post(
    "/{slug}/vram_observation",
    response_model=VramObservationResponse,
    status_code=status.HTTP_200_OK,
)
def post_vram_observation(
    slug: str, body: VramObservationRequest, request: Request
) -> VramObservationResponse:
    """worker が自プロセスの VRAM 占有を報告 → workload 行の peak を更新。
    install-multi-worker.sh の自動 N 算定が次回起動時にこの値を読む。
    """
    w = _get_or_404(request, slug)
    updated = _repo(request).record_vram_observation(
        w.slug, body.used_mb, worker_id=body.worker_id,
    )
    if updated is None:
        return VramObservationResponse(accepted=False)
    return VramObservationResponse(
        accepted=True,
        observed_vram_mb_peak=updated.observed_vram_mb_peak,
        observed_vram_sample_count=updated.observed_vram_sample_count,
    )


@router.get("/{slug}/runs", response_model=RunsListResponse)
def list_runs(
    slug: str,
    request: Request,
    limit: int = Query(50, ge=1, le=500),
) -> RunsListResponse:
    _get_or_404(request, slug)  # 404 for unknown slug
    rows = _runs_repo(request).list_for_workload(slug, limit=limit)
    return RunsListResponse(runs=[RunRecord(**r) for r in rows], total=len(rows))
