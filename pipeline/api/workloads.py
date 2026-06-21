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


def _repo(request: Request) -> WorkloadRepository:
    return WorkloadRepository(request.app.state.db)


def _queue_repo(request: Request) -> QueueRepository:
    return QueueRepository(request.app.state.db)


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
def enqueue_batch(slug: str, body: EnqueueBatchRequest, request: Request) -> EnqueueResponse:
    w = _get_or_404(request, slug)
    items = [(it.pk, it.extra) for it in body.items]
    n = _queue_repo(request).enqueue_many(w.queue_table, items)
    return EnqueueResponse(inserted=n, duplicates=len(items) - n)


@router.get("/{slug}/queue", response_model=QueueStats)
def get_queue_stats(slug: str, request: Request) -> QueueStats:
    w = _get_or_404(request, slug)
    by_state = _queue_repo(request).count_by_state(w.queue_table)
    return QueueStats(by_state=by_state, total=sum(by_state.values()))


@router.get("/{slug}/runs", response_model=RunsListResponse)
def list_runs(
    slug: str,
    request: Request,
    limit: int = Query(50, ge=1, le=500),
) -> RunsListResponse:
    _get_or_404(request, slug)  # 404 for unknown slug
    rows = _runs_repo(request).list_for_workload(slug, limit=limit)
    return RunsListResponse(runs=[RunRecord(**r) for r in rows], total=len(rows))
