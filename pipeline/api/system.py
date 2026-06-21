"""/api/v1/{status,health,runs} — システム全体の状態 + 横断 runs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from pipeline import __version__
from pipeline.repositories.runs import RunsRepository

router = APIRouter(prefix="/api/v1", tags=["system"])


class HealthResponse(BaseModel):
    ok: bool
    version: str


class StatusResponse(BaseModel):
    version: str
    mode: str
    db_url: str
    now: datetime


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


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """軽量ヘルスチェック。LB / k8s 用。"""
    return HealthResponse(ok=True, version=__version__)


@router.get("/status", response_model=StatusResponse)
def status(request: Request) -> StatusResponse:
    """ダッシュボード用の全体状態。"""
    settings = request.app.state.settings
    return StatusResponse(
        version=__version__,
        mode=settings.mode,
        db_url=settings.db_url,
        now=datetime.now(timezone.utc),
    )


@router.get("/runs", response_model=RunsListResponse)
def list_recent_runs(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
) -> RunsListResponse:
    """全 workload 横断で最新 runs を返す (UI のサービスログ用)。"""
    rows = RunsRepository(request.app.state.db).list_recent(limit=limit)
    return RunsListResponse(runs=[RunRecord(**r) for r in rows], total=len(rows))
