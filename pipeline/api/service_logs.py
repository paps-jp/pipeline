"""/api/v1/service-logs — daemon stdout を集約 + UI で表示."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from pipeline.repositories.service_logs import ServiceLogsRepository

router = APIRouter(prefix="/api/v1", tags=["service-logs"])


class ServiceLogRecord(BaseModel):
    ts: str = Field(..., description="ISO8601 (millisec)")
    host: str
    service: str
    worker_id: str | None = None
    level: str = "INFO"
    logger: str | None = None
    message: str
    exc_info: str | None = None


class ServiceLogPostBody(BaseModel):
    records: list[ServiceLogRecord]


class ServiceLogListResponse(BaseModel):
    records: list[dict[str, Any]]
    total: int
    max_id: int | None = None


@router.post("/service-logs")
def post_service_logs(body: ServiceLogPostBody, request: Request) -> dict[str, Any]:
    repo = ServiceLogsRepository(request.app.state.db)
    n = repo.insert_many([r.model_dump() for r in body.records])
    return {"inserted": n}


@router.get("/service-logs", response_model=ServiceLogListResponse)
def list_service_logs(
    request: Request,
    limit: int = Query(500, ge=1, le=2000),
    since_id: int | None = Query(None),
    host: str | None = Query(None),
    service: str | None = Query(None),
    worker_id: str | None = Query(None),
    min_level: str | None = Query(None, pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$"),
) -> ServiceLogListResponse:
    repo = ServiceLogsRepository(request.app.state.db)
    rows = repo.list_recent(
        limit=limit,
        since_id=since_id,
        host=host,
        service=service,
        worker_id=worker_id,
        min_level=min_level,
    )
    max_id = max((r["id"] for r in rows), default=None)
    return ServiceLogListResponse(records=rows, total=len(rows), max_id=max_id)
