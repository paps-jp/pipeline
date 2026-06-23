"""/api/v1/dashboard — 「今何が動いてるか」ホーム panel 用集約 endpoint。

複数 repo を 1 リクエストで集約する事で、
UI から N+1 (workload ごとに /queue を叩く) を避ける。
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from pipeline.repositories.queue import QueueRepository, _validate_queue_table
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workloads import WorkloadRepository

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


class RunningRun(BaseModel):
    id: str
    workload_slug: str
    pk: str
    worker_id: str
    attempt: int
    started_at: str


class RecentFailure(BaseModel):
    id: str
    workload_slug: str
    pk: str
    worker_id: str
    started_at: str
    reason: str | None


class QueueDepth(BaseModel):
    workload_slug: str
    by_state: dict[str, int]
    total: int


class WorkloadRunsSummary(BaseModel):
    """各 workload の直近 N 件 run の成否 (sparkline 用)。"""

    workload_slug: str
    bits: list[int]  # 1=success, 0=failure, -1=unknown (新しい順)
    success_rate: float | None  # 0.0–1.0、unknown を除いた割合


class OverviewResponse(BaseModel):
    running: list[RunningRun]
    recent_failures: list[RecentFailure]
    queue_depths: list[QueueDepth]


def _short_reason(error: str | None, stderr: str | None) -> str | None:
    """RunsDrawer の shortReason と同等。"""
    import re

    for text in (error, stderr):
        if not text:
            continue
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        for ln in reversed(lines):
            m = re.match(r"^([A-Z][A-Za-z0-9_.]*(?:Error|Exception|Failed|Failure)):?\s*(.*)$", ln)
            if m:
                return f"{m.group(1)}: {m.group(2)}".rstrip(": ").strip()
        if lines:
            return lines[-1][:200]
    return None


@router.get("/overview", response_model=OverviewResponse)
def overview(request: Request) -> OverviewResponse:
    """Dashboard 用横断集約。"""
    db = request.app.state.db
    runs_repo = RunsRepository(db)
    workloads_repo = WorkloadRepository(db)
    queue_repo = QueueRepository(db)

    # 1. running は recent から、 failures は別 query (= 高スループット workload で
    #    recent window が成功 run で埋まり failure が見えなくなるのを防ぐ)
    recent = runs_repo.list_recent(limit=300)

    running = [
        RunningRun(
            id=r["id"],
            workload_slug=r["workload_slug"],
            pk=r["pk"],
            worker_id=r["worker_id"] or "",
            attempt=r["attempt"],
            started_at=r["started_at"],
        )
        for r in recent
        if r.get("finished_at") is None
    ]

    failures = [
        RecentFailure(
            id=r["id"],
            workload_slug=r["workload_slug"],
            pk=r["pk"],
            worker_id=r["worker_id"] or "",
            started_at=r["started_at"],
            reason=_short_reason(r.get("error"), r.get("stderr")),
        )
        for r in runs_repo.list_recent_failures(limit=10)
    ]

    # 2. 各 workload の queue depth (enabled 限定)
    depths: list[QueueDepth] = []
    for w in workloads_repo.list_all():
        if not w.enabled:
            continue
        try:
            _validate_queue_table(w.queue_table)
            by_state = queue_repo.count_by_state(w.queue_table)
        except Exception:
            by_state = {}
        depths.append(
            QueueDepth(
                workload_slug=w.slug,
                by_state=by_state,
                total=sum(by_state.values()),
            )
        )

    return OverviewResponse(
        running=running,
        recent_failures=failures,
        queue_depths=depths,
    )


@router.get("/workloads-runs-summary", response_model=list[WorkloadRunsSummary])
def workloads_runs_summary(request: Request) -> list[WorkloadRunsSummary]:
    """各 workload の直近 20 件 run 成否 (sparkline)。 新しい順。"""
    db = request.app.state.db
    runs_repo = RunsRepository(db)
    workloads_repo = WorkloadRepository(db)

    out: list[WorkloadRunsSummary] = []
    for w in workloads_repo.list_all():
        rows = runs_repo.list_for_workload(w.slug, limit=20)
        bits: list[int] = []
        for r in rows:
            s = r.get("success")
            if s is True:
                bits.append(1)
            elif s is False:
                bits.append(0)
            else:
                bits.append(-1)
        known = [b for b in bits if b >= 0]
        rate = (sum(known) / len(known)) if known else None
        out.append(
            WorkloadRunsSummary(
                workload_slug=w.slug,
                bits=bits,
                success_rate=rate,
            )
        )
    return out
