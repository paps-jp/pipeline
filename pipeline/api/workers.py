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

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel, Field

from pipeline.models.workload import Workload
from pipeline.repositories.queue import QueueRepository
from pipeline.repositories.runs import RunsRepository
from pipeline.repositories.workers import WorkerNotFound, WorkerRepository
from pipeline.repositories.workloads import WorkloadNotFound, WorkloadRepository

router = APIRouter(prefix="/api/v1/workers", tags=["workers"])


# ---------------- fair-share helpers (= KAI 風 weight ベース) -------------
# 直近 N 秒の処理数を workload 別に集計、 10s キャッシュ。 高頻度な workloads
# endpoint 呼び出しに対して runs テーブルを毎回叩かないため。

_FAIR_CACHE: dict[str, Any] = {"ts": 0.0, "counts": {}}
_FAIR_TTL_S = 10.0
_FAIR_WINDOW_S = 300  # 直近 5 分


def _recent_run_counts(db) -> dict[str, int]:
    now = time.monotonic()
    if now - _FAIR_CACHE["ts"] < _FAIR_TTL_S:
        return _FAIR_CACHE["counts"]
    since = (datetime.now(timezone.utc) - timedelta(seconds=_FAIR_WINDOW_S)).isoformat()
    counts: dict[str, int] = {}
    try:
        with db.transaction() as conn:
            cur = conn.execute(
                "SELECT workload_slug, COUNT(*) AS n FROM runs "
                "WHERE started_at > :s GROUP BY workload_slug",
                {"s": since},
            )
            counts = {r["workload_slug"]: int(r["n"]) for r in cur.fetchall()}
    except Exception:
        pass  # fair-share は best-effort、 失敗時は全 0 (= 既存 slug 順)
    _FAIR_CACHE.update({"ts": now, "counts": counts})
    return counts


def _fair_share_key(w: Any, recent_counts: dict[str, int]) -> float:
    """大きいほど優先 (= 高 weight + 直近処理少ない = under-served)。
    weight=1, recent=0  → 1.0
    weight=3, recent=50 → 0.0588
    weight=1, recent=100→ 0.0099
    """
    weight = max(float(w.weight or 1.0), 0.01)
    actual = recent_counts.get(w.slug, 0)
    return weight / (actual + 1)


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
    mem_util_pct: float | None = None
    mem_total_mb: int | None = None
    power_w: float | None = None
    sm_clock_mhz: int | None = None
    mem_clock_mhz: int | None = None


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


class RunStartRequest(BaseModel):
    workload_slug: str
    pk: str
    attempt: int
    started_at: str


class RunFinishRequest(BaseModel):
    success: bool
    exit_code: int | None = None
    duration_ms: int = 0
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
    db = request.app.state.db
    secondary = getattr(request.app.state, "secondary_db", None)
    repo = QueueRepository(db, secondary)
    if secondary is not None:
        # workload.queue_backend='mariadb' の queue を secondary に振り替え
        repo.wire_from_workloads(WorkloadRepository(db).list_all())
    return repo


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
                    "(worker_id, gpu_idx, ts, temp_c, util_pct, mem_used_mb, "
                    " mem_util_pct, mem_total_mb, power_w, sm_clock_mhz, mem_clock_mhz) "
                    "VALUES (:wid, :gi, :ts, :tc, :up, :mu, :mup, :mt, :pw, :sc, :mc)",
                    {"wid": worker_id, "gi": g.gpu_idx, "ts": ts,
                     "tc": g.temp_c, "up": g.util_pct, "mu": g.mem_used_mb,
                     "mup": g.mem_util_pct, "mt": g.mem_total_mb,
                     "pw": g.power_w, "sc": g.sm_clock_mhz, "mc": g.mem_clock_mhz},
                )
    return WorkerInfo(**rec)


@router.get("/metrics", response_model=dict[str, Any])
def list_workers_metrics(request: Request, minutes: int = 30) -> dict[str, Any]:
    """過去 N 分の全 worker の GPU metrics を返す。 UI Dashboard graph 用.
    アクティブ worker (workers テーブルに存在) のみ返す。"""
    from datetime import datetime, timedelta, timezone
    since = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    db = request.app.state.db
    with db.transaction() as conn:
        active_ids = {
            r["id"] for r in conn.execute(
                "SELECT id FROM workers WHERE state = 'active'"
            ).fetchall()
        }
        cur = conn.execute(
            "SELECT worker_id, gpu_idx, ts, temp_c, util_pct, mem_used_mb, "
            "       mem_util_pct, mem_total_mb, power_w, sm_clock_mhz, mem_clock_mhz "
            "FROM worker_metrics WHERE ts >= :since ORDER BY worker_id, gpu_idx, ts",
            {"since": since},
        )
        rows = [dict(r) for r in cur.fetchall()]
    by_worker: dict[str, dict[int, list[dict[str, Any]]]] = {}
    for r in rows:
        if r["worker_id"] not in active_ids:
            continue
        by_worker.setdefault(r["worker_id"], {}).setdefault(r["gpu_idx"], []).append({
            "ts": r["ts"],
            "temp_c": r["temp_c"],
            "util_pct": r["util_pct"],
            "mem_used_mb": r["mem_used_mb"],
            "mem_util_pct": r["mem_util_pct"],
            "mem_total_mb": r["mem_total_mb"],
            "power_w": r["power_w"],
            "sm_clock_mhz": r["sm_clock_mhz"],
            "mem_clock_mhz": r["mem_clock_mhz"],
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

    返却順は **priority 降順 → fair-share key 降順 → slug**:
    - 1st: priority 高 → 低 (= strict 優先度: 100 が先、 50 が後)
    - 2nd: 同 priority 内では fair-share key (= weight / (直近 5min 処理数 + 1)) 降順
      → 高 weight + under-served が先 (= KAI Scheduler 流の weighted fair-share)
      → starvation 防止 + operator が weight で配分比を制御可能
    - 3rd: tie breaker = slug alphabetical
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
    recent_counts = _recent_run_counts(request.app.state.db)
    out.sort(key=lambda w: (
        -int(w.priority or 0),
        -_fair_share_key(w, recent_counts),
        w.slug,
    ))
    return WorkloadsForWorkerResponse(workloads=out)


@router.get("/{worker_id}/higher-pending", response_model=dict[str, Any])
def higher_pending(worker_id: str, than: int, request: Request) -> dict[str, Any]:
    """この worker の host_affinity を踏まえ、 priority > `than` の enabled workload
    のうち pending タスクがあるものを返す (Lv2 preemption 用)。
    worker は batch 完了後にこれを叩き、 True なら現 workload を抜け次 _drain_once で
    最高 priority から再開する。
    """
    worker = _get_worker_or_404(request, worker_id)
    worker_host = worker.get("host") if isinstance(worker, dict) else getattr(worker, "host", None)
    qrepo = _qrepo(request)
    higher_slugs: list[str] = []
    for w in _wlrepo(request).list_all():
        if not w.enabled:
            continue
        if int(w.priority or 0) <= than:
            continue
        affinity = list(w.host_affinity or [])
        if affinity and worker_host not in affinity:
            continue
        # 安全な queue_table のみ count、 失敗時 (= 表未作成等) は skip
        try:
            counts = qrepo.count_by_state(w.queue_table)
        except Exception:
            continue
        if int(counts.get("pending", 0)) > 0:
            higher_slugs.append(w.slug)
            if len(higher_slugs) >= 5:  # = 早期 break、 高 priority N 件あれば十分
                break
    return {"has_pending": bool(higher_slugs), "slugs": higher_slugs, "than": than}


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


@router.post("/{worker_id}/runs/start", status_code=status.HTTP_201_CREATED)
def start_run(worker_id: str, body: RunStartRequest, request: Request) -> dict[str, str]:
    _get_worker_or_404(request, worker_id)
    rid = _rrepo(request).start(
        workload_slug=body.workload_slug,
        pk=body.pk,
        worker_id=worker_id,
        attempt=body.attempt,
        started_at=body.started_at,
    )
    return {"id": rid}


@router.post("/{worker_id}/runs/{run_id}/finish", status_code=status.HTTP_204_NO_CONTENT)
def finish_run(worker_id: str, run_id: str, body: RunFinishRequest, request: Request) -> None:
    _get_worker_or_404(request, worker_id)
    _rrepo(request).finish(
        run_id,
        success=body.success,
        exit_code=body.exit_code,
        duration_ms=body.duration_ms,
        stdout=body.stdout,
        stderr=body.stderr,
        output_json=body.output_json,
        error=body.error,
    )


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
