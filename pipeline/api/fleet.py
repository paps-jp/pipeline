"""/api/v1/fleet — パイプライン全体の一括停止 / 再開。

    POST /api/v1/fleet/stop     稼働中の workload を記録してから全部止める
    POST /api/v1/fleet/resume   記録にあるものだけ動かす
    GET  /api/v1/fleet/state    いま停止中か / 何を記録しているか
    GET  /api/v1/fleet/history  過去の停止・再開履歴

--- なぜ「記録してから止める」のか ---
単に全 workload を enabled=0 にすると、 再開時に 「もともと運用の都合で止めていた
もの」 まで一緒に動き出してしまう。 停止時点で enabled=1 だったものだけを記録し、
再開はその集合に限定する。

--- なぜ supervisor を特別扱いするのか ---
supervisor は patch_workload で他 workload の enabled を書き換える。 これを先に
止めないと、 こちらが止めている端から復活させられて停止が完了しない。 逆に再開時は
最後に戻す (= フリートが立ち上がりきる前に supervisor が再配置を始めると
placement churn を起こすため)。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel

from pipeline.repositories.fleet_stop import FleetStopRepository
from pipeline.repositories.workloads import WorkloadNotFound, WorkloadRepository

router = APIRouter(prefix="/api/v1/fleet", tags=["fleet"])

# 他 workload の enabled を書き換える側。 停止は最初、 再開は最後。
SUPERVISOR_SLUG = "pipeline-supervisor"


def _wrepo(request: Request) -> WorkloadRepository:
    return WorkloadRepository(request.app.state.db)


def _srepo(request: Request) -> FleetStopRepository:
    return FleetStopRepository(request.app.state.db)


class FleetActionRequest(BaseModel):
    by: str | None = None


class FleetStateResponse(BaseModel):
    stopped: bool
    stopped_at: str | None = None
    stopped_by: str | None = None
    recorded_slugs: list[str] = []
    enabled_now: list[str] = []


class FleetActionResponse(BaseModel):
    ok: bool
    stopped: bool
    changed: list[str] = []
    failed: list[dict[str, Any]] = []
    recorded_slugs: list[str] = []
    message: str = ""


def _ordered_for_stop(slugs: list[str]) -> list[str]:
    """supervisor を先頭に。 残りは名前順 (= 再現性のため)。"""
    rest = sorted(s for s in slugs if s != SUPERVISOR_SLUG)
    return ([SUPERVISOR_SLUG] if SUPERVISOR_SLUG in slugs else []) + rest


def _ordered_for_resume(slugs: list[str]) -> list[str]:
    """supervisor を最後に。"""
    rest = sorted(s for s in slugs if s != SUPERVISOR_SLUG)
    return rest + ([SUPERVISOR_SLUG] if SUPERVISOR_SLUG in slugs else [])


def _set_enabled(repo: WorkloadRepository, slugs: list[str], enabled: bool
                 ) -> tuple[list[str], list[dict[str, Any]]]:
    changed: list[str] = []
    failed: list[dict[str, Any]] = []
    for slug in slugs:
        try:
            repo.set_enabled(slug, enabled)
            changed.append(slug)
        except WorkloadNotFound:
            # 停止中に workload が消された等。 再開を止める理由にはしない。
            failed.append({"slug": slug, "error": "not found"})
        except Exception as e:  # noqa: BLE001
            failed.append({"slug": slug, "error": str(e)[:200]})
    return changed, failed


@router.get("/state", response_model=FleetStateResponse)
def fleet_state(request: Request) -> FleetStateResponse:
    active = _srepo(request).active()
    enabled_now = sorted(w.slug for w in _wrepo(request).list_all() if w.enabled)
    if active is None:
        return FleetStateResponse(stopped=False, enabled_now=enabled_now)
    return FleetStateResponse(
        stopped=True,
        stopped_at=active.get("stopped_at"),
        stopped_by=active.get("stopped_by"),
        recorded_slugs=active.get("slugs") or [],
        enabled_now=enabled_now,
    )


@router.post("/stop", response_model=FleetActionResponse)
def fleet_stop(request: Request, body: FleetActionRequest | None = None
               ) -> FleetActionResponse:
    """稼働中の workload を記録してから全部止める。"""
    srepo = _srepo(request)
    if srepo.active() is not None:
        # 二度押しで記録を空リストに上書きしてしまうと再開できなくなる。
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="already stopped — resume してから再度停止してください",
        )
    wrepo = _wrepo(request)
    enabled = [w.slug for w in wrepo.list_all() if w.enabled]

    rec = srepo.begin(enabled, (body.by if body else None))
    changed, failed = _set_enabled(wrepo, _ordered_for_stop(enabled), False)
    return FleetActionResponse(
        ok=not failed,
        stopped=True,
        changed=changed,
        failed=failed,
        recorded_slugs=rec["slugs"],
        message=f"{len(changed)} 件を停止し記録しました",
    )


@router.post("/resume", response_model=FleetActionResponse)
def fleet_resume(request: Request, body: FleetActionRequest | None = None
                 ) -> FleetActionResponse:
    """記録にある workload だけを動かす。"""
    srepo = _srepo(request)
    active = srepo.active()
    if active is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="停止記録がありません (= 全停止していない)",
        )
    slugs = active.get("slugs") or []
    changed, failed = _set_enabled(_wrepo(request), _ordered_for_resume(slugs), True)
    srepo.finish(int(active["id"]), (body.by if body else None))
    return FleetActionResponse(
        ok=not failed,
        stopped=False,
        changed=changed,
        failed=failed,
        recorded_slugs=slugs,
        message=f"{len(changed)} 件を再開しました",
    )


@router.get("/history")
def fleet_history(request: Request, limit: int = Query(20, ge=1, le=200)
                  ) -> dict[str, Any]:
    return {"history": _srepo(request).history(limit)}
