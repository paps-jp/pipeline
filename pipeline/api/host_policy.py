"""/api/v1/host-policy — ホスト単位の能力/配置ポリシー。

異機種フリート (10GB 民生 3080 と 16GB Blackwell 混在) を supervisor が正しく扱うための
ホスト設定。 2 面を merge して返す:
  - auto 面   : agent が nvidia-smi で自動検出 (agent_desired.last_vram_total_mb / last_gpu_model)
  - manual 面 : operator が UI で上書き (host_policy.tier / max_gpu_workers / vram_override_mb)
effective = manual があれば manual、 無ければ auto。

- GET  /api/v1/host-policy            → 全ホスト (auto ∪ manual, effective 込み)
- PUT  /api/v1/host-policy/{host}     → 手動ポリシーを部分更新 (upsert)
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from pipeline.repositories.host_policy import HostPolicyRepository

router = APIRouter(prefix="/api/v1", tags=["host-policy"])

# VRAM からの tier 自動推定しきい値 (手動 tier 未設定時)。
_TIER_16G_MIN_MB = 14000


def _hrepo(request: Request) -> HostPolicyRepository:
    return HostPolicyRepository(request.app.state.db)


def _infer_tier(vram_mb: int | None) -> str | None:
    if not vram_mb:
        return None
    return "gpu-16g-pro" if vram_mb >= _TIER_16G_MIN_MB else "gpu-10g-consumer"


def _fetch_agent_auto(request: Request) -> dict[str, dict[str, Any]]:
    """agent_desired から host→{vram_total, gpu_model, gpu_children_alive} を取得 (auto 面)。"""
    db = request.app.state.db
    out: dict[str, dict[str, Any]] = {}
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT host, last_vram_total_mb, last_vram_free_mb, last_gpu_model, "
            "last_disk_total_mb, last_disk_free_mb, "
            "last_children_json, last_seen_at FROM agent_desired"
        )
        for r in cur.fetchall():
            children = []
            try:
                children = json.loads(r["last_children_json"]) if r["last_children_json"] else []
            except Exception:
                children = []
            gpu_alive = sum(1 for c in children
                            if isinstance(c, dict) and c.get("gpu") and c.get("alive"))
            dt, df = r["last_disk_total_mb"], r["last_disk_free_mb"]
            out[r["host"]] = {
                "vram_total_mb": r["last_vram_total_mb"],
                "vram_free_mb": r["last_vram_free_mb"],
                "gpu_model": r["last_gpu_model"],
                "gpu_children_alive": gpu_alive,
                "disk_total_mb": dt,
                "disk_free_mb": df,
                # 使用率は「非特権が使える空き」基準。 root 予約 5% を空きに数えないので
                # 100% に届く前に警告が出る (= ENOENT で全 workload が死ぬ手前で気付ける)。
                "disk_used_pct": (round(100.0 * (dt - df) / dt, 1)
                                  if dt and df is not None and dt > 0 else None),
                "last_seen_at": r["last_seen_at"],
            }
    return out


class HostView(BaseModel):
    host: str
    # auto 面
    gpu_model: str | None = None
    vram_total_mb: int | None = None
    vram_free_mb: int | None = None
    gpu_children_alive: int | None = None
    disk_total_mb: int | None = None
    disk_free_mb: int | None = None
    disk_used_pct: float | None = None
    last_seen_at: str | None = None
    # manual 面
    tier: str | None = None
    max_gpu_workers: int | None = None
    vram_override_mb: int | None = None
    labels: list[str] = []
    notes: str | None = None
    enabled: int = 1
    updated_at: str | None = None
    updated_by: str | None = None
    # effective (merge 結果)
    tier_effective: str | None = None
    vram_effective_mb: int | None = None


class HostPolicyListResponse(BaseModel):
    hosts: list[HostView]


class HostPolicyUpdate(BaseModel):
    tier: str | None = None
    max_gpu_workers: int | None = None
    vram_override_mb: int | None = None
    labels: list[str] | None = None
    notes: str | None = None
    enabled: bool | None = None
    updated_by: str | None = None


def _merge(host: str, auto: dict[str, Any], pol: dict[str, Any] | None) -> HostView:
    pol = pol or {}
    vram_eff = pol.get("vram_override_mb") or auto.get("vram_total_mb")
    tier_eff = pol.get("tier") or _infer_tier(vram_eff)
    return HostView(
        host=host,
        gpu_model=auto.get("gpu_model"),
        vram_total_mb=auto.get("vram_total_mb"),
        vram_free_mb=auto.get("vram_free_mb"),
        gpu_children_alive=auto.get("gpu_children_alive"),
        disk_total_mb=auto.get("disk_total_mb"),
        disk_free_mb=auto.get("disk_free_mb"),
        disk_used_pct=auto.get("disk_used_pct"),
        last_seen_at=auto.get("last_seen_at"),
        tier=pol.get("tier"),
        max_gpu_workers=pol.get("max_gpu_workers"),
        vram_override_mb=pol.get("vram_override_mb"),
        labels=pol.get("labels") or [],
        notes=pol.get("notes"),
        enabled=int(pol.get("enabled", 1)),
        updated_at=pol.get("updated_at"),
        updated_by=pol.get("updated_by"),
        tier_effective=tier_eff,
        vram_effective_mb=vram_eff,
    )


@router.get("/host-policy", response_model=HostPolicyListResponse)
def list_host_policy(request: Request) -> HostPolicyListResponse:
    repo = _hrepo(request)
    auto = _fetch_agent_auto(request)
    pols = {p["host"]: p for p in repo.list()}
    hosts = sorted(set(auto) | set(pols))
    return HostPolicyListResponse(
        hosts=[_merge(h, auto.get(h, {}), pols.get(h)) for h in hosts]
    )


@router.put("/host-policy/{host}", response_model=HostView)
def update_host_policy(host: str, payload: HostPolicyUpdate, request: Request) -> HostView:
    repo = _hrepo(request)
    fields = payload.model_dump(exclude_unset=True, exclude={"updated_by"})
    repo.upsert(host, fields, updated_by=payload.updated_by or "operator")
    auto = _fetch_agent_auto(request)
    return _merge(host, auto.get(host, {}), repo.get(host))
