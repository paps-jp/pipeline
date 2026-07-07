"""pipeline-agent 制御 API (P2)。

- POST /api/v1/agents/{host}/sync : agent が状態(VRAM/子)を報告し、 desired を受領。
- PUT  /api/v1/agents/{host}/desired : operator/supervisor が desired を設定。
- GET  /api/v1/agents : agent 一覧 (desired + 最新報告状態)。

これにより agent はローカル desired.json でなく control plane から desired を取得でき、
supervisor が VRAM 安全な desired を中央算定・配信して agent ホストを制御できる。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel

from pipeline.repositories.agents import AgentRepository

router = APIRouter(prefix="/api/v1/agents", tags=["agents"])


class AgentChild(BaseModel):
    child_id: str
    workload: str
    gpu: bool = False
    alive: bool = True


class AgentSyncBody(BaseModel):
    vram_total_mb: int | None = None
    vram_free_mb: int | None = None
    children: list[AgentChild] = []


class AgentDesiredBody(BaseModel):
    # {slug: {"count": int, "gpu": bool, "vram_mb": int}}
    workloads: dict[str, Any]


@router.post("/{host}/sync")
def agent_sync(host: str, body: AgentSyncBody, req: Request) -> dict[str, Any]:
    """agent が状態を報告し、 現在の desired を受け取る (未設定なら desired=null)。"""
    repo = AgentRepository(req.app.state.db)
    desired = repo.sync(
        host,
        vram_total_mb=body.vram_total_mb,
        vram_free_mb=body.vram_free_mb,
        children=[c.model_dump() for c in body.children],
    )
    return {"host": host, "desired": desired}


@router.put("/{host}/desired")
def set_agent_desired(host: str, body: AgentDesiredBody, req: Request) -> dict[str, Any]:
    """operator が上限テンプレ(max intent)を設定。 effective も初期値として同値で埋める
    (supervisor planner が VRAM から算定して effective を後で上書きする)。"""
    repo = AgentRepository(req.app.state.db)
    repo.set_template(host, {"workloads": body.workloads}, by="operator")
    return {"host": host, "ok": True, "template": body.workloads}


@router.put("/{host}/effective")
def set_agent_effective(host: str, body: AgentDesiredBody, req: Request) -> dict[str, Any]:
    """supervisor planner が VRAM から算定した effective desired を設定 (template は不変)。"""
    repo = AgentRepository(req.app.state.db)
    repo.set_effective(host, {"workloads": body.workloads}, by="supervisor")
    return {"host": host, "ok": True, "effective": body.workloads}


@router.get("")
def list_agents(req: Request) -> dict[str, Any]:
    repo = AgentRepository(req.app.state.db)
    return {"agents": repo.list_all()}
