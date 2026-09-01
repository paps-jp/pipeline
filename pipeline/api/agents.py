"""pipeline-agent 制御 API (P2)。

- POST /api/v1/agents/{host}/sync : agent が状態(VRAM/子)を報告し、 desired を受領。
- PUT  /api/v1/agents/{host}/desired : operator/supervisor が desired を設定。
- GET  /api/v1/agents : agent 一覧 (desired + 最新報告状態)。
- POST /api/v1/agents/{host}/restart : agent を systemctl restart (watchdog 用)。

これにより agent はローカル desired.json でなく control plane から desired を取得でき、
supervisor が VRAM 安全な desired を中央算定・配信して agent ホストを制御できる。
"""
from __future__ import annotations

import logging
import os
import subprocess
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from pipeline.repositories.agents import AgentRepository

log = logging.getLogger(__name__)

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


# ---------------------------------------------------------------------------
# agent restart (host 単位の復旧プリミティブ)
# ---------------------------------------------------------------------------
# supervisor は非特権ユーザで動き root@他ホストへ SSH できないので、 特権操作は
# deploy key を持つ control plane に集約する (= workers.restart_worker と同型)。
#
# **worker 単位の restart とは別物**であることが要点。 agent ホストの子 worker は
# systemd unit を持たない (`pipeline-worker-gpu` は not-found) ので、 個別 worker の
# restart はそもそも成立しない。 ホストごと agent を再起動すると全子が作り直され、
# 「ホストは生きているのに仕事が一切完了しない」 型の故障から自己回復できる
# (2026-09-02 ai-gpu3: / 満杯で tempfile が ENOENT → 全 workload が 1 週間無音停止)。
#
# 接続先は workers.py と同じ PIPELINE_WATCHDOG_HOSTS / PIPELINE_DEPLOY_KEY を使う。


def _agent_ssh_target(host: str) -> str | None:
    """host → ssh 先 IP。 PIPELINE_WATCHDOG_HOSTS 未登録なら None (= restart 不可)。"""
    from pipeline.api.workers import _WATCHDOG_HOST_IP
    return _WATCHDOG_HOST_IP.get(host)


@router.post("/{host}/restart")
def restart_agent(host: str, req: Request) -> dict[str, Any]:
    """agent ホストの pipeline-agent.service を SSH で restart する。

    agent が全子を作り直すので、 個別 worker では抜けられない状態 (ローカル FS 破損・
    CUDA context 汚染・plugin の恒久例外ループ) から復帰できる。 呼び出し側は
    ホスト全体が停止する前提でクールダウンを持つこと。
    """
    repo = AgentRepository(req.app.state.db)
    if repo.get(host) is None:
        raise HTTPException(status_code=404, detail=f"agent 未登録の host: {host}")
    ip = _agent_ssh_target(host)
    if ip is None:
        raise HTTPException(
            status_code=400,
            detail=f"PIPELINE_WATCHDOG_HOSTS に {host} が無い (= restart 経路が未設定)")
    key = os.environ.get("PIPELINE_DEPLOY_KEY",
                         os.path.expanduser("~/.ssh/id_ed25519"))
    cmd = ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
           "-o", "ConnectTimeout=10", f"root@{ip}",
           "systemctl restart pipeline-agent.service"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ssh failed: {e}")
    log.warning("watchdog restart agent host=%s ip=%s rc=%s", host, ip, r.returncode)
    return {"host": host, "ip": ip, "ok": r.returncode == 0,
            "rc": r.returncode, "stderr": (r.stderr or "")[:300]}
