"""/api/v1/workers/{id}/admin-cmd — daemon の long poll 経路 + 結果報告.

仕組み:
  - daemon が `GET /api/v1/workers/<id>/admin-cmd?host=<host>` を long-poll
  - control plane に pending cmd があれば即返す、 なければ最大 25 秒待ってから 204 を返す
  - daemon が実行 → `POST .../admin-cmd/<cmd_id>/complete` で結果報告

cmd_type:
  - "exec_shell": payload={"script": "..."} を bash で実行
  - "fetch_archive": payload={"url": ..., "dst": "..."} で control plane から tar.gz を取得して dst に展開
  - "install_systemd": payload={"unit_name": "...", "content": "..."} で /etc/systemd/system/<unit_name> を配置 + reload + restart
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from pipeline.repositories.worker_admin_cmds import WorkerAdminCmdsRepository

router = APIRouter(prefix="/api/v1/workers", tags=["admin-cmds"])
admin_router = APIRouter(prefix="/api/v1/admin", tags=["admin-cmds"])  # enqueue 用
log = logging.getLogger("pipeline.api.admin_cmds")

LONG_POLL_TIMEOUT_S = 25.0
POLL_INTERVAL_S = 1.0


class AdminCmd(BaseModel):
    id: int
    target_host: str
    cmd_type: str
    cmd_payload: dict[str, Any]


class CompleteBody(BaseModel):
    success: bool
    exit_code: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    error: str | None = None


class EnqueueBody(BaseModel):
    target_host: str
    cmd_type: str
    cmd_payload: dict[str, Any]
    ttl_secs: int = 600


@router.get("/{worker_id}/admin-cmd")
async def poll_admin_cmd(
    worker_id: str,
    request: Request,
    host: str = Query(..., description="自分の host (= deploy_targets.host)"),
):
    """long poll: pending cmd があれば即返す、 なければ最大 25 秒待つ."""
    repo = WorkerAdminCmdsRepository(request.app.state.db)
    deadline = asyncio.get_event_loop().time() + LONG_POLL_TIMEOUT_S
    while True:
        cmd = repo.claim_next(host, worker_id)
        if cmd is not None:
            return AdminCmd(**{k: cmd[k] for k in ("id", "target_host", "cmd_type", "cmd_payload")})
        if asyncio.get_event_loop().time() >= deadline:
            return Response(status_code=204)
        await asyncio.sleep(POLL_INTERVAL_S)


@router.post("/{worker_id}/admin-cmd/{cmd_id}/complete")
def complete_admin_cmd(worker_id: str, cmd_id: int, body: CompleteBody, request: Request):
    repo = WorkerAdminCmdsRepository(request.app.state.db)
    cmd = repo.get(cmd_id)
    if cmd is None:
        raise HTTPException(404, detail=f"cmd {cmd_id} not found")
    repo.complete(cmd_id, success=body.success, exit_code=body.exit_code,
                  stdout=body.stdout, stderr=body.stderr, error=body.error)
    return {"ok": True}


# ========== 管理画面用: enqueue + history ==========

@admin_router.post("/admin-cmds", response_model=dict)
def enqueue_admin_cmd(body: EnqueueBody, request: Request) -> dict:
    """admin cmd を enqueue (= 任意 host への一括指示も target_host='*' で可)."""
    repo = WorkerAdminCmdsRepository(request.app.state.db)
    cid = repo.enqueue(target_host=body.target_host, cmd_type=body.cmd_type,
                       cmd_payload=body.cmd_payload, ttl_secs=body.ttl_secs)
    return {"id": cid, "target_host": body.target_host, "cmd_type": body.cmd_type}


@admin_router.get("/admin-cmds")
def list_admin_cmds(request: Request, target_host: str | None = None, limit: int = 50) -> list[dict]:
    repo = WorkerAdminCmdsRepository(request.app.state.db)
    return repo.list_recent(target_host=target_host, limit=limit)


@admin_router.get("/admin-cmds/{cmd_id}")
def get_admin_cmd(cmd_id: int, request: Request) -> dict:
    repo = WorkerAdminCmdsRepository(request.app.state.db)
    cmd = repo.get(cmd_id)
    if cmd is None:
        raise HTTPException(404, detail=f"cmd {cmd_id} not found")
    return cmd
