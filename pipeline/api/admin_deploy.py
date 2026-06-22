"""/api/v1/admin/deploy + /deploy-targets — GPU 箱への code 配信 + 配信先管理."""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from pipeline.repositories.deploy_paths import DeployPathsRepository
from pipeline.repositories.deploy_targets import DeployTargetsRepository
from pipeline.repositories.worker_admin_cmds import WorkerAdminCmdsRepository

router = APIRouter(prefix="/api/v1/admin", tags=["admin-deploy"])
bootstrap_router = APIRouter(tags=["bootstrap"])  # root-level (= /bootstrap.sh)
log = logging.getLogger("pipeline.api.admin_deploy")

DEPLOY_SCRIPT = str(Path(__file__).resolve().parents[2] / "scripts" / "deploy-to-gpu.sh")
LOG_HISTORY_MAX = 20  # 直近 20 回ぶん保持

# プロセス内のシンプルな履歴 (= 永続化はしない、 restart で消える)
_history: deque[dict[str, Any]] = deque(maxlen=LOG_HISTORY_MAX)
_current_run: dict[str, Any] | None = None  # 走行中の run (= 同時 deploy ガード用)
_lock = asyncio.Lock()


class DeployTriggerBody(BaseModel):
    hosts: list[str] | None = Field(None, description="配信先 host 上書き (default: deploy_targets.enabled=1)")
    skip_restart: bool = Field(False, description="rsync のみで restart しない")
    dry_run: bool = Field(False, description="rsync --dry-run")
    via_daemon: bool = Field(False, description="ssh の代わりに daemon admin cmd 経由で実行 (= 実験中、 Phase C)")


class DeployTarget(BaseModel):
    id: int
    label: str
    host: str
    ssh_user: str = "root"
    ssh_port: int = 22
    enabled: bool = True
    notes: str | None = None
    last_deploy_at: str | None = None
    last_deploy_ok: bool | None = None
    created_at: str | None = None
    updated_at: str | None = None


class DeployTargetCreate(BaseModel):
    label: str = Field(..., min_length=1, max_length=64)
    host: str = Field(..., min_length=1, max_length=128)
    ssh_user: str = "root"
    ssh_port: int = Field(22, ge=1, le=65535)
    enabled: bool = True
    notes: str | None = None


class DeployTargetUpdate(BaseModel):
    label: str | None = None
    host: str | None = None
    ssh_user: str | None = None
    ssh_port: int | None = Field(None, ge=1, le=65535)
    enabled: bool | None = None
    notes: str | None = None


class DeployRun(BaseModel):
    id: str
    started_at: str
    finished_at: str | None = None
    duration_s: float | None = None
    success: bool | None = None
    exit_code: int | None = None
    log: str = ""
    hosts: list[str]
    skip_restart: bool
    dry_run: bool


def _now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def _run_deploy(run_id: str, hosts: list[str], skip_restart: bool, dry_run: bool,
                      paths_json: str | None = None) -> None:
    global _current_run
    env = os.environ.copy()
    if hosts:
        env["GPU_HOSTS"] = " ".join(hosts)
    if paths_json is not None:
        env["PATHS_JSON"] = paths_json
    if skip_restart:
        env["SKIP_RESTART"] = "1"
    if dry_run:
        env["DRY_RUN"] = "1"

    started = time.time()
    record: dict[str, Any] = {
        "id": run_id, "started_at": _now(), "finished_at": None, "duration_s": None,
        "success": None, "exit_code": None, "log": "",
        "hosts": hosts, "skip_restart": skip_restart, "dry_run": dry_run,
    }
    _current_run = record
    try:
        proc = await asyncio.create_subprocess_exec(
            "/bin/bash", DEPLOY_SCRIPT,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        # stdout を逐次 readline で record["log"] に append (= UI polling でリアルタイム反映)
        log_buf: list[str] = []
        async def _stream_log():
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                s = line.decode("utf-8", errors="replace")
                log_buf.append(s)
                record["log"] = "".join(log_buf)
        try:
            await asyncio.wait_for(
                asyncio.gather(_stream_log(), proc.wait()),
                timeout=900,
            )
            record["exit_code"] = proc.returncode
            record["success"] = proc.returncode == 0
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            log_buf.append("\n[ERROR] deploy timeout (15 min)\n")
            record["log"] = "".join(log_buf)
            record["exit_code"] = -1
            record["success"] = False
    except Exception as e:
        record["log"] = f"deploy 起動失敗: {e}"
        record["exit_code"] = -2
        record["success"] = False
    finally:
        record["finished_at"] = _now()
        record["duration_s"] = round(time.time() - started, 2)
        _history.appendleft(dict(record))
        _current_run = None


async def _run_deploy_via_daemon(run_id: str, hosts: list[str], skip_restart: bool,
                                  dry_run: bool, db) -> None:
    """ssh の代わりに daemon admin cmd queue 経由で配信。

    各 host × 各 path で:
      1. fetch_archive cmd: control plane の /api/v1/admin/deploy-paths/<id>/archive を GET → dst 展開
      2. exec_shell cmd (setup_command)
      3. install_systemd cmd (service_command)
    各 cmd は daemon が long poll で受信 → 実行 → 結果報告 → control plane が UI に反映。
    """
    global _current_run
    import time as _time
    started = _time.time()
    record: dict[str, Any] = {
        "id": run_id, "started_at": _now(), "finished_at": None, "duration_s": None,
        "success": None, "exit_code": None, "log": "",
        "hosts": hosts, "skip_restart": skip_restart, "dry_run": dry_run,
    }
    _current_run = record
    log_lines: list[str] = []
    def _log(msg: str):
        log_lines.append(f"[{_now()}] {msg}")
        record["log"] = "\n".join(log_lines)

    try:
        paths_repo = DeployPathsRepository(db)
        cmds_repo = WorkerAdminCmdsRepository(db)
        paths = paths_repo.list_all(enabled_only=True)
        _log(f"=== via_daemon deploy: hosts={len(hosts)}, paths={len(paths)} ===")
        if dry_run:
            _log("(dry_run=true) — admin cmd を enqueue しないでスキップ")
            record["success"] = True
            record["exit_code"] = 0
            return

        enqueued: list[int] = []
        for host in hosts:
            for p in paths:
                # 1. fetch_archive (= ファイルセット)
                cid = cmds_repo.enqueue(
                    target_host=host, cmd_type="fetch_archive",
                    cmd_payload={
                        "url": f"/api/v1/admin/deploy-paths/{p['id']}/archive",
                        "dst": p["dst_path"],
                    },
                    ttl_secs=600,
                )
                enqueued.append(cid)
                _log(f"  enq cmd#{cid}: fetch p{p['id']} {p['label']} → {host}:{p['dst_path']}")
                # 2. setup_command
                if p.get("setup_command"):
                    cid2 = cmds_repo.enqueue(
                        target_host=host, cmd_type="exec_shell",
                        cmd_payload={"script": p["setup_command"], "cwd": p["dst_path"]},
                        ttl_secs=600,
                    )
                    enqueued.append(cid2)
                    _log(f"  enq cmd#{cid2}: setup → {host}")
                # 3. service_command (= systemd unit)
                if p.get("service_command"):
                    label_safe = "".join(c if c.isalnum() else "_" for c in p["label"])
                    unit_name = f"pipeline-deploy-{label_safe}.service"
                    content = (
                        "[Unit]\n"
                        f"Description=Pipeline deploy: {p['label']}\n"
                        "After=network-online.target\n\n"
                        "[Service]\n"
                        "Type=simple\n"
                        f"WorkingDirectory={p['dst_path']}\n"
                        f"ExecStart={p['service_command']}\n"
                        "Restart=always\nRestartSec=10\n\n"
                        "[Install]\nWantedBy=multi-user.target\n"
                    )
                    cid3 = cmds_repo.enqueue(
                        target_host=host, cmd_type="install_systemd",
                        cmd_payload={"unit_name": unit_name, "content": content, "restart": True},
                        ttl_secs=600,
                    )
                    enqueued.append(cid3)
                    _log(f"  enq cmd#{cid3}: systemd {unit_name} → {host}")

        # 投入完了 → 全 cmd の完了を待つ (= polling)
        _log(f"  enqueued {len(enqueued)} cmds, waiting for completion (max 5 min)...")
        deadline = _time.time() + 300
        while _time.time() < deadline:
            done = 0
            failed = 0
            pending = 0
            for cid in enqueued:
                c = cmds_repo.get(cid)
                if c is None:
                    continue
                if c["state"] == "done":
                    done += 1
                elif c["state"] == "failed":
                    failed += 1
                else:
                    pending += 1
            if pending == 0:
                _log(f"  all done: success={done}, failed={failed}")
                record["success"] = failed == 0
                record["exit_code"] = 0 if failed == 0 else 1
                break
            await asyncio.sleep(2)
        else:
            _log("  timeout after 5 min")
            record["success"] = False
            record["exit_code"] = -1

    except Exception as e:
        _log(f"ERROR: {e}")
        record["success"] = False
        record["exit_code"] = -2
        record["error"] = str(e)
    finally:
        record["finished_at"] = _now()
        record["duration_s"] = round(_time.time() - started, 2)
        _history.appendleft(dict(record))
        _current_run = None


@router.post("/deploy", response_model=DeployRun)
async def trigger_deploy(body: DeployTriggerBody, request: Request) -> DeployRun:
    """deploy 実行 (= rsync + restart)。 同時走行は禁止 (= 409).

    via_daemon=true なら ssh の代わりに daemon admin cmd queue 経由で実行。
    """
    async with _lock:
        if _current_run is not None:
            raise HTTPException(409, detail=f"deploy already running: id={_current_run['id']}")
        run_id = f"deploy-{int(time.time())}"
        if body.hosts:
            hosts = body.hosts
        else:
            repo = DeployTargetsRepository(request.app.state.db)
            hosts = [t["host"] for t in repo.list_all(enabled_only=True)]
            if not hosts:
                raise HTTPException(400, detail="deploy_targets が空です")

        if body.via_daemon:
            # daemon 経由配信 (= ssh 廃止)
            asyncio.create_task(
                _run_deploy_via_daemon(run_id, hosts, body.skip_restart, body.dry_run,
                                        request.app.state.db)
            )
        else:
            import json as _json
            paths_repo = DeployPathsRepository(request.app.state.db)
            paths_json = _json.dumps([
                {k: v for k, v in p.items()
                 if k in ("id", "label", "src_path", "dst_path", "enabled", "delete_mode",
                          "setup_command", "service_command")}
                for p in paths_repo.list_all(enabled_only=True)
            ])
            asyncio.create_task(_run_deploy(run_id, hosts, body.skip_restart, body.dry_run, paths_json))
        record = {
            "id": run_id, "started_at": _now(), "finished_at": None, "duration_s": None,
            "success": None, "exit_code": None, "log": "(running...)",
            "hosts": hosts, "skip_restart": body.skip_restart, "dry_run": body.dry_run,
        }
        return DeployRun(**record)


@router.get("/deploy", response_model=list[DeployRun])
async def list_deploys() -> list[DeployRun]:
    """直近の deploy 履歴 (最大 20 件、 新しい順)。 running があれば先頭。"""
    out: list[dict[str, Any]] = []
    if _current_run is not None:
        out.append(dict(_current_run))
    for r in _history:
        if _current_run is not None and r["id"] == _current_run["id"]:
            continue
        out.append(r)
    return [DeployRun(**r) for r in out]


@router.get("/deploy/{run_id}", response_model=DeployRun)
async def get_deploy(run_id: str) -> DeployRun:
    """特定 run の詳細 (log 全文)."""
    if _current_run is not None and _current_run["id"] == run_id:
        return DeployRun(**_current_run)
    for r in _history:
        if r["id"] == run_id:
            return DeployRun(**r)
    raise HTTPException(404, detail=f"unknown deploy run: {run_id}")


# ========== 配信先 (deploy_targets) CRUD ==========
# 注: /pubkey は /{target_id:int} より先に登録 (FastAPI path 順序)

@router.get("/deploy-targets/pubkey", response_model=dict)
def get_pubkey() -> dict:
    """配信元 (paps-ai) の SSH 公開鍵を返す。 各 GPU 箱の /root/.ssh/authorized_keys に追加する文字列。"""
    candidates = [
        Path("/home/paps-ai/.ssh/id_ed25519.pub"),
        Path("/home/paps-ai/.ssh/id_rsa.pub"),
    ]
    for p in candidates:
        if p.exists():
            return {"pubkey": p.read_text().strip(), "source": str(p)}
    return {"pubkey": None, "source": None,
            "hint": "paps-ai ユーザに SSH 鍵未生成。 .7 上で `sudo -u paps-ai ssh-keygen -t ed25519 -N \"\" -f ~/.ssh/id_ed25519` を実行"}


@router.get("/deploy-targets", response_model=list[DeployTarget])
def list_targets(request: Request, enabled_only: bool = False) -> list[DeployTarget]:
    repo = DeployTargetsRepository(request.app.state.db)
    return [DeployTarget(**t) for t in repo.list_all(enabled_only=enabled_only)]


@router.post("/deploy-targets", response_model=DeployTarget)
def create_target(body: DeployTargetCreate, request: Request) -> DeployTarget:
    repo = DeployTargetsRepository(request.app.state.db)
    # 同 host が既存ならそれを返す (= 冪等性、 bootstrap の重複 join 防止)
    for t in repo.list_all():
        if t["host"] == body.host:
            return DeployTarget(**t)
    repo.create(label=body.label, host=body.host, ssh_user=body.ssh_user,
                ssh_port=body.ssh_port, enabled=body.enabled, notes=body.notes)
    for t in repo.list_all():
        if t["host"] == body.host:
            return DeployTarget(**t)
    raise HTTPException(500, detail="created but cannot fetch back")


@router.put("/deploy-targets/{target_id}", response_model=DeployTarget)
def update_target(target_id: int, body: DeployTargetUpdate, request: Request) -> DeployTarget:
    repo = DeployTargetsRepository(request.app.state.db)
    if repo.get(target_id) is None:
        raise HTTPException(404, detail=f"target {target_id} not found")
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    repo.update(target_id, **fields)
    t = repo.get(target_id)
    return DeployTarget(**t)


@router.delete("/deploy-targets/{target_id}", status_code=204)
def delete_target(target_id: int, request: Request) -> None:
    repo = DeployTargetsRepository(request.app.state.db)
    if repo.get(target_id) is None:
        raise HTTPException(404, detail=f"target {target_id} not found")
    repo.delete(target_id)


# ========== bootstrap (= 新規 GPU 箱への 1 行 install) ==========

import io
import tarfile

from fastapi.responses import PlainTextResponse, StreamingResponse

# project root は admin_deploy.py の位置 (= <root>/pipeline/api/admin_deploy.py) から導出
# こうしておけば install パスが /opt/pipeline でも /home/paps-ai/ai/pipeline でも動く
PIPELINE_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SCRIPT_PATH = PIPELINE_ROOT / "scripts" / "bootstrap.sh"
PIPELINE_SOURCE_DIRS = ["pipeline", "scripts"]   # tar.gz に含めるディレクトリ
# pip install -e . で必要な root レベルファイル (= 新規 box で venv 自前作成時に必須)
PIPELINE_SOURCE_FILES = ["pyproject.toml", "README.md", "LICENSE"]


@router.get("/bootstrap/source.tar.gz")
def get_bootstrap_source() -> StreamingResponse:
    """新規 GPU 箱に展開する pipeline source の tar.gz を on-the-fly 生成。"""
    if not PIPELINE_ROOT.exists():
        raise HTTPException(500, f"pipeline root not found: {PIPELINE_ROOT}")

    def _stream():
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            # __pycache__ / .pyc / .bak 除外
            def _filter(ti: tarfile.TarInfo):
                name = ti.name
                if "/__pycache__" in name or name.endswith(".pyc") or name.endswith(".bak"):
                    return None
                return ti
            for d in PIPELINE_SOURCE_DIRS:
                src = PIPELINE_ROOT / d
                if not src.exists():
                    continue
                tar.add(str(src), arcname=d, filter=_filter)
            # pip install -e . に必要な root 直下ファイル (pyproject.toml / README.md / LICENSE)
            for f in PIPELINE_SOURCE_FILES:
                src = PIPELINE_ROOT / f
                if src.exists():
                    tar.add(str(src), arcname=f)
        buf.seek(0)
        yield buf.getvalue()

    return StreamingResponse(
        _stream(),
        media_type="application/gzip",
        headers={"Content-Disposition": "attachment; filename=pipeline-source.tar.gz"},
    )


@bootstrap_router.get("/bootstrap.sh", response_class=PlainTextResponse)
def get_bootstrap_script() -> str:
    """新規 GPU 箱で `curl -sSL .../bootstrap.sh | sudo bash` 用のシェルスクリプト。"""
    if not BOOTSTRAP_SCRIPT_PATH.exists():
        raise HTTPException(500, f"bootstrap.sh not found: {BOOTSTRAP_SCRIPT_PATH}")
    return BOOTSTRAP_SCRIPT_PATH.read_text(encoding="utf-8")


# ========== 配信パス (deploy_paths) CRUD ==========

class DeployPath(BaseModel):
    id: int
    label: str
    src_path: str
    dst_path: str
    enabled: bool = True
    delete_mode: bool = False
    setup_command: str | None = None
    service_command: str | None = None
    notes: str | None = None
    last_synced_at: str | None = None
    last_synced_ok: bool | None = None
    created_at: str | None = None
    updated_at: str | None = None


class DeployPathCreate(BaseModel):
    label: str = Field(..., min_length=1, max_length=64)
    src_path: str = Field(..., min_length=1)
    dst_path: str = Field(..., min_length=1)
    enabled: bool = True
    delete_mode: bool = False
    setup_command: str | None = None
    service_command: str | None = None
    notes: str | None = None


class DeployPathUpdate(BaseModel):
    label: str | None = None
    src_path: str | None = None
    dst_path: str | None = None
    enabled: bool | None = None
    delete_mode: bool | None = None
    setup_command: str | None = None
    service_command: str | None = None
    notes: str | None = None


@router.get("/deploy-paths", response_model=list[DeployPath])
def list_paths(request: Request, enabled_only: bool = False) -> list[DeployPath]:
    repo = DeployPathsRepository(request.app.state.db)
    return [DeployPath(**p) for p in repo.list_all(enabled_only=enabled_only)]


@router.post("/deploy-paths", response_model=DeployPath)
def create_path(body: DeployPathCreate, request: Request) -> DeployPath:
    repo = DeployPathsRepository(request.app.state.db)
    repo.create(**body.model_dump())
    # 同 (src, dst) の最新行を取って返す (= label + src + dst の組合せで一意推定)
    for p in repo.list_all():
        if p["src_path"] == body.src_path and p["dst_path"] == body.dst_path and p["label"] == body.label:
            return DeployPath(**p)
    raise HTTPException(500, detail="created but cannot fetch back")


@router.put("/deploy-paths/{path_id}", response_model=DeployPath)
def update_path(path_id: int, body: DeployPathUpdate, request: Request) -> DeployPath:
    repo = DeployPathsRepository(request.app.state.db)
    if repo.get(path_id) is None:
        raise HTTPException(404, detail=f"deploy_path {path_id} not found")
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    repo.update(path_id, **fields)
    return DeployPath(**repo.get(path_id))


@router.delete("/deploy-paths/{path_id}", status_code=204)
def delete_path(path_id: int, request: Request) -> None:
    repo = DeployPathsRepository(request.app.state.db)
    if repo.get(path_id) is None:
        raise HTTPException(404, detail=f"deploy_path {path_id} not found")
    repo.delete(path_id)


@router.get("/deploy-paths/{path_id}/archive")
def get_deploy_path_archive(path_id: int, request: Request) -> StreamingResponse:
    """指定 deploy_path の src_path を tar.gz 化して返す (= daemon の fetch_archive 用)."""
    repo = DeployPathsRepository(request.app.state.db)
    p = repo.get(path_id)
    if p is None:
        raise HTTPException(404, detail=f"deploy_path {path_id} not found")
    src = Path(p["src_path"])
    if not src.exists():
        raise HTTPException(500, detail=f"src not found on control plane: {src}")

    def _stream():
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            def _filter(ti: tarfile.TarInfo):
                if "/__pycache__" in ti.name or ti.name.endswith(".pyc") or ti.name.endswith(".bak"):
                    return None
                return ti
            # src がディレクトリなら配下を展開時 dst/ 直下に並ぶように arcname="."
            # src がファイルならファイル名のみ
            if src.is_dir():
                tar.add(str(src), arcname=".", filter=_filter)
            else:
                tar.add(str(src), arcname=src.name, filter=_filter)
        buf.seek(0)
        yield buf.getvalue()

    return StreamingResponse(_stream(), media_type="application/gzip")
