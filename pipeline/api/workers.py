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

import json
import logging
import os
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
    # 起動時の env-fallback filter (= PIPELINE_WORKLOAD_FILTER)。
    # None = env 未設定 (= 全 workload 受け)、 list = この list のみ。
    env_filter: list[str] | None = None


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
    # 自動切替: 制御プレーンが保持する workload allow-list (= 空/None=フィルタ解除)。
    # worker daemon は 30s 毎にこれを poll し、 変化があれば executor cache を捨てて
    # 反映する (= プロセス再起動なしの runtime 切替)。
    workload_filter: list[str] | None = None
    filter_updated_at: str | None = None
    filter_updated_by: str | None = None
    # 起動時の systemd PIPELINE_WORKLOAD_FILTER env (= DB filter=null 時の fallback)
    env_filter: list[str] | None = None


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
    host_cpu_pct: float | None = None    # /proc/loadavg ベース、 lane scaler が読む
    gpu_throttle: bool | None = None     # サーマルスロットル発動中
    gpu_temp_c: float | None = None      # GPU 温度 (報告用)


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


class SetWorkerFilterRequest(BaseModel):
    # mode の意味:
    # - replace: workloads で完全上書き (= 既存の DB filter を捨てる)
    # - add:     workloads を **追加**。 DB filter=None なら env_filter を base に union
    # - remove:  workloads を **除去**。 DB filter=None なら env_filter を base に差分
    # 既存 client 互換: mode 未指定なら "replace"。
    mode: str = "replace"
    # mode=replace: None / [] = 解除 (= env fallback)
    # mode=add/remove: 追加 or 削除する slug の list
    workloads: list[str] | None = None
    # 監査用: "supervisor:rule-xyz" / "operator" 等。 未指定 = "operator"。
    updated_by: str | None = None


class WorkerConfigResponse(BaseModel):
    """worker daemon が poll する設定。 SoT は workers テーブル。"""
    workload_filter: list[str] | None = None
    filter_updated_at: str | None = None
    filter_updated_by: str | None = None
    # 将来の拡張用: 1 ペイロードでまとめて返す (= round-trip 削減)
    # 例: idle_sleep_s, claim_limit_override 等。 現状は filter のみ。


# ---------------- helpers ----------------


def _host_matches_affinity(worker_host: str | None,
                            affinity: list[str]) -> bool:
    """worker.host (= "ai-gpu1-1" のような systemd instance suffix 付き形式)が
    workload.host_affinity (= "ai-gpu1" のような host family、 もしくは完全一致 host)
    にマッチするか。

    マッチ条件:
      - 完全一致 ("ai-gpu1-1" == "ai-gpu1-1")
      - host family のプレフィックスマッチ ("ai-gpu1-1" starts with "ai-gpu1-")
        ← supervisor の `add_host_affinity` が `_host_stats` 由来の "ai-gpu1" 形式で
          書くため、 worker の "ai-gpu1-1" がここでマッチするように。
    """
    if not affinity:
        return True
    if not worker_host:
        return False
    for entry in affinity:
        if not isinstance(entry, str):
            continue
        if worker_host == entry:
            return True
        if worker_host.startswith(entry + "-"):
            return True
    return False


def _host_family(worker_host: str) -> str:
    """systemd instance suffix を剥がして "ai-gpu1-3" → "ai-gpu1" にする。
    suffix が数字以外なら原文をそのまま返す (= "delian-prod" 等は触らない)。
    """
    parts = worker_host.rsplit("-", 1)
    return parts[0] if (len(parts) == 2 and parts[1].isdigit()) else worker_host


# --- worker restart (supervisor watchdog 用): hung worker を deploy key で SSH 再起動 ---
_WATCHDOG_HOST_IP = {
    "ai-gpu1": "10.10.50.23", "ai-gpu3": "10.10.50.28",
    "ai-gpu4": "10.10.50.29", "ai-gpu5": "10.10.50.30",
}
_WATCHDOG_DEPLOY_KEY = os.environ.get("PIPELINE_DEPLOY_KEY", "/home/paps-ai/.ssh/id_ed25519")


def _restart_target(worker_host: str) -> tuple[str, str] | None:
    """worker.host → (ssh_ip, systemd_unit)。未対応形式は None。
    ai-gpu1-cpu3→(.23, pipeline-worker-cpu@3) / ai-gpu1-2→(.23, pipeline-worker-gpu@2)
    / ai-gpu3→(.28, pipeline-worker-gpu)。"""
    import re
    m = re.fullmatch(r"(ai-gpu\d+)-cpu(\d+)", worker_host or "")
    if m:
        fam, unit = m.group(1), f"pipeline-worker-cpu@{m.group(2)}"
    elif (m := re.fullmatch(r"(ai-gpu\d+)-(\d+)", worker_host or "")):
        fam, unit = m.group(1), f"pipeline-worker-gpu@{m.group(2)}"
    elif re.fullmatch(r"ai-gpu\d+", worker_host or ""):
        fam, unit = worker_host, "pipeline-worker-gpu"
    else:
        return None
    ip = _WATCHDOG_HOST_IP.get(fam)
    return (ip, unit) if ip else None


def _count_host_concurrency(db: Any, worker_host: str, slug: str) -> int:
    """同じ host で current_workload=slug の worker 数 (= 自分自身も含む)。

    `max_concurrent_per_host` ガード用。 worker のホスト名は systemd instance
    suffix 付き (ai-gpu1-1, ai-gpu1-2…) で、 同一物理 GPU を共有するため、
    suffix を剥がして家族単位で数える。

    "active" 系の state のみカウント (= idle/dead/connecting は除外、 false-block 防止)。
    best-effort: race で多少超えても次 cycle で収束する想定。
    """
    if not worker_host:
        return 0
    family = _host_family(worker_host)
    family_glob = family + "-%"
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) AS cnt FROM workers "
            "WHERE current_workload = :s "
            "  AND state IN ('active','running','claiming','draining') "
            "  AND (host = :exact OR host LIKE :glob)",
            {"s": slug, "exact": family, "glob": family_glob},
        )
        row = cur.fetchone()
    # sqlite3.Row は string キーのみ、 数値 index は KeyError になる環境がある。
    return int(row["cnt"]) if row else 0


def _count_total_concurrency(db: Any, slug: str) -> int:
    """fleet 全体 (= 全 host) で current_workload=slug の active worker 数 (= 自分含む)。

    `max_concurrent_total` ガード用。 max_concurrent_per_host の host 制約を外した版。
    best-effort: claim race で多少超えても次 cycle で収束する想定。
    """
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) AS cnt FROM workers "
            "WHERE current_workload = :s "
            "  AND state IN ('active','running','claiming','draining')",
            {"s": slug},
        )
        row = cur.fetchone()
    return int(row["cnt"]) if row else 0


def _host_vram_budget(
    db: Any,
    worker_host: str,
    self_worker_id: str | None,
    cost_by_slug: dict[str, int],
) -> tuple[int, int]:
    """同じ host family の active workers の VRAM 使用見積と host capacity を返す。

    返り値: (used_mb, capacity_mb)
      - used_mb     = 他 active worker の current_workload の cost (= avg or p95) 合計
      - capacity_mb = 同 family worker が `resources.gpu_vram_mb` で申告した最大値
                      (= host 全 GPU メモリ容量。 0 = 申告なし → fail-open)

    `cost_by_slug` は呼出側で「実態の VRAM 占有」 を反映した dict を渡す。
    既存 peak ベースだと過大評価で worker idle 化したため、 2026-06-28 から
    avg 寄り (= 通常時の値) を使う。 burst 耐性は呼出側で new workload の peak を
    足す形で確保する。

    CPU instance (= hostname suffix "cpu" 含む) は resources.gpu_vram_mb=16311 と
    申告するが、 物理的に GPU 使わないので capacity 計算から除外する。

    `self_worker_id` を渡せばその worker 自身は used から除外する。

    fail-open: capacity_mb=0 (= 申告ない / 全 CPU instance) なら呼び出し側で skip。
    """
    if not worker_host:
        return 0, 0
    family = _host_family(worker_host)
    family_glob = family + "-%"
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT id, host, current_workload, resources FROM workers "
            "WHERE state IN ('active','running','claiming','draining') "
            "  AND (host = :exact OR host LIKE :glob)",
            {"exact": family, "glob": family_glob},
        )
        rows = cur.fetchall()
    used_mb = 0
    capacity_mb = 0
    for row in rows:
        wid = row["id"]
        host = row["host"] or ""
        cw = row["current_workload"]
        res_raw = row["resources"]
        # CPU instance は GPU を物理的に使わない → used にも capacity にも入れない
        is_cpu_instance = "cpu" in host.lower()
        if not is_cpu_instance and wid != self_worker_id and cw:
            used_mb += int(cost_by_slug.get(cw, 0) or 0)
        if is_cpu_instance:
            continue
        try:
            res = json.loads(res_raw) if isinstance(res_raw, str) else (res_raw or {})
        except Exception:
            res = {}
        gpu_mb = int(res.get("gpu_vram_mb") or 0)
        if gpu_mb > capacity_mb:
            capacity_mb = gpu_mb
    return used_mb, capacity_mb


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


def _resolve_worker_filter(worker: dict[str, Any]) -> set[str] | None:
    """workload_filter → env_filter の順で有効な allowlist を返す。
    両方 None/空 の場合は None (= 無制限)。
    workload_filter=None かつ env_filter あり の時 env_filter にフォールバックする
    ことで、 env_filter 専用 worker が env 外の高 priority workload に preempt される
    バグ (2026-06-30) を防ぐ。
    """
    def _parse(raw: Any) -> set[str] | None:
        if not raw:
            return None
        try:
            lst = json.loads(raw) if isinstance(raw, str) else list(raw)
            return set(lst) if lst else None
        except Exception:
            return None

    wf = _parse(worker.get("workload_filter"))
    if wf is not None:
        return wf
    return _parse(worker.get("env_filter"))


# ---------------- registry ----------------


@router.post("/{worker_id}/restart")
def restart_worker(worker_id: str, request: Request) -> dict[str, Any]:
    """hung worker を制御プレーンから SSH で systemctl restart (supervisor watchdog 用)。
    worker daemon が httpx stuck 等で応答不能 (= admin cmd も届かない) 場合の外部復旧手段。"""
    import subprocess
    worker = _get_worker_or_404(request, worker_id)
    host = worker.get("host") or ""
    tgt = _restart_target(host)
    if tgt is None:
        raise HTTPException(status_code=400, detail=f"restart 未対応の host 形式: {host}")
    ip, unit = tgt
    cmd = ["ssh", "-i", _WATCHDOG_DEPLOY_KEY, "-o", "StrictHostKeyChecking=no",
           "-o", "ConnectTimeout=10", f"root@{ip}", f"systemctl restart {unit}"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ssh failed: {e}")
    logging.getLogger(__name__).warning(
        "watchdog restart worker=%s host=%s unit=%s rc=%s", worker_id, host, unit, r.returncode)
    return {"worker_id": worker_id, "host": host, "unit": unit,
            "ok": r.returncode == 0, "rc": r.returncode, "stderr": (r.stderr or "")[:300]}


@router.post("", response_model=WorkerInfo, status_code=status.HTTP_201_CREATED)
def register_worker(body: WorkerRegisterRequest, request: Request) -> WorkerInfo:
    rec = _wrepo(request).register(
        host=body.host, pid=body.pid,
        tags=body.tags, resources=body.resources,
        worker_id=body.worker_id,
        env_filter=body.env_filter,
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
    # CPU% / サーマル状態は DB 不要 (= 揮発で十分)、 in-memory store
    if body.host_cpu_pct is not None or body.gpu_throttle is not None or body.gpu_temp_c is not None:
        from datetime import datetime, timezone
        store = getattr(request.app.state, "worker_cpu", None)
        if store is None:
            store = {}
            request.app.state.worker_cpu = store
        existing = store.get(worker_id, {})
        store[worker_id] = {
            "cpu_pct": float(body.host_cpu_pct) if body.host_cpu_pct is not None else existing.get("cpu_pct"),
            "host": rec.get("host"),
            "ts": datetime.now(timezone.utc).isoformat(),
            "gpu_throttle": bool(body.gpu_throttle) if body.gpu_throttle is not None else existing.get("gpu_throttle", False),
            "gpu_temp_c": float(body.gpu_temp_c) if body.gpu_temp_c is not None else existing.get("gpu_temp_c"),
        }
    return WorkerInfo(**rec)


@router.get("/cpu", response_model=dict[str, Any])
def list_workers_cpu(request: Request) -> dict[str, Any]:
    """各 worker (= host) の最新 CPU 利用率を返す。 lane scaler が読む。

    形式:
      {"per_worker": {"<wid>": {"host": "...", "cpu_pct": 23.4, "ts": "..."}, ...},
       "per_host":   {"<host>": {"cpu_pct": 23.4, "n_workers": 7, "ts": "..."}},
       "cluster_max": 79.0}
    """
    from datetime import datetime, timezone, timedelta
    store: dict[str, dict[str, Any]] = getattr(request.app.state, "worker_cpu", {}) or {}
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=120)  # 2 分以内の値のみ採用
    per_worker: dict[str, dict[str, Any]] = {}
    per_host_vals: dict[str, list[float]] = {}
    per_host_ts: dict[str, str] = {}
    for wid, v in store.items():
        try:
            ts_dt = datetime.fromisoformat(v["ts"].replace("Z", "+00:00"))
        except Exception:
            continue
        if ts_dt < cutoff:
            continue
        per_worker[wid] = v
        host = v.get("host") or wid
        per_host_vals.setdefault(host, []).append(float(v["cpu_pct"]))
        # 最新 ts を host 代表に
        if host not in per_host_ts or v["ts"] > per_host_ts[host]:
            per_host_ts[host] = v["ts"]
    per_host = {
        h: {"cpu_pct": round(max(vs), 1),    # 同 host 上の worker 全 reading から max
            "n_workers": len(vs), "ts": per_host_ts.get(h)}
        for h, vs in per_host_vals.items()
    }
    cluster_max = max((d["cpu_pct"] for d in per_host.values()), default=0.0)
    return {
        "per_worker": per_worker,
        "per_host": per_host,
        "cluster_max": cluster_max,
        "now": now.isoformat(),
    }


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


@router.get("/{worker_id}/config", response_model=WorkerConfigResponse)
def worker_config(worker_id: str, request: Request) -> WorkerConfigResponse:
    """worker daemon が 30s 毎に poll するエンドポイント。

    返却される `workload_filter` が現在 daemon が知ってる filter と違えば、
    daemon は executor cache を捨てて新 filter を適用 (= プロセス再起動なし)。
    """
    rec = _get_worker_or_404(request, worker_id)
    return WorkerConfigResponse(
        workload_filter=rec.get("workload_filter"),
        filter_updated_at=rec.get("filter_updated_at"),
        filter_updated_by=rec.get("filter_updated_by"),
    )


@router.post("/{worker_id}/filter", response_model=WorkerInfo)
def set_worker_filter(
    worker_id: str, body: SetWorkerFilterRequest, request: Request
) -> WorkerInfo:
    """worker の workload_filter を変更。 supervisor / operator から叩く。

    mode:
      - replace (default): body.workloads で完全上書き。 None/[] = 解除 (= env fallback)
      - add:    body.workloads を **追加** (= 既存 + 新規の union)。
                DB filter=None の worker は env_filter を base にして安全マージ
      - remove: body.workloads を **除去**。 結果が env_filter と同じなら null に
    """
    try:
        rec = _wrepo(request).set_filter(
            worker_id,
            filter_list=body.workloads,
            mode=body.mode,
            updated_by=body.updated_by,
        )
    except WorkerNotFound as e:
        raise HTTPException(404, detail=str(e)) from e
    except ValueError as e:
        raise HTTPException(400, detail=str(e)) from e
    return WorkerInfo(**rec)


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
    worker_current = worker.get("current_workload") if isinstance(worker, dict) else getattr(worker, "current_workload", None)
    # worker.workload_filter (= operator 設定の class 分離 SoT) を hub 側でも適用。
    # worker daemon が古い list で claim 呼んで filter 違反する事故を防ぐ
    # (= 2026-06-28 nas-cpu が video-face-extract 取りに行った bug 修正)。
    # workload_filter=None の時は env_filter (= systemd 固定 allowlist) にフォールバック。
    worker_filter = _resolve_worker_filter(worker)
    all_wls = _wlrepo(request).list_all()
    # VRAM 予算チェック用 lookup
    # cost = peak ベース (= 常時 peak 近く使う plugin で avg 採用すると race で OOM 再発、
    # 2026-06-28 Phase 1D で検証して逆戻り)。 avg/p95 は UI 表示用にとどめる。
    cost_by_slug = {w.slug: int(w.observed_vram_mb_peak or 0) for w in all_wls}
    new_cost_by_slug = cost_by_slug
    # 同 family の active workers の VRAM 使用見積 + host capacity。
    # capacity_mb=0 (= CPU instance 群 or 申告なし) は fail-open。
    host_used_mb, host_capacity_mb = _host_vram_budget(
        request.app.state.db, worker_host, worker_id, cost_by_slug
    )
    safety = float(os.environ.get("PIPELINE_VRAM_SAFETY_FRAC", "0.85") or "0.85")
    out = []
    for w in all_wls:
        if not w.enabled:
            continue
        if worker_filter is not None and w.slug not in worker_filter:
            continue
        affinity = list(w.host_affinity or [])
        if not _host_matches_affinity(worker_host, affinity):
            continue
        # 同一 host 上の同時実行ワーカー数を上限制御 (= 横方向、 簡易ガード)。
        limit = w.max_concurrent_per_host
        if limit is not None and limit > 0 and worker_host:
            active = _count_host_concurrency(request.app.state.db, worker_host, w.slug)
            own = 1 if worker_current == w.slug else 0
            if max(0, active - own) >= limit:
                continue
        # 横方向 (fleet 全体): max_concurrent_total で全 host 合計の同時実行数を制限。
        # 単一 writer (embed-write=1) 等、 host をまたいだ絶対上限を保証する。
        tlimit = w.max_concurrent_total
        if tlimit is not None and tlimit > 0:
            tactive = _count_total_concurrency(request.app.state.db, w.slug)
            town = 1 if worker_current == w.slug else 0
            if max(0, tactive - town) >= tlimit:
                continue
        # 縦方向: VRAM 予算チェック。 「他 active worker の avg 合計 + この workload
        # の p95」 が host capacity * safety を超える時 claim 候補から外す。
        # 他 worker は avg (= 通常時)、 自分が乗せる新分は p95 (= burst 想定) で
        # 安全側に倒す。 capacity=0 (= CPU instance / 容量未申告) は skip。
        if host_capacity_mb > 0:
            new_cost = new_cost_by_slug.get(w.slug, 0)
            if new_cost > 0:
                budget_mb = int(host_capacity_mb * safety)
                if host_used_mb + new_cost > budget_mb:
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
    # worker_filter は workloads_for_worker と同じく適用 (= filter 違反 workload を
    # higher と判定して drain 誘発するのを防ぐ、 2026-06-28)。
    # workload_filter=None の時は env_filter にフォールバック。
    worker_filter = _resolve_worker_filter(worker)
    qrepo = _qrepo(request)
    higher_slugs: list[str] = []
    for w in _wlrepo(request).list_all():
        if not w.enabled:
            continue
        if int(w.priority or 0) <= than:
            continue
        if worker_filter is not None and w.slug not in worker_filter:
            continue
        affinity = list(w.host_affinity or [])
        if not _host_matches_affinity(worker_host, affinity):
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
    worker = _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    # enabled=0 (= operator が停止指定) なら新規 claim 拒否。
    if not w.enabled:
        return ClaimResponse(workload_slug=w.slug, tasks=[])
    # worker.workload_filter が設定されていて、 リクエストの slug がそこに無いなら
    # 拒否。 workloads_for_worker は filter 適用するが、 worker daemon が古い list で
    # claim 呼ぶと filter ない workload も通っていた (= nas-cpu worker が
    # video-face-extract claim → movie2face 不在で setup fail 連続、 2026-06-28)。
    wf = worker.get("workload_filter") if isinstance(worker, dict) else None
    if wf:
        try:
            allowed = json.loads(wf) if isinstance(wf, str) else list(wf)
        except Exception:
            allowed = None
        if allowed is not None and w.slug not in allowed:
            return ClaimResponse(workload_slug=w.slug, tasks=[])
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
