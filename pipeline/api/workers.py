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
    # Track B (単一 workload 移行): worker の担当 workload (= 派生スカラ, read-only)。
    # workload_filter が要素1のときのみその slug、None/空/複数 (= 移行残) は None。
    # elastic の "1 worker = 1 workload" 既定はこれを正典とする。
    workload: str | None = None
    # 自動切替: 制御プレーンが保持する workload allow-list (= 空/None=フィルタ解除)。
    # worker daemon は 30s 毎にこれを poll し、 変化があれば executor cache を捨てて
    # 反映する (= プロセス再起動なしの runtime 切替)。 Track B で単一 slug へ移行中。
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


class RunStartItem(BaseModel):
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


class RunStartBatchRequest(BaseModel):
    workload_slug: str
    items: list[RunStartItem] = Field(min_length=1)


class RunStartBatchResponse(BaseModel):
    # items と同じ順序の run id。 worker 側は zip して対応付ける。
    ids: list[str]


class BatchResultItem(BaseModel):
    pk: str
    attempt: int = 0
    # start-batch / runs/start で採番済なら渡す。 None なら run 行をここで新規作成する。
    run_id: str | None = None
    # run_id=None のときだけ必要 (= run 行の started_at)。
    started_at: str | None = None
    success: bool
    exit_code: int | None = None
    duration_ms: int = 0
    stdout: str | None = None
    stderr: str | None = None
    output_json: dict[str, Any] | None = None
    error: str | None = None


class BatchResultRequest(BaseModel):
    workload_slug: str
    results: list[BatchResultItem] = Field(min_length=1)


class BatchResultResponse(BaseModel):
    completed: int          # queue から削除した件数
    failed: int             # fail 処理した件数
    retry_pks: list[str]    # pending に戻った pk (= まだ attempt が残っている)
    dead_pks: list[str]     # max_attempts に達して failed で残置した pk


class WorkloadsForWorkerResponse(BaseModel):
    workloads: list[Workload]
    # claim 候補が 1 件以上ある workload の slug。 worker はこれに載っていない slug への
    # claim を省略できる (= 空振り claim の削減)。
    # **workloads からは外さない**。 worker daemon は offer されなくなった slug の executor を
    # 破棄する (service.py の offered_slugs) ため、 queue が一時的に空になっただけで
    # model load 数十秒の python_module executor が捨てられ、 rebuild churn を起こす。
    # 旧 worker はこのフィールドを読まないので、 従来通り全 slug を claim しにいくだけ。
    claimable: list[str] = Field(default_factory=list)


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
    """systemd instance suffix を剥がして物理ホスト単位にまとめる。
    GPU instance  "ai-gpu1-3"    → "ai-gpu1"
    CPU instance  "ai-gpu1-cpu4" → "ai-gpu1"
    suffix がどちらでもなければ原文のまま (= "delian-prod" / "nas-c2-cpu" 等は触らない)。

    注意: 以前は数字 suffix (`-3`) しか剥がしていなかったため、 CPU worker の
    "ai-gpu1-cpu2" と "ai-gpu1-cpu4" が別 family 扱いになり、
    `max_concurrent_per_host` が同一物理ホスト上の CPU workload (= image-pull 等) を
    全く制限できていなかった (2026-07-21 に image-pull が ai-gpu1 で 2 台同時稼働し
    MariaDB 接続が 300 上限に張り付いた根本原因)。 "-cpu<N>" も剥がして修正。
    """
    parts = worker_host.rsplit("-", 1)
    if len(parts) == 2:
        suf = parts[1]
        if suf.isdigit() or (suf.startswith("cpu") and suf[3:].isdigit()):
            return parts[0]
    return worker_host


# --- worker restart (supervisor watchdog 用): hung worker を deploy key で SSH 再起動 ---
# host family → SSH 接続先はサイト固有なので env で与える。
#   PIPELINE_WATCHDOG_HOSTS="ai-gpu1=192.0.2.23,ai-gpu4=192.0.2.29"
# 未設定なら watchdog による再起動は無効 (= _restart_target が None を返す)。
def _parse_watchdog_hosts(spec: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in spec.split(","):
        fam, _, addr = item.partition("=")
        fam, addr = fam.strip(), addr.strip()
        if fam and addr:
            out[fam] = addr
    return out


_WATCHDOG_HOST_IP = _parse_watchdog_hosts(os.environ.get("PIPELINE_WATCHDOG_HOSTS", ""))
_WATCHDOG_DEPLOY_KEY = os.environ.get(
    "PIPELINE_DEPLOY_KEY", os.path.expanduser("~/.ssh/id_ed25519"))


def _restart_target(worker_host: str) -> tuple[str, str] | None:
    """worker.host → (ssh_ip, systemd_unit)。未対応形式・未登録 host は None。
    ai-gpu1-cpu3→pipeline-worker-cpu@3 / ai-gpu1-2→pipeline-worker-gpu@2
    / ai-gpu3→pipeline-worker-gpu。 接続先は PIPELINE_WATCHDOG_HOSTS から引く。"""
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


# 同時実行カウントの freshness window。 claimed_slug_at がこの秒数以内なら
# 「今もその slug を回している」とみなす。 workload の lease_secs をベースにしつつ、
# 停止した worker が延々とスロットを占有し続けないよう上限で頭打ちにする
# (= handoff が最悪この秒数で成立する)。
_CONCURRENCY_HOLD_CAP_S = 180
_CONCURRENCY_HOLD_MIN_S = 30


def _hold_window_s(lease_secs: int | None) -> int:
    lease = int(lease_secs or 0)
    if lease <= 0:
        return _CONCURRENCY_HOLD_MIN_S
    return max(_CONCURRENCY_HOLD_MIN_S, min(lease, _CONCURRENCY_HOLD_CAP_S))


def _fresh_cutoff_iso(window_s: int) -> str:
    """claimed_slug_at 比較用の下限 ISO 文字列。 _utcnow 系と同じ format
    (isoformat microseconds, +00:00) なので辞書順比較=時刻順比較が成立する。"""
    return (datetime.now(timezone.utc) - timedelta(seconds=max(1, window_s))).isoformat(
        timespec="microseconds"
    )


def _count_host_concurrency(
    db: Any, worker_host: str, slug: str, window_s: int, exclude_id: str | None = None
) -> int:
    """同じ host で「今 slug を回している」worker 数。

    `max_concurrent_per_host` ガード用。 worker のホスト名は systemd instance
    suffix 付き (ai-gpu1-1, ai-gpu1-2…) で、 同一物理 GPU を共有するため、
    suffix を剥がして家族単位で数える。

    「稼働中」の判定は current_workload ではなく claimed_slug + claimed_slug_at を使う。
    current_workload は task 実行中しかセットされず batch 間で None に戻るため
    過小カウントし、 二重稼働の検知漏れ→接続 leak の原因になっていた (2026-07-21)。
    claimed_slug は claim 毎に stamp され、 window 内なら稼働中と数える。

    "active" 系の state のみカウント (= idle/dead/connecting は除外、 false-block 防止)。
    exclude_id を渡すとその worker (= 自分) は除外する。
    """
    if not worker_host:
        return 0
    family = _host_family(worker_host)
    family_glob = family + "-%"
    cutoff = _fresh_cutoff_iso(window_s)
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) AS cnt FROM workers "
            "WHERE claimed_slug = :s AND claimed_slug_at >= :cutoff "
            "  AND state IN ('active','running','claiming','draining') "
            "  AND id IS NOT :self "
            "  AND (host = :exact OR host LIKE :glob)",
            {"s": slug, "cutoff": cutoff, "self": exclude_id,
             "exact": family, "glob": family_glob},
        )
        row = cur.fetchone()
    # sqlite3.Row は string キーのみ、 数値 index は KeyError になる環境がある。
    return int(row["cnt"]) if row else 0


def _count_total_concurrency(
    db: Any, slug: str, window_s: int, exclude_id: str | None = None
) -> int:
    """fleet 全体 (= 全 host) で「今 slug を回している」active worker 数。

    `max_concurrent_total` ガード用。 max_concurrent_per_host の host 制約を外した版。
    判定根拠は _count_host_concurrency と同じ claimed_slug + freshness window。
    exclude_id を渡すとその worker (= 自分) は除外する。
    """
    cutoff = _fresh_cutoff_iso(window_s)
    with db.transaction() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) AS cnt FROM workers "
            "WHERE claimed_slug = :s AND claimed_slug_at >= :cutoff "
            "  AND state IN ('active','running','claiming','draining') "
            "  AND id IS NOT :self",
            {"s": slug, "cutoff": cutoff, "self": exclude_id},
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
    # 要求元が CPU instance なら GPU VRAM budget の対象外 (= fail-open で skip)。
    # CPU worker は物理的に GPU VRAM を使わないので、 GPU workload の逼迫で
    # CPU workload (image-pull 等) の claim 候補が外れるのは誤り。
    # 以前は CPU worker の family が自分だけ (= capacity 0) で自然に skip されていたが、
    # _host_family が物理ホスト単位に畳む修正 (2026-07-21 per-host cap 修正) の副作用で
    # 同居 GPU worker の budget を被り、 image-pull が GPU VRAM 逼迫時に claim 候補から
    # 外れて evict→rebuild churn (再接続 storm + setup コスト) を起こしていた。 明示 skip で修正。
    if "cpu" in worker_host.lower():
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


def _claim_none_is_idle() -> bool:
    """Track B B4 フラグ: True なら「filter 無し = idle (= 何も claim しない)」、
    False (既定) なら旧挙動「filter 無し = 優先度で何でも claim」。 claim パス=生命線
    ゆえ既定 off。 PIPELINE_CLAIM_NONE_IS_IDLE=1 + pipeline-oss 再起動で段階有効化 →
    問題あれば env を戻して即 rollback。 full-elastic では全 worker が単一 slug 担当
    (elastic 割当) になるので、 filter 無し worker は「割当待ち/停止待ちの idle」であるべき。"""
    return os.environ.get("PIPELINE_CLAIM_NONE_IS_IDLE", "0") == "1"


def _resolve_worker_filter(worker: dict[str, Any]) -> set[str] | None:
    """workload_filter → env_filter の順で有効な allowlist を返す。
    両方 None/空 の場合: 旧挙動=None (= 無制限)。 B4 有効時=空集合 (= idle, 何も許可しない)。
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
    ef = _parse(worker.get("env_filter"))
    if ef is not None:
        return ef
    # B4: 両 filter 空 → flag on なら空集合(idle)、off(既定)なら None(無制限=旧挙動)。
    return frozenset() if _claim_none_is_idle() else None


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


# --- Elastic Workers: 制御プレーン側 systemctl primitives (2026-07-04) ---
# supervisor は非特権ユーザで動き root@他ホストへ SSH できない。 privileged な
# systemctl (spawn/stop/inventory) は control plane (deploy key を持つユーザ) に集約
# する (= restart_worker と同型)。 supervisor はこれらを HTTP で叩く。

class ElasticOpRequest(BaseModel):
    family: str = Field(description="host family, e.g. 'ai-gpu5'")
    group: str = Field(description="'cpu' | 'gpu'")
    instance: int = Field(ge=1, le=100)
    # P2 (spawn-with-filter): spawn 時に焼き込む担当 slug。 instance の systemd drop-in に
    # PIPELINE_WORKLOAD_FILTER として書いてから start する → worker は起動直後から
    # env_filter=この slug で claim し、 無フィルタ期間の誤 claim が起きない (= 生誕時から単一)。
    filter: list[str] | None = Field(default=None, description="spawn 時に焼き込む担当 slug")


def _elastic_ssh(host_ip: str, cmd: str, timeout: int = 12) -> dict[str, Any]:
    import subprocess
    args = ["ssh", "-i", _WATCHDOG_DEPLOY_KEY, "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5",
            f"root@{host_ip}", cmd]
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return {"ok": r.returncode == 0, "stdout": (r.stdout or "").strip(),
                "stderr": (r.stderr or "").strip()[:200], "rc": r.returncode}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200], "rc": -1}


def _elastic_parse_instances(stdout: str, group: str) -> dict[str, str]:
    """systemctl list-units 出力 → {instance_no(str): active_state}。"""
    import re
    out: dict[str, str] = {}
    for line in (stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        m = re.fullmatch(rf"pipeline-worker-{group}@(\d+)\.service", parts[0])
        if m:
            out[m.group(1)] = parts[2]   # systemctl の ACTIVE 列
    return out


@router.get("/elastic/inventory")
def elastic_inventory(request: Request, hosts: str = "") -> dict[str, Any]:
    """hosts=CSV family。 各 family の pipeline-worker-{cpu,gpu}@N instance を列挙 (read-only)。
    supervisor の elastic scaler が需要判断の材料に使う。"""
    families = [h.strip() for h in hosts.split(",") if h.strip()]
    result: dict[str, Any] = {}
    for fam in families:
        ip = _WATCHDOG_HOST_IP.get(fam)
        if not ip:
            result[fam] = {"error": "unknown family"}
            continue
        inv: dict[str, Any] = {}
        for group in ("cpu", "gpu"):
            r = _elastic_ssh(
                ip,
                "systemctl list-units --all --no-legend --plain --type=service "
                f"'pipeline-worker-{group}@*.service'",
            )
            if r.get("ok"):
                inv[group] = _elastic_parse_instances(r.get("stdout", ""), group)
            else:
                inv[group] = {}
                inv[f"{group}_error"] = r.get("stderr") or r.get("error") or "ssh failed"
        result[fam] = inv
    return {"inventory": result}


@router.post("/elastic/spawn")
def elastic_spawn(body: ElasticOpRequest) -> dict[str, Any]:
    """pipeline-worker-{group}@N を start (reset-failed で StartLimit クリアしてから)。"""
    if body.group not in ("cpu", "gpu"):
        raise HTTPException(status_code=400, detail="group must be 'cpu' or 'gpu'")
    ip = _WATCHDOG_HOST_IP.get(body.family)
    if not ip:
        raise HTTPException(status_code=400, detail=f"unknown family: {body.family}")
    unit = f"pipeline-worker-{body.group}@{body.instance}.service"
    # 既に稼働中の instance には spawn しない。 stale inventory で稼働中 slot を free と
    # 誤認すると、下の drop-in 書込みが稼働 worker の filter.conf を clobber し (start は
    # no-op でも) 次回再起動で誤 workload をロードする (2026-07-05: supervisor cpu@4 が
    # crawl-face-cleanup に化けた事故)。 fresh に is-active を確認して稼働中なら skip。
    act = _elastic_ssh(ip, f"systemctl is-active {unit}", timeout=8)
    if (act.get("stdout") or "").strip() == "active":
        logging.getLogger(__name__).warning(
            "elastic spawn %s %s SKIP (already active)", body.family, unit)
        return {"family": body.family, "unit": unit, "ok": False,
                "rc": 0, "skipped": "already_active"}
    # P2: spawn 前に担当 slug を drop-in へ焼き込む (無フィルタ期間の誤 claim を無くす)。
    # slug は英数と - _ のみ許可してシェル注入を防ぐ。 書込み失敗は非致命 (旧挙動 =
    # 無フィルタ start + reconcile 後付け に degrade)。 daemon-reload してから start。
    if body.filter:
        safe = [s.strip() for s in body.filter
                if s.strip() and all(c.isalnum() or c in "-_" for c in s.strip())]
        if safe:
            slugs = ",".join(safe)
            d = f"/etc/systemd/system/{unit}.d"
            drop = (f"mkdir -p {d} && "
                    f"printf '[Service]\\nEnvironment=PIPELINE_WORKLOAD_FILTER=%s\\n' "
                    f"'{slugs}' > {d}/filter.conf && systemctl daemon-reload")
            _elastic_ssh(ip, drop, timeout=15)
    _elastic_ssh(ip, f"systemctl reset-failed {unit}")
    r = _elastic_ssh(ip, f"systemctl start {unit}", timeout=25)
    logging.getLogger(__name__).warning(
        "elastic spawn %s %s filter=%s rc=%s",
        body.family, unit, body.filter, r.get("rc"))
    return {"family": body.family, "unit": unit, "ok": bool(r.get("ok")),
            "rc": r.get("rc"), "stderr": (r.get("stderr") or r.get("error") or "")[:200]}


@router.post("/elastic/stop")
def elastic_stop(body: ElasticOpRequest) -> dict[str, Any]:
    """pipeline-worker-{group}@N を stop (graceful drain は呼出側 supervisor が担保)。"""
    if body.group not in ("cpu", "gpu"):
        raise HTTPException(status_code=400, detail="group must be 'cpu' or 'gpu'")
    ip = _WATCHDOG_HOST_IP.get(body.family)
    if not ip:
        raise HTTPException(status_code=400, detail=f"unknown family: {body.family}")
    unit = f"pipeline-worker-{body.group}@{body.instance}.service"
    r = _elastic_ssh(ip, f"systemctl stop {unit}", timeout=35)
    logging.getLogger(__name__).warning(
        "elastic stop %s %s rc=%s", body.family, unit, r.get("rc"))
    return {"family": body.family, "unit": unit, "ok": bool(r.get("ok")),
            "rc": r.get("rc"), "stderr": (r.get("stderr") or r.get("error") or "")[:200]}


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
        # 同一 host 上の同時実行ワーカー数を上限制御 (= 横方向)。
        # 自分 (worker_id) は exclude_id で除外して「他に何台か」を数える。
        hold_window = _hold_window_s(w.lease_secs)
        limit = w.max_concurrent_per_host
        if limit is not None and limit > 0 and worker_host:
            active = _count_host_concurrency(
                request.app.state.db, worker_host, w.slug, hold_window, exclude_id=worker_id
            )
            if active >= limit:
                continue
        # 横方向 (fleet 全体): max_concurrent_total で全 host 合計の同時実行数を制限。
        # 単一 writer (embed-write=1) 等、 host をまたいだ絶対上限を保証する。
        tlimit = w.max_concurrent_total
        if tlimit is not None and tlimit > 0:
            tactive = _count_total_concurrency(
                request.app.state.db, w.slug, hold_window, exclude_id=worker_id
            )
            if tactive >= tlimit:
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
    # claim 候補がある slug を backend ごとに 1 transaction で判定して同梱する。
    # これが無いと worker は offer された workload 全部に claim を撃つので、
    # 16 worker x 12 workload = 1 cycle あたり 192 回の空振りが構造的に発生していた
    # (perf/control-plane-bottleneck.md §3)。 判定失敗時は fail-open で「候補あり」 扱い。
    claimable: list[str] = []
    if out:
        try:
            tables = _qrepo(request).claimable_tables(
                [(w.queue_table, w.lease_secs) for w in out]
            )
            claimable = [w.slug for w in out if w.queue_table in tables]
        except Exception:
            logging.getLogger(__name__).exception(
                "claimable probe failed; offering all slugs as claimable")
            claimable = [w.slug for w in out]
    return WorkloadsForWorkerResponse(workloads=out, claimable=claimable)


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
    candidates = [
        w for w in _wlrepo(request).list_all()
        if w.enabled
        and int(w.priority or 0) > than
        and (worker_filter is None or w.slug in worker_filter)
        and _host_matches_affinity(worker_host, list(w.host_affinity or []))
    ]
    if not candidates:
        return {"has_pending": False, "slugs": [], "than": than}
    # 旧実装は workload ごとに count_by_state を撃っていたので、 workload 12 件で
    # 14 transaction 張っていた (perf/control-plane-bottleneck.md §2)。 この endpoint は
    # batch 完了ごとに呼ばれる hot path なので、 backend ごと 1 transaction の
    # EXISTS 判定に置き換える。 include_expired=False で従来と同じ 「pending があるか」 の意味論。
    # fail_open=False: 判定できなかった表は 「pending 無し」 に倒す (= 旧実装の
    # `except: continue` と同じ)。 preempt を誤検知すると worker が現 workload を
    # 手放し続けるので、 ここは安全側が閉じる方向。
    with_work = qrepo.claimable_tables(
        [(w.queue_table, w.lease_secs) for w in candidates],
        include_expired=False,
        fail_open=False,
    )
    # 早期 break は維持 (= 高 priority が N 件あれば preempt 判断には十分)
    higher_slugs = [w.slug for w in candidates if w.queue_table in with_work][:5]
    return {"has_pending": bool(higher_slugs), "slugs": higher_slugs, "than": than}


@router.post("/{worker_id}/claim", response_model=ClaimResponse)
def claim(worker_id: str, body: ClaimRequest, request: Request) -> ClaimResponse:
    worker = _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    # enabled=0 (= operator が停止指定) なら新規 claim 拒否。
    if not w.enabled:
        return ClaimResponse(workload_slug=w.slug, tasks=[])
    # B4 (Track B): flag on かつ filter 無し (= 真の free) worker は idle 扱いで claim 拒否。
    # workloads_for_worker が空を返すので daemon は claim しないが、 古い list で叩いても
    # ここで止める defense-in-depth。 flag off (既定) なら旧挙動 (下の wf チェックのみ)。
    if _claim_none_is_idle() and not (
        worker.get("workload_filter") or worker.get("env_filter")):
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
    # 同時実行 cap を **claim 時点で強制** する (= GET /workloads の勧告フィルタを
    # すり抜けた二重 claim をここで止める最後の砦)。 判定は claimed_slug + freshness
    # window ベースの耐久カウント。 自分は exclude して「他に何台稼働中か」を数え、
    # 上限以上なら空を返して claim させない。 これが無かったため 2026-07-21 に
    # image-pull が同一 host で 2 台走り MariaDB 接続が 300 上限に張り付いた。
    worker_host = worker.get("host") if isinstance(worker, dict) else None
    hold_window = _hold_window_s(w.lease_secs)
    limit = w.max_concurrent_per_host
    if limit is not None and limit > 0 and worker_host:
        others = _count_host_concurrency(
            request.app.state.db, worker_host, w.slug, hold_window, exclude_id=worker_id
        )
        if others >= limit:
            return ClaimResponse(workload_slug=w.slug, tasks=[])
    tlimit = w.max_concurrent_total
    if tlimit is not None and tlimit > 0:
        tothers = _count_total_concurrency(
            request.app.state.db, w.slug, hold_window, exclude_id=worker_id
        )
        if tothers >= tlimit:
            return ClaimResponse(workload_slug=w.slug, tasks=[])
    tasks = _qrepo(request).claim(
        w.queue_table,
        worker_id=worker_id,
        limit=min(body.limit, w.batch_size),
        lease_secs=w.lease_secs,
    )
    # 実際に task を取れた時だけ「稼働中」印を更新 (= 空 claim では holder にしない)。
    # これで次 cycle 以降、 他の worker の同 slug claim は上の cap で弾かれ収束する。
    if tasks:
        _wrepo(request).stamp_claim(worker_id, w.slug)
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
    # NOTE: 以前は `for pk in body.pks: _qrepo(request).complete(...)` だった。
    # _qrepo() は secondary_db 有効時に WorkloadRepository.list_all() を実行するため、
    # pk 1 件ごとに workloads 全表スキャン + pydantic parse が走り、 pks=10 の complete が
    # 22 transaction になっていた (perf/control-plane-bottleneck.md §2)。
    # repo 生成をループ外に出し、 DELETE も 1 transaction に畳んで 3 transaction にする。
    _qrepo(request).complete_many(w.queue_table, list(body.pks))


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


@router.post("/{worker_id}/runs/start-batch", response_model=RunStartBatchResponse,
             status_code=status.HTTP_201_CREATED)
def start_runs_batch(
    worker_id: str, body: RunStartBatchRequest, request: Request
) -> RunStartBatchResponse:
    """batch 分の run 行を 1 リクエスト / 1 transaction で作る (= runs/start の bulk 版)。

    Dashboard の 「実行中」 パネルは runs.finished_at IS NULL を見ているので、
    実行前に run 行を作る意味論は維持したまま往復だけ畳む。
    """
    _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    ids = _rrepo(request).start_many([
        {
            "workload_slug": w.slug,
            "pk": it.pk,
            "worker_id": worker_id,
            "attempt": it.attempt,
            "started_at": it.started_at,
        }
        for it in body.items
    ])
    return RunStartBatchResponse(ids=ids)


@router.post("/{worker_id}/batch-result", response_model=BatchResultResponse)
def batch_result(
    worker_id: str, body: BatchResultRequest, request: Request
) -> BatchResultResponse:
    """batch 分の 「run 完了 + queue の complete/fail」 を 1 リクエストにまとめる。

    従来は 1 task につき runs/start → complete → runs/{id}/finish の 3 往復で、
    各リクエストが更に 2〜4 transaction を張っていた。 これが control plane の
    スループット上限を決めていた (perf/control-plane-bottleneck.md §2/§4)。
    ここでは task 数によらず transaction を 4 本以下に抑える。

    冪等性は従来と同じ (= at-least-once)。 同じ pk を 2 回送ると 2 回目の DELETE は
    0 行になるだけで、 run 行は run_id 指定なら上書き更新される。
    """
    _get_worker_or_404(request, worker_id)
    w = _get_workload_or_404(request, body.workload_slug)
    rrepo = _rrepo(request)

    # 1) run 行: run_id 済 (= start-batch 経由) は UPDATE、 未採番は INSERT+UPDATE。
    finish_items = [
        {
            "run_id": r.run_id,
            "success": r.success,
            "exit_code": r.exit_code,
            "duration_ms": r.duration_ms,
            "stdout": r.stdout,
            "stderr": r.stderr,
            "output_json": r.output_json,
            "error": r.error,
        }
        for r in body.results if r.run_id
    ]
    if finish_items:
        rrepo.finish_many(finish_items)
    unrecorded = [r for r in body.results if not r.run_id]
    if unrecorded:
        rrepo.record_many([
            {
                "workload_slug": w.slug,
                "pk": r.pk,
                "worker_id": worker_id,
                "attempt": r.attempt,
                "started_at": r.started_at or datetime.now(timezone.utc).isoformat(
                    timespec="microseconds"),
                "success": r.success,
                "exit_code": r.exit_code,
                "duration_ms": r.duration_ms,
                "stdout": r.stdout,
                "stderr": r.stderr,
                "output_json": r.output_json,
                "error": r.error,
            }
            for r in unrecorded
        ])

    # 2) queue: 成功は一括 DELETE、 失敗は attempt を進めて pending or failed へ。
    qrepo = _qrepo(request)
    ok_pks = [r.pk for r in body.results if r.success]
    ng = [(r.pk, r.error or r.stderr or "non-zero exit") for r in body.results if not r.success]
    completed = qrepo.complete_many(w.queue_table, ok_pks) if ok_pks else 0
    states = qrepo.fail_many(w.queue_table, ng, max_attempts=w.max_attempts) if ng else {}
    return BatchResultResponse(
        completed=completed,
        failed=len(ng),
        retry_pks=[pk for pk, st in states.items() if st == "pending"],
        dead_pks=[pk for pk, st in states.items() if st == "failed"],
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
