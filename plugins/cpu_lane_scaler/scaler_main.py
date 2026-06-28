"""cpu-lane-scaler: CPU 利用率を見て CPU-only workload の同時実行ティック (= レーン)
数を動的に調整する。

設計:
- 各 worker daemon の heartbeat に既に /proc/loadavg ベースの host_cpu_pct が
  乗っているので、 `/api/v1/workers/cpu` から cluster_max を取得 (= ssh 不要)。
- 管理対象 workload (= CPU-only と判明している self-loop 群) ごとに:
   target_lanes = (cpu < 40%) → lanes_max
                  (cpu < 60%) → 線形補間
                  (cpu < 80%) → lanes_min
                  else        → 1 (= baseline = 既存 self-loop のみ)
   current_pending = pipeline-oss /workloads/<slug>/queue
   不足分 (= target_lanes - current_pending) を tick PK で enqueue。
- 既存 self-loop は触らない (= baseline 1 lane は維持、 scaler が上に積む形)。

外部設定:
- workload list は init_kwargs.workloads (CSV) で指定。
- host list は init_kwargs.hosts (CSV) で指定。
- 閾値・上下限も init_kwargs。

副次効果:
- queue が深まりすぎないよう lanes_max が cap。
- ssh per tick の overhead は 3 host x ~50ms = 150ms 程度。
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_CONTROL_URL = "http://10.10.50.7:8001"
DEFAULT_WORKLOADS = (
    "paprika-links-pull,paprika-image-pull,paprika-video-pull,"
    "image-dispatcher,video-dispatcher"
)


def setup(**kwargs) -> dict[str, Any]:
    state = {
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": kwargs.get("workload_slug") or "cpu-lane-scaler",
        "managed_workloads": [
            w.strip() for w in (kwargs.get("managed_workloads") or DEFAULT_WORKLOADS).split(",")
            if w.strip()
        ],
        "lanes_min": int(kwargs.get("lanes_min") or 1),
        "lanes_max": int(kwargs.get("lanes_max") or 5),
        "cpu_low_pct": float(kwargs.get("cpu_low_pct") or 40.0),
        "cpu_high_pct": float(kwargs.get("cpu_high_pct") or 80.0),
        "interval_s": int(kwargs.get("interval_s") or 20),
        "counter": 0,
        "hostname": os.uname().nodename,
    }
    # bootstrap も guard: 既に pending あれば追加しない (= multiple workers が
    # 同時に setup→bootstrap して fanout する事故を防ぐ)。
    try:
        own_pending = _get_queue_depth(state["control_url"], state["workload_slug"])
        if not own_pending or own_pending == 0:
            _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
            log.info("cpu-lane-scaler: bootstrap tick-1 enqueued")
        else:
            log.info("cpu-lane-scaler: bootstrap skipped (%d pending)", own_pending)
    except Exception as e:
        log.warning("cpu-lane-scaler: bootstrap enqueue failed: %s", e)
    return state


def _self_enqueue_next_tick(control_url: str, workload_slug: str, tick_id: int, *, pk_suffix: str = "") -> None:
    pk = f"tick-{tick_id}-{int(time.time())}{pk_suffix}"
    req = urllib.request.Request(
        f"{control_url}/api/v1/workloads/{workload_slug}/tasks",
        data=json.dumps({"pk": pk}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except Exception as e:
        log.warning("self-enqueue failed for %s: %s", workload_slug, e)


def _fetch_cluster_cpu(control_url: str) -> tuple[float | None, dict[str, float]]:
    """GET /api/v1/workers/cpu → (cluster_max, per_host_dict)。失敗時 (None, {})."""
    try:
        req = urllib.request.Request(
            f"{control_url}/api/v1/workers/cpu",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=4) as r:
            data = json.loads(r.read().decode("utf-8")) or {}
        per_host_full = data.get("per_host") or {}
        per_host = {h: float(v.get("cpu_pct") or 0.0) for h, v in per_host_full.items()}
        cluster_max = float(data.get("cluster_max") or 0.0)
        return (cluster_max if per_host else None), per_host
    except Exception:
        return None, {}


def _compute_target_lanes(cpu_pct: float, state: dict) -> int:
    """CPU 利用率 → target_lanes の写像 (= 線形補間)。"""
    lo = state["cpu_low_pct"]
    hi = state["cpu_high_pct"]
    lmin = state["lanes_min"]
    lmax = state["lanes_max"]
    if cpu_pct <= lo:
        return lmax
    if cpu_pct >= hi:
        return 1  # baseline (= 既存 self-loop の分のみ)
    # lo < cpu < hi: 線形補間 (lmax → lmin)
    ratio = (cpu_pct - lo) / (hi - lo)
    lanes = round(lmax - ratio * (lmax - lmin))
    return max(lmin, min(lmax, int(lanes)))


def _get_queue_depth(control_url: str, slug: str) -> int | None:
    try:
        req = urllib.request.Request(
            f"{control_url}/api/v1/workloads/{slug}/queue",
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=3) as r:
            data = json.loads(r.read().decode("utf-8")) or {}
        # 期待形: {"pending": N, ...} or {"depth": N, ...}
        v = data.get("pending") or data.get("depth")
        if v is None and isinstance(data.get("queue"), dict):
            v = data["queue"].get("pending") or data["queue"].get("depth")
        return int(v) if v is not None else None
    except Exception:
        return None


def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}

    # 1. cluster CPU を /api/v1/workers/cpu から取得
    cluster_cpu, host_cpu = _fetch_cluster_cpu(state["control_url"])
    if cluster_cpu is None:
        cluster_cpu = 50.0  # 取得失敗時は middle (= 保守的中庸)
        out["cpu_fetch_error"] = True
    out["host_cpu"] = host_cpu
    out["cluster_cpu_pct"] = cluster_cpu

    # 2. 各 workload の current pending → target lanes 計算 → 不足分 enqueue
    target_lanes = _compute_target_lanes(cluster_cpu, state)
    out["target_lanes"] = target_lanes

    actions: dict[str, dict[str, Any]] = {}
    for slug in state["managed_workloads"]:
        depth = _get_queue_depth(state["control_url"], slug)
        action = {"pending": depth, "target": target_lanes, "added": 0}
        if depth is None:
            action["error"] = "queue depth fetch failed"
        elif depth < target_lanes:
            need = target_lanes - depth
            for i in range(need):
                _self_enqueue_next_tick(
                    state["control_url"], slug, state["counter"],
                    pk_suffix=f"-lane{depth + i + 1}",
                )
            action["added"] = need
        actions[slug] = action
    out["actions"] = actions

    out["dispatch_secs"] = round(time.time() - started, 2)

    if any(a.get("added", 0) > 0 for a in actions.values()):
        log.info(
            "cpu-lane-scaler: cpu=%.1f%% target=%d added=%s in %.2fs",
            cluster_cpu, target_lanes,
            {s: a.get("added") for s, a in actions.items() if a.get("added")},
            out["dispatch_secs"],
        )

    # 次 tick (= interval_s 待ち)。 single-lane を維持するため、
    # 自 workload の pending が 0 のときだけ enqueue (= 複数 worker が同時に
    # claim → fanout 暴走を防ぐ)。
    sleep_s = max(1, int(state["interval_s"]) - int(out["dispatch_secs"]))
    time.sleep(sleep_s)
    own_pending = _get_queue_depth(state["control_url"], state["workload_slug"])
    if own_pending and own_pending > 0:
        log.debug("skip self-enqueue: %d ticks already pending", own_pending)
        out["next_tick_scheduled"] = False
    else:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], state["counter"] + 1)
        out["next_tick_scheduled"] = True
    return out


def teardown(state) -> None:
    log.info("cpu_lane_scaler: done %d ticks on %s",
             state.get("counter", 0), state.get("hostname"))
