"""pipeline-supervisor: パイプライン全体のオーケストレーター。

30s ごとに pipeline-oss control plane から:
 - GPU host metrics (/api/v1/workers/metrics)
 - workload list (/api/v1/workloads)
 - 各 workload の queue 深さ (/api/v1/workloads/<slug>/queue)
 - 直近 run output_json (/api/v1/runs)

を取得し、 外部 yaml ルールを当てて必要なら PUT で workload 設定を書き換える。

race 防止: host_affinity 1 host 限定 (= plugin.yaml で固定)。
dry-run 既定 (= apply_mode=0)。
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)

DEFAULT_CONTROL_URL = "http://127.0.0.1:8001"
# 既定はプラグイン dir 隣接の `orchestration_rules.yaml`。
# 制御プレーンと別ホストで動かす場合は init_kwargs `rules_path` で絶対パス上書き。
DEFAULT_RULES_PATH = str(
    Path(__file__).resolve().parent / "orchestration_rules.yaml"
)

# 過去 throughput / 連続性判定用
_HISTORY_LEN = 30   # = 30 tick × 30s = 15 min 分


def _utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _self_enqueue_next_tick(control_url: str, workload_slug: str, tick_id: int) -> None:
    pk = f"tick-{tick_id}-{int(time.time())}"
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
        log.warning("supervisor self-enqueue failed: %s", e)


def setup(**kwargs) -> dict[str, Any]:
    state = {
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "rules_path": kwargs.get("rules_path") or DEFAULT_RULES_PATH,
        "interval_s": int(kwargs.get("interval_s") or 30),
        "metrics_minutes": int(kwargs.get("metrics_minutes") or 10),
        "throughput_window_min": int(kwargs.get("throughput_window_min") or 10),
        "apply_mode": bool(int(kwargs.get("apply_mode") or 0)),
        "max_priority": int(kwargs.get("max_priority") or 200),
        "workload_slug": kwargs.get("workload_slug") or "pipeline-supervisor",
        "counter": 0,
        "hostname": os.uname().nodename,
        # streak カウンタ: (rule_id, target_id) → 連続ヒット回数
        "streak": defaultdict(int),
        # cooldown: (rule_id, target_id) → 残り tick
        "cooldown": defaultdict(int),
        # LLM advisor: 最後にコールした時刻 (monotonic 秒)。 0 = 未コール。
        "llm_last_call_ts": 0.0,
        # LLM 適用済 action の cooldown (= action key → 残り tick)
        "llm_action_cooldown": defaultdict(int),
        # adaptive bin-packing balancer 設定 (= Phase 0 既定: dry-run)
        "balancer_cfg": {
            "enabled": bool(int(kwargs.get("balancer_enabled") or 0)),
            "apply_mode": bool(int(kwargs.get("balancer_apply_mode") or 0)),
            "vram_safety_frac": float(kwargs.get("balancer_vram_safety_frac") or 0.85),
            "tasks_per_worker_hint": int(kwargs.get("balancer_tasks_per_worker_hint") or 100),
            "min_workers_per_workload": int(kwargs.get("balancer_min_workers_per_workload") or 1),
            "max_workers_per_workload": int(kwargs.get("balancer_max_workers_per_workload") or 6),
            "min_dwell_s": float(kwargs.get("balancer_min_dwell_s") or 300),
            "swap_margin": float(kwargs.get("balancer_swap_margin") or 1.3),
            "max_swaps_per_cycle": int(kwargs.get("balancer_max_swaps_per_cycle") or 6),
            "oom_window_min": int(kwargs.get("balancer_oom_window_min") or 10),
            "oom_demote_threshold": int(kwargs.get("balancer_oom_demote_threshold") or 3),
            "oom_cooldown_s": float(kwargs.get("balancer_oom_cooldown_s") or 1800),
            "oom_cooldown_multiplier": float(kwargs.get("balancer_oom_cooldown_multiplier") or 2.0),
            "cold_start_boost": float(kwargs.get("balancer_cold_start_boost") or 5.0),
            "respect_updated_by_prefixes": (
                kwargs.get("balancer_respect_updated_by_prefixes")
                or ["operator", "claude"]
            ),
        },
        # balancer 状態 (= flap 抑制と OOM cool-down)
        "balancer_last_swap_mono": {},     # worker_id → monotonic 秒
        "balancer_oom_cooldown": {},        # (host, slug) → expire monotonic 秒
    }
    log.info("supervisor: control=%s rules=%s apply_mode=%s",
             state["control_url"], state["rules_path"], state["apply_mode"])
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
    except Exception as e:
        log.warning("supervisor: bootstrap enqueue failed: %s", e)
    return state


# ---------------- 評価コンテキスト構築 ---------------- #

def _http_get_json(url: str, timeout: float = 10.0) -> Any:
    r = requests.get(url, timeout=timeout, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def _host_stats(metrics: dict) -> list[dict[str, Any]]:
    """worker metrics を host 単位で集約 (= util_avg / power_avg 等)."""
    by_host: dict[str, list[dict]] = defaultdict(list)
    workers = metrics.get("workers") or {}
    for wid, gpus in workers.items():
        # wid = "w_ai_gpu1_3_c0da" → host = "ai_gpu1" → normalize "ai-gpu1"
        host = "unknown"
        if wid.startswith("w_"):
            parts = wid[2:].split("_")
            if len(parts) >= 2:
                host = parts[0] + "-" + parts[1]    # ai-gpu1
        for gpu_id, pts in (gpus or {}).items():
            for p in pts:
                by_host[host].append(p)
    out = []
    for host, pts in by_host.items():
        if not pts:
            continue
        utils = [p.get("util_pct") for p in pts if isinstance(p.get("util_pct"), (int, float))]
        powers = [p.get("power_w") for p in pts if isinstance(p.get("power_w"), (int, float))]
        mems = [p.get("mem_used_mb") for p in pts if isinstance(p.get("mem_used_mb"), (int, float))]
        totals = [p.get("mem_total_mb") for p in pts if isinstance(p.get("mem_total_mb"), (int, float))]
        temps = [p.get("temp_c") for p in pts if isinstance(p.get("temp_c"), (int, float))]
        # VRAM 空き = 直近サンプル中の peak used を total から引く (= 一番厳しい瞬間で判定)。
        # 同一 GPU を MPS で共有してる worker は全員が同じ used/total を報告するので
        # max でも問題ない。 total が取れない (= 古い metrics row) なら未知扱い (= None)。
        used_peak = max(mems) if mems else 0
        total_peak = max(totals) if totals else 0
        vram_free = max(0, int(total_peak - used_peak)) if total_peak > 0 else None
        out.append({
            "id": host,
            "util_avg": round(sum(utils) / len(utils), 1) if utils else 0.0,
            "util_max": round(max(utils), 1) if utils else 0.0,
            "power_avg": round(sum(powers) / len(powers), 1) if powers else 0.0,
            "mem_avg": round(sum(mems) / len(mems), 1) if mems else 0.0,
            "mem_used_peak_mb": int(used_peak) if mems else None,
            "mem_total_mb": int(total_peak) if totals else None,
            "vram_free_mb": vram_free,
            # GPU 温度 (= サーマルスロットル予兆判定用)
            "temp_c_avg": round(sum(temps) / len(temps), 1) if temps else None,
            "temp_c_max": round(max(temps), 1) if temps else None,
            "sample_n": len(pts),
        })
    return sorted(out, key=lambda h: h["id"])


# ---------------- worker filter helpers (= 自動切替) ---------------- #

def _parse_host_from_wid(wid: str) -> str:
    """worker_id `w_ai_gpu1_3_c0da` → host `ai-gpu1` (= _host_stats と同規約)。

    インスタンス suffix (= `_3`) と末尾 random は捨て、 最初の 2 セグメントを
    `-` で繋ぐ (= "ai-gpu1")。 形式が違う wid は wid 自身を返す (= no-op fallback)。
    """
    if not wid.startswith("w_"):
        return wid
    parts = wid[2:].split("_")
    if len(parts) >= 2:
        return parts[0] + "-" + parts[1]
    return wid


def _list_workers_on_host(control_url: str, host: str) -> list[dict[str, Any]]:
    """指定 host (= _host_stats の id 形式: "ai-gpu1") に居る active worker を返す。

    /api/v1/workers が返す worker.host は systemd instance suffix 付き
    (= "ai-gpu1-6")。 supervisor 側の host id は suffix なし。 wid 由来で正規化照合。
    """
    try:
        resp = _http_get_json(f"{control_url}/api/v1/workers", timeout=5)
    except Exception as e:
        log.warning("list workers failed: %s", e)
        return []
    out: list[dict[str, Any]] = []
    for w in (resp.get("workers") or []):
        wid = w.get("id") or ""
        if w.get("state") != "active":
            continue
        if _parse_host_from_wid(wid) == host:
            out.append(w)
    return out


def _post_worker_filter(control_url: str, worker_id: str,
                       workloads: list[str] | None, updated_by: str,
                       apply_mode: bool) -> dict[str, Any]:
    """POST /api/v1/workers/{id}/filter で filter を上書き。 dry-run 時は計画のみ。"""
    if not apply_mode:
        return {"ok": True, "dry_run": True, "worker_id": worker_id,
                "new_filter": workloads}
    try:
        r = requests.post(
            f"{control_url}/api/v1/workers/{worker_id}/filter",
            json={"workloads": workloads, "updated_by": updated_by},
            timeout=10,
        )
        r.raise_for_status()
        return {"ok": True, "applied": True, "worker_id": worker_id,
                "new_filter": workloads}
    except Exception as e:
        return {"ok": False, "error": f"POST failed: {e}", "worker_id": worker_id}


def _workload_stats(control_url: str, wls: list[dict], runs: list[dict],
                    window_min: int,
                    workers: list[dict] | None = None) -> list[dict[str, Any]]:
    """各 workload の backlog / throughput / drain_eta / dup_ratio + fail_ratio
    + claimable_workers を集計."""
    now = _dt.datetime.now(_dt.timezone.utc)
    cutoff = now - _dt.timedelta(minutes=window_min)

    # runs を slug 別に success / fail で分類
    succ_by_slug: dict[str, list[dict]] = defaultdict(list)
    fail_by_slug: dict[str, list[dict]] = defaultdict(list)
    for r in runs:
        slug = r.get("workload_slug")
        if not slug:
            continue
        fin = r.get("finished_at")
        if not fin:
            continue
        try:
            fdt = (fin if isinstance(fin, _dt.datetime)
                   else _dt.datetime.fromisoformat(str(fin).replace("Z", "+00:00")))
            if fdt.tzinfo is None:
                fdt = fdt.replace(tzinfo=_dt.timezone.utc)
        except Exception:
            continue
        if fdt < cutoff:
            continue
        if r.get("success"):
            succ_by_slug[slug].append(r)
        else:
            fail_by_slug[slug].append(r)

    # claimable_workers: 各 slug について、 active で filter が受ける worker 数を数える。
    # filter=None (= 解除) は全 workload 受け入れの扱い。
    # workers が None (= 旧 caller) なら 0 ではなく None で返して rule 評価対象から外す。
    workers = workers or []
    claimable_count: dict[str, int | None] = {}
    for w in wls:
        slug = w.get("slug")
        if not slug:
            continue
        if not workers:
            claimable_count[slug] = None
            continue
        n = 0
        for worker in workers:
            if worker.get("state") != "active":
                continue
            f = worker.get("workload_filter")
            if f is None or slug in f:
                n += 1
        claimable_count[slug] = n

    out = []
    for w in wls:
        slug = w.get("slug")
        if not slug or not w.get("enabled"):
            continue
        # queue 取得
        try:
            q = _http_get_json(f"{control_url}/api/v1/workloads/{slug}/queue", timeout=5)
        except Exception as e:
            log.debug("queue fetch %s: %s", slug, e)
            q = {"by_state": {}, "total": 0}
        by = q.get("by_state") or {}
        pending = int(by.get("pending") or 0)
        claimed = int(by.get("claimed") or 0)
        backlog = pending + claimed

        # throughput
        succ = succ_by_slug.get(slug) or []
        throughput = round(len(succ) / max(1, window_min), 2)

        # drain_eta
        if throughput > 0:
            drain_eta = round(backlog / throughput, 1)
        else:
            drain_eta = float("inf") if backlog > 0 else 0.0

        # dup_ratio (= 直近 succ の output_json から推定)
        dup_total = 0
        ins_total = 0
        for r in succ:
            out_j = r.get("output_json") or {}
            d = out_j.get("dup")
            i = out_j.get("inserted")
            if isinstance(d, (int, float)) and isinstance(i, (int, float)):
                dup_total += int(d)
                ins_total += int(i)
        dup_ratio = (dup_total / (dup_total + ins_total)) if (dup_total + ins_total) > 0 else 0.0

        # fail_ratio = window 内の fail / (succ+fail)。 succ=fail=0 なら None
        # (= 未観測時に rule 条件 `lt: 0.5` が偽でも真でもならない安全側)。
        fails = fail_by_slug.get(slug) or []
        denom = len(succ) + len(fails)
        fail_ratio = round(len(fails) / denom, 3) if denom > 0 else None
        out.append({
            "slug": slug,
            "enabled": bool(w.get("enabled")),
            # GPU 必須かどうか (= balancer の classify が階級判定に使う)。
            # workload model の bool 列。 dispatcher 系は False、 embed 系は True。
            "requires_gpu": bool(w.get("requires_gpu")),
            # supervisor が patch/filter 変更で介入することを許可するか。
            # False なら streak/cooldown は数えるが action は no-op。 既定 True。
            "supervisor_enabled": bool(w.get("supervisor_enabled", True)),
            "priority": int(w.get("priority") or 100),
            "batch_size": int(w.get("batch_size") or 1),
            "lease_secs": int(w.get("lease_secs") or 300),
            "host_affinity": w.get("host_affinity") or [],
            # workload 別の VRAM 観測値 (= LLM が GPU 配分判断に使える)
            "observed_vram_mb_peak": w.get("observed_vram_mb_peak"),
            "resources_vram_mb": (w.get("resources") or {}).get("vram_mb"),
            "backlog": backlog,
            "pending": pending,
            "claimed": claimed,
            "throughput_min": throughput,
            "drain_eta_min": drain_eta,
            "dup_ratio": round(dup_ratio, 3),
            "fail_ratio": fail_ratio,
            "succ_n": len(succ),
            "fail_n": len(fails),
            # filter で claim 可能な active worker 数 (= 0 なら誰も取りに行けない)
            "claimable_workers": claimable_count.get(slug),
        })
    return out


# ---------------- rule engine ---------------- #

def _match_cond(value: Any, cond: dict[str, Any]) -> bool:
    """{lt: x} {gt: x} {eq: x} {gte: x} {lte: x} の単純照合."""
    if not isinstance(cond, dict):
        return False
    try:
        for op, v in cond.items():
            if op == "lt" and not (value < v):
                return False
            elif op == "gt" and not (value > v):
                return False
            elif op == "eq" and not (value == v):
                return False
            elif op == "gte" and not (value >= v):
                return False
            elif op == "lte" and not (value <= v):
                return False
    except Exception:
        return False
    return True


def _eval_rules(rules: list[dict], hosts: list[dict], workloads: list[dict],
                state: dict) -> list[dict[str, Any]]:
    """rules を評価し、 適用すべき action リストを返す。 streak / cooldown を更新。"""
    actions: list[dict[str, Any]] = []

    # rules の各 entry を host / workload それぞれに当てる
    for rule in rules:
        rid = rule.get("id", "?")
        when = rule.get("when") or {}
        req_streak = int(when.get("streak") or 1)
        cooldown_ticks = int(rule.get("cooldown_ticks") or 0)

        host_conds = {k: v for k, v in when.items()
                      if k.startswith("host.")}
        wl_conds = {k: v for k, v in when.items()
                    if k.startswith("workload.")}
        # host 系 rule
        if host_conds:
            for h in hosts:
                hit = all(
                    _match_cond(h.get(k.split(".", 1)[1]), c)
                    for k, c in host_conds.items()
                )
                key = (rid, h["id"])
                if hit:
                    state["streak"][key] += 1
                else:
                    state["streak"][key] = 0
                if (state["streak"][key] >= req_streak
                        and state["cooldown"][key] == 0):
                    a = dict(rule.get("action") or {})
                    a["_rule_id"] = rid
                    a["_target_kind"] = "host"
                    a["_target_id"] = h["id"]
                    a["_streak"] = state["streak"][key]
                    actions.append(a)
                    state["cooldown"][key] = cooldown_ticks

        # workload 系 rule
        if wl_conds:
            for w in workloads:
                hit = all(
                    _match_cond(w.get(k.split(".", 1)[1]), c)
                    for k, c in wl_conds.items()
                )
                key = (rid, w["slug"])
                if hit:
                    state["streak"][key] += 1
                else:
                    state["streak"][key] = 0
                if (state["streak"][key] >= req_streak
                        and state["cooldown"][key] == 0):
                    a = dict(rule.get("action") or {})
                    a["_rule_id"] = rid
                    a["_target_kind"] = "workload"
                    a["_target_id"] = w["slug"]
                    a["_streak"] = state["streak"][key]
                    actions.append(a)
                    state["cooldown"][key] = cooldown_ticks

    # cooldown decrement
    for k, v in list(state["cooldown"].items()):
        if v > 0:
            state["cooldown"][k] = v - 1
    return actions


def _resolve_numeric(expr: Any, current: int | float) -> float | None:
    """`+20` `-10` `*0.7` `100` などを current 基準で解決."""
    if isinstance(expr, (int, float)):
        return float(expr)
    if not isinstance(expr, str):
        return None
    s = expr.strip()
    try:
        if s.startswith("+"):
            return float(current) + float(s[1:])
        if s.startswith("-"):
            return float(current) - float(s[1:])
        if s.startswith("*"):
            return float(current) * float(s[1:])
        if s.startswith("/"):
            return float(current) / float(s[1:])
        return float(s)
    except Exception:
        return None


def _apply_workload_action(control_url: str, slug: str, patch: dict[str, Any],
                           apply_mode: bool, max_priority: int) -> dict[str, Any]:
    """workload の現状を GET → 差分パッチ → PUT する (= dry-run なら計画だけ)."""
    try:
        cur = _http_get_json(f"{control_url}/api/v1/workloads/{slug}", timeout=5)
    except Exception as e:
        return {"ok": False, "error": f"GET failed: {e}"}

    # サーバ pydantic は extra="forbid" なので、 GET で返ってくる読み取り専用列を
    # 全部削ぐ必要がある。 漏らすと 422 で **全 PUT が silent fail** する
    # (= 2026-06-26 にこの漏れで supervisor の patch_workload が 22h 効いてなかった)。
    _READONLY = {
        "slug", "created_at", "updated_at", "created_by", "schema_version",
        "queue_table",
        "observed_state", "observed_at",
        "observed_run_id", "observed_run_started_at", "observed_run_finished_at",
        "observed_depth", "observed_age_secs", "observed_rate",
        "observed_vram_mb_peak", "observed_vram_sample_count", "observed_vram_updated_at",
    }
    new_body = {k: v for k, v in cur.items() if k not in _READONLY}
    changes: dict[str, Any] = {}

    if "priority" in patch:
        v = _resolve_numeric(patch["priority"], cur.get("priority") or 100)
        if v is not None:
            new_prio = max(0, min(int(max_priority), int(round(v))))
            if new_prio != cur.get("priority"):
                new_body["priority"] = new_prio
                changes["priority"] = (cur.get("priority"), new_prio)
    if "lease_secs" in patch:
        v = _resolve_numeric(patch["lease_secs"], cur.get("lease_secs") or 300)
        if v is not None:
            new_lease = max(15, min(86400, int(round(v))))
            if new_lease != cur.get("lease_secs"):
                new_body["lease_secs"] = new_lease
                changes["lease_secs"] = (cur.get("lease_secs"), new_lease)
    if "host_affinity_add" in patch:
        host = patch["host_affinity_add"]
        if isinstance(host, str) and host:
            ha = list(cur.get("host_affinity") or [])
            if host not in ha:
                ha.append(host)
                new_body["host_affinity"] = ha
                changes["host_affinity"] = (cur.get("host_affinity"), ha)
    if "enabled" in patch:
        new_body["enabled"] = bool(patch["enabled"])
        if new_body["enabled"] != cur.get("enabled"):
            changes["enabled"] = (cur.get("enabled"), new_body["enabled"])

    if not changes:
        return {"ok": True, "noop": True, "slug": slug}

    if not apply_mode:
        return {"ok": True, "dry_run": True, "slug": slug, "changes": changes}

    try:
        r = requests.put(
            f"{control_url}/api/v1/workloads/{slug}",
            json=new_body, timeout=10,
        )
        r.raise_for_status()
        return {"ok": True, "applied": True, "slug": slug, "changes": changes}
    except Exception as e:
        return {"ok": False, "error": f"PUT failed: {e}", "changes": changes}


def _load_rules(path: str) -> list[dict[str, Any]]:
    try:
        import yaml  # type: ignore
    except ImportError:
        log.warning("PyYAML missing; rules disabled")
        return []
    p = Path(path)
    if not p.exists():
        log.debug("rules file not found: %s", path)
        return []
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as e:
        log.warning("rules parse failed: %s", e)
        return []
    return list(data.get("rules") or [])


# ---------------- adaptive bin-packing balancer ---------------- #
# 「VRAM 85% を埋めながら tank pressure に応じて worker N と workload を流動配置」
# する自動オーケストレーション。 既存 rule engine と並列に走る。
# - Phase 0 既定: balancer_apply_mode=False (= log のみ)
# - 安全側: operator 固定 / supervisor_enabled=False / max_swaps_per_cycle / hysteresis
#   (min_dwell_s, swap_margin) で flap 抑制。 worker N 変更も別カウンタで上限。
# - OOM 検知: runs.error が _OOM_RE にヒットしたら降格、 cool-down で復活試行。

# OOM 文字列パターン (= service.py の _OOM_RE 準拠、 ここで再定義して plugin 単体で完結)
import re as _re_balancer
_BALANCER_OOM_RE = _re_balancer.compile(
    r"(out of memory|OutOfMemoryError|cudaErrorMemoryAllocation|"
    r"CUDA out of memory|HIP out of memory|memory allocation failed|"
    r"cannot allocate.*\d+.*(MB|GiB|bytes))",
    _re_balancer.I,
)

# class の降格チェイン (= OOM 時に重い → 軽い の順で fallback)
_BALANCER_CLASS_CHAIN = ["heavy", "medium", "light", "cpu"]


def _balancer_classify(w: dict[str, Any]) -> str:
    """workload を VRAM 階級に分類。

    heavy   = peak >= 2 GB (image-embed 等)
    medium  = 0.3 <= peak < 2 GB (image-hash-extract 等)
    light   = peak < 0.3 GB だが GPU 必要 (video-face-extract 等)
    cpu     = requires_gpu=False / peak=0 (= 並列度の制約が VRAM じゃない)

    重要: `requires_gpu` を最優先で判定。 CPU plugin (paprika-image-pull,
    image-dispatcher 等) は Python プロセスメモリ占有で observed_vram_mb_peak が
    >0 に立つことがあるが、 実 GPU 計算はしないので heavy/medium に誤分類すると
    GPU instance を CPU 仕事に流用してしまう (= VRAM 詰まらず instance 浪費)。
    requires_gpu=False は無条件で cpu 階級。
    """
    if not w.get("requires_gpu"):
        return "cpu"
    peak = int(w.get("observed_vram_mb_peak") or 0)
    vram = int(w.get("resources_vram_mb") or 0)
    # peak は実測。 無い場合は宣言値 vram_mb を仮置き。 両方無ければ 0。
    eff = peak or vram
    if eff == 0:
        return "cpu"
    if eff >= 2048:
        return "heavy"
    if eff >= 300:
        return "medium"
    return "light"


def _balancer_density(w: dict[str, Any], cold_start_boost: float = 5.0) -> float:
    """workload の「1 MB あたりの片付ける価値」 (= pack 順序のスコア)。

    backlog (= pending + claimed) で判定。 pending だけ見ると batch_size=100 の
    workload で claimed=100 / pending=0 のスナップショットを取ったとき density=0
    に転落して配置候補から外れる (= 1 cycle で取りこぼし → 別 cycle で復帰の flap
    の元になる)。 claimed も「処理中=しばらく VRAM を要求する」 ので密度に含める。

    backlog=0 → 0 (空 tank は配置価値なし)。
    rate=0 で backlog > 0 → priority だけで cold-start boost (= deadlock 解放)。
    通常 → (drain_eta_min × priority) / peak_vram_mb。
    """
    pending = int(w.get("pending") or 0)
    claimed = int(w.get("claimed") or 0)
    backlog = pending + claimed
    if backlog == 0:
        return 0.0
    peak = max(int(w.get("observed_vram_mb_peak") or w.get("resources_vram_mb") or 0), 1)
    priority = max(int(w.get("priority") or 100), 1)
    rate = float(w.get("throughput_min") or 0.0)
    if rate <= 0.0:
        # 未着手 backlog: priority だけで決める
        return (priority * cold_start_boost) / peak
    eta = backlog / rate                          # 分
    return (eta * (priority / 100.0)) / peak


def _balancer_pick_lighter(slug: str, workloads: list[dict],
                            classify_fn) -> str | None:
    """slug の class を 1 段下げて、 pending あり最大の workload を選ぶ.
    cpu まで降りて何も無ければ None。"""
    target = next((w for w in workloads if w.get("slug") == slug), None)
    if not target:
        return None
    cur_class = classify_fn(target)
    if cur_class not in _BALANCER_CLASS_CHAIN:
        return None
    idx = _BALANCER_CLASS_CHAIN.index(cur_class)
    if idx + 1 >= len(_BALANCER_CLASS_CHAIN):
        return None
    next_class = _BALANCER_CLASS_CHAIN[idx + 1]
    cands = [w for w in workloads
             if classify_fn(w) == next_class
                and int(w.get("pending") or 0) > 0
                and w.get("enabled")
                and w.get("supervisor_enabled", True)]
    if not cands:
        return None
    return max(cands, key=lambda w: int(w.get("pending") or 0)).get("slug")


def _balancer_oom_events(runs: list[dict], window_min: int,
                          wid_to_host: dict[str, str]) -> list[dict[str, Any]]:
    """直近 window 分の runs から OOM 失敗を抽出。

    error / stderr に _BALANCER_OOM_RE が当たれば 1 件。 1 cycle 内で
    (host, slug) 別に集計して降格判定に使う。
    """
    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=window_min)
    out: list[dict[str, Any]] = []
    for r in runs:
        if r.get("success") is not False:
            continue
        fin = r.get("finished_at") or r.get("started_at")
        if not fin:
            continue
        try:
            fdt = (fin if isinstance(fin, _dt.datetime)
                    else _dt.datetime.fromisoformat(str(fin).replace("Z", "+00:00")))
            if fdt.tzinfo is None:
                fdt = fdt.replace(tzinfo=_dt.timezone.utc)
        except Exception:
            continue
        if fdt < cutoff:
            continue
        blob = (r.get("error") or "") + " " + (r.get("stderr") or "")
        if not _BALANCER_OOM_RE.search(blob):
            continue
        wid = r.get("worker_id") or ""
        host = wid_to_host.get(wid) or _parse_host_from_wid(wid)
        out.append({
            "worker_id": wid,
            "worker_host_family": host,
            "workload_slug": r.get("workload_slug"),
            "ts": fin,
        })
    return out


def _balancer_pack_host(
    host_family: str,
    capacity_mb: int,
    workloads: list[dict],
    excluded_slugs: set[str],
    cfg: dict[str, Any],
) -> tuple[dict[str, int], int]:
    """host 1 つの最適配分を greedy で算出.

    返り値: (plan={slug: n}, used_mb)。
    capacity_mb × vram_safety_frac を予算とし、 density 高い順に詰める。
    excluded_slugs: cool-down 中 / supervisor_enabled=False で除外する slug。
    """
    budget = int(capacity_mb * float(cfg["vram_safety_frac"]))
    used = 0
    plan: dict[str, int] = {}
    cand = [w for w in workloads
            if w.get("enabled")
            and w.get("supervisor_enabled", True)
            and _balancer_classify(w) in ("heavy", "medium", "light")
            and w.get("slug") not in excluded_slugs]
    # host_affinity チェック (= workload が host limit を持ってればそれを尊重)
    cand = [w for w in cand
            if not w.get("host_affinity")
                or host_family in (w.get("host_affinity") or [])]
    cand.sort(key=lambda w: _balancer_density(w, float(cfg.get("cold_start_boost", 5.0))),
              reverse=True)
    for w in cand:
        d = _balancer_density(w, float(cfg.get("cold_start_boost", 5.0)))
        if d <= 0.0:
            continue
        peak = int(w.get("observed_vram_mb_peak") or w.get("resources_vram_mb") or 0)
        if peak <= 0:
            continue
        room = (budget - used) // peak
        if room <= 0:
            break
        # tank_cap も backlog ベース (= claimed in-flight 分も「これから処理する量」 として加算)
        backlog = int(w.get("pending") or 0) + int(w.get("claimed") or 0)
        tank_cap = max(1, backlog // int(cfg.get("tasks_per_worker_hint", 100)))
        n = min(int(room), int(cfg.get("max_workers_per_workload", 6)), tank_cap)
        if n <= 0:
            continue
        plan[w["slug"]] = n
        used += n * peak
    return plan, used


def _balancer_run(state: dict[str, Any], hosts: list[dict],
                   workloads: list[dict], workers: list[dict],
                   runs: list[dict]) -> dict[str, Any]:
    """1 cycle 分の balancer 評価本体。 dry-run でも必ず計画ログを出す。

    apply_mode=True なら POST /workers/{id}/filter を実発行する (= worker 配分のみ)。
    systemd instance 数の変更は別フラグ scale_apply で更に追加保護。
    """
    cfg = state["balancer_cfg"]
    if not cfg.get("enabled"):
        return {"skipped": "balancer disabled"}

    # 1) signal 整形
    # workers を host_family ごとに分類、 GPU lane (= gpu_vram_mb>0) のみ対象
    wid_to_host = {w["id"]: _parse_host_from_wid(w["id"]) for w in workers}
    eligible: dict[str, list[dict]] = defaultdict(list)
    pinned_count = 0
    for w in workers:
        if w.get("state") != "active":
            continue
        if int((w.get("resources") or {}).get("gpu_vram_mb") or 0) <= 0:
            continue
        # operator/claude 固定 worker は触らない
        upd_by = (w.get("filter_updated_by") or "")
        if any(upd_by.startswith(p) for p in cfg["respect_updated_by_prefixes"]):
            pinned_count += 1
            continue
        eligible[_parse_host_from_wid(w["id"])].append(w)

    # 2) host capacity を確定。 _host_stats の mem_total_mb を優先、 無ければ worker
    #    申告 (gpu_vram_mb) を採用。
    host_caps: dict[str, int] = {}
    for h in hosts:
        cap = int(h.get("mem_total_mb") or 0)
        if cap > 0:
            host_caps[h["id"]] = cap
    for fam, wlist in eligible.items():
        if fam in host_caps:
            continue
        caps = [int((w.get("resources") or {}).get("gpu_vram_mb") or 0) for w in wlist]
        if caps:
            host_caps[fam] = max(caps)

    # 3) OOM events 集計 (= host 単位)
    oom_events = _balancer_oom_events(runs, int(cfg["oom_window_min"]), wid_to_host)
    oom_by_host: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for e in oom_events:
        oom_by_host[e["worker_host_family"]][e["workload_slug"]] += 1

    # 4) cool-down の expire / extend 管理
    now_mono = time.monotonic()
    cooldown = state["balancer_oom_cooldown"]   # (host, slug) → expire_mono
    for key, exp in list(cooldown.items()):
        if exp <= now_mono:
            del cooldown[key]
    # 新規 OOM を cool-down 登録 (既存があれば multiplier で延長)
    new_demote_marks: list[dict[str, Any]] = []
    for host_fam, slug_counts in oom_by_host.items():
        for slug, cnt in slug_counts.items():
            if cnt < int(cfg["oom_demote_threshold"]):
                continue
            key = (host_fam, slug)
            base = float(cfg["oom_cooldown_s"])
            if key in cooldown:
                base *= float(cfg.get("oom_cooldown_multiplier", 2.0))
            cooldown[key] = now_mono + base
            new_demote_marks.append({
                "host": host_fam, "slug": slug,
                "oom_count": cnt, "cooldown_s": int(base),
            })

    # 5) 各 host の plan を計算
    plans: dict[str, dict[str, int]] = {}
    used_per_host: dict[str, int] = {}
    for fam, wlist in eligible.items():
        cap = host_caps.get(fam, 0)
        if cap <= 0:
            continue
        # cool-down 中の (host, slug) を除外
        excluded = {slug for (h, slug), _ in cooldown.items() if h == fam}
        plan, used = _balancer_pack_host(fam, cap, workloads, excluded, cfg)
        plans[fam] = plan
        used_per_host[fam] = used

    # 6) target_n と現状から swap 計画 (= worker filter の書換)
    swaps: list[dict[str, Any]] = []
    cur_assign_log: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    last_swap = state["balancer_last_swap_mono"]
    swap_margin = float(cfg["swap_margin"])
    min_dwell_s = float(cfg["min_dwell_s"])
    max_swaps = int(cfg["max_swaps_per_cycle"])

    def _density_lookup(slug: str) -> float:
        for w in workloads:
            if w.get("slug") == slug:
                return _balancer_density(w, float(cfg.get("cold_start_boost", 5.0)))
        return 0.0

    for fam, wlist in eligible.items():
        plan = plans.get(fam, {})
        # 各 worker の現在 filter (= 単一 slug filter のみ識別、 複数 / 全許可は __any__)
        slot_of: dict[str, str] = {}        # worker_id → "slug" or "__any__"
        for w in wlist:
            f = w.get("workload_filter") or w.get("env_filter") or []
            if isinstance(f, list) and len(f) == 1:
                slot_of[w["id"]] = f[0]
                cur_assign_log[fam][f[0]] += 1
            else:
                slot_of[w["id"]] = "__any__"
                cur_assign_log[fam]["__any__"] += 1

        # plan に書かれていない slug を持つ worker を「余剰候補」 リスト化
        surplus_workers: list[dict] = []
        for w in wlist:
            cur_slug = slot_of[w["id"]]
            if cur_slug == "__any__":
                surplus_workers.append(w)
            elif cur_slug not in plan:
                surplus_workers.append(w)
            elif sum(1 for ww in wlist
                      if slot_of[ww["id"]] == cur_slug) > plan[cur_slug]:
                surplus_workers.append(w)

        # plan に足りない slug を埋める
        for plan_slug, target_n in plan.items():
            cur_n = sum(1 for ww in wlist if slot_of[ww["id"]] == plan_slug)
            need = target_n - cur_n
            if need <= 0:
                continue
            new_score = _density_lookup(plan_slug)
            # 余剰 worker から hysteresis 通る人を順番に取る
            for victim in list(surplus_workers):
                if need <= 0:
                    break
                if len(swaps) >= max_swaps:
                    break
                # dwell チェック
                last = last_swap.get(victim["id"], 0.0)
                if last > 0 and (now_mono - last) < min_dwell_s:
                    continue
                # swap_margin チェック (= 現 score の swap_margin 倍を新 score が超える)
                cur_slug = slot_of[victim["id"]]
                cur_score = 0.0 if cur_slug == "__any__" else _density_lookup(cur_slug)
                if cur_score > 0 and new_score < cur_score * swap_margin:
                    continue
                # VRAM 二重チェックは server 側 _host_vram_budget に任せる
                swaps.append({
                    "host": fam,
                    "worker_id": victim["id"],
                    "from": cur_slug,
                    "to": plan_slug,
                    "cur_score": round(cur_score, 2),
                    "new_score": round(new_score, 2),
                    "reason": f"pack target n[{plan_slug}]={target_n}",
                })
                surplus_workers.remove(victim)
                slot_of[victim["id"]] = plan_slug
                need -= 1
            if len(swaps) >= max_swaps:
                break
        if len(swaps) >= max_swaps:
            break

    # 7) apply
    applied = 0
    apply_mode = bool(cfg.get("apply_mode"))
    for s in swaps:
        if not apply_mode:
            continue
        rp = _post_worker_filter(
            state["control_url"], s["worker_id"], [s["to"]],
            updated_by="supervisor:tank-balancer",
            apply_mode=True,
        )
        if rp.get("ok"):
            applied += 1
            last_swap[s["worker_id"]] = now_mono

    # 8) report
    cap_total = sum(host_caps.values())
    used_total = sum(used_per_host.values())
    util = round(used_total / max(cap_total, 1) * 100.0, 1)
    report = {
        "enabled": True,
        "apply_mode": apply_mode,
        "pinned_workers": pinned_count,
        "hosts": [
            {
                "host": fam,
                "capacity_mb": host_caps.get(fam, 0),
                "target_used_mb": used_per_host.get(fam, 0),
                "target_util_pct": round(used_per_host.get(fam, 0) / max(host_caps.get(fam, 1), 1) * 100.0, 1),
                "plan": plans.get(fam, {}),
                "current": dict(cur_assign_log.get(fam, {})),
            }
            for fam in sorted(eligible.keys())
        ],
        "oom_events_n": len(oom_events),
        "oom_demote_marks": new_demote_marks,
        "cooldown_active": [
            {"host": h, "slug": s, "remaining_s": int(exp - now_mono)}
            for (h, s), exp in cooldown.items()
        ],
        "swaps_planned": len(swaps),
        "swaps_applied": applied,
        "swap_details": swaps,
        "vram_target_util_pct": util,
    }
    # 簡潔 1 行サマリ (= journalctl で grep しやすい形)
    summary_line = (
        f"[balancer] mode={'APPLY' if apply_mode else 'DRY'} "
        f"hosts={len(report['hosts'])} pinned={pinned_count} "
        f"vram_target={util}% swaps={len(swaps)} oom={len(oom_events)} "
        f"cooldowns={len(cooldown)}"
    )
    log.info(summary_line)
    if swaps:
        for s in swaps[:10]:
            log.info("[balancer] swap host=%s wid=%s %s→%s score=%.1f→%.1f (%s)",
                     s["host"], s["worker_id"], s["from"], s["to"],
                     s["cur_score"], s["new_score"], s["reason"])
    if new_demote_marks:
        for m in new_demote_marks:
            log.warning("[balancer] OOM demote mark: host=%s slug=%s count=%d cooldown=%ds",
                         m["host"], m["slug"], m["oom_count"], m["cooldown_s"])
    return report


# ---------------- main process ---------------- #

def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    control_url = state["control_url"]

    # 1. 各データ取得
    try:
        metrics = _http_get_json(
            f"{control_url}/api/v1/workers/metrics?minutes={state['metrics_minutes']}"
        )
    except Exception as e:
        metrics = {"workers": {}}
        out["metrics_error"] = str(e)[:120]
    try:
        wls_resp = _http_get_json(f"{control_url}/api/v1/workloads")
        wls = wls_resp.get("workloads") or []
    except Exception as e:
        wls = []
        out["workloads_error"] = str(e)[:120]
    try:
        runs_resp = _http_get_json(
            f"{control_url}/api/v1/runs?limit=500"
        )
        runs = runs_resp.get("runs") or []
    except Exception as e:
        runs = []
        out["runs_error"] = str(e)[:120]
    # workers list (= claimable_workers 集計と全 active 数把握用)
    try:
        workers_resp = _http_get_json(f"{control_url}/api/v1/workers")
        wks = workers_resp.get("workers") or []
    except Exception as e:
        wks = []
        out["workers_error"] = str(e)[:120]

    # 2. 集約
    hosts = _host_stats(metrics)
    workloads = _workload_stats(
        control_url, wls, runs, state["throughput_window_min"], workers=wks,
    )
    out["host_count"] = len(hosts)
    out["wl_count"] = len(workloads)

    # 3. rule engine
    rules = _load_rules(state["rules_path"])
    out["rules_loaded"] = len(rules)
    actions = _eval_rules(rules, hosts, workloads, state)

    # 4. action 実行
    # supervisor_enabled=False の workload を覚えておく (= 介入禁止リスト)
    disabled_slugs = {w["slug"] for w in workloads if not w.get("supervisor_enabled", True)}
    results: list[dict[str, Any]] = []
    for a in actions:
        rid = a.get("_rule_id")
        kind = a.get("_target_kind")
        target = a.get("_target_id")

        # opt-out: 当該 workload が supervisor_enabled=False の場合は
        # action を skip (log のみ)。 host-side rule で workload を targets する
        # 場合 (= add_workload_to_host_workers 等) も同様にチェック。
        target_slug: str | None = None
        if kind == "workload":
            target_slug = target
        elif kind == "host":
            for key in ("add_workload_to_host_workers",
                        "remove_workload_from_host_workers",
                        "add_host_affinity"):
                cfg = a.get(key)
                if isinstance(cfg, dict):
                    target_slug = cfg.get("workload") or cfg.get("slug")
                    if target_slug:
                        break
        if target_slug and target_slug in disabled_slugs:
            log.info("[%s] skip %s (= supervisor_enabled=false)", rid, target_slug)
            results.append({"rule": rid, "target": target, "ok": True,
                            "skipped": True, "reason": f"{target_slug} supervisor_enabled=false"})
            continue

        # log action は常に出力
        if "log" in a and isinstance(a["log"], dict):
            msg = a["log"].get("message", "")
            try:
                msg = msg.format(slug=target, host=target)
            except Exception:
                pass
            log.info("[%s] %s", rid, msg)
            results.append({"rule": rid, "log": msg, "target": target})
            continue

        # patch_workload
        if kind == "workload" and "patch_workload" in a:
            r = _apply_workload_action(
                control_url, target, a["patch_workload"],
                state["apply_mode"], state["max_priority"],
            )
            r["rule"] = rid
            r["target"] = target
            results.append(r)
            if r.get("changes"):
                log.info("[%s] %s %s → %s",
                         rid, target,
                         "dry" if r.get("dry_run") else "APPLIED",
                         r["changes"])

        # add_host_affinity (= host-side rule + workload に host を追加)
        if kind == "host" and "add_host_affinity" in a:
            cfg = a["add_host_affinity"]
            slug = cfg.get("slug")
            host = target
            if slug and host:
                r = _apply_workload_action(
                    control_url, slug, {"host_affinity_add": host},
                    state["apply_mode"], state["max_priority"],
                )
                r["rule"] = rid
                r["target"] = f"{slug} += {host}"
                results.append(r)
                if r.get("changes"):
                    log.info("[%s] add affinity %s ← %s (%s)",
                             rid, slug, host,
                             "dry" if r.get("dry_run") else "APPLIED")

        # ---------- 自動切替 actions (= worker filter SoT 直接操作) ----------
        # add_workload_to_host_workers: 指定 host の全 worker の filter に
        # 指定 workload を「足す」 (= 重複は無視)。 元 filter が None (= no filter)
        # ならその worker は対象外 (= もう全 workload 受けてるので追加不要)。
        if kind == "host" and "add_workload_to_host_workers" in a:
            cfg = a["add_workload_to_host_workers"]
            wl_to_add = cfg.get("workload")
            host = target
            if wl_to_add and host:
                workers = _list_workers_on_host(control_url, host)
                changed = 0
                for w in workers:
                    cur_filter = w.get("workload_filter")
                    if cur_filter is None:
                        continue  # no filter = 既に全 workload 対象
                    if wl_to_add in cur_filter:
                        continue
                    new_filter = sorted(set(cur_filter) | {wl_to_add})
                    rp = _post_worker_filter(
                        control_url, w["id"], new_filter,
                        updated_by=f"supervisor:{rid}",
                        apply_mode=state["apply_mode"],
                    )
                    rp["rule"] = rid
                    rp["target"] = f"{host}/{w['id']}: +{wl_to_add}"
                    results.append(rp)
                    if rp.get("ok"):
                        changed += 1
                log.info("[%s] add %s on %s: %d worker(s) updated (%s)",
                         rid, wl_to_add, host, changed,
                         "dry" if not state["apply_mode"] else "APPLIED")

        # remove_workload_from_host_workers: 指定 host の全 worker の filter から
        # 指定 workload を外す。 filter が None (= 全受) の worker は「明示の filter」
        # を作って引き算する (= 他 workload を列挙 → 当該を除外)。 ただし「列挙元」 を
        # 知らないと filter を作れないので、 オプション `set_to` で「外した後の filter」 を
        # 直接指定可能 (= 望ましい運用)。 `set_to` 未指定時は現 filter が list の worker のみ操作。
        if kind == "host" and "remove_workload_from_host_workers" in a:
            cfg = a["remove_workload_from_host_workers"]
            wl_to_remove = cfg.get("workload")
            set_to_override = cfg.get("set_to")    # 任意: 明示の新 filter
            host = target
            if wl_to_remove and host:
                workers = _list_workers_on_host(control_url, host)
                changed = 0
                for w in workers:
                    cur_filter = w.get("workload_filter")
                    if set_to_override is not None:
                        new_filter = list(set_to_override)
                    elif cur_filter is None:
                        # 「no filter」 = 全受の worker は方針が決まらないので skip
                        # (= rule で `set_to: [...]` を指定すれば操作可能)
                        continue
                    elif wl_to_remove not in cur_filter:
                        continue
                    else:
                        new_filter = sorted(set(cur_filter) - {wl_to_remove})
                    rp = _post_worker_filter(
                        control_url, w["id"], new_filter,
                        updated_by=f"supervisor:{rid}",
                        apply_mode=state["apply_mode"],
                    )
                    rp["rule"] = rid
                    rp["target"] = f"{host}/{w['id']}: -{wl_to_remove}"
                    results.append(rp)
                    if rp.get("ok"):
                        changed += 1
                log.info("[%s] remove %s on %s: %d worker(s) updated (%s)",
                         rid, wl_to_remove, host, changed,
                         "dry" if not state["apply_mode"] else "APPLIED")

        # set_worker_filter: host 内の worker 全員を「指定 list 完全に置換」。
        # 強い手なので最後の砦的 rule で使う (= 例: 緊急 shedding)。
        if kind == "host" and "set_worker_filter" in a:
            cfg = a["set_worker_filter"]
            new_filter = cfg.get("workloads")   # None or []=解除、 list=固定
            host = target
            workers = _list_workers_on_host(control_url, host)
            for w in workers:
                # 同値なら no-op (= server 側も same check するが round-trip 削減)
                cur = w.get("workload_filter")
                if cur == new_filter:
                    continue
                rp = _post_worker_filter(
                    control_url, w["id"], new_filter,
                    updated_by=f"supervisor:{rid}",
                    apply_mode=state["apply_mode"],
                )
                rp["rule"] = rid
                rp["target"] = f"{host}/{w['id']}: ={new_filter}"
                results.append(rp)
            log.info("[%s] set filter on %s = %s (%d worker(s), %s)",
                     rid, host, new_filter, len(workers),
                     "dry" if not state["apply_mode"] else "APPLIED")

    out["actions"] = len(actions)
    out["results"] = results
    out["dispatch_secs"] = round(time.time() - started, 2)
    out["apply_mode"] = state["apply_mode"]

    # ---------- 4.5. adaptive bin-packing balancer ----------
    # 既存 rule 評価とは独立に走る。 enabled=False なら no-op (skip 記録のみ)。
    try:
        bal = _balancer_run(state, hosts, workloads, wks, runs)
        out["balancer"] = bal
    except Exception as e:
        log.warning("balancer pass failed: %s", e, exc_info=True)
        out["balancer"] = {"error": str(e)[:200]}

    # ---------- 5. LLM advisor (= 設定で enabled なら N min 毎) ----------
    try:
        llm_out = _maybe_run_llm_advisor(state, hosts, workloads, wks, results)
        if llm_out is not None:
            out["llm"] = llm_out
    except Exception as e:
        log.warning("LLM advisor pass failed: %s", e, exc_info=False)
        out["llm"] = {"error": str(e)[:200]}

    # 6. sleep + 次 tick
    sleep_s = max(1, state["interval_s"] - int(out["dispatch_secs"]))
    time.sleep(sleep_s)
    _self_enqueue_next_tick(control_url, state["workload_slug"], state["counter"] + 1)
    out["next_tick_scheduled"] = True
    return out


# ---------------- LLM advisor 統合 ---------------- #

def _fetch_llm_config(control_url: str) -> dict[str, Any] | None:
    """control plane の /api/v1/settings から LLM 設定を取得し dict 化。
    settings endpoint は secret を mask するので、 ここでは内部で
    `?include_secret=1` 的な公開 API は無い → supervisor は workload と同じ host
    で動く前提で server-side repository を直接読みたいが、 plugin は別プロセスなので
    API 経由でしか取れない。 だが api_key 生値が必要なので、 ローカルファイル
    (= /etc/pipeline/llm.json) からも読めるようにする。"""
    import os
    # 優先順位 1: 環境変数 (= systemd EnvironmentFile で渡す方式)
    env_endpoint = os.environ.get("PIPELINE_LLM_ENDPOINT")
    env_key = os.environ.get("PIPELINE_LLM_API_KEY")
    env_model = os.environ.get("PIPELINE_LLM_MODEL")
    if env_endpoint and env_key:
        return {
            "enabled": bool(int(os.environ.get("PIPELINE_LLM_ENABLED", "0"))),
            "apply_mode": bool(int(os.environ.get("PIPELINE_LLM_APPLY_MODE", "0"))),
            "endpoint": env_endpoint,
            "api_key": env_key,
            "model": env_model or "deepseek-chat",
            "interval_min": int(os.environ.get("PIPELINE_LLM_INTERVAL_MIN", "15")),
            "max_actions_per_cycle": int(os.environ.get("PIPELINE_LLM_MAX_ACTIONS", "5")),
            "confidence_threshold": float(os.environ.get("PIPELINE_LLM_MIN_CONFIDENCE", "0.7")),
            "timeout_s": int(os.environ.get("PIPELINE_LLM_TIMEOUT_S", "60")),
            "_source": "env",
        }
    # 優先順位 2: control plane API (= UI から設定された)
    # /api/v1/settings/_llm_raw を呼ぶ専用 endpoint を別途 server-side で追加。
    try:
        r = requests.get(f"{control_url}/api/v1/_internal/llm_config", timeout=5)
        if r.status_code == 200:
            cfg = r.json()
            cfg["_source"] = "api"
            return cfg
    except Exception:
        pass
    return None


def _maybe_run_llm_advisor(
    state: dict[str, Any],
    hosts: list[dict[str, Any]],
    workloads: list[dict[str, Any]],
    workers: list[dict[str, Any]],
    recent_actions: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """LLM advisor: N min 間隔で 1 回コール、 提案を action 適用。 設定無効/不在なら no-op。"""
    cfg = _fetch_llm_config(state["control_url"])
    if not cfg or not cfg.get("enabled"):
        return None
    # 間隔チェック
    now = time.monotonic()
    interval_s = max(60, int(cfg.get("interval_min", 15)) * 60)
    last = float(state.get("llm_last_call_ts") or 0)
    if last and (now - last) < interval_s:
        return {"skipped": "interval_not_reached",
                "next_in_s": int(interval_s - (now - last))}
    state["llm_last_call_ts"] = now

    # 遅延 import (= server プロセスだけが requirements 持ってる)
    try:
        from . import llm_advisor
    except Exception:
        import importlib.util as _u
        spec = _u.spec_from_file_location(
            "_pl_llm_advisor",
            str(_PathHelper.llm_advisor_path()),
        )
        llm_advisor = _u.module_from_spec(spec); spec.loader.exec_module(llm_advisor)  # type: ignore

    snapshot = llm_advisor.build_state_snapshot(
        control_url=state["control_url"],
        hosts=hosts, workloads=workloads, workers=workers,
        recent_actions=recent_actions,
    )
    max_actions = max(1, int(cfg.get("max_actions_per_cycle", 5)))
    result = llm_advisor.call_llm(
        endpoint=cfg["endpoint"],
        api_key=cfg["api_key"],
        model=cfg.get("model") or "deepseek-chat",
        snapshot=snapshot,
        max_actions=max_actions,
        timeout_s=int(cfg.get("timeout_s", 60)),
    )

    # 監査 record (= control plane 経由で llm_calls table へ書く)
    record_id = _record_llm_call(state["control_url"], cfg, result, applied=0)

    if not result.get("ok"):
        return {"ok": False, "error": result.get("error"),
                "duration_ms": result.get("duration_ms"),
                "log_id": record_id}

    # 安全ガード適用
    actions = result.get("actions") or []
    confidence = float(result.get("confidence") or 0)
    threshold = float(cfg.get("confidence_threshold", 0.7))
    applied = 0
    apply_results: list[dict[str, Any]] = []
    disabled_slugs = {w["slug"] for w in workloads
                       if not w.get("supervisor_enabled", True)}
    if cfg.get("apply_mode") and confidence >= threshold:
        for a in actions[:max_actions]:
            res = _apply_llm_action(state, a, disabled_slugs)
            apply_results.append(res)
            if res.get("applied"):
                applied += 1

    # 監査 record を applied 数で更新 (= 同 id を上書き)
    if record_id:
        _update_llm_call_applied(state["control_url"], record_id, applied)

    return {
        "ok": True,
        "analysis": (result.get("analysis") or "")[:500],
        "confidence": confidence,
        "actions_proposed": len(actions),
        "actions_applied": applied,
        "duration_ms": result.get("duration_ms"),
        "usage": result.get("usage"),
        "log_id": record_id,
        "apply_results": apply_results,
        "apply_mode": cfg.get("apply_mode"),
        "dry_run_reason": (
            None if cfg.get("apply_mode") and confidence >= threshold
            else f"apply_mode={cfg.get('apply_mode')} confidence={confidence} threshold={threshold}"
        ),
    }


def _apply_llm_action(state: dict[str, Any], action: dict[str, Any],
                       disabled_slugs: set[str]) -> dict[str, Any]:
    """LLM が提案した 1 action を適用。 disabled_slugs と type 検証。"""
    typ = action.get("type")
    control_url = state["control_url"]
    if typ == "patch_workload":
        slug = action.get("slug")
        if not slug or slug in disabled_slugs:
            return {"applied": False, "reason": "slug missing or supervisor_enabled=false",
                    "action": action}
        patch: dict[str, Any] = {}
        if "priority" in action and action["priority"] is not None:
            patch["priority"] = int(action["priority"])
        if "batch_size" in action and action["batch_size"] is not None:
            patch["batch_size"] = int(action["batch_size"])
        if "lease_secs" in action and action["lease_secs"] is not None:
            patch["lease_secs"] = int(action["lease_secs"])
        if not patch:
            return {"applied": False, "reason": "no recognized fields", "action": action}
        r = _apply_workload_action(
            control_url, slug, patch, apply_mode=True,
            max_priority=state.get("max_priority", 200),
        )
        return {"applied": bool(r.get("ok") and r.get("changes")), "result": r,
                "action": action}
    if typ == "set_worker_filter":
        wid = action.get("worker_id")
        if not wid:
            return {"applied": False, "reason": "worker_id missing", "action": action}
        wl = action.get("workloads")
        if wl is not None and not isinstance(wl, list):
            return {"applied": False, "reason": "workloads must be list or null",
                    "action": action}
        # mode: LLM が "add"/"remove"/"replace" を指定可。 未指定なら "replace"
        # (= 既存 API 互換)。 LLM プロンプトは "add" を推奨。
        mode = action.get("mode") or "replace"
        if mode not in ("replace", "add", "remove"):
            return {"applied": False,
                    "reason": f"invalid mode: {mode}", "action": action}
        try:
            r = requests.post(
                f"{control_url}/api/v1/workers/{wid}/filter",
                json={"workloads": wl, "mode": mode, "updated_by": "llm-advisor"},
                timeout=10,
            )
            if r.status_code >= 400:
                return {"applied": False, "reason": f"HTTP {r.status_code}: {r.text[:200]}",
                        "action": action}
            return {"applied": True, "action": action}
        except Exception as e:
            return {"applied": False, "reason": str(e)[:200], "action": action}
    return {"applied": False, "reason": f"unknown action type: {typ}",
            "action": action}


def _record_llm_call(control_url: str, cfg: dict[str, Any],
                      result: dict[str, Any], applied: int) -> int:
    """LLM コール結果を control plane の llm_calls table に書く。"""
    try:
        usage = result.get("usage") or {}
        body = {
            "provider": cfg.get("_source", "unknown"),
            "model": cfg.get("model") or "",
            "prompt_messages": result.get("prompt_messages") or [],
            "response_text": result.get("response_text"),
            "analysis": result.get("analysis"),
            "actions": result.get("actions") or [],
            "actions_applied": int(applied),
            "success": bool(result.get("ok")),
            "error": result.get("error"),
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "total_tokens": usage.get("total_tokens"),
            "duration_ms": int(result.get("duration_ms") or 0),
        }
        r = requests.post(
            f"{control_url}/api/v1/_internal/llm_call_record",
            json=body, timeout=5,
        )
        if r.status_code < 400:
            return int((r.json() or {}).get("id") or 0)
    except Exception:
        log.warning("llm call record failed", exc_info=False)
    return 0


def _update_llm_call_applied(control_url: str, record_id: int, applied: int) -> None:
    try:
        requests.patch(
            f"{control_url}/api/v1/_internal/llm_call_record/{record_id}",
            json={"actions_applied": int(applied)}, timeout=5,
        )
    except Exception:
        pass


class _PathHelper:
    @staticmethod
    def llm_advisor_path():
        from pathlib import Path
        return Path(__file__).parent / "llm_advisor.py"


def teardown(state) -> None:
    log.info("supervisor: done %d ticks", state.get("counter", 0))
