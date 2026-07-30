"""Workload の CRUD。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database
from pipeline.models.workload import Workload, WorkloadCreate, WorkloadUpdate, queue_table_for


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# observed_vram_mb_peak の write 時クランプ係数。 単一サンプル (arena 過大確保 / OOM
# ×1.2 ラチェット / 一時 spike) が peak を declared の K 倍超へ暴走させるのを源流で止める。
# 精密な host 容量ベース上限と robust 再推定は reconcile_vram_peaks (60s loop) が行う。
# K は reconcile の hard ceiling (= min(declared*4, 最小GPU容量*0.6)) を必ず下回る値にする
# こと。 上回ると write が peak を ceiling 超へ押し上げ、 loop が毎周期押し戻して振動する。
_VRAM_WRITE_CLAMP_K = 3


def _row_to_workload(row: dict[str, Any]) -> Workload:
    """SQLite row → Pydantic Workload。JSON 列をパースする。"""
    parsed = dict(row)
    for jcol in (
        "executor_config",
        "success_criteria",
        "resources",
        "host_affinity",
        "on_success",
        "on_failure",
    ):
        v = parsed.get(jcol)
        if v is None:
            continue
        if isinstance(v, str):
            parsed[jcol] = json.loads(v) if v else None
    # SQLite boolean は 0/1
    if isinstance(parsed.get("enabled"), int):
        parsed["enabled"] = bool(parsed["enabled"])
    if isinstance(parsed.get("supervisor_enabled"), int):
        parsed["supervisor_enabled"] = bool(parsed["supervisor_enabled"])
    if isinstance(parsed.get("requires_gpu"), int):
        parsed["requires_gpu"] = bool(parsed["requires_gpu"])
    return Workload(**parsed)


class WorkloadNotFound(LookupError):
    """対象 slug が存在しない時に raise。"""


class WorkloadAlreadyExists(ValueError):
    """slug が既に存在する時に raise。"""


class WorkloadRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ---------- READ ----------

    def list_all(self) -> list[Workload]:
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT * FROM workloads ORDER BY slug")
            return [_row_to_workload(r) for r in cur.fetchall()]

    def get(self, slug: str) -> Workload:
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT * FROM workloads WHERE slug = :slug", {"slug": slug})
            row = cur.fetchone()
            if row is None:
                raise WorkloadNotFound(slug)
            return _row_to_workload(row)

    # ---------- CREATE ----------

    def create(self, payload: WorkloadCreate, created_by: str | None = None) -> Workload:
        # 既存チェック
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT 1 FROM workloads WHERE slug = :slug", {"slug": payload.slug})
            if cur.fetchone() is not None:
                raise WorkloadAlreadyExists(payload.slug)

        queue_table = queue_table_for(payload.slug)
        now = _utcnow()
        params = {
            "slug": payload.slug,
            "name": payload.name,
            "description": payload.description,
            "enabled": 1 if payload.enabled else 0,
            "queue_table": queue_table,
            "executor_type": payload.executor_type,
            "executor_config": json.dumps(payload.executor_config),
            "success_criteria": json.dumps(payload.success_criteria),
            "priority": payload.priority,
            "weight": payload.weight,
            "batch_size": payload.batch_size,
            "lease_secs": payload.lease_secs,
            "max_attempts": payload.max_attempts,
            "resources": json.dumps(payload.resources),
            "host_affinity": json.dumps(payload.host_affinity),
            "on_success": json.dumps(payload.on_success) if payload.on_success else None,
            "on_failure": json.dumps(payload.on_failure) if payload.on_failure else None,
            "supervisor_enabled": 1 if payload.supervisor_enabled else 0,
            "max_concurrent_per_host": payload.max_concurrent_per_host,
            "max_concurrent_total": payload.max_concurrent_total,
            "requires_gpu": 1 if payload.requires_gpu else 0,
            "queue_backend": payload.queue_backend,
            "min_resident_workers": payload.min_resident_workers,
            "max_workers": payload.max_workers,
            "created_by": created_by,
            "created_at": now,
            "updated_at": now,
        }
        sql = """
        INSERT INTO workloads (
            slug, name, description, enabled, queue_table,
            executor_type, executor_config, success_criteria,
            priority, weight, batch_size, lease_secs, max_attempts,
            resources, host_affinity, on_success, on_failure,
            supervisor_enabled, max_concurrent_per_host, max_concurrent_total, requires_gpu,
            queue_backend, min_resident_workers, max_workers,
            created_by, created_at, updated_at
        ) VALUES (
            :slug, :name, :description, :enabled, :queue_table,
            :executor_type, :executor_config, :success_criteria,
            :priority, :weight, :batch_size, :lease_secs, :max_attempts,
            :resources, :host_affinity, :on_success, :on_failure,
            :supervisor_enabled, :max_concurrent_per_host, :max_concurrent_total, :requires_gpu,
            :queue_backend, :min_resident_workers, :max_workers,
            :created_by, :created_at, :updated_at
        )
        """
        with self.db.transaction() as conn:
            conn.execute(sql, params)
        # <slug>_queue 表を併せて作成
        if hasattr(self.db, "ensure_workload_queue"):
            self.db.ensure_workload_queue(queue_table)  # type: ignore[attr-defined]
        return self.get(payload.slug)

    # ---------- UPDATE ----------

    def update(self, slug: str, payload: WorkloadUpdate) -> Workload:
        # 存在チェック
        self.get(slug)  # raises WorkloadNotFound
        now = _utcnow()
        params = {
            "slug": slug,
            "name": payload.name,
            "description": payload.description,
            "enabled": 1 if payload.enabled else 0,
            "executor_type": payload.executor_type,
            "executor_config": json.dumps(payload.executor_config),
            "success_criteria": json.dumps(payload.success_criteria),
            "priority": payload.priority,
            "weight": payload.weight,
            "batch_size": payload.batch_size,
            "lease_secs": payload.lease_secs,
            "max_attempts": payload.max_attempts,
            "resources": json.dumps(payload.resources),
            "host_affinity": json.dumps(payload.host_affinity),
            "on_success": json.dumps(payload.on_success) if payload.on_success else None,
            "on_failure": json.dumps(payload.on_failure) if payload.on_failure else None,
            "supervisor_enabled": 1 if payload.supervisor_enabled else 0,
            "max_concurrent_per_host": payload.max_concurrent_per_host,
            "max_concurrent_total": payload.max_concurrent_total,
            "requires_gpu": 1 if payload.requires_gpu else 0,
            "queue_backend": payload.queue_backend,
            "min_resident_workers": payload.min_resident_workers,
            "max_workers": payload.max_workers,
            "updated_at": now,
        }
        sql = """
        UPDATE workloads SET
            name = :name,
            description = :description,
            enabled = :enabled,
            executor_type = :executor_type,
            executor_config = :executor_config,
            success_criteria = :success_criteria,
            priority = :priority,
            weight = :weight,
            batch_size = :batch_size,
            lease_secs = :lease_secs,
            max_attempts = :max_attempts,
            resources = :resources,
            host_affinity = :host_affinity,
            on_success = :on_success,
            on_failure = :on_failure,
            supervisor_enabled = :supervisor_enabled,
            max_concurrent_per_host = :max_concurrent_per_host,
            max_concurrent_total = :max_concurrent_total,
            requires_gpu = :requires_gpu,
            queue_backend = :queue_backend,
            min_resident_workers = :min_resident_workers,
            max_workers = :max_workers,
            updated_at = :updated_at
        WHERE slug = :slug
        """
        with self.db.transaction() as conn:
            conn.execute(sql, params)
        return self.get(slug)

    def set_enabled(self, slug: str, enabled: bool) -> Workload:
        self.get(slug)  # raises if not found
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE workloads SET enabled = :en, updated_at = :ts WHERE slug = :slug",
                {"en": 1 if enabled else 0, "ts": _utcnow(), "slug": slug},
            )
        return self.get(slug)

    def set_host_affinity(self, slug: str, hosts: list[str]) -> Workload:
        """host_affinity だけを差し替える (他列は不変)。

        UI の「このホストでこの workload を許可する」トグル用。 PUT /workloads/{slug} は
        WorkloadUpdate 全体置換なので、 affinity だけ変えたい呼び出しで他列 (executor_config
        や lease_secs 等) を取りこぼす事故が起きる。 専用の部分更新にする。
        """
        self.get(slug)  # raises if not found
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE workloads SET host_affinity = :ha, updated_at = :ts WHERE slug = :slug",
                {"ha": json.dumps(list(hosts)), "ts": _utcnow(), "slug": slug},
            )
        return self.get(slug)

    def set_supervisor_enabled(self, slug: str, enabled: bool) -> Workload:
        """supervisor の自動介入を許可するか個別 toggle。"""
        self.get(slug)  # raises if not found
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE workloads SET supervisor_enabled = :en, updated_at = :ts "
                "WHERE slug = :slug",
                {"en": 1 if enabled else 0, "ts": _utcnow(), "slug": slug},
            )
        return self.get(slug)

    def record_vram_observation(
        self, slug: str, used_mb: int, worker_id: str | None = None,
    ) -> Workload | None:
        """worker からの VRAM 観測値を peak に反映 + raw sample を保存。

        peak: max(prev * 0.95, incoming) で平滑化 (= 急増は即時、減少はゆるく)。
        raw: vram_observations 表に (slug, worker_id, ts, used_mb) で INSERT。
             配置設計 (= avg/p95) 用、 reaper で 1h より古いものは削除。
        slug 未登録 (= 削除済) なら None。
        """
        if used_mb is None or used_mb < 0:
            return None
        try:
            current = self.get(slug)
        except WorkloadNotFound:
            return None
        prev = current.observed_vram_mb_peak or 0
        new_peak = max(int(prev * 0.95), int(used_mb))
        # 単一サンプルによる peak 暴走 (arena 過大確保 / OOM ラチェット) を write 時点で
        # 粗くクランプ。 host 容量ベースの精密上限と robust 再推定は 60s loop が行う。
        declared = int((current.resources or {}).get("vram_mb") or 0)
        if declared > 0:
            new_peak = min(new_peak, declared * _VRAM_WRITE_CLAMP_K)
        ts = _utcnow()
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE workloads SET
                       observed_vram_mb_peak = :peak,
                       observed_vram_sample_count = observed_vram_sample_count + 1,
                       observed_vram_updated_at = :ts
                   WHERE slug = :slug""",
                {"peak": new_peak, "ts": ts, "slug": slug},
            )
            # raw sample 保存 (= avg/p95 集計の元データ)。 worker_id 未指定でも保存する
            # (= "unknown" worker として記録)、 PK 衝突は ts の microsecond で実質回避。
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO vram_observations
                           (slug, worker_id, ts, used_mb)
                       VALUES (:slug, :wid, :ts, :mb)""",
                    {
                        "slug": slug,
                        "wid": worker_id or "unknown",
                        "ts": ts,
                        "mb": int(used_mb),
                    },
                )
            except Exception:
                # raw 保存失敗で peak 更新を壊さないように吸収
                pass
        return self.get(slug)

    def aggregate_vram_avg_p95(self, window_minutes: int = 60) -> int:
        """vram_observations から workload ごとの直近 N 分の avg/p95 を計算して
        workloads.observed_vram_mb_avg/p95 を一括 UPDATE。 返り値 = 更新 workload 数。

        p95 は SQLite に組込みが無いので Python 側で計算 (= small dataset 前提、
        1h で 4-12k 行程度想定)。
        """
        import datetime as _dt
        cutoff = (_dt.datetime.now(_dt.timezone.utc)
                  - _dt.timedelta(minutes=window_minutes)).isoformat()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT slug, used_mb FROM vram_observations WHERE ts >= :c",
                {"c": cutoff},
            )
            rows = cur.fetchall()
        if not rows:
            return 0
        # group by slug
        from collections import defaultdict
        samples: dict[str, list[int]] = defaultdict(list)
        for r in rows:
            s = r["slug"] if hasattr(r, "keys") else r[0]
            mb = r["used_mb"] if hasattr(r, "keys") else r[1]
            try:
                samples[s].append(int(mb))
            except Exception:
                continue
        updated = 0
        ts = _utcnow()
        with self.db.transaction() as conn:
            for slug, vals in samples.items():
                vals.sort()
                avg = int(sum(vals) / len(vals))
                p95_idx = max(0, int(len(vals) * 0.95) - 1)
                p95 = vals[p95_idx]
                try:
                    conn.execute(
                        """UPDATE workloads SET
                               observed_vram_mb_avg = :a,
                               observed_vram_mb_p95 = :p,
                               observed_vram_updated_at = :ts
                           WHERE slug = :s""",
                        {"a": avg, "p": p95, "ts": ts, "s": slug},
                    )
                    updated += 1
                except Exception:
                    continue
        return updated

    def _smallest_gpu_capacity_mb(self) -> int:
        """active な GPU worker が resources.gpu_vram_mb で申告した最小の物理 VRAM 容量。
        hard ceiling (= どの GPU にも必ず載る絶対上限) の算定に使う。 申告が無ければ 0。"""
        smallest = 0
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT resources FROM workers "
                "WHERE state IN ('active','running','claiming','draining')"
            ).fetchall()
        for r in rows:
            raw = r["resources"] if hasattr(r, "keys") else r[0]
            try:
                res = json.loads(raw) if isinstance(raw, str) else (raw or {})
            except Exception:
                continue
            mb = int(res.get("gpu_vram_mb") or 0)
            if mb > 0 and (smallest == 0 or mb < smallest):
                smallest = mb
        return smallest

    def reconcile_vram_peaks(
        self,
        *,
        obs_window_min: int = 30,
        ceil_declared_k: int = 4,
        ceil_capacity_frac: float = 0.6,
        stale_decay: float = 0.90,
        rel_change_min: float = 0.02,
    ) -> list[dict[str, Any]]:
        """observed_vram_mb_peak の poison-pill を毎周期補正する (2026-07-26)。

        真因: 混雑 GPU の arena 過大確保 / OOM ×1.2 ラチェット / 一時 spike が peak を
        実需の数倍へ吊り上げ、 その peak は「新観測が来た時だけ ×0.95 減衰」 する仕様の
        ため、 VRAM 予算ゲート (workloads_for_worker) が workload を全 host から締め出すと
        新観測が途絶えて peak が永久固着 → image-hash-extract 全停止。

        v1 の方針は **peak を下げる方向のみ** (over-provision 回帰を作らない):
          1. hard ceiling = max(1, min(declared*K, smallest_gpu_capacity*frac)):
             観測が壊れていても・全滅していても、 どの GPU にも必ず載る絶対上限。 これ 1 つで
             「全 host 除外」 は構造的に起きなくなる。
          2. stale 減衰: 窓内に観測が無い (= starve 疑い) slug の高すぎる peak を declared に
             向けて ×stale_decay で下げ、 凍結を解く。
        観測が正常に流れている間の accurate な追従は record_vram_observation の max(prev*0.95,
        used) に任せる (= v1 は触らない)。 両端 outlier を棄却する robust 再推定 (cross-host
        統計) は onnxruntime arena の実上限固定と対でないと OOM/密度いずれかを回帰させるため
        v2 に回す。

        返り値 = 変更した workload の {slug, from, to, reason, ...} のリスト。
        """
        import datetime as _dt

        cutoff = (_dt.datetime.now(_dt.timezone.utc)
                  - _dt.timedelta(minutes=obs_window_min)).isoformat()
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT DISTINCT slug FROM vram_observations WHERE ts >= :c",
                {"c": cutoff},
            ).fetchall()
        fresh_slugs = {(r["slug"] if hasattr(r, "keys") else r[0]) for r in rows}
        smallest_cap = self._smallest_gpu_capacity_mb()

        changes: list[dict[str, Any]] = []
        for w in self.list_all():
            declared = int((w.resources or {}).get("vram_mb") or 0)
            if not (w.requires_gpu or declared > 0):
                continue
            peak = int(w.observed_vram_mb_peak or 0)
            if peak <= 0:
                continue
            # hard ceiling (絶対上限)。 算定材料が無ければ None (= ceiling 無効)。
            ceil_cands: list[int] = []
            if declared > 0:
                ceil_cands.append(declared * ceil_declared_k)
            if smallest_cap > 0:
                ceil_cands.append(int(smallest_cap * ceil_capacity_frac))
            ceiling = max(1, min(ceil_cands)) if ceil_cands else None

            new_peak = peak
            reason: str | None = None
            # 1) stale 減衰: 観測が無く、 かつ declared より高い peak を declared 方向へ下げる。
            if w.slug not in fresh_slugs and declared > 0 and peak > declared:
                decayed = max(declared, int(peak * stale_decay))
                if decayed < new_peak:
                    new_peak, reason = decayed, "stale-decay"
            # 2) hard ceiling: 常に絶対上限でクランプ。
            if ceiling is not None and new_peak > ceiling:
                new_peak, reason = ceiling, "ceiling"

            # v1 は下げ方向のみ (record_vram_observation の上げ追従を邪魔しない)。
            if new_peak < peak and (peak - new_peak) >= max(1, int(peak * rel_change_min)):
                with self.db.transaction() as conn:
                    conn.execute(
                        "UPDATE workloads SET observed_vram_mb_peak = :p WHERE slug = :s",
                        {"p": int(new_peak), "s": w.slug},
                    )
                changes.append({
                    "slug": w.slug, "from": peak, "to": int(new_peak),
                    "reason": reason, "declared": declared, "ceiling": ceiling,
                })
        return changes

    def update_observed_rates(self, rates: dict[str, float]) -> int:
        """slug → 件数/min を workloads.observed_rate に一括 UPDATE (= 既存列流用)。

        2026-06-30: flow snapshot で「捌いた件数/min」 を表示するために、
        scheduler の 30s aggregate tick がこの関数を呼び observed_rate を更新する。
        返り値 = 更新行数。 rates に無い slug は 0 のままなので、 全 slug を渡す側
        (= aggregate loop) が一回の集計で全 workload を網羅する必要がある。
        """
        if not rates:
            return 0
        updated = 0
        with self.db.transaction() as conn:
            for slug, rate in rates.items():
                try:
                    conn.execute(
                        "UPDATE workloads SET observed_rate = :r WHERE slug = :s",
                        {"r": float(rate), "s": slug},
                    )
                    updated += 1
                except Exception:
                    continue
        return updated

    def prune_vram_observations(self, retain_minutes: int = 60) -> int:
        """古い vram_observations を削除。 返り値 = 削除行数。"""
        import datetime as _dt
        cutoff = (_dt.datetime.now(_dt.timezone.utc)
                  - _dt.timedelta(minutes=retain_minutes)).isoformat()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM vram_observations WHERE ts < :c", {"c": cutoff}
            )
            return cur.rowcount or 0

    # ---------- DELETE ----------

    def delete(self, slug: str) -> None:
        self.get(slug)  # raises if not found
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM workloads WHERE slug = :slug", {"slug": slug})
        # NOTE: <slug>_queue 表は残す (データ消失防止)。明示的に消すなら別 API。
