"""Workload の CRUD。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database
from pipeline.models.workload import Workload, WorkloadCreate, WorkloadUpdate, queue_table_for


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
            queue_backend,
            created_by, created_at, updated_at
        ) VALUES (
            :slug, :name, :description, :enabled, :queue_table,
            :executor_type, :executor_config, :success_criteria,
            :priority, :weight, :batch_size, :lease_secs, :max_attempts,
            :resources, :host_affinity, :on_success, :on_failure,
            :supervisor_enabled, :max_concurrent_per_host, :max_concurrent_total, :requires_gpu,
            :queue_backend,
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
