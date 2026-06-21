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
            created_by, created_at, updated_at
        ) VALUES (
            :slug, :name, :description, :enabled, :queue_table,
            :executor_type, :executor_config, :success_criteria,
            :priority, :weight, :batch_size, :lease_secs, :max_attempts,
            :resources, :host_affinity, :on_success, :on_failure,
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

    # ---------- DELETE ----------

    def delete(self, slug: str) -> None:
        self.get(slug)  # raises if not found
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM workloads WHERE slug = :slug", {"slug": slug})
        # NOTE: <slug>_queue 表は残す (データ消失防止)。明示的に消すなら別 API。
