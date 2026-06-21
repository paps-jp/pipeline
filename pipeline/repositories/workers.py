"""workers テーブル CRUD (worker daemon の registry).

worker daemon が起動時に register、5 秒毎に heartbeat、shutdown 時に deregister。
control plane は 90 秒見えない worker を state='lost' に。
"""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _new_worker_id(host: str) -> str:
    """host + 短いランダムを組み合わせて識別子に。"""
    safe_host = "".join(c if c.isalnum() else "_" for c in host)[:24]
    return f"w_{safe_host}_{secrets.token_hex(2)}"


class WorkerNotFound(LookupError):
    pass


class WorkerRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def register(
        self,
        *,
        host: str,
        pid: int | None = None,
        tags: list[str] | None = None,
        resources: dict[str, Any] | None = None,
        worker_id: str | None = None,
    ) -> dict[str, Any]:
        """新規 worker を登録 (or 既存 worker_id なら updated)。
        worker_id 未指定なら deterministic な ID を生成。
        """
        wid = worker_id or _new_worker_id(host)
        now = _utcnow_iso()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id FROM workers WHERE id = :id", {"id": wid}
            )
            existing = cur.fetchone()
            if existing is None:
                conn.execute(
                    """
                    INSERT INTO workers (
                        id, host, pid, tags, resources,
                        state, started_at, last_seen_at
                    ) VALUES (
                        :id, :host, :pid, :tags, :resources,
                        :state, :now, :now
                    )
                    """,
                    {
                        "id": wid,
                        "host": host,
                        "pid": pid,
                        "tags": json.dumps(tags or []),
                        "resources": json.dumps(resources or {}),
                        "state": "active",
                        "now": now,
                    },
                )
            else:
                conn.execute(
                    """
                    UPDATE workers SET
                        host = :host, pid = :pid,
                        tags = :tags, resources = :resources,
                        state = 'active', last_seen_at = :now,
                        started_at = COALESCE(started_at, :now)
                    WHERE id = :id
                    """,
                    {
                        "id": wid,
                        "host": host,
                        "pid": pid,
                        "tags": json.dumps(tags or []),
                        "resources": json.dumps(resources or {}),
                        "now": now,
                    },
                )
        return self.get(wid)

    def heartbeat(
        self,
        worker_id: str,
        *,
        current_workload: str | None = None,
        current_phase: str | None = None,
        rows_processed_delta: int = 0,
        errors_total_delta: int = 0,
    ) -> dict[str, Any]:
        now = _utcnow_iso()
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE workers
                SET last_seen_at = :now,
                    current_workload = :cw,
                    current_phase = :cp,
                    rows_processed = rows_processed + :rd,
                    errors_total   = errors_total + :ed,
                    state = CASE WHEN state = 'lost' THEN 'active' ELSE state END
                WHERE id = :id
                """,
                {
                    "id": worker_id, "now": now,
                    "cw": current_workload, "cp": current_phase,
                    "rd": int(rows_processed_delta), "ed": int(errors_total_delta),
                },
            )
            if cur.rowcount == 0:
                raise WorkerNotFound(worker_id)
        return self.get(worker_id)

    def deregister(self, worker_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM workers WHERE id = :id", {"id": worker_id})

    def mark_lost(self, worker_id: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE workers SET state = 'lost' WHERE id = :id",
                {"id": worker_id},
            )

    def prune_stale(self, lost_after_s: int = 60, delete_after_s: int = 600,
                    metrics_retain_s: int = 86400) -> dict[str, int]:
        """heartbeat が止まった worker を state='lost' に、 更に古ければ DELETE。
        worker_metrics の orphan (= 既に worker テーブルに無い worker_id) + 古い row も削除。

        - last_seen_at < now - lost_after_s 且つ state='active' → state='lost'
        - last_seen_at < now - delete_after_s → 完全 DELETE
        - worker_metrics.ts < now - metrics_retain_s → 削除 (= 既定 24h)
        - worker_metrics.worker_id NOT IN (workers) → 削除 (= orphan)
        """
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        lost_threshold = (now - timedelta(seconds=lost_after_s)).isoformat()
        delete_threshold = (now - timedelta(seconds=delete_after_s)).isoformat()
        metrics_threshold = (now - timedelta(seconds=metrics_retain_s)).isoformat()
        lost_n = 0
        deleted_n = 0
        metrics_deleted_n = 0
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE workers SET state = 'lost' WHERE last_seen_at < :t AND state = 'active'",
                {"t": lost_threshold},
            )
            lost_n = cur.rowcount
            cur = conn.execute(
                "DELETE FROM workers WHERE last_seen_at < :t",
                {"t": delete_threshold},
            )
            deleted_n = cur.rowcount
            # metrics cleanup (= orphan + 古い ts)
            try:
                cur = conn.execute(
                    "DELETE FROM worker_metrics WHERE ts < :t "
                    "OR worker_id NOT IN (SELECT id FROM workers)",
                    {"t": metrics_threshold},
                )
                metrics_deleted_n = cur.rowcount
            except Exception:
                pass  # table 未存在 (= migration 前) は無視
        return {"marked_lost": lost_n, "deleted": deleted_n, "metrics_deleted": metrics_deleted_n}

    def get(self, worker_id: str) -> dict[str, Any]:
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT * FROM workers WHERE id = :id", {"id": worker_id})
            row = cur.fetchone()
            if row is None:
                raise WorkerNotFound(worker_id)
            return self._row(row)

    def list_all(self) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT * FROM workers ORDER BY started_at DESC, id"
            )
            return [self._row(r) for r in cur.fetchall()]

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        out = dict(r)
        for c in ("tags", "resources"):
            v = out.get(c)
            if isinstance(v, str):
                try:
                    out[c] = json.loads(v)
                except Exception:
                    out[c] = []
        return out
