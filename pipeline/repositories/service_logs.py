"""service_logs テーブル: daemon が push する Python logging stream。"""

from __future__ import annotations

from typing import Any

from pipeline.db.base import Database


class ServiceLogsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def insert_many(self, records: list[dict[str, Any]]) -> int:
        """records: [{ts, host, service, worker_id, level, logger, message, exc_info}, ...]"""
        if not records:
            return 0
        with self.db.transaction() as conn:
            for r in records:
                conn.execute(
                    """
                    INSERT INTO service_logs
                        (ts, host, service, worker_id, level, logger, message, exc_info)
                    VALUES
                        (:ts, :host, :svc, :wid, :lvl, :lg, :msg, :exc)
                    """,
                    {
                        "ts": r["ts"],
                        "host": r["host"],
                        "svc": r["service"],
                        "wid": r.get("worker_id"),
                        "lvl": r["level"],
                        "lg": r.get("logger"),
                        "msg": r["message"],
                        "exc": r.get("exc_info"),
                    },
                )
        return len(records)

    def list_recent(
        self,
        *,
        limit: int = 500,
        since_id: int | None = None,
        host: str | None = None,
        service: str | None = None,
        worker_id: str | None = None,
        min_level: str | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: dict[str, Any] = {"lim": int(limit)}
        if since_id is not None:
            where.append("id > :sid")
            params["sid"] = int(since_id)
        if host:
            where.append("host = :host")
            params["host"] = host
        if service:
            where.append("service = :svc")
            params["svc"] = service
        if worker_id:
            where.append("worker_id = :wid")
            params["wid"] = worker_id
        if min_level:
            level_order = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
            target = level_order.get(min_level.upper(), 20)
            keep = [k for k, v in level_order.items() if v >= target]
            placeholders = ",".join(f":lv{i}" for i in range(len(keep)))
            where.append(f"level IN ({placeholders})")
            for i, k in enumerate(keep):
                params[f"lv{i}"] = k
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"""
                SELECT id, ts, host, service, worker_id, level, logger, message, exc_info
                FROM service_logs
                {where_sql}
                ORDER BY id DESC
                LIMIT :lim
                """,
                params,
            )
            rows = cur.fetchall()
        # 古い順で返す (UI が console 風に表示)
        return [self._row(r) for r in reversed(rows)]

    def prune_old(self, *, keep_rows: int = 50_000) -> int:
        """容量制御: 古い行を削除して keep_rows 件残す。"""
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                DELETE FROM service_logs
                WHERE id <= (
                    SELECT id FROM (
                        SELECT id FROM service_logs ORDER BY id DESC LIMIT 1 OFFSET :keep
                    ) AS t
                )
                """,
                {"keep": int(keep_rows)},
            )
            return cur.rowcount

    @staticmethod
    def _row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(r["id"]),
            "ts": r["ts"],
            "host": r["host"],
            "service": r["service"],
            "worker_id": r["worker_id"],
            "level": r["level"],
            "logger": r["logger"],
            "message": r["message"],
            "exc_info": r["exc_info"],
        }
